#!/usr/bin/env python
"""Build a master list of convertible ASTEC archives from the filesystem.

This scans the upload directory directly and resolves, for every upload, where
the ASTEC binary archive actually lives.  It deliberately does not talk to
MongoDB: the database's ``system_path`` is written verbatim from
``upload_info.pickle`` at registration time and is wrong for the majority of
uploads, so the filesystem is the more reliable source.

Resolution order for each recorded archive path:

1. the recorded path itself, when it is a non-empty directory,
2. a sibling whose name matches after normalisation (``-`` -> ``_``, casefold)
   and which is a non-empty directory -- this recovers the ``SBO-fb`` vs
   ``SBO_fb`` spelling mismatch,
3. a sibling tarball (``<name>.tar``), which yields state NEEDS_UNTAR.

Uploads whose ``upload_info.pickle`` is missing or corrupt are still scanned,
so an archive present on disk is never dropped just because its bookkeeping is
broken.

The resulting CSV uses the same column names as the database documents so that
``assas_job_generator.py`` can consume either source interchangeably.
"""

import argparse
import csv
import logging
import os
import pickle
import sys

from dataclasses import dataclass, asdict, fields
from pathlib import Path
from typing import Iterator, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_UPLOAD_DIRECTORY = os.environ.get(
    "ASSAS_UPLOAD_DIRECTORY", "/lsdf/kit/scc/projects/ASSAS/upload_datahub"
)
UPLOAD_INFO_FILE_NAME = "upload_info.pickle"
TAR_SUFFIXES = (".tar", ".tgz", ".tar.gz")

STATE_READY = "READY"
STATE_NEEDS_UNTAR = "NEEDS_UNTAR"
STATE_EMPTY = "EMPTY"
STATE_MISSING = "MISSING"

PROCESSABLE_STATES = (STATE_READY, STATE_NEEDS_UNTAR)


@dataclass
class MasterListEntry:
    """One convertible archive, addressed by upload uuid and resolved path."""

    system_upload_uuid: str
    meta_name: str
    meta_description: str
    system_path: str
    system_result: str
    system_number_of_samples: str
    source_tar: str
    state: str
    recorded_path: str
    resolution: str


def normalise(name: str) -> str:
    """Return a comparable form of an archive directory name."""
    return name.casefold().replace("-", "_")


def is_non_empty_directory(path: Path) -> bool:
    """Return True when path is a directory containing at least one entry."""
    try:
        with os.scandir(path) as entries:
            return any(True for _ in entries)
    except (NotADirectoryError, FileNotFoundError, PermissionError, OSError):
        return False


def count_samples(archive_path: Path) -> Optional[int]:
    """Return the number of ``saving_*`` entries in an extracted archive."""
    try:
        with os.scandir(archive_path) as entries:
            return sum(1 for entry in entries if entry.name.startswith("saving_"))
    except OSError:
        return None


def find_tarball(parent: Path, basename: str) -> Optional[Path]:
    """Return a sibling tarball whose extracted form would be basename."""
    wanted = {normalise(basename) + suffix for suffix in TAR_SUFFIXES}
    try:
        candidates = list(os.scandir(parent))
    except OSError:
        return None

    for entry in candidates:
        if entry.is_file() and normalise(entry.name) in wanted:
            return Path(entry.path)

    return None


def find_name_variant(parent: Path, basename: str) -> Optional[Path]:
    """Return a non-empty sibling directory matching basename after normalising.

    Only an unambiguous match is accepted.  Empty directories are rejected on
    purpose: registration creates empty decoy directories at the *wrong* path
    (see the malformed ``result_path`` in ``AssasDatabaseManager``), and
    matching one of those would reintroduce the very bug this resolves.
    """
    wanted = normalise(basename)
    try:
        candidates = list(os.scandir(parent))
    except OSError:
        return None

    matches = [
        Path(entry.path)
        for entry in candidates
        if entry.is_dir()
        and normalise(entry.name) == wanted
        and is_non_empty_directory(Path(entry.path))
    ]

    if len(matches) == 1:
        return matches[0]

    if len(matches) > 1:
        logger.warning(
            "Ambiguous name variants for %s in %s: %s",
            basename,
            parent,
            [match.name for match in matches],
        )

    return None


def resolve_archive(upload_directory: Path, recorded: str) -> tuple:
    """Resolve one recorded archive path against the filesystem.

    Returns a (state, archive_path, tar_path, resolution) tuple.
    """
    recorded_path = upload_directory / recorded.lstrip("/")
    basename = recorded_path.name
    parent = recorded_path.parent

    if is_non_empty_directory(recorded_path):
        return STATE_READY, recorded_path, None, "recorded"

    variant = find_name_variant(parent, basename)
    if variant is not None:
        return STATE_READY, variant, None, "name-variant"

    tarball = find_tarball(parent, basename)
    if tarball is not None:
        # The tarballs embed the ``<name>.bin/`` prefix, so extracting in the
        # parent directory reproduces the recorded layout exactly.
        extracted = parent / basename
        return STATE_NEEDS_UNTAR, extracted, tarball, "tarball"

    if recorded_path.is_dir():
        return STATE_EMPTY, recorded_path, None, "empty"

    return STATE_MISSING, recorded_path, None, "missing"


def read_upload_info(upload_path: Path) -> Optional[dict]:
    """Return the parsed upload_info.pickle, or None when unusable."""
    info_file = upload_path / UPLOAD_INFO_FILE_NAME
    if not info_file.is_file():
        return None

    try:
        with open(info_file, "rb") as handle:
            return pickle.load(handle)
    except Exception as exception:  # noqa: BLE001 - any failure means "unusable"
        logger.warning("Cannot read %s: %s", info_file, exception)
        return None


def discover_archives_without_info(upload_path: Path, max_depth: int = 6) -> List[Path]:
    """Find archives on disk for uploads whose bookkeeping is missing."""
    found = []

    def walk(directory: Path, depth: int) -> None:
        if depth > max_depth:
            return
        try:
            entries = list(os.scandir(directory))
        except OSError:
            return
        for entry in entries:
            if entry.is_file() and normalise(entry.name).endswith(TAR_SUFFIXES):
                if normalise(entry.name).replace(".tar", "").endswith(".bin"):
                    found.append(Path(entry.path))
            elif entry.is_dir(follow_symlinks=False):
                if entry.name.endswith(".bin"):
                    if is_non_empty_directory(Path(entry.path)):
                        found.append(Path(entry.path))
                else:
                    walk(Path(entry.path), depth + 1)

    walk(upload_path, 0)
    return found


def result_path_for(upload_path: Path) -> Path:
    """Return the canonical HDF5 result path for one UUID upload."""
    return upload_path / "result" / "dataset.h5"


def build_entries(
    upload_directory: Path,
    with_samples: bool = False,
) -> Iterator[MasterListEntry]:
    """Yield one entry per archive found under the upload directory."""
    for name in sorted(os.listdir(upload_directory)):
        upload_path = upload_directory / name
        if not upload_path.is_dir():
            continue

        info = read_upload_info(upload_path)

        if info is None:
            archives = discover_archives_without_info(upload_path)
            if not archives:
                logger.debug("No archive and no usable info for %s.", name)
                continue
            logger.info(
                "Recovered %d archive(s) for %s without usable upload info.",
                len(archives),
                name,
            )
            for archive in archives:
                is_tar = archive.is_file()
                extracted = (
                    archive.parent / archive.name.split(".tar")[0]
                    if is_tar
                    else archive
                )
                yield MasterListEntry(
                    system_upload_uuid=name,
                    meta_name=extracted.stem,
                    meta_description="recovered without upload_info.pickle",
                    system_path=str(extracted),
                    system_result=str(result_path_for(upload_path)),
                    system_number_of_samples="",
                    source_tar=str(archive) if is_tar else "",
                    state=STATE_NEEDS_UNTAR if is_tar else STATE_READY,
                    recorded_path="",
                    resolution="discovered",
                )
            continue

        recorded_paths = [str(path) for path in (info.get("archive_paths") or [])]
        multiple = len(recorded_paths) > 1

        for index, recorded in enumerate(recorded_paths):
            state, archive_path, tar_path, resolution = resolve_archive(
                upload_path, recorded
            )

            samples = ""
            if with_samples and state == STATE_READY:
                counted = count_samples(archive_path)
                if counted is not None:
                    samples = str(counted)

            base_name = info.get("name") or archive_path.stem
            yield MasterListEntry(
                system_upload_uuid=name,
                meta_name=f"{base_name}_{index}" if multiple else str(base_name),
                meta_description=str(info.get("description") or ""),
                system_path=str(archive_path),
                system_result=str(result_path_for(upload_path)),
                system_number_of_samples=samples,
                source_tar=str(tar_path) if tar_path else "",
                state=state,
                recorded_path=recorded,
                resolution=resolution,
            )


def main() -> int:
    """Build the master list and write it to CSV."""
    parser = argparse.ArgumentParser(
        description="Build a master list of convertible ASTEC archives."
    )
    parser.add_argument(
        "-u",
        "--upload-directory",
        default=DEFAULT_UPLOAD_DIRECTORY,
        help="directory holding the per-uuid upload folders",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="master_list.csv",
        help="path of the CSV master list to write",
    )
    parser.add_argument(
        "--all-states",
        action="store_true",
        help="also write EMPTY and MISSING rows (default: processable only)",
    )
    parser.add_argument(
        "--count-samples",
        action="store_true",
        help=(
            "count saving_* entries for extracted archives; accurate but slow, "
            "and unavailable for archives that still need extraction"
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    upload_directory = Path(args.upload_directory)
    if not upload_directory.is_dir():
        logger.error("Upload directory %s does not exist.", upload_directory)
        return 1

    logger.info("Scanning %s.", upload_directory)

    counts = {}
    written = 0
    column_names = [field.name for field in fields(MasterListEntry)]

    with open(args.output, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=column_names)
        writer.writeheader()

        for entry in build_entries(upload_directory, with_samples=args.count_samples):
            counts[entry.state] = counts.get(entry.state, 0) + 1
            if args.all_states or entry.state in PROCESSABLE_STATES:
                writer.writerow(asdict(entry))
                written += 1

    for state in sorted(counts):
        logger.info("%6d  %s", counts[state], state)
    logger.info("Wrote %d rows to %s.", written, args.output)

    return 0


if __name__ == "__main__":
    sys.exit(main())
