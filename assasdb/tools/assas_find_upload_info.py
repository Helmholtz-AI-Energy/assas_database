#!/usr/bin/env python3
"""Scan LSDF upload folders for upload_info.pickle.

Behavior:
- Scans the upload root directory and checks each immediate child directory.
- Searches for TARGET_FILENAME in the child directory or one level deeper.
- If found one level deeper, copies it to the child directory top-level.

Optional fix mode:
- Reads database backup entries and for matching rows
- (user + samples=-1 + size='0.0 B')
  adjusts upload_info.pickle (archive_paths) and creates a reload marker file.

Configuration (env vars):
- LSDF_DATA_DIR: absolute path to upload root OR relative path under LSDF root
- (default: /mnt/ASSAS/upload_datahub)
- TARGET_FILENAME: filename to search for (default: upload_info.pickle)
- BACKUP_DIRECTORY: backup directory used by AssasDatabaseHandler
- DEfault (default: /mnt/ASSAS/backup_mongodb)
- LSDF_ROOT: optional root for relative LSDF_DATA_DIR
- LSDFPROJECTS: legacy optional root for relative LSDF_DATA_DIR (HPC)

CLI:
- --lsdf-root: override LSDF_ROOT/LSDFPROJECTS (only needed if
- LSDF_DATA_DIR is relative)
- --scan-only: only scan/copy, do not modify upload_info content
- --user: user to fix for
- --no-prompt: skip confirmation prompt (dangerous)
"""

import argparse
import logging
import os
import pickle
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import re

from assasdb import AssasDatabaseHandler, AssasDatabaseManager

# logging setup
log_file_path = Path(__file__).parent / "find_upload_info.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(log_file_path), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

LSDF_DATA_DIR = os.environ.get("LSDF_DATA_DIR", "/mnt/ASSAS/upload_datahub")
TARGET_FILENAME = os.environ.get("TARGET_FILENAME", "upload_info.pickle")
BACKUP_DIRECTORY = os.environ.get("BACKUP_DIRECTORY", "/mnt/ASSAS/backup_mongodb")
CONNECTIONSTRING = os.environ.get("CONNECTIONSTRING", "mongodb://localhost:27017/")
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "assasdb")

ENV_LSDF_ROOT = os.environ.get("LSDF_ROOT", "").strip()  # optional
ENV_LSDFPROJECTS = os.environ.get("LSDFPROJECTS", "").strip()  # optional legacy


def resolve_lsdf_upload_root(lsdf_root: Optional[Path]) -> Path:
    """Resolve the upload root directory.

    - If LSDF_DATA_DIR is absolute: returns it directly (no root needed).
    - If LSDF_DATA_DIR is relative: lsdf_root must be provided and is prepended.
    """
    data_dir = Path(LSDF_DATA_DIR)
    if data_dir.is_absolute():
        return data_dir

    if lsdf_root is None:
        raise ValueError(
            "LSDF_DATA_DIR is relative but no LSDF root was provided. "
            "Set LSDF_ROOT or LSDFPROJECTS or pass --lsdf-root, "
            "or set LSDF_DATA_DIR to an absolute path."
        )
    return Path(lsdf_root) / data_dir


def get_database_entries(user: Optional[str] = None) -> pd.DataFrame:
    """Return database entries (projected), optionally filtered by user.

    Note: and known 'bad' markers.
    """
    database_manager = AssasDatabaseManager(
        database_handler=AssasDatabaseHandler(
            connection_string=CONNECTIONSTRING,
            database_name=MONGO_DB_NAME,
            backup_directory=BACKUP_DIRECTORY,
        ),
    )

    logger.info(f"Loading database entries from backup directory: {BACKUP_DIRECTORY}")

    projection = {
        "system_user": 1,
        "system_number_of_samples": 1,
        "system_size": 1,
        "system_upload_uuid": 1,
        "meta_name": 1,
    }

    # Always load (projected) then filter in pandas
    # (avoids DB-filter mismatch surprises)
    limit = (
        0  # 0 = no limit (if your manager treats it differently, set a large number)
    )
    batch_size = 100
    max_time_ms = 12000

    database_entries = database_manager.get_all_database_entries_safe(
        projection=projection,
        limit=limit,
        batch_size=batch_size,
        max_time_ms=max_time_ms,
    )

    logger.info(
        "Loaded df rows=%s cols=%s dtypes=%s",
        len(database_entries),
        list(database_entries.columns),
        {
            c: str(database_entries[c].dtype)
            for c in projection.keys()
            if c in database_entries.columns
        },
    )

    # Robust filtering
    df = database_entries

    if "system_user" in df.columns and user:
        df = df[df["system_user"].astype(str).str.strip() == str(user).strip()]

    if "system_number_of_samples" in df.columns:
        samples_num = pd.to_numeric(df["system_number_of_samples"], errors="coerce")
        df = df[samples_num == -1]

    if "system_size" in df.columns:
        size_str = df["system_size"].astype(str).str.strip()
        df = df[size_str.str.match(r"^0(\.0+)?\s*B$", na=False)]

    logger.info(
        "After filter user=%r: rows=%s (top sizes=%s)",
        user,
        len(df),
        (
            df["system_size"].astype(str).str.strip().value_counts().head(5).to_dict()
            if "system_size" in df.columns
            else {}
        ),
    )
    return df


def find_upload_info_in_project(project_dir: Path) -> Optional[Path]:
    """Check for TARGET_FILENAME in project_dir or one level deeper."""
    if not project_dir.exists() or not project_dir.is_dir():
        return None

    top = project_dir / TARGET_FILENAME
    if top.exists():
        return top

    for child in project_dir.iterdir():
        if not child.is_dir():
            continue
        nested = child / TARGET_FILENAME
        if nested.exists():
            return nested

    return None


def scan_lsdf_uploads(lsdf_root: Optional[Path]) -> list[dict]:
    """Scan directories under the resolved LSDF upload root."""
    base = resolve_lsdf_upload_root(lsdf_root)
    results: list[dict] = []

    if not base.exists():
        logger.error(f"Base directory does not exist: {base}")
        return results

    logger.info(f"Scanning upload base: {base}")

    for entry in sorted(base.iterdir()):
        if not entry.is_dir():
            continue

        found_path = find_upload_info_in_project(entry)
        if found_path is None:
            results.append(
                {
                    "project_dir": str(entry),
                    "found": False,
                    "found_path": None,
                    "search_depth": None,
                    "copied_to_top": False,
                }
            )
            logger.info(f"Not found: {entry}")
            continue

        found_path = Path(found_path)
        depth = 0 if (found_path.parent == entry) else 1

        copied = False
        if depth == 1:
            top_target = entry / TARGET_FILENAME
            try:
                shutil.copy2(found_path, top_target)
                copied = True
                logger.info(f"Copied {found_path} -> {top_target}")
            except Exception as e:
                logger.error(f"Failed to copy {found_path} -> {top_target}: {e}")

        results.append(
            {
                "project_dir": str(entry),
                "found": True,
                "found_path": str(found_path),
                "search_depth": depth,
                "copied_to_top": copied,
            }
        )
        logger.info(
            f"Found (depth={depth}) {found_path} ,"
            f"for project {entry} (copied_to_top={copied})"
        )

    return results


def get_upload_info(upload_info_path: Path) -> Optional[dict]:
    """Load upload_info.pickle."""
    if not upload_info_path.exists():
        logger.error(f"Upload info file does not exist: {upload_info_path}")
        return None
    try:
        with open(upload_info_path, "rb") as f:
            return pickle.load(f)
    except Exception as e:
        logger.error(f"Failed to load upload info from {upload_info_path}: {e}")
        return None


def write_upload_info(upload_info_path: Path, upload_info: dict) -> bool:
    """Write upload_info.pickle."""
    try:
        with open(upload_info_path, "wb") as f:
            pickle.dump(upload_info, f)
        return True
    except Exception as e:
        logger.error(f"Failed to save upload info to {upload_info_path}: {e}")
        return False


def correct_archive_paths_in_upload_info(
    upload_info_path: Path, new_archive_paths: list[str]
) -> bool:
    """Load, modify, and save upload_info.pickle with new archive_paths."""
    upload_info = get_upload_info(upload_info_path)
    if not upload_info:
        return False

    old_paths = upload_info.get("archive_paths", None)
    upload_info["archive_paths"] = new_archive_paths

    ok = write_upload_info(upload_info_path, upload_info)
    if ok:
        logger.info(
            f"Updated archive_paths in {upload_info_path}: ,"
            f"{old_paths} -> {new_archive_paths}"
        )
    return ok


def generate_reload_file(upload_dir: Path, upload_uuid: str) -> None:
    """Trigger a reload by touching a marker file."""
    try:
        reload_file = upload_dir / f"{upload_uuid}_reload"
        subprocess.run(["touch", str(reload_file)], check=True)
        logger.info(f"Generated reload file: {reload_file}")
    except Exception as e:
        logger.error(f"Failed to generate reload file for {upload_uuid}: {e}")


def correct_archive_paths_for_user(lsdf_root: Optional[Path], user: str) -> int:
    """Fix upload_info.pickle for rows matching.

    Example:
        system_user == user
        system_number_of_samples == -1
        system_size == '0.0 B'

    """
    upload_root = resolve_lsdf_upload_root(lsdf_root)
    df = get_database_entries(user=user)

    # Display matches first
    if df.empty:
        logger.info("No matching bad entries found for user=%r", user)
        return 0

    logger.info(
        "Bad entries preview:\n%s",
        df[
            [
                "system_user",
                "system_number_of_samples",
                "system_size",
                "system_upload_uuid",
            ]
        ]
        .head(50)
        .to_string(index=False),
    )

    corrected = 0
    for idx, row in df.iterrows():
        try:
            user_val = str(row.get("system_user", "")).strip()
            samples = pd.to_numeric(
                row.get("system_number_of_samples"),
                errors="coerce",
            )
            size = str(row.get("system_size", "")).strip()

            size_is_zero_b = bool(re.match(r"^0(\.0+)?\s*B$", size))

            if (
                (user_val != str(user).strip())
                or (samples != -1)
                or (not size_is_zero_b)
            ):
                logger.info(
                    (
                        "Row %s: does not match criteria, "
                        "skipping (user=%r, samples=%r[%s], size=%r)"
                    ),
                    idx,
                    user_val,
                    row.get("system_number_of_samples"),
                    type(row.get("system_number_of_samples")).__name__,
                    size,
                )
                continue

            upload_uuid = row.get("system_upload_uuid")
            if not upload_uuid:
                logger.warning("Row %s: missing system_upload_uuid; skipping.", idx)
                continue

            upload_dir = upload_root / str(upload_uuid)
            upload_info_path = upload_dir / TARGET_FILENAME

            upload_info = get_upload_info(upload_info_path)
            if not upload_info:
                logger.warning(
                    (
                        f"Row {idx}: cannot read upload_info: "
                        f"({idx} - {upload_info_path})"
                    )
                )
                continue

            archive_name = str(upload_info.get("name"))
            new_archive_paths = [
                f"/{archive_name}/LOCA_6I_CL_1300_LIKE_SIMPLIFIED_ASSAS_FILT.bin"
            ]

            if correct_archive_paths_in_upload_info(
                upload_info_path=upload_info_path,
                new_archive_paths=new_archive_paths,
            ):
                generate_reload_file(upload_dir, str(upload_uuid))
                corrected += 1

        except Exception as e:
            logger.error("Row %s: failed to process: %s", idx, e)

    logger.info("Corrected archives: %s", corrected)
    return corrected


def determine_lsdf_root(cli_lsdf_root: str) -> Optional[Path]:
    """LSDF root is optional.

    Example:
        - If LSDF_DATA_DIR is absolute -> no root needed (returns None).
        - If LSDF_DATA_DIR is relative -> returns the first available root from:
            1) --lsdf-root
            2) LSDF_ROOT
            3) LSDFPROJECTS
        If none is provided, resolve_lsdf_upload_root() will raise.

    """
    if Path(LSDF_DATA_DIR).is_absolute():
        return None

    if cli_lsdf_root.strip():
        return Path(cli_lsdf_root.strip())
    if ENV_LSDF_ROOT:
        return Path(ENV_LSDF_ROOT)
    if ENV_LSDFPROJECTS:
        return Path(ENV_LSDFPROJECTS)
    return None


def main(argv: list[str]) -> int:
    """Entry point."""
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--user", default="hf8003", help="User to fix for (default: hf8003)"
    )
    ap.add_argument(
        "--scan-only",
        action="store_true",
        help="Only scan/copy upload_info.pickle; do not modify upload_info content",
    )
    ap.add_argument(
        "--no-prompt",
        action="store_true",
        help="Skip the interactive confirmation prompt",
    )
    ap.add_argument(
        "--correct-only", action="store_true", help="Only attempt corrections"
    )
    ap.add_argument(
        "--lsdf-root",
        default="",
        help=(
            "Optional root directory used when LSDF_DATA_DIR is "
            "relative (overrides LSDF_ROOT/LSDFPROJECTS).",
        ),
    )
    ap.add_argument(
        "--results-csv",
        default=str(Path(__file__).parent / "upload_info_scan_results.csv"),
        help="Where to write scan results CSV",
    )
    args = ap.parse_args(argv)

    if not args.no_prompt:
        confirmation = input(
            "DANGER: This script can modify LSDF data. "
            "Type 'I UNDERSTAND' to proceed: ",
        )
        if confirmation.strip() != "I UNDERSTAND":
            logger.error("Confirmation not provided. Aborting.")
            return 1

    lsdf_root = determine_lsdf_root(args.lsdf_root)

    if args.scan_only:
        try:
            scanned = scan_lsdf_uploads(lsdf_root)
            out_csv = Path(args.results_csv)
            pd.DataFrame(scanned).to_csv(out_csv, index=False)
            logger.info(f"Scan complete. Results written to {out_csv}")
        except ValueError as e:
            logger.error(str(e))
            return 1

    if args.correct_only:
        try:
            # Only run corrections, skip scanning/copying
            # Manual changed.
            correct_archive_paths_for_user(lsdf_root, user=args.user)
        except ValueError as e:
            logger.error(str(e))
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
