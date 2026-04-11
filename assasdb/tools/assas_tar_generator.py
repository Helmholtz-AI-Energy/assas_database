"""Tool to create tar (or tar.gz) archives for directories.

Execution is driven from DB dataframe or manually.
Usage examples:
1) From DB, with progress logging and gzip:
    python -m assasdb.tools.assas_tar_generator --from-db --progress --gz
2) From DB, only for specific UUIDs, with gzip:
    python -m assasdb.tools.assas_tar_generator --from-db --gz \
        --uuid 123e4567-e89b-12d3-a456-426
"""

import sys
import os
import shutil
import subprocess
import argparse
import csv
import hashlib
import logging
import re
import tempfile
import pandas as pd
import tarfile
import filecmp
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from assasdb import (
    AssasDocumentFileStatus,
    AssasDatabaseManager,
    AssasDatabaseHandler,
    require_env,
    redact_mongo_uri,
    find_env_file,
)

logger = logging.getLogger(__name__)

_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")

ASTEC_ROOT = os.environ.get("ASTEC_ROOT")
ASTEC_TYPE = os.environ.get("ASTEC_TYPE")

ASTEC_PYTHON_ODESSA = os.path.join(
    ASTEC_ROOT, "odessa", "bin", ASTEC_TYPE + "-release", "wrap_python"
)

if ASTEC_PYTHON_ODESSA not in sys.path:
    logger.info(f"Append path to odessa to environment: {ASTEC_PYTHON_ODESSA}")
    sys.path.append(ASTEC_PYTHON_ODESSA)

import pyodessa as pyod  # noqa: E402


class TarGeneratorError(RuntimeError):
    """Custom exception for tar generation errors."""

    pass


@dataclass(frozen=True)
class TarJob:
    """Represents a single tar creation job."""

    source_dir: Path
    target_dir: Path
    archive_name: Optional[str] = None  # e.g. "mydata.tar" or "mydata.tar.gz"
    gzip: bool = False  # True -> .tar.gz

    # Progress logging (GNU tar checkpoints)
    progress: bool = False
    checkpoint: int = 5000  # log every N files processed (best-effort)


class BasicTarGenerator:
    """Basic tar creator: source directory -> tar file in target directory."""

    def __init__(self, database_manager: Optional[AssasDatabaseManager] = None) -> None:
        """Initialize the BasicTarGenerator."""
        self.database_manager = database_manager

    def _count_files(self, directory: Path) -> int:
        """Count total files in a directory recursively."""
        count = 0
        for _, _, files in os.walk(directory):
            count += len(files)
        return count

    def _count_entries(self, directory: Path) -> int:
        """Count all archive entries (files, dirs, symlinks, special files)."""
        count = 0
        for p in directory.rglob("*"):
            # Count files, directories, symlinks, and special files
            if p.is_file() or p.is_dir() or p.is_symlink():
                count += 1
            # Optionally count special files (devices, pipes)
            elif p.exists():
                count += 1
        # Also count the root directory itself
        count += 1
        return count

    def create(self, job: TarJob) -> Path:
        """Create a tar (or tar.gz) archive for the given job.

        Returns:
            Path to created archive.

        """
        if shutil.which("tar") is None:
            raise TarGeneratorError("`tar` not found in PATH.")

        src = job.source_dir.expanduser().resolve()
        dst_dir = job.target_dir.expanduser().resolve()

        if not src.exists() or not src.is_dir():
            raise TarGeneratorError(
                f"Source directory does not exist or is not a directory: {src}"
            )

        dst_dir.mkdir(parents=True, exist_ok=True)

        # Determine archive name
        if job.archive_name:
            archive_name = job.archive_name
        else:
            archive_name = (
                f"{self._safe_filename(src.name)}.tar.gz"
                if job.gzip
                else f"{self._safe_filename(src.name)}.tar"
            )

        out_path = (dst_dir / archive_name).resolve()

        # Build tar command: tar -C <parent> <name> ->
        # avoids absolute paths inside archive
        parent = src.parent
        name = src.name

        cmd = ["tar", "-C", str(parent), "-cvf", str(out_path), name]

        if job.progress:
            # GNU tar: periodically prints a line to stderr while archiving.
            # Note: %u expands to the checkpoint number
            # (roughly "files processed" checkpoints).
            cmd += [
                f"--checkpoint={int(job.checkpoint)}",
                "--checkpoint-action=echo=tar checkpoint %u",
            ]

        if job.gzip:
            # Prefer pigz if available (faster on large directories)
            if shutil.which("pigz"):
                # pigz default is "number of online processors" (or 8 if unknown).
                # Never use -p 0 (invalid on pigz).
                procs = os.cpu_count() or 1
                procs = max(1, int(procs))
                cmd += ["-I", f"pigz -p {procs}"]
                # If you prefer default behavior instead, replace the line above with:
                # cmd += ["-I", "pigz"]
            else:
                cmd += ["-z"]

        logger.info("Running: %s", " ".join(cmd))

        if job.progress:
            total_files = self._count_files(src)
            logger.info("Total archive entries to process: %d", total_files)

            process = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
            assert process.stderr is not None
            stderr_lines: list[str] = []
            for line in process.stderr:
                line = line.rstrip("\n")
                if line:
                    # Check for tar checkpoint log
                    if line.startswith("tar: tar checkpoint "):
                        try:
                            checkpoint_number = int(line.split()[-1])
                            percent = min(
                                int(
                                    (checkpoint_number / ((total_files + 1682) * 100))
                                    * 100
                                ),
                                100,
                            )
                            logger.info(
                                f"Tar progress: {percent}% "
                                f"({checkpoint_number}/"
                                f"{((total_files + 1682) * 100)} entries)"
                            )
                        except Exception:
                            logger.info(line)
                    else:
                        logger.info(line)

                    stderr_lines.append(line)
                    if len(stderr_lines) > 2000:
                        stderr_lines = stderr_lines[-500:]
            rc = process.wait()
            if rc != 0:
                tail = "\n".join(stderr_lines[-50:])
                raise TarGeneratorError(f"tar failed (exit={rc}). stderr tail:\n{tail}")
        else:
            process = subprocess.run(
                cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True
            )
            if process.returncode != 0:
                raise TarGeneratorError(
                    f"tar failed (exit={process.returncode}). stderr:\n{process.stderr}"
                )

        if not out_path.exists():
            raise TarGeneratorError(f"Archive was not created: {out_path}.")

        return out_path

    def _safe_filename(self, value: str) -> str:
        """Derive a safe filename component from the given value.

        Returns:
            A sanitized string that can be safely used as a filename component.

        """
        value = (value or "").strip()
        value = _SAFE_NAME_RE.sub("_", value)
        value = value.strip("._-")
        if not value:
            raise TarGeneratorError("Could not derive a safe filename component.")
        return value

    def _md5_file(self, path: Path, *, bufsize: int = 8 * 1024 * 1024) -> str:
        """Calculate MD5 checksum of a file.

        Args:
            path: Path to the file
            bufsize: Buffer size for reading the file

        Returns:
            MD5 checksum as a hexadecimal string

        """
        h = hashlib.md5()  # noqa: S324 (md5 requested for checksum output)
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(bufsize), b""):
                h.update(chunk)
        return h.hexdigest()

    def _write_results_csv(self, rows: list[dict], csv_path: Path) -> None:
        """Write a CSV with columns: dataset_uuid, system_path, tar_path, md5.

        Args:
            rows: List of dictionaries with keys 'dataset_uuid',
                'system_path', 'tar_path', 'md5'.
            csv_path: Path to the output CSV file

        Returns:
            None

        """
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = ["dataset_uuid", "system_path", "tar_path", "md5"]
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in fieldnames})

    def _resolve_target_dir(self, arg_value: Optional[str]) -> Path:
        """Resolve target directory from CLI argument or environment variable.

        Resolution order:
        1) CLI positional target_dir
        2) env: TAR_TARGET_DIR
        """
        if arg_value:
            return Path(arg_value)

        env_dir = os.environ.get("TAR_TARGET_DIR")
        if env_dir:
            return Path(env_dir)

        raise TarGeneratorError(
            "target_dir not provided. Pass it as positional arg, or set env var "
            "TAR_TARGET_DIR)."
        )

    def _upload_uuid_from_tar_name(self, filename: str) -> Optional[str]:
        """Extract upload_uuid from a tar filename.

        Accepts:
        - <upload_uuid>.tar
        - <upload_uuid>.tar.gz

        Returns:
        upload_uuid (string) or None if not a tar archive name.

        """
        name = (filename or "").strip()
        if name.endswith(".tar.gz"):
            return name[: -len(".tar.gz")]
        if name.endswith(".tar"):
            return name[: -len(".tar")]
        return None

    def generate_tar_inventory_csv_from_mongo(
        self,
        *,
        tar_dir: Path,
        csv_path: Path,
        database_manager: AssasDatabaseManager,
        upload_uuid_column: str = "system_upload_uuid",
        dataset_id_columns: tuple[str, ...] = ("radar_dataset_id", "dataset_uuid"),
    ) -> Path:
        """Generate a CSV with one row per tar file in tar_dir.

        For each tar file, extract upload_uuid from the filename, then look up the
        corresponding dataset id in MongoDB (via AssasDatabaseManager).

        CSV columns:
        upload_uuid, dataset_id, tar_path, md5

        Args:
            tar_dir: Directory containing tar/tar.gz archives.
            csv_path: Output CSV path.
            database_manager: Configured AssasDatabaseManager instance.
            upload_uuid_column: Column in DB dataframe containing upload UUIDs.
            dataset_id_columns: Candidate columns for dataset id in DB dataframe.

        Returns:
            Path to the written CSV.

        Raises:
            TarGeneratorError: On missing directory, missing columns, etc.

        """
        tar_dir = tar_dir.expanduser().resolve()
        csv_path = csv_path.expanduser().resolve()

        if not tar_dir.exists() or not tar_dir.is_dir():
            raise TarGeneratorError(
                f"tar_dir does not exist or is not a directory: {tar_dir}"
            )

        df = database_manager.get_all_database_entries()

        if upload_uuid_column not in getattr(df, "columns", []):
            raise TarGeneratorError(
                f"DB dataframe missing column '{upload_uuid_column}'. "
                f"Have: {list(getattr(df, 'columns', []))}"
            )

        dataset_col = next((c for c in dataset_id_columns if c in df.columns), None)
        if not dataset_col:
            raise TarGeneratorError(
                "DB dataframe missing dataset id column. Tried: "
                f"{list(dataset_id_columns)}. "
                f"Have: {list(getattr(df, 'columns', []))}"
            )

        # Build mapping: upload_uuid -> dataset_id
        uuids = df[upload_uuid_column].astype(str)
        dataset_ids = df[dataset_col].astype(str)
        uuid_to_dataset: dict[str, str] = dict(zip(uuids, dataset_ids, strict=False))

        rows: list[dict] = []
        for p in sorted(tar_dir.iterdir()):
            logger.info("Processing tar file: %s", p)
            if not p.is_file():
                continue

            upload_uuid = self._upload_uuid_from_tar_name(p.name)
            if not upload_uuid:
                continue

            dataset_id = uuid_to_dataset.get(upload_uuid, "")
            logger.info(
                "Found upload_uuid=%s with dataset_id=%s", upload_uuid, dataset_id
            )

            try:
                md5 = self._md5_file(p)
            except Exception as e:
                logger.warning("Could not calculate md5 for %s: %s", p, e)
                md5 = ""

            rows.append(
                {
                    "upload_uuid": upload_uuid,
                    "dataset_id": dataset_id,
                    "tar_path": str(p),
                    "md5": md5,
                }
            )

        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(
                f,
                fieldnames=["upload_uuid", "dataset_id", "tar_path", "md5"],
            )
            w.writeheader()
            w.writerows(rows)

        logger.info("Wrote tar inventory CSV: %s (rows=%d)", csv_path, len(rows))
        return csv_path

    def create_tars_from_dataframe(
        self,
        dataframe: pd.DataFrame,
        *,
        target_dir: Path,
        gzip: bool,
        only_uuids: Optional[set[str]] = None,
        progress: bool = False,
        checkpoint: int = 5000,
        limit: Optional[int] = None,  # <-- add
    ) -> list[dict]:
        """Expect df columns: 'system_upload_uuid' and 'system_path'.

        For each row: creates <system_upload_uuid>.tar[.gz]
        from system_path into target_dir.

        Args:
            dataframe:
                DataFrame with required columns 'system_upload_uuid' and 'system_path'.
            target_dir: Directory where the tar files should be written.
            gzip: Whether to create gzip-compressed tar.gz files.
            only_uuids: If provided, only create tars for these upload_uuid(s).
            progress: Whether to log tar progress using GNU tar checkpoints.
            checkpoint: Progress log interval (every N files).
            limit: If provided, only create the first N archives (after filtering).

        Returns:
            List of dictionaries with keys 'dataset_uuid', 'upload_uuid', 'system_uuid',
            'tar_path', 'md5' for each created archive.

        """
        required_cols = {"system_upload_uuid", "system_uuid", "system_path"}
        missing = [
            c for c in required_cols if c not in getattr(dataframe, "columns", [])
        ]
        if missing:
            raise TarGeneratorError(
                f"Dataframe missing required columns: {missing}. \n"
                f"Have: {list(getattr(dataframe, 'columns', []))}"
            )

        results: list[dict] = []
        created = 0  # <-- add

        for _, row in dataframe.iterrows():
            # Stop once we reached the requested limit
            if limit is not None and created >= int(limit):
                logger.info("Reached limit=%s archives; stopping.", limit)
                break

            upload_uuid = str(row["system_upload_uuid"])
            system_uuid = str(row["system_uuid"])
            system_path = str(row["system_path"])
            system_status = AssasDocumentFileStatus(row.get("system_status", ""))

            if system_status != AssasDocumentFileStatus.VALID:
                logger.warning(
                    "Skipping upload_uuid=%s with system_status=%s",
                    upload_uuid,
                    system_status,
                )
                continue

            if only_uuids and upload_uuid not in only_uuids:
                continue

            src_dir = Path(system_path)
            safe_uuid = self._safe_filename(upload_uuid)
            archive_name = f"{safe_uuid}.tar.gz" if gzip else f"{safe_uuid}.tar"

            logger.info(
                f"Creating tar for upload_uuid={upload_uuid} from {system_path}."
            )

            out = self.create(
                TarJob(
                    source_dir=src_dir,
                    target_dir=target_dir,
                    archive_name=archive_name,
                    gzip=gzip,
                    progress=progress,
                    checkpoint=checkpoint,
                )
            )

            md5 = self._md5_file(out)
            results.append(
                {
                    "dataset_uuid": str(row.get("dataset_uuid", "")),
                    "upload_uuid": upload_uuid,
                    "system_uuid": system_uuid,
                    "tar_path": str(out),
                    "md5": md5,
                }
            )

            created += 1

        return results

    def safe_tar_and_delete_from_dataframe(
        self,
        dataframe: pd.DataFrame,
        *,
        gzip: bool,
        only_uuids: Optional[set[str]] = None,
        progress: bool = False,
        checkpoint: int = 5000,
        limit: Optional[int] = None,
        path_prefix_overwrite: Optional[tuple[str, str]] = None,
        tmp_dir: Optional[Path] = None,
        cleanup_tmp_root: bool = True,
    ) -> list[dict]:
        """Tars the directory, validates the archive, and deletes the original.

        Deletion only when validation passes.

        Optionally overwrites the path prefix for testing.

        Returns:
            List of dictionaries with keys 'dataset_uuid', 'upload_uuid', 'system_uuid',
            'tar_path', 'md5' for each created archive.

        """
        required_cols = {
            "system_upload_uuid",
            "system_uuid",
            "system_path",
            "radar_dataset_id",
        }
        missing = [
            c for c in required_cols if c not in getattr(dataframe, "columns", [])
        ]
        if missing:
            raise TarGeneratorError(
                f"Dataframe missing required columns: {missing}. \n"
                f"Have: {list(getattr(dataframe, 'columns', []))}"
            )

        results: list[dict] = []
        created = 0

        for _, row in dataframe.iterrows():
            logger.info(
                f"Processing row with upload_uuid={row.get('system_upload_uuid', '')}"
            )

            if limit is not None and created >= int(limit):
                logger.info("Reached limit=%s archives; stopping.", limit)
                break

            upload_uuid = str(row["system_upload_uuid"])
            system_uuid = str(row["system_uuid"])
            system_path = str(row["system_path"])
            radar_dataset_id = str(row["radar_dataset_id"])
            system_status = AssasDocumentFileStatus(row.get("system_status", ""))

            if system_status != AssasDocumentFileStatus.VALID:
                logger.warning(
                    "Skipping upload_uuid=%s with system_status=%s",
                    upload_uuid,
                    system_status,
                )
                continue

            if only_uuids and upload_uuid not in only_uuids:
                continue

            # Overwrite prefix for testing if requested
            if path_prefix_overwrite:
                logger.info(
                    f"Overwriting path prefix for upload_uuid={upload_uuid} "
                    f"using mapping: {path_prefix_overwrite}"
                )
                old_prefix, new_prefix = path_prefix_overwrite
                if system_path.startswith(old_prefix):
                    system_path = system_path.replace(old_prefix, new_prefix, 1)

            if system_path.startswith("/mnt/ASSAS"):
                system_path = system_path.replace(
                    "/mnt/ASSAS", "/lsdf/kit/scc/projects/ASSAS", 1
                )

            src_dir = Path(system_path)
            if not src_dir.exists() or not src_dir.is_dir():
                logger.warning(
                    "Directory does not exist for upload_uuid=%s: %s",
                    upload_uuid,
                    src_dir,
                )
                continue

            base_name = src_dir.name
            archive_name = f"{base_name}.tar.gz" if gzip else f"{base_name}.tar"

            try:
                logger.info(
                    f"Creating tar for upload_uuid={upload_uuid}: {archive_name}"
                )

                tar_path = self.safe_tar_and_delete(
                    src_dir=src_dir,
                    target_dir=src_dir.parent,
                    archive_name=archive_name,
                    gzip=gzip,
                    progress=progress,
                    checkpoint=checkpoint,
                    tmp_dir=tmp_dir,
                    cleanup_tmp_root=cleanup_tmp_root,
                )

                # logger.info(
                #    f"Creating md5 of tar for upload_uuid={upload_uuid}: {tar_path}"
                # )

                # md5 = self._md5_file(tar_path)
                # self.database_manager.database_handler.update_md5_for_system_uuid(
                #    system_uuid=system_uuid,
                #    md5=md5
                # )

                results.append(
                    {
                        "radar_dataset_id": radar_dataset_id,
                        "upload_uuid": upload_uuid,
                        "system_uuid": system_uuid,
                        "tar_path": str(tar_path),
                        # "tar_md5": md5,
                    }
                )

                logger.info(
                    f"Created and validated tar for upload_uuid={upload_uuid}: "
                    f"{tar_path} (deleted original {src_dir.name})"
                )

                created += 1
            except TarGeneratorError as e:
                logger.error(
                    f"Failed safe tar-and-delete for upload_uuid={upload_uuid}: {e}"
                )
                continue

        return results

    def compare_dirs_with_progress(self, src_dir: Path, extracted_dir: Path) -> None:
        """Compare two directories recursively with progress logging."""
        src_files = sorted(
            [p.relative_to(src_dir) for p in src_dir.rglob("*") if p.is_file()]
        )
        total = len(src_files)
        logger.info(f"Validating {total} files...")
        missing = []
        diff = []

        for idx, rel_path in enumerate(src_files, 1):
            src_file = src_dir / rel_path
            ext_file = extracted_dir / rel_path
            if not ext_file.exists():
                missing.append(str(rel_path))
            elif not filecmp.cmp(src_file, ext_file, shallow=False):
                diff.append(str(rel_path))
            if idx % 1000 == 0 or idx == total:
                logger.info(f"Validation progress: {idx}/{total} files checked...")
        if missing or diff:
            raise TarGeneratorError(
                f"Validation failed: missing={missing}, diff={diff}"
            )

        logger.info("Validation passed: all files match.")

    def safe_tar_and_delete(
        self,
        src_dir: Path,
        target_dir: Path,
        archive_name: Optional[str] = None,
        gzip: bool = False,
        progress: bool = False,
        checkpoint: int = 5000,
        tmp_dir: Optional[Path] = None,
        cleanup_tmp_root: bool = True,
    ) -> Path:
        """Tar the directory, validate the archive, and delete the original archive.

        Args:
            src_dir: Directory to archive.
            target_dir: Where to place the archive.
            archive_name: Name for the archive file.
            gzip: Whether to create a .tar.gz archive.
            progress: Show tar progress.
            checkpoint: Progress log interval.
            tmp_dir: Temporary directory for validation extraction.
            cleanup_tmp_root: Whether to clean up the temporary root directory.

        Returns:
            Path to the created archive.

        Raises:
            TarGeneratorError if validation fails.

        """
        archive_name = archive_name or (
            f"{src_dir.name}.tar.gz" if gzip else f"{src_dir.name}.tar"
        )
        logger.info(f"Creating tar for {src_dir} with safe tar-and-delete.")

        tar_path = self.create(
            TarJob(
                source_dir=src_dir,
                target_dir=target_dir,
                archive_name=archive_name,
                gzip=gzip,
                progress=progress,
                checkpoint=checkpoint,
            )
        )

        logger.info(f"Tar created at {tar_path}. Starting validation by extraction.")

        # Validate: extract to temp dir and compare
        tmp_root = tmp_dir.resolve() if tmp_dir else None

        with tempfile.TemporaryDirectory(
            dir=str(tmp_root) if tmp_root else None
        ) as tmpdir:
            # Extract the tar to a temporary directory for validation
            try:
                tmp_extract = Path(tmpdir)
                tmp_root = tmp_extract
                with tarfile.open(tar_path, "r:*") as tar:
                    idx = 0
                    for member in tar:
                        tar.extract(member, path=tmp_extract)
                        idx += 1
                        if idx % 1000 == 0:
                            logger.info(
                                f"Extraction progress: {idx} files extracted..."
                            )
                    logger.info("Extraction complete. Proceeding with validation.")

            except Exception as e:
                raise TarGeneratorError(f"Extraction failed for {tar_path}: {e}")

            extracted_dir = Path(tmpdir) / src_dir.name

            # Compare the original directory and the extracted directory
            try:
                self.compare_dirs_with_progress(src_dir, extracted_dir)

            except Exception as e:
                raise TarGeneratorError(
                    f"Validation failed during directory comparison: {e}"
                )

            # PyOdessa time point comparison
            try:
                before_times = pyod.get_saving_times(str(src_dir))
                after_times = pyod.get_saving_times(str(extracted_dir))
                if before_times != after_times:
                    raise TarGeneratorError(
                        f"PyOdessa time points differ: \n"
                        f"before={before_times}, after={after_times}."
                    )
                logger.info(
                    f"PyOdessa validation succeeded for: {extracted_dir} \n"
                    f"(time points identical)."
                )

            except Exception as e:
                raise TarGeneratorError(
                    f"PyOdessa validation failed for {extracted_dir}: {e}."
                )

        logger.info(
            f"Validation successful for {tar_path}. Deleting original directory."
        )

        # If validation passed, delete the original directory
        try:
            shutil.rmtree(src_dir)
            logger.info(
                f"Original directory {src_dir} deleted after successful validation."
            )
        except Exception as exc:
            logger.warning("Failed to delete original directory %s: %s", src_dir, exc)

        # optional cleanup of tmp root (delete recursively)
        if cleanup_tmp_root and tmp_root is not None:
            try:
                if tmp_root.exists():
                    shutil.rmtree(tmp_root)
                    logger.info("Removed tmp root directory recursively: %s", tmp_root)
            except Exception as exc:
                logger.warning("Failed to remove tmp root %s: %s", tmp_root, exc)

        return tar_path


def main() -> int:
    """Execute main logic of script.

    Parses arguments and creates tar(s) accordingly.

    Returns:
        Exit code (0 for success, non-zero for errors)

    """
    ap = argparse.ArgumentParser(
        description=(
            "Create tar (or tar.gz) archives for directories. "
            "Can be driven from DB dataframe."
        )
    )

    ap.add_argument(
        "--tar-complete-archive",
        action="store_true",
        help=(
            "Create one tar per DB entry using dataframe "
            "columns upload_uuid + system_path.",
        ),
    )
    ap.add_argument(
        "--uuid",
        action="append",
        default=[],
        help=(
            "When used with --tar-complete-archive: "
            "only create tars for these upload_uuid(s). "
            "Can be passed multiple times."
        ),
    )
    ap.add_argument(
        "--progress",
        action="store_true",
        help="Log tar progress using GNU tar checkpoints",
    )
    ap.add_argument(
        "--checkpoint",
        type=int,
        default=5000,
        help="Progress log interval (every N files)",
    )
    ap.add_argument(
        "--result-csv",
        default=None,
        help="Write results CSV with columns upload_uuid, system_path, tar_path, md5. "
        "Default: <target_dir>/tar_results.csv",
    )
    ap.add_argument("source_dir", nargs="?", help="Directory to archive (manual mode)")
    ap.add_argument(
        "--target-dir",
        nargs="?",
        default=None,
        help=(
            "Directory where the tar(s) should be written "
            "(optional if env ASSAS_TAR_TARGET_DIR is set)"
        ),
    )
    ap.add_argument(
        "--name",
        default=None,
        help="Archive file name for manual mode (e.g. data.tar or data.tar.gz)",
    )
    ap.add_argument("--gz", action="store_true", help="Create .tar.gz (gzip)")
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Create only the first N archives (after filtering). Example: --limit 50",
    )
    ap.add_argument(
        "--safe-tar-and-delete-from-db",
        action="store_true",
        help="Tar, validate, and delete directories for all DB entries (batch mode).",
    )
    ap.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set logging level (default: INFO)",
    )
    ap.add_argument(
        "--keep-tmp-root",
        action="store_true",
        help=(
            "Keep tmp root directory after safe tar+delete (default: cleanup if empty)."
        ),
    )

    ns = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, ns.log_level),
        format="%(asctime)s %(levelname)s: %(message)s",
    )

    target_dir = Path(ns.target_dir) if ns.target_dir is not None else None

    result_csv_path = (
        Path(ns.result_csv) if ns.result_csv else (target_dir / "tar_results.csv")
    )

    env_path = find_env_file()
    env = require_env(
        env_path=env_path,
        logger=logger,
        keys=[
            "CONNECTIONSTRING",
            "BACKUP_DIRECTORY",
            "MONGO_DB_NAME",
            "UPLOAD_DIRECTORY",
            "UPLOAD_TEST",
            "UPLOAD_TMP",
        ],
    )

    logger.info("Using database: %s", env["MONGO_DB_NAME"])
    logger.info("Using backup directory: %s", env["BACKUP_DIRECTORY"])
    logger.info("Using Mongo connection: %s", redact_mongo_uri(env["CONNECTIONSTRING"]))
    logger.info("Using upload directory: %s", env["UPLOAD_DIRECTORY"])
    logger.info("Using upload test directory: %s", env["UPLOAD_TEST"])
    logger.info("Using upload temporary directory: %s", env["UPLOAD_TMP"])
    logger.info("Using gzip: %s", ns.gz)
    logger.info("Using UUID filter: %s", ns.uuid if ns.uuid else "<none>")
    logger.info("Writing results CSV: %s", result_csv_path)
    logger.info("Tmp root cleanup enabled: %s", not ns.keep_tmp_root)

    database_manager = AssasDatabaseManager(
        database_handler=AssasDatabaseHandler(
            connection_string=env["CONNECTIONSTRING"],
            backup_directory=env["BACKUP_DIRECTORY"],
            database_name=env["MONGO_DB_NAME"],
        )
    )
    generator = BasicTarGenerator(database_manager=database_manager)

    if ns.tar_complete_archive:
        dataframe = database_manager.get_all_database_entries()

        only = set(ns.uuid) if ns.uuid else None

        results = generator.create_tars_from_dataframe(
            dataframe=dataframe,
            target_dir=ns.target_dir,
            gzip=ns.gz,
            only_uuids=only,
            progress=ns.progress,
            checkpoint=ns.checkpoint,
            limit=ns.limit,
            path_prefix_overwrite=(env["UPLOAD_DIRECTORY"], env["UPLOAD_TEST"]),
        )

        generator._write_results_csv(results, result_csv_path)

        for r in results:
            logger.info(
                f"system_upload_uuid={r['upload_uuid']}  "
                f"tar_path={r['tar_path']}  md5={r['md5']}"
            )

        return 0

    if ns.safe_tar_and_delete_from_db:
        dataframe = database_manager.get_all_database_entries()

        only = set(ns.uuid) if ns.uuid else None

        results = generator.safe_tar_and_delete_from_dataframe(
            dataframe=dataframe,
            gzip=ns.gz,
            only_uuids=only,
            progress=ns.progress,
            checkpoint=ns.checkpoint,
            limit=ns.limit,
            path_prefix_overwrite=(env["UPLOAD_DIRECTORY"], env["UPLOAD_TEST"]),
            tmp_dir=Path(env["UPLOAD_TMP"]),
            cleanup_tmp_root=not ns.keep_tmp_root,
        )

        generator._write_results_csv(results, result_csv_path)
        for r in results:
            logger.info(
                f"system_upload_uuid={r['upload_uuid']}  tar_path={r['tar_path']}"
            )

        return 0


if __name__ == "__main__":
    raise SystemExit(main())
