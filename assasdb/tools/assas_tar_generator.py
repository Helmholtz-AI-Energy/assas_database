"""Tool to create tar (or tar.gz) archives for directories.

Execution is driven from DB dataframe or manually.
Usage examples:
1) From DB, with progress logging and gzip:
    python -m assasdb.tools.assas_tar_generator --from-db --progress --gz
2) From DB, only for specific UUIDs, with gzip:
    python -m assasdb.tools.assas_tar_generator --from-db --gz \
        --uuid 123e4567-e89b-12d3-a456-426
"""

import os
import shutil
import subprocess
import argparse
import csv
import hashlib
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

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

        cmd = ["tar", "-C", str(parent), "-cf", str(out_path), name]

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
            # Stream stderr live so progress shows up during long runs
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
                    logger.info("%s", line)
                    stderr_lines.append(line)
                    # cap memory on extremely noisy output
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

            created += 1  # <-- add

        return results


def main() -> int:
    """Execute main logic of script.

    Parses arguments and creates tar(s) accordingly.

    Returns:
        Exit code (0 for success, non-zero for errors)

    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s"
    )

    ap = argparse.ArgumentParser(
        description=(
            "Create tar (or tar.gz) archives for directories. "
            "Can be driven from DB dataframe."
        )
    )

    ap.add_argument(
        "--from-db",
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
            "When used with --from-db: only create tars for these upload_uuid(s). "
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
        "target_dir",
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

    ns = ap.parse_args()

    generator = BasicTarGenerator()

    target_dir = generator._resolve_target_dir(ns.target_dir)
    result_csv_path = (
        Path(ns.result_csv) if ns.result_csv else (target_dir / "tar_results.csv")
    )

    if ns.from_db:
        env_path = find_env_file()
        env = require_env(
            env_path=env_path,
            logger=logger,
            keys=[
                "CONNECTIONSTRING",
                "BACKUP_DIRECTORY",
                "MONGO_DB_NAME",
                "UPLOAD_DIRECTORY",
            ],
        )

        logger.info("Using database: %s", env["MONGO_DB_NAME"])
        logger.info("Using backup directory: %s", env["BACKUP_DIRECTORY"])
        logger.info(
            "Using Mongo connection: %s", redact_mongo_uri(env["CONNECTIONSTRING"])
        )
        logger.info("Using upload directory: %s", env["UPLOAD_DIRECTORY"])
        logger.info("Using gzip: %s", ns.gz)
        logger.info("Using UUID filter: %s", ns.uuid if ns.uuid else "<none>")
        logger.info("Writing results CSV: %s", result_csv_path)

        database_manager = AssasDatabaseManager(
            database_handler=AssasDatabaseHandler(
                connection_string=env["CONNECTIONSTRING"],
                backup_directory=env["BACKUP_DIRECTORY"],
                database_name=env["MONGO_DB_NAME"],
            )
        )
        dataframe = database_manager.get_all_database_entries()

        only = set(ns.uuid) if ns.uuid else None
        results = generator.create_tars_from_dataframe(
            dataframe=dataframe,
            target_dir=target_dir,
            gzip=ns.gz,
            only_uuids=only,
            progress=ns.progress,
            checkpoint=ns.checkpoint,
            limit=ns.limit,
        )

        generator._write_results_csv(results, result_csv_path)

        for r in results:
            logger.info("%s  md5=%s", r["tar_path"], r["md5"])
        return 0

    if not ns.source_dir:
        raise TarGeneratorError(
            "Manual mode requires source_dir. Either pass source_dir or use --from-db."
        )

    out = generator.create(
        TarJob(
            source_dir=Path(ns.source_dir),
            target_dir=target_dir,
            archive_name=ns.name,
            gzip=ns.gz,
            progress=ns.progress,
            checkpoint=ns.checkpoint,
        )
    )
    md5 = generator._md5_file(out)
    generator._write_results_csv(
        [
            {
                "upload_uuid": "",
                "system_path": str(Path(ns.source_dir)),
                "tar_path": str(out),
                "md5": md5,
            }
        ],
        result_csv_path,
    )
    logger.info("%s  md5=%s", str(out), md5)
    logger.info("Results CSV: %s", result_csv_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
