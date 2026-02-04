"""ASSAS MongoDB Atlas Handler using mongodump/mongorestore tools."""

import os
import logging
import subprocess
import tempfile
import shutil
import argparse

from datetime import datetime
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


class AssasMongodbBackupHandler:
    """Handler for MongoDB backups using mongodump and mongorestore.

    All environment-variable reading and core initialization is done in __init__.
    """

    def __init__(
        self,
        connection_string: str | None = None,
        db_name: str | None = None,
        backup_directory: str | None = None,
        source_connection_string: str | None = None,
        target_connection_string: str | None = None,
    ) -> None:
        """Initialize the backup handler with configuration.

        Args:
            connection_string: MongoDB connection string for backup/restore.
            db_name: Name of the database to back up/restore.
            backup_directory: Directory to store backups.
            source_connection_string: Source connection string for migration.
            target_connection_string: Target connection string for migration.

        Returns:
            None

        """
        # Core backup/restore config (from env unless overridden)
        self.connection_string: str | None = connection_string or os.getenv(
            "CONNECTIONSTRING"
        )
        self.db_name: str | None = db_name or os.getenv("MONGO_DB_NAME")
        self.backup_directory: str | None = backup_directory or os.getenv(
            "BACKUP_DIRECTORY"
        )

        # Optional migration config (from env unless overridden)
        self.source_connection_string: str | None = (
            source_connection_string or os.getenv("CONNECTIONSTRING_SOURCE")
        )
        self.target_connection_string: str | None = (
            target_connection_string or os.getenv("CONNECTIONSTRING_TARGET")
        )

    def require_backup_config(self) -> tuple[str, str, str]:
        """Validate that backup/restore config is present and return it."""
        if not self.backup_directory:
            raise RuntimeError(
                "BACKUP_DIRECTORY is not set (and no backup_directory provided)."
            )
        if not self.connection_string:
            raise RuntimeError(
                "CONNECTIONSTRING is not set (and no connection_string provided)."
            )
        if not self.db_name:
            raise RuntimeError("MONGO_DB_NAME is not set (and no db_name provided).")
        return self.connection_string, self.db_name, self.backup_directory

    def run_tool(self, cmd: List[str], *, verbose: bool) -> None:
        """Run a MongoDB database tools command safely (avoid logging credentials)."""
        try:
            result = subprocess.run(
                cmd,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except FileNotFoundError as e:
            tool = cmd[0] if cmd else "command"
            raise RuntimeError(
                f"{tool} not found. Install MongoDB Database Tools in this environment."
            ) from e
        except subprocess.CalledProcessError as e:
            msg = (e.stderr or e.stdout or str(e)).strip()
            raise RuntimeError(f"{cmd[0]} failed: {msg}") from e

        if verbose:
            if result.stdout:
                logger.info("%s stdout:\n%s", cmd[0], result.stdout.strip())
            if result.stderr:
                logger.info("%s stderr:\n%s", cmd[0], result.stderr.strip())

    def mongodump(
        self,
        *,
        uri: str,
        db_name: str,
        out_dir: str | Path,
        collections: Optional[List[str]] = None,
        verbose: bool = False,
    ) -> None:
        """Run mongodump with the specified parameters.

        Args:
            uri: MongoDB connection string.
            db_name: Name of the database to dump.
            out_dir: Output directory for the dump.
            collections: Optional list of collection names to dump. If None, dumps all.
            verbose: If True, enables verbose output from mongodump.

        Raises:
            RuntimeError: If mongodump fails.

        """
        cmd: List[str] = [
            "mongodump",
            f"--uri={uri}",
            f"--db={db_name}",
            f"--out={str(out_dir)}",
        ]
        if verbose:
            cmd.append("--verbose")
        if collections:
            for c in collections:
                cmd.append(f"--collection={c}")
        self.run_tool(cmd, verbose=verbose)

    def mongorestore(
        self,
        *,
        uri: str,
        db_name: str,
        dump_db_dir: str | Path,
        drop: bool = False,
        verbose: bool = False,
        ns_includes: Optional[List[str]] = None,
    ) -> None:
        """Run mongorestore with the specified parameters.

        Args:
            uri: MongoDB connection string.
            db_name: Name of the database to restore.
            dump_db_dir: Directory containing the database dump.
            drop: If True, drop collections before restoring.
            verbose: If True, enables verbose output from mongorestore.
            ns_includes: Optional list of namespace filters to include.

        Raises:
            RuntimeError: If mongorestore fails.

        """
        cmd: List[str] = [
            "mongorestore",
            f"--uri={uri}",
            f"--db={db_name}",
            f"--dir={str(dump_db_dir)}",
        ]
        if drop:
            cmd.append("--drop")
        if verbose:
            cmd.append("--verbose")
        if ns_includes:
            for ns in ns_includes:
                cmd.append(f"--nsInclude={ns}")
        self.run_tool(cmd, verbose=verbose)

    def backup_with_mongodump(
        self,
        output_base_dir: str | None = None,
        collections: Optional[List[str]] = None,
        verbose: bool = False,
    ) -> Path:
        """Create a MongoDB backup using mongodump.

        Args:
            output_base_dir: Optional override for the base output directory.
            collections: Optional list of collection names to dump. If None, dumps all.
            verbose: If True, enables verbose output from mongodump.

        Returns:
            Path to the created backup directory.

        """
        uri, db_name, default_base_dir = self.require_backup_config()
        base_dir = output_base_dir or default_base_dir

        normalized_collections: Optional[List[str]] = None
        if collections is not None:
            normalized_collections = [c.strip() for c in collections if str(c).strip()]
            if len(normalized_collections) == 0:
                normalized_collections = None  # treat empty list as "dump all"

        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        out_dir = Path(base_dir) / ts
        out_dir.mkdir(parents=True, exist_ok=True)

        if normalized_collections:
            logger.info(
                "Running mongodump for db='%s' (collections=%s) into '%s'",
                db_name,
                normalized_collections,
                out_dir,
            )
        else:
            logger.info("Running mongodump for db='%s' into '%s'", db_name, out_dir)

        self.mongodump(
            uri=uri,
            db_name=db_name,
            out_dir=out_dir,
            collections=normalized_collections,
            verbose=verbose,
        )
        return out_dir

    def restore_with_mongorestore(
        self,
        backup_dir: str | Path,
        collections: Optional[List[str]] = None,
        drop: bool = False,
        verbose: bool = False,
    ) -> Path:
        """Restore MongoDB data from a mongodump directory using mongorestore."""
        uri, db_name, _ = self.require_backup_config()

        backup_dir = Path(backup_dir)
        db_dump_dir = backup_dir / db_name
        if not db_dump_dir.exists():
            raise FileNotFoundError(
                f"Backup does not contain expected db folder '{db_name}': {db_dump_dir}"
            )

        normalized_collections: Optional[List[str]] = None
        if collections is not None:
            normalized_collections = [c.strip() for c in collections if str(c).strip()]
            if len(normalized_collections) == 0:
                normalized_collections = None

        ns_includes: Optional[List[str]] = None
        if normalized_collections:
            ns_includes = [f"{db_name}.{c}" for c in normalized_collections]

        logger.info(
            "Running mongorestore for db='%s' from '%s'%s",
            db_name,
            backup_dir,
            " (drop enabled)" if drop else "",
        )

        self.mongorestore(
            uri=uri,
            db_name=db_name,
            dump_db_dir=db_dump_dir,
            drop=drop,
            verbose=verbose,
            ns_includes=ns_includes,
        )
        return backup_dir

    def restore_latest_backup(
        self,
        output_base_dir: str | None = None,
        collections: Optional[List[str]] = None,
        drop: bool = False,
        verbose: bool = False,
    ) -> Path:
        """Restore from the latest backup directory."""
        _, _, default_base_dir = self.require_backup_config()
        base_dir = output_base_dir or default_base_dir

        latest = self.latest_backup_dir(base_dir)
        return self.restore_with_mongorestore(
            backup_dir=latest,
            collections=collections,
            drop=drop,
            verbose=verbose,
        )

    def latest_backup_dir(self, base_dir: str | Path) -> Path:
        """Get the latest backup directory under the given base directory."""
        base = Path(base_dir)
        if not base.exists():
            raise FileNotFoundError(f"Backup base directory does not exist: {base}")

        candidates = [p for p in base.iterdir() if p.is_dir()]
        if not candidates:
            raise FileNotFoundError(f"No backup directories found in: {base}")

        candidates.sort(key=lambda p: p.name)
        return candidates[-1]

    def push_source_to_target_using_tools(
        self,
        source_connection_string: str | None = None,
        target_connection_string: str | None = None,
        source_db_name: str = "assas",
        target_db_name: str = "assas",
        drop_existing: bool = False,
        collections: Optional[List[str]] = None,
        verbose: bool = False,
    ) -> None:
        """Push data from local MongoDB to Atlas using mongodump/mongorestore tools.

        Refactored to use shared base functions (_mongodump/_mongorestore).
        Note: collection filtering is applied at dump-time
            (most reliable when DB names differ).
        """
        src = source_connection_string or self.source_connection_string
        tgt = target_connection_string or self.target_connection_string
        if not src:
            raise RuntimeError(
                "CONNECTIONSTRING_SOURCE not set "
                "(and no source_connection_string provided)."
            )
        if not tgt:
            raise RuntimeError(
                "CONNECTIONSTRING_TARGET not set "
                "(and no target_connection_string provided)."
            )

        normalized_collections: Optional[List[str]] = None
        if collections is not None:
            normalized_collections = [c.strip() for c in collections if str(c).strip()]
            if len(normalized_collections) == 0:
                normalized_collections = None

        logger.info("Starting migration using mongodump/mongorestore...")
        logger.info("Source DB: %s, Target DB: %s", source_db_name, target_db_name)
        if normalized_collections:
            logger.info("Collections filter: %s", normalized_collections)

        temp_dir = Path(tempfile.mkdtemp(prefix="mongo_migration_"))
        logger.info("Using temporary directory: %s", temp_dir)

        try:
            # Dump into temp_dir/<source_db_name>/...
            self.mongodump(
                uri=src,
                db_name=source_db_name,
                out_dir=temp_dir,
                collections=normalized_collections,
                verbose=verbose,
            )

            # Restore from temp_dir/<source_db_name> into target_db_name
            source_db_dump_dir = temp_dir / source_db_name
            if not source_db_dump_dir.exists():
                raise FileNotFoundError(
                    f"Expected dump dir not found after mongodump: {source_db_dump_dir}"
                )

            self.mongorestore(
                uri=tgt,
                db_name=target_db_name,
                dump_db_dir=source_db_dump_dir,
                drop=drop_existing,
                verbose=verbose,
                ns_includes=None,  # collection filtering already done at dump-time
            )

            logger.info("Migration completed successfully.")

        finally:
            logger.info("Cleaning up temporary directory: %s", temp_dir)
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

    parser = argparse.ArgumentParser(
        description="Push source MongoDB to target using mongodump/mongorestore."
    )
    parser.add_argument(
        "--source-db", default="assas", help="Local source database name"
    )
    parser.add_argument(
        "--target-db", default="assas", help="Atlas target database name"
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop existing target collections before restore",
    )
    parser.add_argument(
        "--collections",
        help=(
            "Comma-separated list of collections to migrate (optional). "
            "Example: col1,col2"
        ),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output from mongodump/mongorestore",
    )
    args = parser.parse_args()

    collections = (
        [c.strip() for c in args.collections.split(",")] if args.collections else None
    )

    handler = AssasMongodbBackupHandler()
    handler.push_source_to_target_using_tools(
        source_db_name=args.source_db,
        target_db_name=args.target_db,
        drop_existing=args.drop_existing,
        collections=collections,
        verbose=args.verbose,
    )
