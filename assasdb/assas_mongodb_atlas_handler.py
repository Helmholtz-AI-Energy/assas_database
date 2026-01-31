"""ASSAS MongoDB Atlas Handler using mongodump/mongorestore tools."""

import os
import logging
import subprocess
import tempfile
import shutil
import argparse

from typing import List, Optional

logger = logging.getLogger(__name__)


def push_local_to_atlas_using_tools(
    source_connection_string: str,
    target_connection_string: str,
    source_db_name: str = "assas",
    target_db_name: str = "assas",
    drop_existing: bool = False,
    collections: Optional[List[str]] = None,
) -> None:
    """Push data from local MongoDB to Atlas using mongodump/mongorestore tools.

    Args:
        source_connection_string (str): Connection string for the local MongoDB.
        target_connection_string (str): Connection string for the MongoDB Atlas.
        source_db_name (str): Name of the source database in local MongoDB.
        target_db_name (str): Name of the target database in MongoDB Atlas.
        drop_existing (bool):
            Whether to drop existing collections in the target database.
        collections (Optional[List[str]]):
            List of specific collections to migrate.
            If None, all collections are migrated.

    Returns:
        None

    """
    logger.info("Starting migration using mongodump/mongorestore...")
    logger.info(f"Source DB: {source_db_name}, Target DB: {target_db_name}")
    if collections:
        logger.info(f"Collections filter: {collections}")

    temp_dir = tempfile.mkdtemp(prefix="mongo_migration_")
    logger.info(f"Using temporary directory: {temp_dir}")

    try:
        # Step 1: Dump from local MongoDB
        logger.info("Step 1: Dumping from local MongoDB...")
        dump_cmd = [
            "mongodump",
            "--uri",
            source_connection_string,
            "--db",
            source_db_name,
            "--out",
            temp_dir,
            "--verbose",
        ]
        if collections:
            for c in collections:
                dump_cmd.extend(["--collection", c])

        dump_result = subprocess.run(dump_cmd, capture_output=True, text=True)

        if dump_result.returncode != 0:
            logger.error(f"mongodump failed: {dump_result.stderr}")
            raise RuntimeError(f"mongodump failed with code {dump_result.returncode}")

        logger.info("Successfully dumped local database")
        logger.info(dump_result.stdout)

        logger.info("Restoring to MongoDB Atlas...")

        cmd = [
            "mongorestore",
            "--uri",
            target_connection_string,
            "--db",
            target_db_name,
            "--dir",
            os.path.join(temp_dir, source_db_name),
            "--verbose",
        ]
        if drop_existing:
            cmd.append("--drop")

        logger.info("Running mongorestore with SRV URI...")
        with subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
        ) as proc:
            for line in proc.stdout:
                logger.info(line.rstrip())
            return_code = proc.wait()

        if return_code != 0:
            raise RuntimeError(f"mongorestore failed with code {return_code}")

        logger.info("\n" + "=" * 60)
        logger.info("Migration completed successfully!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise
    finally:
        logger.info(f"Cleaning up temporary directory: {temp_dir}")
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

    parser = argparse.ArgumentParser(
        description="Push local MongoDB to Atlas using mongodump/mongorestore."
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
            "Example: col1,col2",
        ),
    )
    args = parser.parse_args()

    collections = (
        [c.strip() for c in args.collections.split(",")] if args.collections else None
    )

    source_connection_string = os.getenv("CONNECTIONSTRING_SOURCE")
    target_connection_string = os.getenv("CONNECTIONSTRING_TARGET")

    push_local_to_atlas_using_tools(
        source_connection_string=source_connection_string,
        target_connection_string=target_connection_string,
        source_db_name=args.source_db,
        target_db_name=args.target_db,
        drop_existing=args.drop_existing,
        collections=collections,
    )
