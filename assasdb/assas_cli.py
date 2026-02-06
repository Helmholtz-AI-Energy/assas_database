"""ASSAS Database Management Command Line Interface (CLI)."""

import os
import logging
import argparse

from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

from assasdb.assas_logging import AssasLoggingConfig, get_logger
from assasdb.assas_database_manager import AssasDatabaseManager
from assasdb.assas_database_handler import AssasDatabaseHandler

logger = None


def setup_logging_cli(log_level: int = logging.INFO) -> None:
    """Create the logger for the CLI tool."""
    global logger

    log_config = AssasLoggingConfig(
        log_dir=Path("logs"),
        log_level=log_level,
        max_bytes=10 * 1024 * 1024,  # 10 MB
        backup_count=5,
        use_timed_rotation=True,  # Use daily rotation
        when="midnight",  # Rotate at midnight
    )

    # Setup root logger
    log_config.setup_root_logger()

    # Setup specific loggers for components
    log_config.setup_logger("assas_app", "assas_app.log")
    log_config.setup_logger("assas_database_manager", "assas_database_manager.log")
    log_config.setup_logger("assas_database_handler", "assas_database_handler.log")
    log_config.setup_logger("assas_converter", "assas_converter.log")

    logger = get_logger("assas_cli")
    logger.info("Logging configured with rolling file handlers")


def load_env() -> None:
    """Load .env from package root if available."""
    try:
        base_dir = Path(__file__).resolve().parent.parent
        env_path = base_dir / ".env"
        load_dotenv(env_path, override=False)
        logger.info(f"Loaded .env from {env_path}")
    except Exception as e:
        logger.warning(f"Could not load .env: {e}")


def create_database_handler_from_env() -> AssasDatabaseHandler:
    """Create DB handler from environment variables."""
    connection_string = os.getenv("CONNECTIONSTRING", "")
    backup_directory = os.getenv("BACKUP_DIRECTORY", "")
    database_name = os.getenv("MONGO_DB_NAME", "")

    return AssasDatabaseManager.create_database_handler(
        connection_string=connection_string,
        backup_directory=backup_directory,
        database_name=database_name,
    )


def build_arg_aprser() -> argparse.ArgumentParser:
    """Build argument parser for command line execution."""
    parser = argparse.ArgumentParser(
        description="ASSAS Database Management CLI",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    parser.add_argument(
        "--collect-number-of-samples",
        action="store_true",
        help="Collect number of samples",
    )
    parser.add_argument(
        "--max-documents", type=int, default=None, help="Limit number of documents"
    )
    parser.add_argument(
        "--show-stats", action="store_true", help="Show database statistics"
    )
    parser.add_argument(
        "--list-users", action="store_true", help="List all users in the database"
    )
    parser.add_argument(
        "--link-users-files",
        action="store_true",
        help="Link users to files in the database",
    )
    parser.add_argument(
        "--backup-internal-database",
        action="store_true",
        help="Backup the internal database",
    )
    parser.add_argument(
        "--restore-internal-database",
        action="store_true",
        help="Restore the internal database",
    )
    parser.add_argument(
        "--reset-sizes-in-progress",
        action="store_true",
        help="Reset archive sizes in the database",
    )
    parser.add_argument(
        "--run-hdf5-recalc",
        action="store_true",
        help="Recalculate HDF5 sizes in the database",
    )

    return parser


if __name__ == "__main__":
    # load_env()

    parser = build_arg_aprser()
    args = parser.parse_args()

    setup_logging_cli(getattr(logging, args.log_level.upper(), logging.INFO))
    logger = get_logger("assas_cli")

    start_time = datetime.now()
    logger.info(f"Starting ASSAS CLI execution at {start_time}.")

    try:
        database_manager = AssasDatabaseManager(
            database_handler=create_database_handler_from_env(),
            upload_directory=os.getenv("UPLOAD_DIRECTORY", ""),
        )

        did_action = False

        if args.collect_number_of_samples:
            database_manager.collect_number_of_samples(max_documents=args.max_documents)
            did_action = True

        if args.show_stats:
            entries = database_manager.get_all_database_entries()
            logger.info(f"Database contains {len(entries)} entries.")
            did_action = True

        if args.list_users:
            database_manager.list_users()
            did_action = True

        if args.link_users_files:
            database_manager.link_files_to_batch_user()
            did_action = True

        if args.backup_internal_database:
            database_manager.backup_internal_database(verbose=True)
            did_action = True

        if args.restore_internal_database:
            database_manager.restore_internal_database(drop=True, verbose=True)
            did_action = True

        if args.reset_sizes_in_progress:
            database_manager.reset_in_progress_archive_sizes()
            did_action = True

        if args.run_hdf5_recalc:
            database_manager.recalc_hdf5_sizes_fast(
                dry_run=True, workers=16, max_documents=200
            )
            did_action = True

        if not did_action:
            parser.print_help()

    except Exception as e:
        logger.error(f"Error during execution: {e}", exc_info=True)
        raise

    finally:
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        logger.info(f"Execution completed at {end_time}.")
        logger.info(
            f"Total execution time: {elapsed_time.total_seconds():.2f} seconds."
        )
