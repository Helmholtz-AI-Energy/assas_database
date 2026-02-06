"""Cron job to process uploads and update archive sizes in the Assas Database."""

import os
import sys
import datetime
import logging

from pathlib import Path
from dotenv import load_dotenv


def _find_env_file() -> Path | None:
    """Find .env deterministically without importing assasdb."""
    explicit = os.getenv("ASSAS_ENV_FILE")
    if explicit:
        p = Path(explicit).expanduser().resolve()
        return p if p.exists() else None

    # Search upwards from this file
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        candidate = parent / ".env"
        if candidate.exists():
            return candidate.resolve()

    # Fallback: cwd
    candidate = (Path.cwd() / ".env").resolve()
    return candidate if candidate.exists() else None


# 1) Load .env BEFORE importing assasdb (critical)
env_path = _find_env_file()
loaded = load_dotenv(env_path, override=True) if env_path else False

# 2) Optional: set required vars manually if missing (also BEFORE importing assasdb)
#    (Use real values for your system.)
os.environ.setdefault("ASTEC_ROOT", "/root/astecV3.1.2/")
os.environ.setdefault("ASTEC_TYPE", "linux64")

# Now it is safe to import assasdb
from assasdb import (  # noqa: E402
    AssasDatabaseHandler,  # noqa: E402
    AssasDatabaseManager,  # noqa: E402
    require_env,  # noqa: E402
    redact_mongo_uri,  # noqa: E402
)  # noqa: E402


def setup_logging(level: int = logging.INFO) -> None:
    """Set up logging configuration."""
    logging.basicConfig(
        format="%(asctime)s %(process)d %(module)s %(levelname)s: %(message)s",
        level=level,
        stream=sys.stdout,
    )


def main() -> None:
    """Process uploads and update archive sizes in the Assas Database."""
    setup_logging(logging.INFO)
    logger = logging.getLogger("assas_app")

    global env_path, loaded

    start_time = datetime.datetime.now()
    logger.info(
        "Start assas_process_job at %s", start_time.isoformat(timespec="seconds")
    )
    logger.info(
        "Loaded dotenv=%s from %s", loaded, str(env_path) if env_path else "<none>"
    )
    logger.info("ASTEC_ROOT=%s", os.getenv("ASTEC_ROOT"))
    logger.info("ASTEC_TYPE=%s", os.getenv("ASTEC_TYPE"))
    logger.info(f"Loaded {loaded}.")

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
    logger.info("Using Mongo connection: %s", redact_mongo_uri(env["CONNECTIONSTRING"]))
    logger.info("Using upload directory: %s", env["UPLOAD_DIRECTORY"])

    database_manager = AssasDatabaseManager(
        database_handler=AssasDatabaseHandler(
            connection_string=env["CONNECTIONSTRING"],
            backup_directory=env["BACKUP_DIRECTORY"],
            database_name=env["MONGO_DB_NAME"],
        )
    )

    database_manager.process_uploads()
    database_manager.process_uploads_with_reload_flag()

    end_time = datetime.datetime.now()
    logger.info("Finished at %s", end_time.isoformat(timespec="seconds"))
    logger.info("Elapsed time: %.2f seconds.", (end_time - start_time).total_seconds())


if __name__ == "__main__":
    main()
