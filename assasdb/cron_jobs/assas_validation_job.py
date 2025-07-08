"""Cron job to update status of archives in the Assas Database.

This script is designed to be run as a cron job to periodically process uploads and
update the status of archives in the Assas Database.
"""

import os
import sys
import datetime
import logging

os.environ["ASTEC_ROOT"] = "/root/astecV3.1.2"
os.environ["ASTEC_TYPE"] = "linux_64"

from assasdb import AssasDatabaseManager, AssasDatabaseHandler


def setup_logging(
    level: int = logging.INFO,
) -> None:
    """Set up logging configuration."""
    logging.basicConfig(
        format="%(asctime)s %(process)d %(module)s %(levelname)s: %(message)s",
        level=level,
        stream=sys.stdout,
    )


def main() -> None:
    """Run the Assas Database Manager methods."""
    setup_logging(logging.INFO)
    logger = logging.getLogger("assas_app")

    start_time = datetime.datetime.now()
    logger.info(f"Start update of archive sizes as cron job at {start_time}.")

    database_manager = AssasDatabaseManager(
        database_handler=AssasDatabaseHandler(
            database_name="assas",
        )
    )

    database_manager.update_archive_sizes(number_of_archives=10)
    database_manager.update_meta_data_of_valid_archives()
    database_manager.collect_number_of_samples_of_uploaded_archives()
    database_manager.collect_maximum_index_value_from_valid_archives()
    database_manager.update_status_of_archives()

    end_time = datetime.datetime.now()
    logger.info(f"Finished update of archives sizes at {end_time}.")

    elapsed_time = end_time - start_time
    logger.info(f"Elapsed time: {elapsed_time.total_seconds()} seconds.")


if __name__ == "__main__":
    main()
