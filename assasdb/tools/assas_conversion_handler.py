#!/usr/bin/env python
"""ASSAS single converter script.

This script is used to convert ASTEC binary archives to HDF5 format.
"""

import os
import argparse
import logging
import time
import tarfile

from dirsync import sync
from shutil import copytree, copy2
from pathlib import Path
from datetime import datetime

from assasdb import AssasDatabaseManager, AssasOdessaNetCDF4Converter
from assasdb import AssasDatabaseHandler
from assasdb.assas_utils import get_duration

logger = logging.getLogger("assas_app")

LSDF_DATA_DIR = "ASSAS/upload_datahub"
LSDF_BACKUP_DIR = "ASSAS/backup_mongodb"


class AssasConversionHandler:
    """Handler for converting ASTEC binary archives to HDF5 format.

    This class manages the conversion process, including setting up logging,
    handling input and output paths, and notifying the status of the conversion.
    It uses the AssasOdessaNetCDF4Converter to perform the actual conversion
    from ASTEC binary archives to HDF5 format.
    """

    def __init__(
        self,
        upload_uuid: str,
        new: bool = False,
        rerun: bool = False,
        time: int = None,
        log_level: str = "WARNING",
        lsdf_data_dir: str = LSDF_DATA_DIR,
        lsdf_backup_dir: str = LSDF_BACKUP_DIR,
    ) -> None:
        """Initialize the AssasSingleConverter class.

        Args:
            upload_uuid (str): The UUID of the upload to be converted.
            new (bool): Flag to indicate if the output file should be overwritten.
            rerun (bool): Flag to indicate if the conversion should be rerun.
            time (int, optional): Number of time points to consider for conversion.
            log_level (str): Logging level to use.
            lsdf_data_dir (str): Directory for LSDF data.
            lsdf_backup_dir (str): Directory for LSDF backup.

        Returns:
            None

        """
        self.lsdf_data_dir = lsdf_data_dir
        self.lsdf_backup_dir = lsdf_backup_dir

        self.lsdf_project_dir = os.environ.get("LSDFPROJECTS")
        self.tmp_dir = os.environ.get("TMPDIR")

        self.database_handler = AssasDatabaseHandler(
            connection_string=os.environ.get("CONNECTIONSTRING"),
            backup_directory=os.environ.get("BACKUP_DIRECTORY"),
            database_name=os.environ.get("MONGO_DB_NAME"),
        )
        database_manager = AssasDatabaseManager(database_handler=self.database_handler)

        dataframe = database_manager.get_all_database_entries()
        assas_archive_meta = dataframe.loc[
            dataframe["system_upload_uuid"] == upload_uuid
        ]
        self.upload_uuid = assas_archive_meta["system_upload_uuid"].iloc[0]
        self.new = new
        self.time = time
        self.rerun = rerun

        self.input_path = Path(
            str(assas_archive_meta["system_path"].iloc[0]).replace(
                "/mnt", f"{self.lsdf_project_dir}"
            )
        )
        if not self.input_path.name.endswith((".tar", ".tar.gz", ".tgz")):
            self.input_path = self.input_path.with_name(self.input_path.name + ".tar")

        self.output_path = Path(
            str(assas_archive_meta["system_result"].iloc[0]).replace(
                "/mnt", f"{self.lsdf_project_dir}"
            )
        )

        self.name = assas_archive_meta["meta_name"].iloc[0]
        self.description = assas_archive_meta["meta_description"].iloc[0]

        self.tmp_path = Path.joinpath(Path(self.tmp_dir), Path(upload_uuid))
        self.tmp_output_path = Path.joinpath(self.tmp_path, "result/dataset.h5")

        log_level = getattr(logging, log_level.upper())

        self.setup_logging(
            custom_level=log_level,
        )
        self.log_config_info()

    def close_resources(self) -> None:
        """Close resources used by the handler."""
        self.database_handler.close()

    def log_config_info(self) -> None:
        """Log the configuration information."""
        logger.info(f"upload_uuid: {str(self.upload_uuid)}")
        logger.info(f"input_path: {str(self.input_path)}")
        logger.info(f"output_path: {str(self.output_path)}")
        logger.info(f"name: {str(self.name)}")
        logger.info(f"description: {str(self.description)}")

        logger.info(f"tmp_dir: {str(self.tmp_dir)}")
        logger.info(f"tmp_path: {str(self.tmp_path)}")

    def setup_logging(
        self,
        custom_level: int = logging.INFO,
    ) -> None:
        """Set up logging configuration.

        This function sets up the logging configuration for the application.
        It creates a log file with a timestamp in the log directory and configures
        the logging level and format.

        Args:
            custom_level (int): The logging level to set. Defaults to logging.INFO.

        Returns:
            None

        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        logfilename = f"{os.path.dirname(os.path.realpath(__file__))}"
        logfilename += f"/log/{timestamp}_assas_single_converter.log"

        os.makedirs(f"{os.path.dirname(os.path.realpath(__file__))}/log", exist_ok=True)
        # Configure logging
        logging.basicConfig(
            level=custom_level,
            format="%(asctime)s %(process)d %(module)s %(levelname)s: %(message)s",
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(
                    logfilename,
                    "w",
                ),
            ],
        )

        remote_logfilename = f"{self.output_path.parent}/"
        remote_logfilename += f"{timestamp}_assas_single_converter.log"

        logger.addHandler(logging.FileHandler(remote_logfilename, "w"))

    def handle_conversion(
        self,
    ) -> None:
        """Handle the conversion process from ASTEC binary archive to HDF5 format.

        This function orchestrates the conversion process by preparing the input
        and output paths, removing any existing temporary directories,
        copying the input files to a temporary directory, and then performing
        the conversion using the `AssasOdessaNetCDF4Converter`.
        It also handles the notification of conversion status and saves the
        converted result to the specified output path.

        Returns:
            None

        """
        logger.info("Start conversion from odessa to hdf5.")

        start_time = time.time()

        self.remove_tmp(tmp_path=self.tmp_path)

        # Ensure tmp exists
        Path(self.tmp_path).mkdir(parents=True, exist_ok=True)

        # Prepare tmp input:
        # - directory: copy to tmp (existing behavior)
        # - tar file: copy tar to tmp and extract streaming
        if Path(self.input_path).is_file() and str(self.input_path).endswith(
            (".tar", ".tar.gz", ".tgz")
        ):
            tar_path = Path(self.tmp_path) / Path(self.input_path).name
            logger.info("Copy tar archive to tmp: %s -> %s", self.input_path, tar_path)
            self.copy2_verbose(source=str(self.input_path), destination=str(tar_path))

            try:
                tmp_extract = Path(self.tmp_path)
                with tarfile.open(tar_path, "r:*") as tar:
                    idx = 0
                    for member in tar:
                        tar.extract(member, path=tmp_extract)
                        idx += 1
                        if idx % 1000 == 0:
                            logger.info(
                                f"Extraction progress: {idx} files extracted..."
                            )
                    logger.info("Extraction complete. Proceeding with conversion.")

            except Exception as e:
                raise RuntimeError(f"Extraction failed for {tar_path}: {e}") from e

            # derive extracted directory name from tar filename
            name = tar_path.name
            if name.endswith(".tar.gz"):
                base = name[: -len(".tar.gz")]
            elif name.endswith(".tgz"):
                base = name[: -len(".tgz")]
            elif name.endswith(".tar"):
                base = name[: -len(".tar")]
            else:
                base = tar_path.stem

            extracted_dir = Path(self.tmp_path) / base
            tmp_input_path = str(extracted_dir)
        else:
            tmp_input_path = self.copytree_verbose_to_tmp_with_process(
                input_path=self.input_path,
                tmp_path=self.tmp_path,
            )

        if not self.new and self.time is not None:
            logger.info(f"Only convert {self.time} time points.")
            logger.info(
                f"Output from lsdf will be copied to tmp {str(self.tmp_output_path)}."
            )

            if not self.tmp_output_path.parent.exists():
                self.tmp_output_path.parent.mkdir(parents=True, exist_ok=True)

            self.copy2_verbose(
                source=self.output_path,
                destination=self.tmp_output_path,
            )

        logger.info(f"tmp_input_path: {str(tmp_input_path)}")
        logger.info(f"tmp_output_path: {str(self.tmp_output_path)}")

        try:
            odessa_converter = AssasOdessaNetCDF4Converter(
                input_path=tmp_input_path,
                output_path=self.tmp_output_path,
            )

            number_of_samples = len(odessa_converter.get_time_points())

            if self.new:
                self.notify_conversion_start(
                    upload_uuid=self.upload_uuid,
                    upload_directory=f"{self.lsdf_project_dir}/{LSDF_DATA_DIR}",
                )

                AssasOdessaNetCDF4Converter.set_general_meta_data(
                    output_path=self.tmp_output_path,
                    archive_name=self.name,
                    archive_description=self.description,
                )

                # Initialization process
                logger.info("Initializing groups in NetCDF4.")
                odessa_converter.initialize_groups_in_netcdf4()

                logger.info("Initializing astec variables with group assignment.")
                odessa_converter.initialize_astec_variables_in_netcdf4()

                logger.info("Creating metadata variables in groups.")
                odessa_converter.create_metadata_variables_in_groups()

                logger.info("Converting metadata from Odessa.")
                odessa_converter.populate_metadata_variables_in_domain_groups()

            else:
                logger.info(f"Using existing output file {str(self.tmp_output_path)}.")

                if self.rerun:
                    logger.info(
                        "Rerun conversion, resetting completed index in netcdf4 file.\n"
                        "Conversion method only converts new ASTEC variables."
                    )
                    AssasOdessaNetCDF4Converter.reset_completed_index_in_netcdf4_file(
                        netcdf4_file=self.tmp_output_path,
                    )
                    self.notify_rerun_conversion(
                        upload_uuid=self.upload_uuid,
                        upload_directory=f"{self.lsdf_project_dir}/{LSDF_DATA_DIR}",
                    )

            # Actual conversion process
            logger.info("Populating data from groups to NetCDF4.")
            odessa_converter.populate_data_from_groups_to_netcdf4(
                maximum_index=self.time
            )

            self.save_hdf5_result(
                local_output_path=self.tmp_output_path,
                lsdf_output_path=self.output_path,
            )

        except Exception as exception:
            logger.error(f"Exception during conversion occured: {exception}.")

            self.notify_invalid_conversion(
                upload_uuid=self.upload_uuid,
                upload_directory=f"{self.lsdf_project_dir}/{LSDF_DATA_DIR}",
            )

        else:
            finished = False
            logger.info("Conversion process to netcf4 finished.")

            if self.time is not None:
                if self.time == number_of_samples:
                    finished = True
                    logger.info(f"Converted all {self.time} time points.")

                else:
                    logger.info(
                        f"Converted only {self.time} time points, "
                        f"but {number_of_samples} are available."
                    )

            else:
                finished = True
                logger.info(f"Converted all {number_of_samples} time points.")

            if finished:
                logger.info("Conversion of all time points finished successfully.")
                self.notify_valid_conversion(
                    upload_uuid=self.upload_uuid,
                    upload_directory=f"{self.lsdf_project_dir}/{LSDF_DATA_DIR}",
                )

            else:
                logger.info(
                    "Conversion did not finish completely, time points are missing."
                )

            end_time = time.time()
            duration_in_seconds = end_time - start_time
            duration_string = get_duration(duration_in_seconds)
            logger.info(f"Conversion from odessa to hdf5 took {duration_string}.")

    def copytree_verbose_to_tmp_with_process(
        self, input_path: str, tmp_path: str
    ) -> str:
        """Copy the input directory to the temporary directory.

        This function copies the entire directory structure from the input path
        to the temporary path, logging the progress of the copy operation.
        It uses the `copy2` function to ensure that file metadata is preserved.

        Args:
            input_path (str): The path to the input directory.
            tmp_path (str): The path to the temporary directory.

        Returns:
            str: The path to the copied directory in the temporary location.

        """
        input_path = Path(input_path)
        tmp_path = Path(tmp_path)

        if not input_path.exists():
            raise FileNotFoundError(f"Input path {input_path} does not exist.")

        if not tmp_path.exists():
            tmp_path.mkdir(parents=True, exist_ok=True)

        # Calculate the total number of files to copy
        total_files = sum(len(files) for _, _, files in os.walk(input_path))
        copied_files = 0

        logger.info(f"Starting copy process from {input_path} to {tmp_path}.")
        logger.info(f"Total files: {total_files}.")

        try:
            for root, dirs, files in os.walk(input_path):
                relative_root = Path(root).relative_to(input_path)
                destination_root = tmp_path / relative_root

                if not destination_root.exists():
                    destination_root.mkdir(parents=True, exist_ok=True)

                for file in files:
                    source_file = Path(root) / file
                    destination_file = destination_root / file

                    copy2(source_file, destination_file)
                    copied_files += 1

                    # Calculate and log progress
                    progress = (copied_files / total_files) * 100
                    logger.info(
                        f"Copied {source_file} to {destination_file} "
                        f"(Progress: {progress:.2f}% complete)."
                    )

            logger.info("Copy process completed successfully.")
            return str(tmp_path)

        except Exception as e:
            logger.error(f"Error during copy process: {e}")
            raise

    def copy2_verbose(
        self,
        source: str,
        destination: str,
    ) -> None:
        """Copy a file from source to destination with verbose logging.

        Args:
            source (str): The path to the source file.
            destination (str): The path to the destination file.

        Returns:
            None

        """
        logger.debug(f"Copy file {source} to {destination}.")
        copy2(source, destination)
        logger.debug(f"Copyied file {source} to {destination}.")

    def sync_imput_and_tmp(self, input_path: str, tmp_path: str) -> None:
        """Synchronize the input directory with the temporary directory.

        Args:
            input_path (str): The path to the input directory.
            tmp_path (str): The path to the temporary directory.

        Returns:
            None

        """
        logger.info(f"Sync input directory {input_path} with tmp directory {tmp_path}.")

        sync(input_path, tmp_path, verbose=True)

    def remove_tmp(self, tmp_path: str) -> None:
        """Remove the temporary directory if it exists.

        Args:
            tmp_path (str): The path to the temporary directory.

        Returns:
            None

        """
        logger.info(f"Remove tmp directory {tmp_path} if it exists.")

        remove_string = f"rm -rf {tmp_path}"

        if os.path.exists(tmp_path):
            logger.info(f"Remove tmp directory {tmp_path}.")
            os.system(remove_string)

    def copytree_verbose_to_tmp(self, input_path: str, tmp_path: str) -> str:
        """Copy the input directory to the temporary directory with verbose logging.

        Args:
            input_path (str): The path to the input directory.
            tmp_path (str): The path to the temporary directory.

        Returns:
            str: The path to the copied directory in the temporary location.

        """
        logger.info(f"Copy input directory {input_path} to tmp directory {tmp_path}.")
        try:
            destination_path = copytree(
                dst=input_path, src=tmp_path, copy_function=self.copy2_verbose
            )

            return destination_path

        except Exception as exception:
            logger.error(f"Error when copy input to tmp: {exception}.")

    def save_hdf5_result(self, local_output_path: str, lsdf_output_path: str) -> None:
        """Save the HDF5 result file from the local output path to the LSDF output path.

        Args:
            local_output_path (str): The path to the local output file.
            lsdf_output_path (str): The path to the LSDF output file.

        Returns:
            None

        """
        try:
            logger.info(
                f"Copy hdf5 result file from {local_output_path} to {lsdf_output_path}."
            )
            self.copy2_verbose(source=local_output_path, destination=lsdf_output_path)

        except Exception as exception:
            logger.error(f"Error when copy result from tmp to lsdf: {exception}.")

    def backup_hdf5_result(
        self,
    ) -> str:
        """Backup the previous HDF5 file to the LSDF backup directory with a timestamp.

        Args:
            lsdf_output_path (str): The path to the LSDF output file.
            lsdf_backup_dir (str): The directory where the backup should be stored.

        Returns:
            str: The path to the backup file.

        """
        try:
            if not Path(self.output_path).exists():
                logger.warning(f"No result file found at {self.output_path} to backup.")
                return ""

            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            backup_dir = self.output_path.parent / "backup"
            backup_file = (
                Path(backup_dir)
                / f"{timestamp}_{self.output_path.stem}{self.output_path.suffix}"
            )

            # Ensure backup directory exists
            Path(backup_dir).mkdir(parents=True, exist_ok=True)

            copy2(self.output_path, backup_file)
            logger.info(f"Backed up {self.output_path} to {backup_file}")
            return str(backup_file)

        except Exception as e:
            logger.error(f"Error backing up result file: {e}")
            return ""

    def notify_valid_conversion(
        self,
        upload_uuid: str,
        upload_directory: str,
    ) -> None:
        """Notify that the conversion has finished valid by creating a touch file.

        This file can be used to track the conversion process.
        The file will be created in the upload directory with the name
        <upload_uuid>_valid.

        Args:
            upload_uuid (str): The UUID of the upload.
            upload_directory (str): The directory where the upload is stored.

        Returns:
            None

        """
        touch_string = f"touch {upload_directory}/{upload_uuid}/{upload_uuid}_valid"
        logger.info(f"Execute command {touch_string}.")

        os.system(touch_string)

    def notify_conversion_start(
        self,
        upload_uuid: str,
        upload_directory: str,
    ) -> None:
        """Notify that the conversion has started by creating a touch file.

        This file can be used to track the conversion process.
        The file will be created in the upload directory with the name
        <upload_uuid>_converting.

        Args:
            upload_uuid (str): The UUID of the upload.
            upload_directory (str): The directory where the upload is stored.

        Returns:
            None

        """
        touch_string = f"touch {upload_directory}/{upload_uuid}/"
        touch_string += f"{upload_uuid}_converting"
        logger.info(f"Execute command {touch_string}.")

        os.system(touch_string)

    def remove_valid_file(self, upload_uuid: str, upload_directory: str) -> None:
        """Remove the file that indicates a valid conversion (<upload_uuid>_valid).

        Args:
            upload_uuid (str): The UUID of the upload.
            upload_directory (str): The directory where the upload is stored.

        Returns:
            None

        """
        valid_file = f"{upload_directory}/{upload_uuid}/{upload_uuid}_valid"
        if os.path.exists(valid_file):
            logger.info(f"Removing valid file: {valid_file}")
            os.remove(valid_file)
        else:
            logger.info(f"No valid file found to remove: {valid_file}")

    def notify_rerun_conversion(
        self,
        upload_uuid: str,
        upload_directory: str,
    ) -> None:
        """Notify that the conversion is being rerun by creating a touch file.

        This file can be used to track the conversion process.
        The file will be created in the upload directory with the name
        <upload_uuid>_converting.

        Args:
            upload_uuid (str): The UUID of the upload.
            upload_directory (str): The directory where the upload is stored.

        Returns:
            None

        """
        touch_string = f"touch {upload_directory}/{upload_uuid}/"
        touch_string += f"{upload_uuid}_rerun"
        logger.info(f"Execute command {touch_string}.")

        os.system(touch_string)

    def notify_invalid_conversion(
        self,
        upload_uuid: str,
        upload_directory: str,
    ) -> None:
        """Notify that the conversion has failed by creating a touch file.

        This file can be used to track the conversion process.
        The file will be created in the upload directory with the name.

        Args:
            upload_uuid (str): The UUID of the upload.
            upload_directory (str): The directory where the upload is stored.

        Returns:
            None

        """
        touch_string = f"touch {upload_directory}/{upload_uuid}/{upload_uuid}_invalid"
        logger.info(f"Execute command {touch_string}.")

        os.system(touch_string)


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        "-uuid",
        "--upload_uuid",
        type=str,
        help="upload uuid of ASTEC binary archive",
        required=False,
    )
    argparser.add_argument(
        "-t",
        "--time",
        type=int,
        help="number of timepoints which will be considered for conversion",
        required=False,
    )
    argparser.add_argument(
        "--log-level",
        type=str,
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: WARNING)",
    )
    argparser.add_argument(
        "-n",
        "--new",
        help="overwrite existing output file",
        required=False,
        action="store_true",
    )
    argparser.add_argument(
        "-r",
        "--rerun",
        help="rerun conversion even if output file exists",
        required=False,
        action="store_true",
    )
    args = argparser.parse_args()

    conversion_handler = AssasConversionHandler(
        upload_uuid=args.upload_uuid,
        new=args.new,
        time=args.time,
        log_level=args.log_level,
        lsdf_data_dir=LSDF_DATA_DIR,
        lsdf_backup_dir=LSDF_BACKUP_DIR,
    )

    conversion_handler.backup_hdf5_result()
    conversion_handler.handle_conversion()
    conversion_handler.close_resources()
