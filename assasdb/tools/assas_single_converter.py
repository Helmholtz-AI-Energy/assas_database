#!/usr/bin/env python
"""ASSAS single converter script.

This script is used to convert ASTEC binary archives to HDF5 format.
"""

import os
import argparse
import logging
import time

from dirsync import sync
from shutil import copytree, copy2
from pathlib import Path
from datetime import datetime

from assasdb import AssasDatabaseManager, AssasOdessaNetCDF4Converter
from assasdb.assas_utils import get_duration

logger = logging.getLogger("assas_app")

LSDF_DATA_DIR = "ASSAS/upload_test"
LSDF_BACKUP_DIR = "ASSAS/backup_mongodb"


def copy2_verbose(
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


def sync_imput_and_tmp(input_path: str, tmp_path: str) -> None:
    """Synchronize the input directory with the temporary directory.

    Args:
        input_path (str): The path to the input directory.
        tmp_path (str): The path to the temporary directory.

    Returns:
        None

    """
    logger.info(f"Sync input directory {input_path} with tmp directory {tmp_path}.")

    sync(input_path, tmp_path, verbose=True)


def remove_tmp(tmp_path: str) -> None:
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


def copytree_verbose_to_tmp(input_path: str, tmp_path: str) -> str:
    """Copy the input directory to the temporary directory with verbose logging.

    Args:
        input_path (str): The path to the input directory.
        tmp_path (str): The path to the temporary directory.

    Returns:
        str: The path to the copied directory in the temporary location.

    """
    logger.info(f"Copy input directory {input_path} to tmp directory {tmp_path}.")
    try:
        destination_path = copytree(input_path, tmp_path, copy_function=copy2_verbose)

        return destination_path

    except Exception as exception:
        logger.error(f"Error when copy input to tmp: {exception}.")


def save_hdf5_result(local_output_path: str, lsdf_output_path: str) -> None:
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
        copy2_verbose(local_output_path, lsdf_output_path)

    except Exception as exception:
        logger.error(f"Error when copy result from tmp to lsdf: {exception}.")


def notify_valid_conversion(
    upload_uuid: str,
    upload_directory: str,
) -> None:
    """Notify that the conversion has finished successfully by creating a touch file.

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
    touch_string = f"touch {upload_directory}/{upload_uuid}/{upload_uuid}_converting"
    logger.info(f"Execute command {touch_string}.")

    os.system(touch_string)


def notify_invalid_conversion(
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
        "-d",
        "--debug",
        help="enable debug logging for conversion",
        required=False,
        action="store_true",
    )
    argparser.add_argument(
        "-n",
        "--new",
        help="overwrite existing output file",
        required=False,
        action="store_true",
    )
    args = argparser.parse_args()

    if args.debug:
        custom_level = logging.DEBUG
    else:
        custom_level = logging.INFO

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    start_time = time.time()
    logfilename = f"{os.path.dirname(os.path.realpath(__file__))}"
    logfilename += f"/logs/{timestamp}_assas_single_converter.log"

    os.makedirs(f"{os.path.dirname(os.path.realpath(__file__))}/logs", exist_ok=True)
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
    lsdf_project_dir = os.environ.get("LSDFPROJECTS")
    tmp_dir = os.environ.get("TMPDIR")

    database_manager = AssasDatabaseManager(
        backup_directory=f"{lsdf_project_dir}/{LSDF_BACKUP_DIR}"
    )
    dataframe = database_manager.get_all_database_entries_from_backup()
    assas_archive_meta = dataframe.loc[
        dataframe["system_upload_uuid"] == args.upload_uuid
    ]
    upload_uuid = assas_archive_meta["system_upload_uuid"].iloc[0]

    input_path = Path(
        str(assas_archive_meta["system_path"].iloc[0]).replace(
            "/mnt", f"{lsdf_project_dir}"
        )
    )
    output_path = Path(
        str(assas_archive_meta["system_result"].iloc[0]).replace(
            "/mnt", f"{lsdf_project_dir}"
        )
    )

    name = assas_archive_meta["meta_name"].iloc[0]
    description = assas_archive_meta["meta_description"].iloc[0]

    tmp_path = Path.joinpath(Path(tmp_dir), Path(upload_uuid))

    logger.addHandler(
        logging.FileHandler(
            f"{str(output_path.parent)}/{timestamp}_assas_single_converter.log", "w"
        )
    )

    logger.info(f"upload_uuid: {str(upload_uuid)}")
    logger.info(f"input_path: {str(input_path)}")
    logger.info(f"output_path: {str(output_path)}")
    logger.info(f"name: {str(name)}")
    logger.info(f"description: {str(description)}")

    logger.info(f"tmp_dir: {str(tmp_dir)}")
    logger.info(f"tmp_path: {str(tmp_path)}")

    remove_tmp(tmp_path=tmp_path)

    tmp_input_path = copytree_verbose_to_tmp(
        input_path=input_path,
        tmp_path=tmp_path,
    )
    tmp_output_path = Path.joinpath(tmp_path, "result/dataset.h5")

    if not args.new and args.time is not None:
        logger.info(f"Only convert {args.time} time points.")
        logger.info(f"Output from lsdf will be copied to tmp {str(tmp_output_path)}.")

        copy2_verbose(
            input_path=output_path,
            tmp_path=tmp_output_path,
        )

    logger.info(f"tmp_input_path: {str(tmp_input_path)}")
    logger.info(f"tmp_output_path: {str(tmp_output_path)}")

    try:
        odessa_converter = AssasOdessaNetCDF4Converter(
            input_path=tmp_input_path,
            output_path=tmp_output_path,
        )

        number_of_samples = len(odessa_converter.get_time_points())

        if args.new:
            notify_conversion_start(
                upload_uuid=upload_uuid,
                upload_directory=f"{lsdf_project_dir}/{LSDF_DATA_DIR}",
            )

            AssasOdessaNetCDF4Converter.set_general_meta_data(
                output_path=tmp_output_path,
                archive_name=name,
                archive_description=description,
            )

        else:
            logger.info(f"Using existing output file {str(tmp_output_path)}.")

        odessa_converter.convert_astec_variables_to_netcdf4(maximum_index=args.time)

        save_hdf5_result(
            local_output_path=tmp_output_path, lsdf_output_path=output_path
        )

    except Exception as exception:
        logger.error(f"Exception during conversion occured: {exception}")

        notify_invalid_conversion(
            upload_uuid=upload_uuid,
            upload_directory=f"{lsdf_project_dir}/{LSDF_DATA_DIR}",
        )

    else:
        logger.info("Conversion from odessa to hdf5 was successful.")

        finished = False

        if args.time is not None:
            if args.time == number_of_samples:
                finished = True
                logger.info(f"Converted all {args.time} time points.")

            else:
                finished = False
                logger.info(
                    f"""Converted only {args.time} time points,
                        but {number_of_samples} are available."""
                )

        else:
            finished = True
            logger.info(f"Converted all {number_of_samples} time points.")

        if finished:
            logger.info("Conversion finished successfully.")
            notify_valid_conversion(
                upload_uuid=upload_uuid,
                upload_directory=f"{lsdf_project_dir}/{LSDF_DATA_DIR}",
            )

        else:
            logger.info("Conversion did not finish completely, timepoints are missing.")

        end_time = time.time()
        duration_in_seconds = end_time - start_time
        duration_string = get_duration(duration_in_seconds)
        logger.info(f"Conversion from odessa to hdf5 took {duration_string}.")
