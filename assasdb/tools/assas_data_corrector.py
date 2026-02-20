"""Correct and update NetCDF4 files based on ASTEC Odessa archives."""

import os
import sys
import logging
import netCDF4 as nc
import pandas as pd
import numpy as np

from pathlib import Path

from assasdb import (
    AssasOdessaNetCDF4Converter,
    AssasDatabaseHandler,
    AssasDatabaseManager,
)

# Set up logging to file and console
log_file_path = Path(__file__).parent / "assas_data_corrector.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(log_file_path), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

LSDF_DATA_DIR = "ASSAS/upload_datahub"
LSDF_BACKUP_DIR = "ASSAS/backup_mongodb"

ASTEC_ROOT = os.environ.get("ASTEC_ROOT")
ASTEC_TYPE = os.environ.get("ASTEC_TYPE")

ASTEC_PYTHON_ODESSA = os.path.join(
    ASTEC_ROOT, "odessa", "bin", ASTEC_TYPE + "-release", "wrap_python"
)

if ASTEC_PYTHON_ODESSA not in sys.path:
    logger.info(f"Append path to odessa to environment: {ASTEC_PYTHON_ODESSA}")
    sys.path.append(ASTEC_PYTHON_ODESSA)

import pyodessa as pyod  # noqa: E402


def get_all_valid_astec_archives(
    dataframe: pd.DataFrame, lsdf_project_dir: str
) -> list[Path]:
    """Get all valid ASTEC archives from the database dataframe."""
    valid_archives = []
    for index, row in dataframe.iterrows():
        archive_path = Path(
            str(row["system_path"]).replace("/mnt", f"{lsdf_project_dir}")
        )
        if row["system_status"] == "Valid":  # Adjust extension as needed
            valid_archives.append(archive_path)
    return valid_archives


def overwrite_netcdf_data_point(
    nc_file_path: str,
    nc_group: str,
    nc_sub_group: str,
    variable_name: str,
    index: int,
    value: float,
) -> None:
    """Overwrite a single data point in a NetCDF4 variable.

    Args:
        nc_file_path (str or Path): Path to the NetCDF4 file.
        nc_group (str): Name of the group containing the variable.
        nc_sub_group (str): Name of the sub-group containing the variable.
        variable_name (str): Name of the variable to modify.
        index (int or tuple): Index (or indices) of the data point to overwrite.
        value: New value to write at the specified index.

    Returns:
        None

    """
    with nc.Dataset(str(nc_file_path), mode="a") as ds:
        logger.info(f"Opened NetCDF file: {nc_file_path}")
        if nc_group not in ds.groups:
            logger.error(f"Group '{nc_group}' not found in the NetCDF file.")
            raise ValueError(f"Group '{nc_group}' not found in the NetCDF file.")

        nc_group_obj = ds.groups[nc_group]
        if nc_sub_group not in nc_group_obj.groups:
            logger.error(f"Sub-group '{nc_sub_group}' not found in group '{nc_group}'.")
            raise ValueError(
                f"Sub-group '{nc_sub_group}' not found in group '{nc_group}'."
            )

        nc_sub_group_obj = nc_group_obj.groups[nc_sub_group]
        if variable_name not in nc_sub_group_obj.variables:
            logger.error(
                f"Variable '{variable_name}' not found in "
                f"group '{nc_group}/{nc_sub_group}'."
            )
            raise ValueError(
                f"Variable '{variable_name}' not found in "
                f"group '{nc_group}/{nc_sub_group}'."
            )

        var = nc_sub_group_obj.variables[variable_name]
        logger.info(
            f"Overwriting {variable_name}[{index}] with {value} in {nc_file_path}."
        )
        logger.debug(f"Shape {var.shape} and type {var.dtype}.")

        if (var.shape[0] - 1) == index:
            logger.info(
                f"Variable '{variable_name}' has correct shape. {str(var[index])}"
            )
            var[index] = value
        else:
            logger.warning(f"Variable '{variable_name}' has incorrect shape.")

    logger.info(f"Overwritten {variable_name}[{index}] with {value} in {nc_file_path}.")


def add_missing_netcdf_variable(
    nc_file_path: str,
    nc_group: str,
    nc_sub_group: str,
    variable_name: str,
    shape: tuple,
    dtype: str,
) -> None:
    """Add a new variable to a NetCDF4 file.

    Args:
        nc_file_path (str or Path): Path to the NetCDF4 file.
        nc_group (str): Name of the group to add the variable to.
        nc_sub_group (str): Name of the sub-group to add the variable to.
        variable_name (str): Name of the variable to add.
        shape (tuple): Shape of the new variable.
        dtype (str): Data type of the new variable.

    Returns:
        None

    """
    with nc.Dataset(str(nc_file_path), mode="a") as ds:
        logger.info(f"Opened NetCDF file: {nc_file_path}")
        if nc_group not in ds.groups:
            logger.error(f"Group '{nc_group}' not found in the NetCDF file.")
            raise ValueError(f"Group '{nc_group}' not found in the NetCDF file.")

        nc_group_obj = ds.groups[nc_group]
        if nc_sub_group not in nc_group_obj.groups:
            logger.error(f"Sub-group '{nc_sub_group}' not found in group '{nc_group}'.")
            raise ValueError(
                f"Sub-group '{nc_sub_group}' not found in group '{nc_group}'."
            )

        nc_sub_group_obj = nc_group_obj.groups[nc_sub_group]
        if variable_name in nc_sub_group_obj.variables:
            return

        var = nc_sub_group_obj.createVariable(
            varname=variable_name, datatype=dtype, dimensions=("time",)
        )
        array_data = np.full(shape, np.nan, dtype=dtype)
        var[:] = array_data

        logger.info(
            f"Created variable '{variable_name}' with shape {shape} "
            f" and type {dtype} in {nc_file_path}."
        )


def extract_tfp_trup_from_archive(saving_path: Path) -> dict | None:
    """Extract TFP and TRUP from an ASTEC Odessa archive."""
    try:
        saving_path_str = str(saving_path)
        all_times = pyod.get_saving_times(saving_path_str)
        if not all_times:
            logger.warning(f"No saving times found for {saving_path_str}")
            return None

        last_time = all_times[-1]
        last_index = len(all_times) - 1

        logger.info(
            f"Processing {saving_path_str} at time {last_time}, {len(all_times)}."
        )
        base = pyod.restore(saving_path_str, last_time)

        path_trup = "SEQUENCE 1: TRUP 1"
        path_tfp = "SEQUENCE 1: TFP 1"

        if not AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(base, path_trup):
            logger.warning(f"TRUP path does not exist: {path_trup}")
            return None
        if not AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(base, path_tfp):
            logger.warning(f"TFP path does not exist: {path_tfp}")
            return None

        logger.info(f"Restored base at {last_time}.")
        tfp = base.get(path_tfp)
        logger.info(f"Extracted TFP: {str(tfp)}")
        trup = base.get(path_trup)
        logger.info(f"Extracted TRUP: {str(trup)}")

        return {
            "path": saving_path,
            "TFP": float(tfp),
            "TRUP": float(trup),
            "length": len(all_times),
            "last_index": last_index,
        }

    except Exception as e:
        logger.error(f"Error processing {saving_path}: {e}")
        return None


if __name__ == "__main__":
    lsdf_data_dir = LSDF_DATA_DIR
    lsdf_backup_dir = LSDF_BACKUP_DIR

    lsdf_project_dir = os.environ.get("LSDFPROJECTS")
    tmp_dir = os.environ.get("TMPDIR")

    database_handler = AssasDatabaseHandler(
        client=None,
        backup_directory=f"{lsdf_project_dir}/{LSDF_BACKUP_DIR}",
    )
    database_manager = AssasDatabaseManager(
        database_handler=database_handler,
    )

    dataframe = database_manager.get_all_database_entries_from_backup()
    dataframe["system_path"] = dataframe["system_path"].apply(
        lambda p: str(p).replace("/mnt", f"{lsdf_project_dir}")
    )
    dataframe["system_result"] = dataframe["system_result"].apply(
        lambda p: str(p).replace("/mnt", f"{lsdf_project_dir}")
    )

    valid_archives = dataframe[dataframe["system_status"] == "Valid"]

    results = []
    for index, archive_row in valid_archives.iterrows():
        archive_path = archive_row["system_path"]
        result_path = archive_row["system_result"]
        result = extract_tfp_trup_from_archive(archive_path)
        if result is None:
            continue

        add_missing_netcdf_variable(
            nc_file_path=result_path,
            nc_group="other",
            nc_sub_group="global",
            variable_name="vessel_release_time",
            shape=(result["length"],),
            dtype=np.float32,
        )
        overwrite_netcdf_data_point(
            nc_file_path=result_path,
            nc_group="other",
            nc_sub_group="global",
            variable_name="vessel_release_time",
            index=result["last_index"],
            value=result["TFP"],
        )
        overwrite_netcdf_data_point(
            nc_file_path=result_path,
            nc_group="other",
            nc_sub_group="global",
            variable_name="vessel_rupture_time",
            index=result["last_index"],
            value=result["TRUP"],
        )

        if result:
            results.append(result)

    df = pd.DataFrame(results)
    file_path = Path(__file__).parent / "correction_results.csv"
    df.to_csv(file_path, index=False)
    logger.info(f"Results saved to {file_path}")
