#!/usr/bin/env python
"""Convert ASTEC binary archive to netCDF4 format using pyodessa.

This module provides the `AssasOdessaNetCDF4Converter` class, which reads an ASTEC
binary archive and converts it to a netCDF4 dataset. It supports various ASTEC
variables and their corresponding parsing strategies. The class can handle
different ASTEC variable types, including primary and secondary pipes, volumes,
junctions, walls, vessel faces, meshes, and more. It also includes methods for
reading ASTEC variable indices and specific vessel properties like magma debris,
fuel, and clad IDs.
"""

import sys
import os
import netCDF4
import logging
import numpy as np
import pandas as pd
import json
import shutil

from importlib.resources import as_file, files
from tqdm import tqdm
from typing import List, Union, Optional
from pathlib import Path
from .assas_netcdf4_meta_config import META_DATA_VAR_NAMES, DOMAIN_GROUP_CONFIG
from .assas_unit_manager import AssasUnitManager
import time

logger = logging.getLogger("assas_app")

LOG_INTERVAL = 100
ASTEC_ROOT = os.environ.get("ASTEC_ROOT")
ASTEC_TYPE = os.environ.get("ASTEC_TYPE")

ASTEC_PYTHON_ODESSA = os.path.join(
    ASTEC_ROOT, "odessa", "bin", ASTEC_TYPE + "-release", "wrap_python"
)

if ASTEC_PYTHON_ODESSA not in sys.path:
    logger.info(f"Append path to odessa to environment: {ASTEC_PYTHON_ODESSA}")
    sys.path.append(ASTEC_PYTHON_ODESSA)

import pyodessa as pyod  # noqa: E402
from .assas_odessa_step_extraction import assas_odessa_step_extraction


class AssasOdessaNetCDF4Converter:
    """Class to convert ASTEC binary archive to netCDF4 format.

    This class reads an ASTEC binary archive and converts it to a netCDF4 dataset.
    """

    def __init__(
        self,
        input_path: Union[str, Path],
        output_path: Union[str, Path],
        variable_index_file_list: Optional[List[str]] = None,
    ) -> None:
        """Initialize AssasOdessaNetCDF4Converter class.

        Args:
            input_path (Union[str, Path]): Path to the ASTEC binary archive.
            output_path (Union[str, Path]): Path to the output netCDF4 file.
            variable_index_file_list (Optional[List[str]]):
                List of ASTEC variable index files. If not provided,
                a default list will be used.

        Returns:
            None

        """
        self.unit_manager = AssasUnitManager()
        logger.info("Initialized AssasunitManager")

        self.input_path = Path(input_path)
        logger.info(f"Input path of ASTEC binary archive is {str(self.input_path)}.")

        self.output_path = Path(output_path)
        logger.info(f"Output path of hdf5 file is {str(self.output_path)}.")

        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        self.time_points = pyod.get_saving_times(str(self.input_path))[1:]

        B = pyod.restore(str(self.input_path),self.time_points[0])
        self.with_cesar_io = B.len("CESAR_IO") > 0
        del(B)

        logger.info(f"Read {len(self.time_points)} time points from ASTEC archive.")
        logger.debug(f"List of time points: {self.time_points}.")

        self.variable_index_file_list = variable_index_file_list or [
            "astec_config/inr/assas_variables_containment_connection.csv",
            "astec_config/inr/assas_variables_containment_dome_pool.csv",
            "astec_config/inr/assas_variables_containment_wall.csv",
            "astec_config/inr/assas_variables_containment_zone.csv",
            "astec_config/inr/assas_variables_vessel.csv",
            "astec_config/inr/assas_variables_vessel_face_ther.csv",
            "astec_config/inr/assas_variables_vessel_general.csv",
            "astec_config/inr/assas_variables_vessel_mesh.csv",
            "astec_config/inr/assas_variables_primary_junction_ther.csv",
            "astec_config/inr/assas_variables_primary_volume_ther.csv",
            "astec_config/inr/assas_variables_primary_wall.csv",
            "astec_config/inr/assas_variables_primary_wall_ther.csv",
            "astec_config/inr/assas_variables_secondar_junction_ther.csv",
            "astec_config/inr/assas_variables_secondar_volume_ther.csv",
            "astec_config/inr/assas_variables_secondar_wall.csv",
            "astec_config/inr/assas_variables_secondar_wall_ther.csv",
            "astec_config/inr/assas_variables_connection.csv",
            "astec_config/inr/assas_variables_sequence.csv",
            "astec_config/inr/assas_variables_private_assas_param.csv",
            "astec_config/inr/assas_variables_cesar_io.csv",
            "astec_config/inr/assas_variables_sensor.csv",
        ]

        self.variable_index = self.read_astec_variable_index_files(report=True)
        self.connection_break_ids = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_connection_break_ids.csv"
        )
        self.connection_feedwater_ids = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_connection_feedwater_ids.csv"
        )
        self.connection_flow_ids = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_connection_flow_ids.csv"
        )
        self.connection_heat_ids = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_connection_heat_ids.csv"
        )
        self.connection_mcci_ids = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_connection_mcci_ids.csv"
        )
        self.connection_source_ids = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_connection_source_ids.csv"
        )
        self.connection_vespour_ids = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_connection_vespour_ids.csv"
        )
        self.magma_debris_ids = self.read_vessel_magma_debris_ids(
            resource_file="astec_config/inr/assas_variables_vessel_magma_debris_ids.csv"
        )
        self.fuel_ids = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_vessel_fuel_ids.csv"
        )
        self.clad_ids = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_vessel_clad_ids.csv"
        )
        self.bono_ids = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_vessel_bono_ids.csv"
        )
        self.crod_ids = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_vessel_crod_ids.csv"
        )
        self.clad_crod_ids = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_vessel_clad_crod_ids.csv"
        )
        self.tguide_ids = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_vessel_tguide_ids.csv"
        )
        self.tinst_ids = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_vessel_tinst_ids.csv"
        )
        self.grid_ids = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_vessel_grid_ids.csv"
        )
        self.LP_wall_ids = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_vessel_LP_wall_ids.csv"
        )
        self.LP_corium_ids = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_vessel_LP_corium_ids.csv"
        )
        self.component_states = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_component_states.csv"
        )

        self.variable_strategy_mapping = {
            "primary_pipe_geom": (
                AssasOdessaNetCDF4Converter.parse_variable_from_primary_pipe_geom
            ),
            "primary_volume_ther": (
                AssasOdessaNetCDF4Converter.parse_variable_from_primary_volume_ther
            ),
            "primary_volume_geom": (
                AssasOdessaNetCDF4Converter.parse_variable_from_primary_volume_geom
            ),
            "primary_junction_ther": (
                AssasOdessaNetCDF4Converter.parse_variable_from_primary_junction_ther
            ),
            "primary_junction_geom": (
                AssasOdessaNetCDF4Converter.parse_variable_from_primary_junction_geom
            ),
            "primary_wall": (
                AssasOdessaNetCDF4Converter.parse_variable_from_primary_wall
            ),
            "primary_wall_ther": (
                AssasOdessaNetCDF4Converter.parse_variable_from_primary_wall_ther
            ),
            "primary_wall_ther_2": (
                AssasOdessaNetCDF4Converter.parse_variable_from_primary_wall_ther_2
            ),
            "primary_wall_geom": (
                AssasOdessaNetCDF4Converter.parse_variable_from_primary_wall_geom
            ),
            "secondar_pipe_geom": (
                AssasOdessaNetCDF4Converter.parse_variable_from_primary_pipe_geom
            ),
            "secondar_volume_ther": (
                AssasOdessaNetCDF4Converter.parse_variable_from_secondar_volume_ther
            ),
            "secondar_junction_ther": (
                AssasOdessaNetCDF4Converter.parse_variable_from_secondar_junction_ther
            ),
            "secondar_junction_geom": (
                AssasOdessaNetCDF4Converter.parse_variable_from_secondar_junction_geom
            ),
            "secondar_wall": (
                AssasOdessaNetCDF4Converter.parse_variable_from_secondar_wall
            ),
            "secondar_wall_ther": (
                AssasOdessaNetCDF4Converter.parse_variable_from_secondar_wall_ther
            ),
            "secondar_wall_ther_2": (
                AssasOdessaNetCDF4Converter.parse_variable_from_secondar_wall_ther_2
            ),
            "secondar_wall_geom": (
                AssasOdessaNetCDF4Converter.parse_variable_from_secondar_wall_geom
            ),
            "vessel_face_ther": (
                AssasOdessaNetCDF4Converter.parse_variable_from_vessel_face_ther
            ),
            "vessel_mesh_ther": (
                AssasOdessaNetCDF4Converter.parse_variable_from_vessel_mesh_ther
            ),
            "vessel_mesh": (
                AssasOdessaNetCDF4Converter.parse_variable_from_vessel_mesh
            ),
            "vessel_general": (
                AssasOdessaNetCDF4Converter.parse_variable_from_vessel_general
            ),
            "fp_heat_vessel": (
                AssasOdessaNetCDF4Converter.parse_variable_from_fp_heat_vessel
            ),
            "systems_pump": (
                AssasOdessaNetCDF4Converter.parse_variable_from_systems_pump
            ),
            "systems_pump_momentum": (
                AssasOdessaNetCDF4Converter.parse_variable_from_systems_pump_momentum
            ),
            "systems_valve": (
                AssasOdessaNetCDF4Converter.parse_variable_from_systems_valve
            ),
            "sensor": (AssasOdessaNetCDF4Converter.parse_variable_from_sensor),
            "sensors": (AssasOdessaNetCDF4Converter.parse_variable_from_sensors),
            "containment_dome": (
                AssasOdessaNetCDF4Converter.parse_variable_from_containment_dome
            ),
            "containment_zone": (
                AssasOdessaNetCDF4Converter.parse_variable_from_containment_zone
            ),
            "containment_zone_ther": (
                AssasOdessaNetCDF4Converter.parse_variable_from_containment_zone_ther
            ),
            "containment_connection": (
                AssasOdessaNetCDF4Converter.parse_variable_from_containment_connection
            ),
            "containment_wall_temp": (
                AssasOdessaNetCDF4Converter.parse_variable_from_containment_wall_temp
            ),
            "containment_pool": (
                AssasOdessaNetCDF4Converter.parse_variable_from_containment_pool
            ),
            "connection": (AssasOdessaNetCDF4Converter.parse_variable_from_connection),
            "connection_break": (self.parse_variable_from_connection_break),
            "connection_feedwater": (self.parse_variable_from_connection_feedwater),
            "connection_flow": (self.parse_variable_from_connection_flow),
            "connection_flow_fp": (self.parse_variable_from_connection_flow_fp),
            "connection_heat": (self.parse_variable_from_connection_heat),
            "connection_mcci": (self.parse_variable_from_connection_mcci),
            "connection_source": (self.parse_variable_from_connection_source),
            "connection_vespour": (self.parse_variable_from_connection_vespour),
            "vessel_magma_debris": self.parse_variable_vessel_magma_debris,
            "vessel_clad": self.parse_variable_vessel_clad,
            "vessel_fuel": self.parse_variable_vessel_fuel,
            "vessel_bono": self.parse_variable_vessel_bono,
            "vessel_crod": self.parse_variable_vessel_crod,
            "vessel_clad_crod": self.parse_variable_vessel_clad_crod,
            "vessel_tguide": self.parse_variable_vessel_tguide,
            "vessel_tinst": self.parse_variable_vessel_tinst,
            "vessel_grid": self.parse_variable_vessel_grid,
            "vessel_LP_wall": self.parse_variable_vessel_LP_wall,
            "vessel_LP_corium": self.parse_variable_vessel_LP_corium,
            "vessel_clad_stat": self.parse_variable_vessel_clad_stat,
            "vessel_fuel_stat": self.parse_variable_vessel_fuel_stat,
            "vessel_trup": AssasOdessaNetCDF4Converter.parse_variable_vessel_trup,
            "private_assas_param": (
                AssasOdessaNetCDF4Converter.parse_variable_private_assas_param
            ),
            "cesar_io": (AssasOdessaNetCDF4Converter.parse_variable_cesar_io),
            "cesar_io_output": (
                AssasOdessaNetCDF4Converter.parse_variable_cesar_io_output
            ),
        }

    def get_time_points(self) -> List[int]:
        """Get the time points from the ASTEC archive.

        Returns:
            List[int]: A list of time points in seconds.

        """
        return self.time_points

    def get_odessa_base_from_index(self, index: int) -> pyod.Base:
        """Get the odessa base from the ASTEC archive at a specific time point.

        Args:
            index (int): The index of the time point to retrieve the odessa base for.

        Returns:
            pyod.lib.od_base: The odessa base object at the specified time point.

        """
        time_point = self.time_points[index]
        return pyod.restore(self.input_path, time_point)

    def get_variable_index(self) -> pd.DataFrame:
        """Get the variable index containing information about ASTEC variables.

        Returns:
            pd.DataFrame: A dataframe containing the variable index.

        """
        return self.variable_index

    def read_astec_variable_index_files(
        self,
        report: bool = False,
        tex_report: bool = False,
    ) -> pd.DataFrame:
        """Read the ASTEC variable index files.

        This method reads multiple CSV files containing ASTEC variable information
        and concatenates them into a single dataframe. The resulting dataframe
        contains the variable names, IDs, and other relevant information.

        Args:
            report (bool): If True, save the dataframe to a CSV file for
            reporting purposes.
            tex_report (bool): If True, save the head of the dataframe to a LaTeX file.

        Returns:
            pd.DataFrame: A dataframe containing the ASTEC variable index.

        """
        file_list = self.variable_index_file_list

        dataframe_list = []
        for file in file_list:
            with as_file(files(__package__).joinpath(file)) as csv_file_path:
                with open(csv_file_path, "r") as csv_file:
                    logger.info(f"Read variable index file: {csv_file_path}")
                    dataframe = pd.read_csv(csv_file)
                    dataframe_list.append(dataframe)

        dataframe = pd.concat(dataframe_list).reset_index(drop=True)
        logger.info(f"Shape of variable index is {dataframe.shape}.")

        if report:
            output_file = (
                os.path.dirname(os.path.realpath(__file__))
                + "/astec_config/assas_variables_wp2_report.csv"
            )
            dataframe.to_csv(output_file)
            logger.info("Saved variable index to file: ", output_file)

            output_file_repo = os.path.join(
                os.getcwd(), "assas_variables_wp2_report.csv"
            )
            dataframe.to_csv(output_file_repo)
            logger.info("Saved variable index to file (repo): ", output_file_repo)

        if tex_report:
            output_file_latex = (
                os.path.dirname(os.path.realpath(__file__))
                + "/astec_config/assas_variables_wp2_report_head.tex"
            )
            indices = list(range(0, 5)) + list(range(104, 110))
            dataframe_head = dataframe[
                [
                    "name",
                    "long_name",
                    "name_odessa",
                    "unit",
                    "domain",
                    "strategy",
                    "dimension",
                ]
            ].iloc[indices]
            with open(output_file_latex, "w") as f:
                logger.info(
                    f"Write head of dataframe to LaTeX file {f.name} "
                    "for reporting purposes."
                )
                dataframe_head.to_latex(
                    buf=f,
                    index=False,
                    escape=False,
                    caption="Selected Variables",
                    label="tab:variables",
                )

        return dataframe

    def read_vessel_magma_debris_ids(
        self,
        resource_file: str,
    ) -> pd.DataFrame:
        """Read names of the ASTEC variables into a dataframe.

        Args:
            resource_file: str
            Path to the resource file containing the ASTEC variable names and mesh IDs.

        Returns:
            pd.DataFrame
            DataFrame containing the ASTEC variable names and their corresponding
            mesh IDs.

        """
        dataframe = self.read_csv_resource_file(resource_file=resource_file)
        dataframe.replace("nan", np.nan)

        return dataframe

    def read_csv_resource_file(
        self,
        resource_file: str,
    ) -> pd.DataFrame:
        """Read a CSV resource file and return its content as a pandas DataFrame.

        Args:
            resource_file (str): Path to the CSV resource file.

        Returns:
            pd.DataFrame: DataFrame containing the data from the CSV file.

        """
        with as_file(files(__package__).joinpath(resource_file)) as csv_file_path:
            with open(csv_file_path, "r") as csv_file:
                logger.info(f"Read csv resource file {csv_file_path}.")
                dataframe = pd.read_csv(csv_file)

        logger.debug(f"{dataframe}")

        return dataframe

    @staticmethod
    def check_if_odessa_path_exists(
        odessa_base: pyod.Base,
        odessa_path: str,
    ) -> bool:
        """Check if a given Odessa path exists in the odessa base.

        Args:
            odessa_base: The odessa base object.
            odessa_path (str): The path to check in the odessa base.

        Returns:
            bool: True if the path exists, False otherwise.

        """
        keys = odessa_path.split(":")
        nkeys = len(keys)
        is_valid_path = True

        logger.debug(f"Keys of odessa_path: {keys}. Depth of path: {nkeys}.")
        #Other implementation could be
        #try:
            #odessa_base.get(odessa_path)
        #except ValueError:
            #is_valid_path = False

        for count, var in enumerate(keys, start=1):
            logger.debug("   ------")
            var = var.strip()
            logger.debug(f"Handle key {var}.")
            num_stru = 1

            if " " in var:
                name_stru = var.split(" ")[0]
                num_stru = var.split(" ")[1]

            elif "[" in var:
                name_stru = var.split("[")[0]

            else:
                name_stru = var

            if count == 1:  # Using initiale base argument
                len_odessa_base = odessa_base.len(name_stru.replace("'", ""))

                if len_odessa_base >= int(num_stru):
                    if count < nkeys:  # getting next structure
                        new_base = odessa_base.get(name_stru + " " + num_stru)
                else:
                    is_valid_path = False
                    break

            else:  # Using substructure
                if type(new_base) == pyod.cls_rg.Rg:
                    len_odessa_base = int(new_base.has_key(name_stru.replace("'", "")))
                elif type(new_base) == pyod.cls_r1.R1:
                    len_odessa_base = len(new_base)
                else:
                    len_odessa_base = new_base.len(name_stru.replace("'", ""))

                if len_odessa_base >= int(num_stru):
                    if count < nkeys:  # getting next structure
                        new_base = new_base.get(name_stru + " " + str(num_stru))
                else:
                    is_valid_path = False
                    break

        return is_valid_path

    @staticmethod
    def convert_odessa_structure_to_float(
        odessa_structure: Union[pyod.R1, float],
    ) -> float:
        """Convert an Odessa structure to a float value.

        Args:
            odessa_structure: The odessa structure to convert.

        Returns:
            float: The converted float value.

        """
        value = []

        if isinstance(odessa_structure, pyod.R1):
            value = odessa_structure[0]
        elif isinstance(odessa_structure, float):
            value = odessa_structure
        else:
            logger.error("Unkown type.")

        return value

    def parse_variable_vessel_magma_debris(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from magma debris.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type vessel_magma_debris.")

        array = np.full((len(self.magma_debris_ids.index)), fill_value=np.nan)
        logger.debug(f"Initialized array with shape {array.shape}.")

        for _, dataframe_row in self.magma_debris_ids.iterrows():
            mesh_id = dataframe_row["mesh_id"]
            variable_id = dataframe_row[variable_name]

            logger.debug(f"Handle mesh_id {mesh_id} and variable_id {variable_id}.")

            if not np.isnan(variable_id):
                odessa_path = f"VESSEL 1: COMP {int(variable_id)}: M 1"

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)

                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[int(mesh_id) - 1] = variable_structure

        return array

    def parse_variable_vessel_fuel(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from vessel fuel.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type vessel_fuel.")

        array = np.full((len(self.fuel_ids.index)), fill_value=np.nan)
        logger.debug(f"Initialized array with shape {array.shape}.")

        for idx, dataframe_row in self.fuel_ids.iterrows():
            comp_id = dataframe_row["fuel_id"]

            logger.debug(f"Handle comp_id {comp_id}.")

            odessa_path = f"VESSEL 1: COMP {int(comp_id)}: {variable_name}"

            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                odessa_base, odessa_path
            ):
                variable_structure = odessa_base.get(odessa_path)

                logger.debug(f"Collect variable structure {variable_structure}.")
                array[idx] = variable_structure

        return array

    def parse_variable_vessel_clad(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from vessel clad.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type vessel_clad.")

        array = np.full((len(self.clad_ids.index)), fill_value=np.nan)
        logger.debug(f"Initialized array with shape {array.shape}.")

        for idx, dataframe_row in self.clad_ids.iterrows():
            comp_id = dataframe_row["clad_id"]

            logger.debug(f"Handle comp_id {comp_id}.")

            odessa_path = f"VESSEL 1: COMP {int(comp_id)}: {variable_name}"

            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                odessa_base, odessa_path
            ):
                variable_structure = odessa_base.get(odessa_path)

                logger.debug(f"Collect variable structure {variable_structure}.")
                array[idx] = variable_structure

        return array

    def parse_variable_vessel_bono(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from vessel bono.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type vessel_bono.")

        array = np.full((len(self.bono_ids.index)), fill_value=np.nan)
        logger.debug(f"Initialized array with shape {array.shape}.")

        for idx, dataframe_row in self.bono_ids.iterrows():
            comp_id = dataframe_row["bono_id"]

            logger.debug(f"Handle comp_id {comp_id}.")

            odessa_path = f"VESSEL 1: COMP {int(comp_id)}: {variable_name}"

            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                odessa_base, odessa_path
            ):
                variable_structure = odessa_base.get(odessa_path)

                logger.debug(f"Collect variable structure {variable_structure}.")
                array[idx] = variable_structure

        return array

    def parse_variable_vessel_crod(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from vessel crod.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type vessel_crod.")

        array = np.full((len(self.crod_ids.index)), fill_value=np.nan)
        logger.debug(f"Initialized array with shape {array.shape}.")

        for idx, dataframe_row in self.crod_ids.iterrows():
            comp_id = dataframe_row["crod_id"]

            logger.debug(f"Handle comp_id {comp_id}.")

            odessa_path = f"VESSEL 1: COMP {int(comp_id)}: {variable_name}"

            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                odessa_base, odessa_path
            ):
                variable_structure = odessa_base.get(odessa_path)

                logger.debug(f"Collect variable structure {variable_structure}.")
                array[idx] = variable_structure

        return array

    def parse_variable_vessel_clad_crod(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from vessel clad_crod.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type vessel_clad_crod.")

        array = np.full((len(self.clad_crod_ids.index)), fill_value=np.nan)
        logger.debug(f"Initialized array with shape {array.shape}.")

        for idx, dataframe_row in self.clad_crod_ids.iterrows():
            comp_id = dataframe_row["clad_crod_id"]

            logger.debug(f"Handle comp_id {comp_id}.")

            odessa_path = f"VESSEL 1: COMP {int(comp_id)}: {variable_name}"

            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                odessa_base, odessa_path
            ):
                variable_structure = odessa_base.get(odessa_path)

                logger.debug(f"Collect variable structure {variable_structure}.")
                array[idx] = variable_structure

        return array

    def parse_variable_vessel_tguide(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from vessel tguide.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type vessel_tguide.")

        array = np.full((len(self.tguide_ids.index)), fill_value=np.nan)
        logger.debug(f"Initialized array with shape {array.shape}.")

        for idx, dataframe_row in self.tguide_ids.iterrows():
            comp_id = dataframe_row["tguide_id"]

            logger.debug(f"Handle comp_id {comp_id}.")

            odessa_path = f"VESSEL 1: COMP {int(comp_id)}: {variable_name}"

            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                odessa_base, odessa_path
            ):
                variable_structure = odessa_base.get(odessa_path)

                logger.debug(f"Collect variable structure {variable_structure}.")
                array[idx] = variable_structure

        return array

    def parse_variable_vessel_tinst(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from vessel tinst.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type vessel_tinst.")

        array = np.full((len(self.tinst_ids.index)), fill_value=np.nan)
        logger.debug(f"Initialized array with shape {array.shape}.")

        for idx, dataframe_row in self.tinst_ids.iterrows():
            comp_id = dataframe_row["tinst_id"]

            logger.debug(f"Handle comp_id {comp_id}.")

            odessa_path = f"VESSEL 1: COMP {int(comp_id)}: {variable_name}"

            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                odessa_base, odessa_path
            ):
                variable_structure = odessa_base.get(odessa_path)

                logger.debug(f"Collect variable structure {variable_structure}.")
                array[idx] = variable_structure

        return array

    def parse_variable_vessel_grid(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from vessel grid.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type vessel_grid.")

        array = np.full((len(self.grid_ids.index)), fill_value=np.nan)
        logger.debug(f"Initialized array with shape {array.shape}.")

        for idx, dataframe_row in self.grid_ids.iterrows():
            comp_id = dataframe_row["grid_id"]

            logger.debug(f"Handle comp_id {comp_id}.")

            odessa_path = f"VESSEL 1: COMP {int(comp_id)}: {variable_name}"

            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                odessa_base, odessa_path
            ):
                variable_structure = odessa_base.get(odessa_path)

                logger.debug(f"Collect variable structure {variable_structure}.")
                array[idx] = variable_structure

        return array

    def parse_variable_vessel_LP_wall(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from vessel LP_wall.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type vessel_LP_wall.")

        array = np.full((len(self.LP_wall_ids.index)), fill_value=np.nan)
        logger.debug(f"Initialized array with shape {array.shape}.")

        for idx, dataframe_row in self.LP_wall_ids.iterrows():
            comp_id = dataframe_row["LP_wall_id"]

            logger.debug(f"Handle comp_id {comp_id}.")

            odessa_path = f"VESSEL 1: COMP {int(comp_id)}: {variable_name}"

            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                odessa_base, odessa_path
            ):
                variable_structure = odessa_base.get(odessa_path)

                logger.debug(f"Collect variable structure {variable_structure}.")
                array[idx] = variable_structure

        return array

    def parse_variable_vessel_LP_corium(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from vessel LP_corium.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type vessel_LP_corium.")

        array = np.full((len(self.LP_corium_ids.index)), fill_value=np.nan)
        logger.debug(f"Initialized array with shape {array.shape}.")

        for idx, dataframe_row in self.LP_corium_ids.iterrows():
            comp_id = dataframe_row["LP_corium_id"]

            logger.debug(f"Handle comp_id {comp_id}.")

            odessa_path = f"VESSEL 1: COMP {int(comp_id)}: {variable_name}"

            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                odessa_base, odessa_path
            ):
                variable_structure = odessa_base.get(odessa_path)

                logger.debug(f"Collect variable structure {variable_structure}.")
                array[idx] = variable_structure

        return array

    def parse_variable_vessel_fuel_stat(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from vessel fuel status.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type vessel_fuel_stat.")

        array = np.full((len(self.fuel_ids.index)), fill_value=np.nan)
        logger.debug(f"Initialized array with shape {array.shape}.")

        for idx, dataframe_row in self.fuel_ids.iterrows():
            comp_id = dataframe_row["fuel_id"]

            logger.debug(f"Handle comp_id {comp_id}.")

            odessa_path = f"VESSEL 1: COMP {int(comp_id)}: {variable_name}"

            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                odessa_base, odessa_path
            ):
                variable_structure = odessa_base.get(odessa_path)

                component_state = self.component_states.loc[
                    self.component_states["state"] == variable_structure
                ]
                component_state_code = component_state["code"]

                logger.debug(
                    f"Collect variable structure string {variable_structure}, "
                    f"what corresponds to code {int(component_state_code.iloc[0])}."
                )
                array[idx] = int(component_state_code.iloc[0])

        return array

    def parse_variable_vessel_clad_stat(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from vessel clad status.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type vessel_clad_stat.")

        array = np.full((len(self.clad_ids.index)), fill_value=np.nan)
        logger.debug(f"Initialized array with shape {array.shape}.")

        for idx, dataframe_row in self.clad_ids.iterrows():
            comp_id = dataframe_row["clad_id"]

            logger.debug(f"Handle comp_id {comp_id}.")

            odessa_path = f"VESSEL 1: COMP {int(comp_id)}: {variable_name} 1"

            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                odessa_base, odessa_path
            ):
                variable_structure = odessa_base.get(odessa_path)

                component_state = self.component_states.loc[
                    self.component_states["state"] == variable_structure
                ]
                component_state_code = component_state["code"]

                logger.debug(
                    f"Collect variable structure string {variable_structure}, "
                    f"what corresponds to code {int(component_state_code.iloc[0])}."
                )
                array[idx] = int(component_state_code.iloc[0])

        return array

    @staticmethod
    def parse_variable_from_vessel_mesh_ther(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from vessel mesh thermal data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type vessel_mesh_ther.")

        vessel_mesh_check_path = "VESSEL 1: MESH 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, vessel_mesh_check_path
        ):
            vessel = odessa_base.get("VESSEL")
            number_of_meshes = vessel.len("MESH")

            array = np.full((number_of_meshes), fill_value=np.nan)
            logger.debug(f"Initialized array with shape {array.shape}.")

            for idx, mesh_number in enumerate(range(1, number_of_meshes + 1)):
                logger.debug(f"Index is {idx}, mesh_number is {mesh_number}.")

                odessa_path = f"VESSEL 1: MESH {mesh_number}: THER 1: {variable_name} 1"

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {vessel_mesh_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_vessel_mesh(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from vessel mesh data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type vessel_mesh.")

        vessel_mesh_check_path = "VESSEL 1: MESH 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, vessel_mesh_check_path
        ):
            vessel = odessa_base.get("VESSEL")
            number_of_meshes = vessel.len("MESH")

            array = np.full((number_of_meshes), fill_value=np.nan)
            logger.debug(f"Initialized array with shape {array.shape}.")

            for idx, mesh_number in enumerate(range(1, number_of_meshes + 1)):
                logger.debug(f"Mesh number {mesh_number}.")

                odessa_path = f"VESSEL 1: MESH {mesh_number}: {variable_name} 1"

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure

        else:
            logger.debug(
                f"Path {vessel_mesh_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_vessel_face_ther(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from vessel face thermal data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type vessel_face_ther.")

        vessel_face_check_path = "VESSEL 1: FACE 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, vessel_face_check_path
        ):
            vessel = odessa_base.get("VESSEL")
            number_of_faces = vessel.len("FACE")

            logger.debug(f"Number of faces in vessel: {number_of_faces}.")

            array = np.full((number_of_faces), fill_value=np.nan)

            for idx, face_number in enumerate(range(1, number_of_faces + 1)):
                odessa_path = f"VESSEL 1: FACE {face_number}: THER 1: {variable_name} 1"

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {vessel_face_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_vessel_general(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from vessel general data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type vessel_general.")

        odessa_path = f"VESSEL 1: GENERAL 1: {variable_name} 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, odessa_path
        ):
            variable_structure = odessa_base.get(odessa_path)
            logger.debug(f"Collect variable structure {variable_structure}.")
            array = np.array([variable_structure])

        else:
            logger.debug(
                f"Variable {variable_name} not in odessa base, "
                "fill datapoint with np.nan."
            )
            array = np.array([np.nan])

        return array

    @staticmethod
    def parse_variable_from_fp_heat_vessel(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from fuel pin heat vessel data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type fp_heat_vessel.")

        odessa_path = f"FP_HEAT 1: VESSEL 1: {variable_name} 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, odessa_path
        ):
            variable_structure = odessa_base.get(odessa_path)
            logger.debug(f"Collect variable structure {variable_structure}.")
            array = np.array([variable_structure[0]])

        else:
            logger.debug(
                f"Variable {variable_name} not in odessa base, "
                "fill datapoint with np.nan."
            )
            array = np.array([np.nan])

        return array

    @staticmethod
    def parse_variable_from_primary_junction_ther(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from primary junction thermal data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(
            f"Parse ASTEC variable {variable_name}, type primary_junction_ther."
        )

        primary_junction_check_path = "PRIMARY 1: JUNCTION 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, primary_junction_check_path
        ):
            primary = odessa_base.get("PRIMARY")
            number_of_junctions = primary.len("JUNCTION")

            logger.debug(f"Number of junctions in primary: {number_of_junctions}.")

            array = np.full((number_of_junctions), fill_value=np.nan)

            for idx, junction_number in enumerate(range(1, number_of_junctions + 1)):
                odessa_path = (
                    f"PRIMARY 1: JUNCTION {junction_number}: THER 1: {variable_name} 1"
                )

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {primary_junction_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_primary_junction_geom(
        odessa_base: pyod.Base, variable_name: str
    ) -> np.ndarray:
        """Parse ASTEC variable from primary junction geometric data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(
            f"Parse ASTEC variable {variable_name}, type primary_junction_geom."
        )

        primary_junction_check_path = "PRIMARY 1: JUNCTION 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, primary_junction_check_path
        ):
            primary = odessa_base.get("PRIMARY")
            number_of_junctions = primary.len("JUNCTION")

            logger.debug(f"Number of junctions in primary: {number_of_junctions}.")

            array = np.full((number_of_junctions), fill_value=np.nan)

            for idx, junction_number in enumerate(range(1, number_of_junctions + 1)):
                odessa_path = (
                    f"PRIMARY 1: JUNCTION {junction_number}: GEOM 1: {variable_name} 1"
                )

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {primary_junction_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_primary_volume_ther(
        odessa_base: pyod.Base, variable_name: str
    ) -> np.ndarray:
        """Parse ASTEC variable from primary volume thermal data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type primary_volume_ther.")

        primary_volume_check_path = "PRIMARY 1: VOLUME 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, primary_volume_check_path
        ):
            primary = odessa_base.get("PRIMARY")
            number_of_volumes = primary.len("VOLUME")

            logger.debug(f"Number of volumes in primary: {number_of_volumes}.")

            array = np.full((number_of_volumes), fill_value=np.nan)

            for idx, volume_number in enumerate(range(1, number_of_volumes + 1)):
                odessa_path = (
                    f"PRIMARY 1: VOLUME {volume_number}: THER 1: {variable_name} 1"
                )

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {primary_volume_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_primary_volume_geom(
        odessa_base: pyod.Base, variable_name: str
    ) -> np.ndarray:
        """Parse ASTEC variable from primary volume geometric data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type primary_volume_geom.")

        primary_volume_check_path = "PRIMARY 1: VOLUME 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, primary_volume_check_path
        ):
            primary = odessa_base.get("PRIMARY")
            number_of_volumes = primary.len("VOLUME")

            logger.debug(f"Number of volumes in primary: {number_of_volumes}.")

            array = np.full((number_of_volumes), fill_value=np.nan)

            for idx, volume_number in enumerate(range(1, number_of_volumes + 1)):
                odessa_path = (
                    f"PRIMARY 1: VOLUME {volume_number}: GEOM 1: {variable_name} 1"
                )

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {primary_volume_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_primary_pipe_geom(
        odessa_base: pyod.Base, variable_name: str
    ) -> np.ndarray:
        """Parse ASTEC variable from primary pipe geometric data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type primary_pipe_geom.")

        primary_pipe_geom_check_path = "PRIMARY 1: PIPE 1: GEOM 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, primary_pipe_geom_check_path
        ):
            primary = odessa_base.get("PRIMARY")
            number_of_pipes = primary.len("PIPE")
            variable_structure = primary.get(f"PIPE 1: GEOM 1: {variable_name} 1")

            logger.debug(
                f"Number of pipes in primary: {number_of_pipes}. "
                f"Length of variable structure: {len(variable_structure)}."
            )

            array = np.full(
                (number_of_pipes, len(variable_structure)), fill_value=np.nan
            )

            for idx, pipe_number in enumerate(range(1, number_of_pipes + 1)):
                odessa_path = (
                    f"PRIMARY 1: PIPE {pipe_number}: GEOM 1: {variable_name} 1"
                )

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {primary_pipe_geom_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_secondar_junction_ther(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from secondary junction thermal data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(
            f"Parse ASTEC variable {variable_name}, type secondar_junction_ther."
        )

        secondar_junction_check_path = "SECONDAR 1: JUNCTION 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, secondar_junction_check_path
        ):
            secondar = odessa_base.get("SECONDAR")
            number_of_junctions = secondar.len("JUNCTION")

            logger.debug(f"Number of junctions in secondar: {number_of_junctions}.")

            array = np.full((number_of_junctions), fill_value=np.nan)

            for idx, junction_number in enumerate(range(1, number_of_junctions + 1)):
                odessa_path = (
                    f"SECONDAR 1: JUNCTION {junction_number}: THER 1: {variable_name} 1"
                )

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {secondar_junction_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_secondar_junction_geom(
        odessa_base: pyod.Base, variable_name: str
    ) -> np.ndarray:
        """Parse ASTEC variable from secondary junction geometric data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(
            f"Parse ASTEC variable {variable_name}, type secondar_junction_geom."
        )

        secondar_junction_check_path = "SECONDAR 1: JUNCTION 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, secondar_junction_check_path
        ):
            secondar = odessa_base.get("SECONDAR")
            number_of_junctions = secondar.len("JUNCTION")

            logger.debug(f"Number of junctions in secondar: {number_of_junctions}.")

            array = np.full((number_of_junctions), fill_value=np.nan)

            for idx, junction_number in enumerate(range(1, number_of_junctions + 1)):
                odessa_path = (
                    f"SECONDAR 1: JUNCTION {junction_number}: GEOM 1: {variable_name} 1"
                )

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {secondar_junction_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_secondar_volume_ther(
        odessa_base: pyod.Base, variable_name: str
    ) -> np.ndarray:
        """Parse ASTEC variable from secondary volume thermal data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(
            f"Parse ASTEC variable {variable_name}, type secondar_volume_ther."
        )

        secondar_volume_check_path = "SECONDAR 1: VOLUME 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, secondar_volume_check_path
        ):
            secondar = odessa_base.get("SECONDAR")
            number_of_volumes = secondar.len("VOLUME")

            logger.debug(f"Number of volumes in secondar: {number_of_volumes}.")

            array = np.full((number_of_volumes), fill_value=np.nan)

            for idx, volume_number in enumerate(range(1, number_of_volumes + 1)):
                odessa_path = (
                    f"SECONDAR 1: VOLUME {volume_number}: THER 1: {variable_name} 1"
                )

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {secondar_volume_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_primary_wall(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from primary wall data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type primary_wall.")

        primary_wall_check_path = "PRIMARY 1: WALL 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, primary_wall_check_path
        ):
            primary = odessa_base.get("PRIMARY")
            number_of_walls = primary.len("WALL")

            logger.debug(f"Number of walls in primary: {number_of_walls}.")

            array = np.full((number_of_walls), fill_value=np.nan)

            for idx, wall_number in enumerate(range(1, number_of_walls + 1)):
                odessa_path = f"PRIMARY 1: WALL {wall_number}: {variable_name} 1"

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure

        else:
            logger.debug(
                f"Path {primary_wall_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_primary_wall_ther(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from primary wall thermal data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type primary_wall_ther.")

        primary_wall_check_path = "PRIMARY 1: WALL 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, primary_wall_check_path
        ):
            primary = odessa_base.get("PRIMARY")
            number_of_walls = primary.len("WALL")

            logger.debug(f"Number of walls in primary: {number_of_walls}.")

            array = np.full((number_of_walls), fill_value=np.nan)

            for idx, wall_number in enumerate(range(1, number_of_walls + 1)):
                odessa_path = (
                    f"PRIMARY 1: WALL {wall_number}: THER 1: {variable_name} 1"
                )

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {primary_wall_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_primary_wall_ther_2(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from primary wall thermal data (alternative).

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type primary_wall_ther_2.")

        primary_wall_check_path = "PRIMARY 1: WALL 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, primary_wall_check_path
        ):
            primary = odessa_base.get("PRIMARY")
            number_of_walls = primary.len("WALL")

            logger.debug(f"Number of walls in primary: {number_of_walls}.")

            array = np.full((number_of_walls), fill_value=np.nan)

            for idx, wall_number in enumerate(range(1, number_of_walls + 1)):
                odessa_path = (
                    f"PRIMARY 1: WALL {wall_number}: THER 2: {variable_name} 1"
                )

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {primary_wall_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_primary_wall_geom(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from primary wall geometric data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type primary_wall_geom.")

        primary_wall_check_path = "PRIMARY 1: WALL 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, primary_wall_check_path
        ):
            primary = odessa_base.get("PRIMARY")
            number_of_walls = primary.len("WALL")

            logger.debug(f"Number of walls in primary: {number_of_walls}.")

            array = np.full((number_of_walls), fill_value=np.nan)

            for idx, wall_number in enumerate(range(1, number_of_walls + 1)):
                odessa_path = (
                    f"PRIMARY 1: WALL {wall_number}: GEOM 1: {variable_name} 1"
                )

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {primary_wall_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_secondar_wall(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from secondary wall data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type secondar_wall.")

        secondar_wall_check_path = "SECONDAR 1: WALL 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, secondar_wall_check_path
        ):
            secondar = odessa_base.get("SECONDAR")
            number_of_walls = secondar.len("WALL")

            logger.debug(f"Number of walls in secondar: {number_of_walls}.")

            array = np.full((number_of_walls), fill_value=np.nan)

            for idx, wall_number in enumerate(range(1, number_of_walls + 1)):
                odessa_path = f"SECONDAR 1: WALL {wall_number}: {variable_name} 1"

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure

        else:
            logger.debug(
                f"Path {secondar_wall_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_secondar_wall_ther(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from secondary wall thermal data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type secondar_wall_ther.")

        secondar_wall_check_path = "SECONDAR 1: WALL 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, secondar_wall_check_path
        ):
            secondar = odessa_base.get("SECONDAR")
            number_of_walls = secondar.len("WALL")

            logger.debug(f"Number of walls in secondar: {number_of_walls}.")

            array = np.full((number_of_walls), fill_value=np.nan)

            for idx, wall_number in enumerate(range(1, number_of_walls + 1)):
                odessa_path = (
                    f"SECONDAR 1: WALL {wall_number}: THER 1: {variable_name} 1"
                )

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {secondar_wall_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_secondar_wall_ther_2(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from secondary wall thermal data (alternative).

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type secondar_wall_ther.")

        secondar_wall_check_path = "SECONDAR 1: WALL 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, secondar_wall_check_path
        ):
            secondar = odessa_base.get("SECONDAR")
            number_of_walls = secondar.len("WALL")

            logger.debug(f"Number of walls in secondar: {number_of_walls}.")

            array = np.full((number_of_walls), fill_value=np.nan)

            for idx, wall_number in enumerate(range(1, number_of_walls + 1)):
                odessa_path = (
                    f"SECONDAR 1: WALL {wall_number}: THER 2: {variable_name} 1"
                )

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {secondar_wall_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_secondar_wall_geom(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from secondary wall geometric data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type secondar_wall_geom.")

        secondar_wall_check_path = "SECONDAR 1: WALL 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, secondar_wall_check_path
        ):
            secondar = odessa_base.get("SECONDAR")
            number_of_walls = secondar.len("WALL")

            logger.debug(f"Number of walls in secondar: {number_of_walls}.")

            array = np.full((number_of_walls), fill_value=np.nan)

            for idx, wall_number in enumerate(range(1, number_of_walls + 1)):
                odessa_path = (
                    f"SECONDAR 1: WALL {wall_number}: GEOM 1: {variable_name} 1"
                )

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {secondar_wall_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_systems_pump(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from systems pump data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type systems_pump.")

        systems_pump_check_path = "SYSTEMS 1: PUMP 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, systems_pump_check_path
        ):
            systems = odessa_base.get("SYSTEMS")
            number_of_pumps = systems.len("PUMP")

            logger.debug(f"Number of pumps in systems: {number_of_pumps}.")

            array = np.full((number_of_pumps), fill_value=np.nan)

            for idx, pump_number in enumerate(range(1, number_of_pumps + 1)):
                odessa_path = f"SYSTEMS 1: PUMP {pump_number}: {variable_name} 1"

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {systems_pump_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_systems_pump_momentum(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from systems pump momentum data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type systems_pump.")

        systems_pump_check_path = "SYSTEMS 1: PUMP 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, systems_pump_check_path
        ):
            systems = odessa_base.get("SYSTEMS")
            number_of_pumps = systems.len("PUMP")

            logger.debug(f"Number of pumps in systems: {number_of_pumps}.")

            array = np.full((number_of_pumps), fill_value=np.nan)

            for idx, pump_number in enumerate(range(1, number_of_pumps + 1)):
                odessa_path = (
                    f"SYSTEMS 1: PUMP {pump_number}: MOMENTUM 1: {variable_name} 1"
                )

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure

        else:
            logger.debug(
                f"Path {systems_pump_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_systems_valve(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from systems valve data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type systems_valve.")

        systems_valve_check_path = "SYSTEMS 1: VALVE 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, systems_valve_check_path
        ):
            systems = odessa_base.get("SYSTEMS")
            number_of_valves = systems.len("VALVE")

            logger.debug(f"Number of valves in systems: {number_of_valves}.")

            array = np.full((number_of_valves), fill_value=np.nan)

            for idx, valve_number in enumerate(range(1, number_of_valves + 1)):
                odessa_path = f"SYSTEMS 1: VALVE {valve_number}: {variable_name} 1"

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {systems_valve_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_sensor(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from sensor data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable from sensor {variable_name}, type sensor.")

        odessa_path = f"SENSOR {variable_name}: value 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, odessa_path
        ):
            variable_structure = odessa_base.get(odessa_path)
            logger.debug(f"Collect variable structure {variable_structure}.")
            array = np.array([variable_structure])

        else:
            logger.debug(
                f"Variable {variable_name} not in odessa base, "
                "fill datapoint with np.nan."
            )
            array = np.array([np.nan])

        return array

    @staticmethod
    def parse_variable_from_sensors(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from sensor data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type sensor.")

        sensor_check_path = f"SENSOR 1: {variable_name} 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, sensor_check_path
        ):
            number_of_sensors = odessa_base.len("SENSOR")

            logger.debug(f"Number of sensors in odessa base: {number_of_sensors}.")

            array = np.full((number_of_sensors), fill_value=np.nan)

            for idx, sensor_number in enumerate(range(1, number_of_sensors + 1)):
                odessa_path = f"SENSOR {sensor_number}: {variable_name} 1"

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure

        else:
            logger.debug(
                f"Path {sensor_check_path} not in odessa base, fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_containment_dome(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from containment dome data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(
            f"Parse ASTEC variable from sensor {variable_name}, type containment_dome."
        )

        odessa_path = f"CONTAINM 1: ZONE 10: THER 1: {variable_name} 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, odessa_path
        ):
            variable_structure = odessa_base.get(odessa_path)
            logger.debug(f"Collect variable structure {variable_structure}.")
            array = np.array([variable_structure[0]])

        else:
            logger.debug(
                f"Variable {variable_name} not in odessa base, "
                "fill datapoint with np.nan."
            )
            array = np.array([np.nan])

        return array

    @staticmethod
    def parse_variable_from_containment_pool(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from containment pool data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(
            f"Parse ASTEC variable from sensor {variable_name}, type containment_pool."
        )

        odessa_path = f"CONTAINM 1: ZONE 11: THER 1: {variable_name} 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, odessa_path
        ):
            variable_structure = odessa_base.get(odessa_path)
            logger.debug(f"Collect variable structure {variable_structure}.")
            array = np.array([variable_structure[0]])

        else:
            logger.debug(
                f"Variable {variable_name} not in odessa base, "
                "fill datapoint with np.nan."
            )
            array = np.array([np.nan])

        return array

    @staticmethod
    def parse_variable_from_containment_zone(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from all containment zones.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type containment_.")

        secondar_wall_check_path = "CONTAINM 1: ZONE 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, secondar_wall_check_path
        ):
            containment = odessa_base.get("CONTAINM")
            number_of_zones = containment.len("ZONE")

            logger.debug(f"Number of zones in containment: {number_of_zones}.")

            array = np.full((number_of_zones), fill_value=np.nan)

            for idx, zone_number in enumerate(range(1, number_of_zones + 1)):
                odessa_path = f"CONTAINM 1: ZONE {zone_number}: {variable_name} 1"

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {secondar_wall_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_containment_zone_ther(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from all containment zone thermal structures.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type containment_.")

        secondar_wall_check_path = "CONTAINM 1: ZONE 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, secondar_wall_check_path
        ):
            containment = odessa_base.get("CONTAINM")
            number_of_zones = containment.len("ZONE")

            logger.debug(f"Number of zones in containment: {number_of_zones}.")

            array = np.full((number_of_zones), fill_value=np.nan)

            for idx, zone_number in enumerate(range(1, number_of_zones + 1)):
                odessa_path = (
                    f"CONTAINM 1: ZONE {zone_number}: THER 1: {variable_name} 1"
                )

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {secondar_wall_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_containment_connection(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from all containment connections.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(
            f"Parse ASTEC variable {variable_name}, type containment_connection."
        )

        containment_zone_check_path = "CONTAINM 1: CONN 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, containment_zone_check_path
        ):
            containment = odessa_base.get("CONTAINM")
            number_of_connections = containment.len("CONN")

            logger.debug(
                f"Number of CONN in containment: {number_of_connections}."
            )

            array = np.full((number_of_connections), fill_value=np.nan)

            for idx, connection_number in enumerate(
                range(1, number_of_connections + 1)
            ):
                odessa_path = f"CONTAINM 1: CONN {connection_number}: {variable_name} 1"

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {containment_zone_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_containment_wall_temp(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from containment wall temperature profile.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(
            f"Parse ASTEC variable {variable_name}, type containment_wall_temperature."
        )

        containment_zone_check_path = f"CONTAINM 1: WALL 1: SLAB 1: {variable_name} 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, containment_zone_check_path
        ):
            containment = odessa_base.get("CONTAINM")
            number_of_walls = containment.len("WALL")

            logger.debug(f"Number of walls in containment: {number_of_walls}.")

            array = np.full((number_of_walls, 20), fill_value=np.nan)

            for idx, wall_number in enumerate(range(1, number_of_walls + 1)):
                odessa_path = (
                    f"CONTAINM 1: WALL {wall_number}: SLAB 1: {variable_name} 1"
                )

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[2:]

        else:
            logger.debug(
                f"Path {containment_zone_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1, 1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_connection(
        odessa_base: pyod.Base,
        variable_name: str,
        connecti_index: list = list(range(1,81))
    ) -> np.ndarray:
        """Parse ASTEC variable from connection data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type connecti.")

        connecti_check_path = "CONNECTI 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, connecti_check_path
        ):
            number_of_connectis = odessa_base.len("CONNECTI")

            logger.debug(f"Number of CONNECTI in BASE: {number_of_connectis}.")

            array = np.full((len(connecti_index)), fill_value=np.nan)

            for idx, connecti_number in enumerate(connecti_index):
                odessa_path = f"CONNECTI {connecti_number}: {variable_name}"

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = (
                        AssasOdessaNetCDF4Converter.convert_odessa_structure_to_float(
                            odessa_structure=variable_structure
                        )
                    )

        else:
            logger.debug(
                f"Path {connecti_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((len(connecti_index)), fill_value=np.nan)

        return array

    def parse_variable_from_connection_break(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from connection break data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type connecti BREAK.")

        return self.parse_variable_from_connection(
            odessa_base, variable_name, self.connection_break_ids["break_id"])

    def parse_variable_from_connection_feedwater(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from connection feedwater data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type connecti FEEDWATE.")

        return self.parse_variable_from_connection(
            odessa_base, variable_name, self.connection_feedwater_ids["feedwater_id"])

    def parse_variable_from_connection_flow(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from connection flow data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type connecti FLOW.")

        return self.parse_variable_from_connection(
            odessa_base, variable_name, self.connection_flow_ids["flow_id"])

    def parse_variable_from_connection_flow_fp(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC fp variable from the first connection flow data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, first type connecti FLOW.")

        return self.parse_variable_from_connection(
            odessa_base, variable_name, [1])

    def parse_variable_from_connection_heat(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from connection heat data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type connecti HEAT.")

        return self.parse_variable_from_connection(
            odessa_base, variable_name, self.connection_heat_ids["heat_id"])

    def parse_variable_from_connection_mcci(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from connection mcci data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type connecti MCCI.")

        return self.parse_variable_from_connection(
            odessa_base, variable_name, self.connection_mcci_ids["mcci_id"])

    def parse_variable_from_connection_source(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from connection source data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type connecti SOURCE.")

        return self.parse_variable_from_connection(
            odessa_base, variable_name, self.connection_source_ids["source_id"])

    def parse_variable_from_connection_vespour(
        self,
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from connection vespour data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type connecti VESPOUR.")

        return self.parse_variable_from_connection(
            odessa_base, variable_name, self.connection_vespour_ids["vespour_id"])

    @staticmethod
    def parse_variable_vessel_trup(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from vessel trup data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type vessel_trup.")

        odessa_path = f"SEQUENCE 1: {variable_name} 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base=odessa_base,
            odessa_path=odessa_path,
        ):
            variable_structure = odessa_base.get(odessa_path)
            logger.debug(f"Collect variable structure {variable_structure}.")
            array = np.array([variable_structure])

        else:
            logger.debug(
                f"Variable {variable_name} not in odessa base, fill array with np.nan."
            )
            array = np.array([np.nan])

        return array

    @staticmethod
    def parse_variable_private_assas_param(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from private ASSAS parameters.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type private_assas_param.")

        odessa_path = f"PRIVATE 1: ASSASpar 1: {variable_name} 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, odessa_path
        ):
            variable_structure = odessa_base.get(odessa_path)
            logger.debug(f"Collect variable structure {variable_structure}.")
            array = np.array([variable_structure])

        else:
            logger.debug(
                f"Variable {variable_name} not in odessa base, fill array with np.nan."
            )
            array = np.array([np.nan])

        return array

    @staticmethod
    def parse_variable_cesar_io(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from cesar_io data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type cesar_io.")

        odessa_path = f"CESAR_IO 1: {variable_name} 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, odessa_path
        ):
            variable_structure = odessa_base.get(odessa_path)
            logger.debug(f"Collect variable structure {variable_structure}.")
            array = np.array([variable_structure])

        else:
            logger.debug(
                f"Variable {variable_name} not in odessa base, fill array with np.nan."
            )
            array = np.array([np.nan])

        return array

    @staticmethod
    def parse_variable_cesar_io_output(
        odessa_base: pyod.Base,
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from cesar_io data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type cesar_io.")

        odessa_path = f"CESAR_IO 1: OUTPUTS 1: {variable_name} 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, odessa_path
        ):
            variable_structure = odessa_base.get(odessa_path)
            logger.debug(f"Collect variable structure {variable_structure}.")
            array = np.array([variable_structure])

        else:
            logger.debug(
                f"Variable {variable_name} not in odessa base, fill array with np.nan."
            )
            array = np.array([np.nan])

        return array

    @staticmethod
    def get_general_meta_data(
        netcdf4_file_path: str,
        attribute_name: str,
    ) -> str:
        """Read general meta data from a netCDF4 file.

        Args:
            netcdf4_file_path (str): Path to the netCDF4 file.
            attribute_name (str): Name of the attribute to read.

        Returns:
            str: Value of the specified attribute.

        """
        netcdf4_path_object = Path(netcdf4_file_path)
        logger.info(
            f"Read general meta data attribute {attribute_name}"
            f"from hdf5 file with path {str(netcdf4_path_object)}."
        )

        value = None
        with netCDF4.Dataset(f"{netcdf4_path_object}", "r", format="NETCDF4") as ncfile:
            value = ncfile.getncattr(attribute_name)

        return value

    @staticmethod
    def set_general_meta_data(
        output_path: str,
        archive_name: str,
        archive_description: str,
    ) -> None:
        """Set general metadata for the NetCDF4 file with unit information."""
        output_path = Path(output_path)

        if not output_path.parent.exists():
            logger.info(f"Create missing directories for output path {output_path}.")
            output_path.parent.mkdir(parents=True, exist_ok=True)

        with netCDF4.Dataset(f"{output_path}", "a", format="NETCDF4") as ncfile:
            ncfile.archive_name = archive_name
            ncfile.archive_description = archive_description
            ncfile.creation_date = str(pd.Timestamp.now())

            logger.info("Set general metadata for the NetCDF4 file.")

    @staticmethod
    def iterate_recursive_over_groups(
        group: netCDF4.Group, result: list[dict], path_prefix: str = ""
    ) -> None:
        """Recursively iterate through the netCDF4 groups and variables.

        Args:
            group: The current netCDF4 group.
            result: The list to store variable metadata.
            path_prefix: The prefix for the current path.

        """
        logger.debug(f"Iterating through group: {group.name}")
        for var_name, var in group.variables.items():
            full_path = f"{path_prefix}/{var_name}" if path_prefix else var_name
            logger.info(
                f"Variable at {full_path}: shape={var.shape}, dtype={var.dtype}"
            )
            if var_name == "time_points":
                variable_dict = {
                    "name": var_name,
                    "dimensions": "(" + ", ".join(str(d) for d in var.dimensions) + ")",
                    "shape": "(" + ", ".join(str(s) for s in var.shape) + ")",
                    "domain": "-",
                }
                result.append(variable_dict)
                continue

            if var.variable_type == "data":
                variable_dict = {
                    "name": var.name,
                    "dimensions": "(" + ", ".join(str(d) for d in var.dimensions) + ")",
                    "shape": "(" + ", ".join(str(s) for s in var.shape) + ")",
                    "domain": (
                        var.getncattr("domain") if "domain" in var.ncattrs() else "-"
                    ),
                }
                result.append(variable_dict)

        for subgroup_name, subgroup in group.groups.items():
            new_prefix = (
                f"{path_prefix}/{subgroup_name}" if path_prefix else subgroup_name
            )
            AssasOdessaNetCDF4Converter.iterate_recursive_over_groups(
                group=subgroup,
                result=result,
                path_prefix=new_prefix,
            )

    @staticmethod
    def read_metadata_from_variables_in_netcdf4(netcdf4_file: str) -> List[dict]:
        """Read metadata from all variables in the NetCDF4 file.

        Returns:
            List[dict]: A list of dictionaries containing variable metadata.

        """
        result = []
        logger.info(f"Reading metadata from netCDF4 file {netcdf4_file}.")

        with netCDF4.Dataset(netcdf4_file, "r") as ncfile:
            logger.info("Starting recursive iteration through netCDF4 groups.")
            AssasOdessaNetCDF4Converter.iterate_recursive_over_groups(ncfile, result)

        return result

    @staticmethod
    def read_variables_meta_values_from_netcdf4(
        netcdf4_file: str,
        group_name: str = None,
    ) -> List[dict]:
        """Read meta values from a netCDF4 file.

        Args:
            netcdf4_file (str): Path to the netCDF4 file.
            group_name (str, optional): Name of the group to read from.
                Defaults to None, which reads from the root group.

        Returns:
            List[dict]: A list of dictionaries containing variable metadata.

        """
        result = []

        with netCDF4.Dataset(f"{netcdf4_file}", "r", format="NETCDF4") as ncfile:
            if group_name is not None:
                logger.info(f"Reading metadata from group {group_name}.")
                ncfile = ncfile.groups[group_name]
            else:
                logger.info("Reading metadata from root group.")
            for variable_name in ncfile.variables.keys():
                variable_dict = {}

                variable_dict["name"] = variable_name
                logger.info(f"Read variable {variable_name}.")

                dimensions = ncfile.variables[variable_name].dimensions

                variable_dict["dimensions"] = (
                    "(" + ", ".join(str(dimension) for dimension in dimensions) + ")"
                )
                logger.debug(f"Dimension string is {variable_dict['dimensions']}.")

                shapes = ncfile.variables[variable_name].shape

                variable_dict["shape"] = (
                    "(" + ", ".join(str(shape) for shape in shapes) + ")"
                )
                logger.debug(f"Shape string is {variable_dict['shape']}.")

                if variable_name == "time_points":
                    domain = "-"
                else:
                    domain = ncfile.variables[variable_name].getncattr("domain")

                variable_dict["domain"] = domain
                logger.debug(f"Domain string is {variable_dict['domain']}.")

                for attr_name in ncfile.variables[variable_name].ncattrs():
                    logger.debug(f"Attribute name {attr_name}.")

                result.append(variable_dict)

        return result

    @staticmethod
    def get_completed_index_from_netcdf4_file(
        netcdf4_file: str,
    ) -> int:
        """Get the completed index from a netCDF4 file.

        Args:
            netcdf4_file (str): Path to the netCDF4 file.

        Returns:
            int: The completed index of time points.

        """
        logger.info(
            f"Get completed index from hdf5 file with path {str(netcdf4_file)}."
        )

        completed_index = 0
        with netCDF4.Dataset(f"{netcdf4_file}", "r", format="NETCDF4") as ncfile:
            dimension_group = ncfile.groups.get("dimensions", None)
            if dimension_group is not None and "time_points" in list(
                dimension_group.variables.keys()
            ):
                completed_index = dimension_group.variables["time_points"].getncattr(
                    "completed_index"
                )
                logger.info(f"Completed index is {completed_index}.")
            else:
                logger.warning(
                    f"No time points found in the netCDF4 file {netcdf4_file}."
                )

        return completed_index

    @staticmethod
    def reset_completed_index_in_netcdf4_file(
        netcdf4_file: str,
    ) -> None:
        """Reset the completed index in a netCDF4 file.

        Args:
            netcdf4_file (str): Path to the netCDF4 file.

        Returns:
            None

        """
        logger.info(
            f"Reset completed index in hdf5 file with path {str(netcdf4_file)}."
        )

        with netCDF4.Dataset(f"{netcdf4_file}", "a", format="NETCDF4") as ncfile:
            if "time_points" in list(ncfile.variables.keys()):
                ncfile.variables["time_points"].setncattr("completed_index", 0)
                logger.info("Completed index reset to 0.")
            else:
                logger.warning("No time points found in the netCDF4 file.")

    def read_meta_data_from_odessa_base(
        self,
        odessa_base: pyod.Base,
        domain: Optional[str] = None,
        element: str = "VOLUME",
        attribute: Union[str, List[str]] = "NAME",
    ) -> List[dict]:
        """Read meta data from the odessa base.

        Returns:
            List[dict]: A list of dictionaries containing metadata.

        """
        meta_data = []
        logger.info(f"Read meta data for {domain} {element} from odessa base.")

        if isinstance(attribute, str):
            attribute = [attribute]

        if domain is None:
            base = odessa_base
            logger.debug("No domain specified, using the entire odessa base.")
        else:
            logger.debug(f"Using domain {domain} from odessa base.")
            path = f"{domain} 1"
            if self.check_if_odessa_path_exists(odessa_base, path):
                base = odessa_base.get(domain)
            else:
                logger.error(f"Domain {domain} not found in odessa base.")
                return meta_data

        number_of_elements = base.len(element)
        logger.debug(
            f"Number of {domain} {element} in odessa base: {number_of_elements}."
        )

        for number in range(1, number_of_elements + 1):
            metadata = {"number": number}
            for attr in attribute:
                path = f"{element} {number}: {attr} 1"
                check_path = f"{domain} 1: {path}" if domain else path
                if self.check_if_odessa_path_exists(odessa_base, check_path):
                    logger.debug(
                        f"Collect {domain} {element} {attr} from path {check_path}."
                    )
                    try:
                        structure = base.get(path)
                    except (ValueError, RuntimeError, Exception) as e:
                        logger.error(
                            f"Error reading {domain} : {element} {number} : {attr} "
                            f"with actual path {path}: {e}."
                        )
                        continue
                else:
                    logger.warning(
                        f"Path {check_path} not found in "
                        f"odessa base for {domain} {element}."
                    )
                    continue

                logger.debug(
                    f"Collect {domain} {element} {attr} structure {structure}."
                )
                metadata[attr.lower()] = structure

            meta_data.append(metadata)

        return meta_data

    def read_meta_data_from_netcdf4(
        self,
        variable_name: str,
        group_name: Optional[str] = None,
    ) -> dict:
        """Read metadata from a NetCDF4 file for a given variable name.

        Args:
            file_path (str): Path to the NetCDF4 file.
            variable_name (str): Name of the variable to read metadata from.
            group_name (str, optional): Name of the group to read from.
                Defaults to None, which reads from the root group.

        Returns:
            dict: A dictionary containing the metadata attributes of the variable.

        """
        try:
            with netCDF4.Dataset(self.output_path, "r") as ncfile:
                if group_name is not None:
                    if group_name not in ncfile.groups:
                        raise ValueError(
                            f"Group '{group_name}' not found in the NetCDF4 file."
                        )
                    ncfile = ncfile.groups[group_name]

                if variable_name not in ncfile.variables:
                    raise ValueError(
                        f"Variable '{variable_name}' not found in the NetCDF4 file."
                    )

                variable = ncfile.variables[variable_name]
                metadata = {}

                # Read all attributes of the variable
                for attr_name in variable.ncattrs():
                    attr_value = variable.getncattr(attr_name)
                    # If the attribute is JSON-encoded, decode it
                    if attr_name == "meta_data":
                        try:
                            attr_value = json.loads(attr_value)
                        except json.JSONDecodeError:
                            pass
                    metadata[attr_name] = attr_value

                return metadata
        except Exception as e:
            raise RuntimeError(
                f"Failed to read metadata from variable '{variable_name}': {e}"
            )

    def convert_metadata_from_odessa_to_netcdf4(
        self,
    ) -> None:
        """Convert metadata from odessa to netCDF4.

        Returns:
            None

        """
        logger.info(
            f"Convert metadata from odessa with path {str(self.input_path)} "
            f"to netCDF4 file with path {str(self.output_path)}."
        )

        meta_data_var_names = META_DATA_VAR_NAMES

        with netCDF4.Dataset(f"{self.output_path}", "a", format="NETCDF4") as ncfile:
            if "metadata" not in ncfile.groups:
                metadata_group = ncfile.createGroup("metadata")
                metadata_group.description = "General metadata variables"
                metadata_group.group_type = "metadata"
                logger.info("Created metadata group")
            else:
                metadata_group = ncfile.groups["metadata"]
                logger.info("Using existing metadata group")

            for meta_data_var_name in list(meta_data_var_names.keys()):
                # Restore Odessa base
                odessa_base = pyod.restore(str(self.input_path), 0)
                meta_data = self.read_meta_data_from_odessa_base(
                    odessa_base,
                    domain=meta_data_var_names[meta_data_var_name]["domain"],
                    element=meta_data_var_names[meta_data_var_name]["element"],
                    attribute=meta_data_var_names[meta_data_var_name]["attribute"],
                )

                # Create dimension for primary volumes
                metadata_group.createDimension(meta_data_var_name, len(meta_data))

                # Create variable for primary volume metadata
                meta_var = metadata_group.createVariable(
                    meta_data_var_name, datatype="S1", dimensions=(meta_data_var_name,)
                )
                meta_var.long_name = (
                    f"meta data variable element for "
                    f"{meta_data_var_names[meta_data_var_name]['domain']}_"
                    f"{meta_data_var_names[meta_data_var_name]['element']}"
                )
                meta_var.unit = "N/A"
                domain_value = META_DATA_VAR_NAMES[meta_data_var_name]["domain"]
                if domain_value is None:
                    domain_value = "None"
                meta_var.domain = domain_value

                # Add metadata dictionary as an attribute
                metadata_dict = {"meta_data": meta_data}
                meta_var.setncattr("meta_data", json.dumps(metadata_dict))

                logger.info(
                    "Added metadata dictionary to 'primary_volume_meta' variable."
                )

    def convert_astec_variables_to_netcdf4(
        self,
        maximum_index: int = None,
    ) -> None:
        """Convert the data for given ASTEC variables from odessa into hdf5.

        Args:
            maximum_index (int): Maximum index to convert. If None, all time points
            are converted.

        Returns:
            None

        """
        logger.info(f"Parse ASTEC data from binary with path {str(self.input_path)}.")

        with netCDF4.Dataset(f"{self.output_path}", "a", format="NETCDF4") as ncfile:
            if "time_points" not in list(ncfile.variables.keys()):
                variable_datasets = {}

                ncfile.createDimension("time", len(self.time_points))
                ncfile.createDimension("mesh", 76)
                ncfile.createDimension("junction_primary", 93)
                ncfile.createDimension("junction_secondar", 76)
                ncfile.createDimension("volume_primary", 85)
                ncfile.createDimension("volume_secondar", 73)
                ncfile.createDimension("face", 140)
                ncfile.createDimension("wall_primary", 92)
                ncfile.createDimension("wall_secondar", 125)
                ncfile.createDimension("wall_containm", 62)
                ncfile.createDimension("connection_break", len(self.connection_break_ids))
                ncfile.createDimension("connection_feedwater", len(self.connection_feedwater_ids))
                ncfile.createDimension("connection_flow", len(self.connection_flow_ids))
                ncfile.createDimension("connection_heat", len(self.connection_heat_ids))
                ncfile.createDimension("connection_mcci", len(self.connection_mcci_ids))
                ncfile.createDimension("connection_source", len(self.connection_source_ids))
                ncfile.createDimension("connection_vespour", len(self.connection_vespour_ids))
                ncfile.createDimension("conn", 68) # only 44 at saving_time 0.
                ncfile.createDimension("component_clad", len(self.clad_ids))
                ncfile.createDimension("component_fuel", len(self.fuel_ids))
                ncfile.createDimension("component_bono", len(self.bono_ids))
                ncfile.createDimension("component_crod", len(self.crod_ids))
                ncfile.createDimension("component_clad_crod", len(self.clad_crod_ids))
                ncfile.createDimension("component_tguide", len(self.tguide_ids))
                ncfile.createDimension("component_tinst", len(self.tinst_ids))
                ncfile.createDimension("component_grid", len(self.grid_ids))
                ncfile.createDimension("component_LP_wall", len(self.LP_wall_ids))
                ncfile.createDimension("component_LP_corium", len(self.LP_corium_ids))
                ncfile.createDimension("cesar_output",  2233 if self.with_cesar_io else 1)
                ncfile.createDimension("wall_profile", 20)
                ncfile.createDimension("sensor", 780)
                ncfile.createDimension("zone", 18)

                time_dataset = ncfile.createVariable(
                    varname="time_points", datatype=np.float32, dimensions="time"
                )
                time_dataset[:] = self.time_points
                time_dataset.completed_index = 0

                for idx, variable in self.variable_index.iterrows():
                    if variable["name"] in list(ncfile.variables.keys()):
                        logger.warning(
                            f"Variable {variable['name']} already "
                            "exists in the netCDF4 file."
                        )
                        continue

                    logger.info(f"Create variable {variable['name']} in netCDF4 file.")
                    dimensions = list(variable["dimension"].split(";"))
                    dimensions.insert(0, "time")
                    dimensions = [
                        dimension for dimension in dimensions if dimension != "none"
                    ]

                    logger.info(
                        f"Create variable dataset for variable {variable['name']} "
                        f"with dimensions {dimensions}."
                    )

                    variable_datasets[variable["name"]] = ncfile.createVariable(
                        varname=variable["name"],
                        datatype=np.float32,
                        dimensions=tuple(dimensions),
                    )

                    variable_datasets[variable["name"]].long_name = variable[
                        "long_name"
                    ]
                    variable_datasets[variable["name"]].unit = variable["unit"]
                    variable_datasets[variable["name"]].domain = variable["domain"]
                    variable_datasets[variable["name"]].strategy = variable["strategy"]

                start_index = 0

            else:
                start_index = (
                    ncfile.variables["time_points"].getncattr("completed_index") + 1
                )

            time_points = self.time_points[start_index:]

            if maximum_index is not None:
                if maximum_index > start_index:
                    time_points = self.time_points[start_index:maximum_index]
                    logger.info(
                        f"Start converting from index {start_index} to {maximum_index}."
                        f" {len(time_points)} time points left."
                    )
                else:
                    logger.warning(
                        f"Requested time points are all converted. "
                        f"{maximum_index} time points are requested but, "
                        f"{start_index} are already completed."
                    )
                    return
            else:
                logger.info(
                    f"Start converting from index {start_index} to "
                    f"{len(self.time_points)}. {len(time_points)} time points left."
                )

            begin = time.perf_counter()


            idx = -1
            for time_point, odessa_base in tqdm(pyod.save_iterator(str(self.input_path), \
                time_points[start_index],time_points[-1]), total=len(time_points[start_index:])):
                idx += 1
                data = assas_odessa_step_extraction.extract_one_time_step(odessa_base)

                for name, values in data.items():
                    ncfile.variables[name][start_index + idx, ...] = values

                ncfile.variables["time_points"].completed_index = start_index + idx


            #progress_bar = tqdm(time_points)
            #for idx, time_point in enumerate(progress_bar):
                #logger.info(f"Restore odessa base for time point {time_point}.")
                #odessa_base = pyod.restore(str(self.input_path), time_point)

                #for _, variable in self.variable_index.iterrows():
                    #if variable["name"] not in list(variable_datasets.keys()):
                        #logger.info(
                            #f"Variable {variable['name']} not required to convert."
                        #)
                        #continue
                    #logger.info(
                        #f"Parse ASTEC variable {variable['name']} for time point "
                        #f"{time_point}."
                    #)
                    #strategy_function = self.variable_strategy_mapping[
                        #variable["strategy"]
                    #]

                    #if np.isnan(variable["index"]):
                        #data_per_timestep = strategy_function(
                            #odessa_base=odessa_base,
                            #variable_name=variable["name_odessa"],
                        #)
                    #else:
                        #data_per_timestep = strategy_function(
                            #odessa_base=odessa_base,
                            #variable_name=variable["name_odessa"],
                            #pos=int(variable["index"]),
                        #)

                    #logger.debug(
                        #f"Read data for {variable['name_odessa']} with "
                        #f"shape {data_per_timestep.shape}. "
                        #f"Odessa index {variable['index']}, "
                        #f"isnan {np.isnan(variable['index'])}."
                    #)

                    #ncfile.variables[variable["name"]][start_index + idx] = (
                        #data_per_timestep
                    #)

                #if progress_bar.n % LOG_INTERVAL == 0:
                    #logger.info(str(progress_bar))

                #ncfile.variables["time_points"].completed_index = start_index + idx


            #end = time.perf_counter()
            #print(f"time : {end - begin:.6f} s")

    def populate_data_from_groups_to_netcdf4(
        self,
        maximum_index: int = None,
    ) -> None:
        """Convert the data for given ASTEC variables from odessa into netCDF4.

        Args:
            maximum_index (int): Maximum index to convert. If None, all time points
            are converted.

        Returns:
            None

        """
        logger.info(f"Parse ASTEC data from binary with path {str(self.input_path)}.")

        with netCDF4.Dataset(f"{self.output_path}", "a", format="NETCDF4") as ncfile:
            dimension_group = ncfile.groups.get("dimensions")
            if dimension_group is None:
                logger.error(
                    "No dimension_groups found in the netCDF4 file. "
                    "Please ensure the file is properly initialized."
                )
                return

            start_index = (
                dimension_group.variables["time_points"].getncattr("completed_index")
                + 1
            )
            logger.info(f"Set start_index to {start_index}.")

            time_points = self.time_points[start_index:]

            if maximum_index is not None:
                if maximum_index > start_index:
                    time_points = self.time_points[start_index:maximum_index]
                    logger.info(
                        f"Start converting from index {start_index} to {maximum_index}."
                        f" {len(time_points)} time points left."
                    )
                else:
                    logger.warning(
                        f"Requested time points are all converted. "
                        f"{maximum_index} time points are requested but, "
                        f"{start_index} are already completed."
                    )
                    return
            else:
                logger.info(
                    f"Start converting from index {start_index} to "
                    f"{len(self.time_points)}. {len(time_points)} time points left."
                )

            # Get all variable datasets from groups and root
            variable_datasets = self.get_all_variable_datasets(ncfile)
            logger.info(f"Found {len(variable_datasets)} variables to populate.")

            progress_bar = tqdm(time_points)
            for idx, time_point in enumerate(progress_bar):
                logger.info(f"Restore odessa base for time point {time_point}.")
                odessa_base = pyod.restore(str(self.input_path), time_point)

                for _, variable in self.variable_index.iterrows():
                    var_name = variable["name"]

                    # Check if variable exists in any location (root or groups)
                    if var_name not in variable_datasets:
                        logger.info(
                            f"Variable {var_name} not found in any location, skipping."
                        )
                        continue

                    # Get the variable dataset and its location info
                    var_info = variable_datasets[var_name]
                    var_dataset = var_info["dataset"]
                    location_path = var_info["location"]

                    logger.info(
                        f"Parse ASTEC variable {var_name} for time point "
                        f"{time_point} in {location_path}."
                    )

                    strategy_function = self.variable_strategy_mapping[
                        variable["strategy"]
                    ]

                    if np.isnan(variable["index"]):
                        data_per_timestep = strategy_function(
                            odessa_base=odessa_base,
                            variable_name=variable["name_odessa"],
                        )
                    else:
                        data_per_timestep = strategy_function(
                            odessa_base=odessa_base,
                            variable_name=variable["name_odessa"],
                            pos=int(variable["index"]),
                        )

                    logger.debug(
                        f"Read data for {variable['name_odessa']} with "
                        f"shape {data_per_timestep.shape}. "
                        f"Odessa index {variable['index']}, "
                        f"isnan {np.isnan(variable['index'])}."
                    )

                    # Populate data in the variable dataset
                    var_dataset[start_index + idx] = data_per_timestep

                if progress_bar.n % LOG_INTERVAL == 0:
                    logger.info(str(progress_bar))

                dimension_group.variables["time_points"].completed_index = (
                    start_index + idx
                )
                logger.info(
                    f"Updated completed index to {start_index + idx} for time points."
                )

    def get_all_variable_datasets(self, ncfile: netCDF4.Dataset) -> dict:
        """Get all variable datasets from root and groups.

        This method now prioritizes variables in groups over root variables.
        """
        variable_datasets = {}

        # First, get variables from groups (these take priority)
        for group_name, group in ncfile.groups.items():
            # Variables directly in group
            for var_name in group.variables.keys():
                variable_datasets[var_name] = {
                    "dataset": group.variables[var_name],
                    "location": f"group/{group_name}",
                    "group": group_name,
                    "subgroup": None,
                }
                logger.debug(f"Found variable {var_name} in group {group_name}.")

            # Variables in subgroups
            for subgroup_name, subgroup in group.groups.items():
                for var_name in subgroup.variables.keys():
                    variable_datasets[var_name] = {
                        "dataset": subgroup.variables[var_name],
                        "location": f"group/{group_name}/{subgroup_name}",
                        "group": group_name,
                        "subgroup": subgroup_name,
                    }
                    logger.debug(
                        f"Found variable {var_name} in "
                        f"subgroup {group_name}/{subgroup_name}."
                    )

        # Then, get variables from root level (only if not already found in groups)
        for var_name in ncfile.variables.keys():
            if var_name == "time_points":
                continue

            # Only add root variables if they're not already in groups
            if var_name not in variable_datasets:
                variable_datasets[var_name] = {
                    "dataset": ncfile.variables[var_name],
                    "location": "root",
                    "group": None,
                    "subgroup": None,
                }
                logger.debug(f"Found variable {var_name} at root level.")
            else:
                # Variable exists in group, mark root version as deprecated
                if hasattr(ncfile.variables[var_name], "moved_to_group"):
                    logger.debug(
                        f"Variable {var_name} at root marked as moved to group"
                    )
                else:
                    logger.warning(
                        f"Variable {var_name} exists in both root and group!"
                    )

        return variable_datasets

    def get_variable_datasets_by_group(
        self, ncfile: netCDF4.Dataset, group_name: str = None
    ) -> dict:
        """Get variable datasets from a specific group or all groups.

        Args:
            ncfile: NetCDF4 dataset object
            group_name (str, optional): Specific group name.
                If None, get from all groups.

        Returns:
            dict: Dictionary mapping variable names to their dataset objects

        """
        variable_datasets = {}

        if group_name is None:
            # Get from all groups
            for gname, group in ncfile.groups.items():
                group_vars = self.extract_variables_from_group(group, gname)
                variable_datasets.update(group_vars)
        else:
            # Get from specific group
            if group_name in ncfile.groups:
                group = ncfile.groups[group_name]
                group_vars = self.extract_variables_from_group(group, group_name)
                variable_datasets.update(group_vars)
            else:
                logger.warning(f"Group {group_name} not found in netCDF4 file")

        return variable_datasets

    def extract_variables_from_group(
        self, group: netCDF4.Group, group_name: str
    ) -> dict:
        """Extract all variables from a group and its subgroups.

        Args:
            group: NetCDF4 group object
            group_name (str): Name of the group

        Returns:
            dict: Dictionary of variables in the group

        """
        variables = {}

        # Variables directly in group
        for var_name in group.variables.keys():
            variables[var_name] = {
                "dataset": group.variables[var_name],
                "location": f"group/{group_name}",
                "group": group_name,
                "subgroup": None,
            }

        # Variables in subgroups
        for subgroup_name, subgroup in group.groups.items():
            for var_name in subgroup.variables.keys():
                variables[var_name] = {
                    "dataset": subgroup.variables[var_name],
                    "location": f"group/{group_name}/{subgroup_name}",
                    "group": group_name,
                    "subgroup": subgroup_name,
                }

        return variables

    def get_variable_locations_summary(self) -> dict:
        """Get a summary of where variables are located in the netCDF4 file.

        Returns:
            dict: Summary of variable locations

        """
        summary = {"root": [], "groups": {}, "total_variables": 0}

        with netCDF4.Dataset(f"{self.output_path}", "r", format="NETCDF4") as ncfile:
            # Root variables
            for var_name in ncfile.variables.keys():
                if var_name != "time_points":
                    summary["root"].append(var_name)

            # Group variables
            for group_name, group in ncfile.groups.items():
                summary["groups"][group_name] = {
                    "direct_variables": list(group.variables.keys()),
                    "subgroups": {},
                }

                for subgroup_name, subgroup in group.groups.items():
                    summary["groups"][group_name]["subgroups"][subgroup_name] = list(
                        subgroup.variables.keys()
                    )

            # Count total variables
            summary["total_variables"] = len(self.get_all_variable_datasets(ncfile))

        return summary

    def populate_specific_group_variables(
        self,
        group_name: str,
        maximum_index: int = None,
    ) -> None:
        """Populate data for variables in a specific group only.

        Args:
            group_name (str): Name of the group to populate
            maximum_index (int): Maximum index to convert

        Returns:
            None

        """
        logger.info(f"Populate variables in group {group_name} from binary data.")

        with netCDF4.Dataset(f"{self.output_path}", "a", format="NETCDF4") as ncfile:
            variable_datasets = self.get_variable_datasets_by_group(ncfile, group_name)
            if not variable_datasets:
                logger.warning(f"No variables found in group {group_name}.")
                return

            if "time_points" not in list(ncfile.variables.keys()):
                start_index = 0
            else:
                start_index = (
                    ncfile.variables["time_points"].getncattr("completed_index") + 1
                )

            time_points = self.time_points[start_index:]

            if maximum_index is not None:
                if maximum_index > start_index:
                    time_points = self.time_points[start_index:maximum_index]
                    logger.info(
                        f"Start converting from index {start_index} to {maximum_index}."
                        f" {len(time_points)} time points left."
                    )
                else:
                    logger.warning(
                        f"Requested time points are all converted. "
                        f"{maximum_index} time points are requested but, "
                        f"{start_index} are already completed."
                    )
                    return
            else:
                logger.info(
                    f"Start converting from index {start_index} to "
                    f"{len(self.time_points)}. {len(time_points)} time points left."
                )

            progress_bar = tqdm(time_points)
            for idx, time_point in enumerate(progress_bar):
                logger.info(f"Restore odessa base for time point {time_point}.")
                odessa_base = pyod.restore(str(self.input_path), time_point)

                for var_name, var_info in variable_datasets.items():
                    var_dataset = var_info["dataset"]
                    location_path = var_info["location"]

                    logger.info(
                        f"Parse ASTEC variable {var_name} for time point "
                        f"{time_point} in {location_path}."
                    )

                    strategy_function = self.variable_strategy_mapping[
                        var_info["dataset"].strategy
                    ]

                    if np.isnan(var_info["dataset"].index):
                        data_per_timestep = strategy_function(
                            odessa_base=odessa_base,
                            variable_name=var_info["dataset"].name_odessa,
                        )
                    else:
                        data_per_timestep = strategy_function(
                            odessa_base=odessa_base,
                            variable_name=var_info["dataset"].name_odessa,
                            index=int(var_info["dataset"].index),
                        )

                    logger.debug(
                        f"Read data for {var_info['dataset'].name_odessa} with "
                        f"shape {data_per_timestep.shape}. "
                        f"Odessa index {var_info['dataset'].index}, "
                        f"isnan {np.isnan(var_info['dataset'].index)}."
                    )

                    # Populate data in the variable dataset
                    var_dataset[start_index + idx] = data_per_timestep

                if progress_bar.n % LOG_INTERVAL == 0:
                    logger.info(str(progress_bar))

    def update_domain_attributes_for_all_variables(self) -> None:
        """Update domain attributes for all variables in the netCDF4 file.

        Returns:
            None

        """
        logger.info(
            f"Updating domain attributes for all variables in {str(self.output_path)}"
        )

        with netCDF4.Dataset(f"{self.output_path}", "a", format="NETCDF4") as ncfile:
            # Get all variables at root level
            variables_updated = 0
            variables_skipped = 0

            for var_name in ncfile.variables.keys():
                if var_name == "time_points":
                    continue  # Skip time_points variable

                var = ncfile.variables[var_name]

                # Find variable in variable_index to get correct domain
                matching_var = self.variable_index[
                    self.variable_index["name"] == var_name
                ]

                if not matching_var.empty:
                    correct_domain = matching_var.iloc[0]["domain"]

                    # Update domain attribute
                    if hasattr(var, "domain"):
                        current_domain = var.domain
                        if current_domain != correct_domain:
                            var.domain = correct_domain
                            variables_updated += 1
                            logger.info(
                                f"Updated domain for {var_name}: "
                                f"{current_domain} -> {correct_domain}."
                            )
                        else:
                            logger.debug(
                                f"Domain for {var_name} already correct: "
                                f"{correct_domain}."
                            )
                    else:
                        var.domain = correct_domain
                        variables_updated += 1
                        logger.info(
                            f"Added domain attribute to {var_name}: {correct_domain}."
                        )
                else:
                    variables_skipped += 1
                    logger.warning(
                        f"Variable {var_name} not found in variable_index, "
                        f"skipping domain update."
                    )

            logger.info(
                f"Domain update complete: {variables_updated} variables "
                f"updated {variables_skipped} skipped."
            )

    def assign_existing_variables_to_groups(self) -> None:
        """Read existing variables from netCDF4 file and assign them to groups.

        Returns:
            None

        """
        logger.info(
            f"Assigning existing variables to groups in {str(self.output_path)}"
        )

        with netCDF4.Dataset(f"{self.output_path}", "a", format="NETCDF4") as ncfile:
            # Get all existing variables at root level
            root_variables = list(ncfile.variables.keys())
            logger.info(f"Found {len(root_variables)} variables at root level")

            # Create a mapping for variables to move
            variables_to_move = {}

            for var_name in root_variables:
                if var_name == "time_points":
                    continue  # Keep time_points at root level

                var = ncfile.variables[var_name]

                # Get domain from variable attributes
                if hasattr(var, "domain"):
                    domain = var.domain
                    group_name, subgroup_name = self.get_group_name_from_domain(domain)

                    if group_name and group_name in ncfile.groups:
                        target_group = ncfile.groups[group_name]
                        if subgroup_name and subgroup_name in target_group.groups:
                            target_group = target_group.groups[subgroup_name]

                        variables_to_move[var_name] = {
                            "target_group": target_group,
                            "variable": var,
                            "group_path": (
                                f"{group_name}/{subgroup_name}"
                                if subgroup_name
                                else group_name
                            ),
                        }

            # Move variables to their appropriate groups
            for var_name, move_info in variables_to_move.items():
                self.move_variable_to_group(ncfile, var_name, move_info)

            logger.info(
                f"Successfully assigned {len(variables_to_move)} variables to groups"
            )

    def move_variable_to_group(
        self,
        ncfile: netCDF4.Dataset,
        var_name: str,
        move_info: dict,
    ) -> None:
        """Move a variable from root to a specific group.

        Args:
            ncfile: NetCDF4 dataset object
            var_name (str): Name of the variable to move
            move_info (dict): Information about where to move the variable

        """
        try:
            # Get original variable
            original_var = ncfile.variables[var_name]
            target_group = move_info["target_group"]
            group_path = move_info["group_path"]

            # Check if variable already exists in target group
            if var_name in target_group.variables:
                logger.warning(
                    f"Variable {var_name} already exists in group {group_path}"
                )
                return

            # Create new variable in target group
            new_var = target_group.createVariable(
                varname=var_name,
                datatype=original_var.dtype,
                dimensions=original_var.dimensions,
                fill_value=getattr(original_var, "_FillValue", None),
            )

            # Copy data
            new_var[:] = original_var[:]

            # Copy all attributes
            for attr_name in original_var.ncattrs():
                attr_value = original_var.getncattr(attr_name)
                new_var.setncattr(attr_name, attr_value)

            # Add group information to variable
            new_var.setncattr("group_path", group_path)
            new_var.setncattr("moved_from_root", True)

            logger.info(f"Successfully moved variable {var_name} to group {group_path}")

            # Note: Cannot delete original variable in netCDF4,
            # but we can mark it as moved
            original_var.setncattr("moved_to_group", group_path)
            original_var.setncattr("deprecated", True)

        except Exception as e:
            logger.error(
                f"Failed to move variable {var_name} to group "
                f"{move_info['group_path']}: {e}."
            )

    def create_variable_with_unit(
        self,
        target_group: netCDF4.Group,
        dimensions_group: netCDF4.Group,
        var_name: str,
        var_dimensions: tuple,
        unit_str: str,
        long_name: str,
        data_type: np.float32 = np.float32,
    ) -> netCDF4.Variable:
        """Create NetCDF4 variable with proper unit handling."""
        # Validate and normalize unit
        is_valid, normalized_unit, validation_info = self.unit_manager.validate_unit(
            unit_str
        )

        if not is_valid:
            logger.warning(
                f"Invalid unit '{unit_str}' for {var_name}: {validation_info}"
            )
            normalized_unit = "dimensionless"

        # Create variable
        for dim_name in var_dimensions:
            if dim_name not in target_group.dimensions:
                if dim_name in dimensions_group.dimensions:
                    # Copy dimension from dimensions group
                    source_dim = dimensions_group.dimensions[dim_name]
                    size = len(source_dim) if not source_dim.isunlimited() else None
                    target_group.createDimension(dim_name, size)
                    logger.debug(
                        f"Copied dimension {dim_name} to group {target_group.name}"
                    )
                else:
                    logger.error(f"Dimension {dim_name} not found in dimensions group")
                    raise ValueError(f"Required dimension {dim_name} not available")

        # Now create the variable
        var = target_group.createVariable(
            var_name,
            data_type,
            var_dimensions,
        )

        # Set attributes with normalized unit
        var.unit = normalized_unit
        var.long_name = long_name

        # Add original unit if different
        if normalized_unit != unit_str:
            var.original_unit = unit_str

        # Add validation info if available
        if validation_info:
            var.unit_validation = validation_info

        # Add CF standard name if possible
        standard_name = self.unit_manager.get_cf_standard_name(
            var_name, normalized_unit
        )
        if standard_name:
            var.standard_name = standard_name

        logger.debug(f"Created variable {var_name} with unit: {normalized_unit}")
        return var

    def initialize_astec_variables_in_netcdf4(self) -> None:
        """Initialize ASTEC variables in netCDF4 file with proper unit handling."""
        logger.info(f"Initialize ASTEC variables with unit in {str(self.output_path)}")

        with netCDF4.Dataset(f"{self.output_path}", "a", format="NETCDF4") as ncfile:
            # First, initialize groups if they don't exist
            if not ncfile.groups:
                logger.info("No groups found, initializing groups first")
                self.create_groups_in_ncfile(ncfile)

            variable_datasets = {}

            # Initialize dimensions at root level
            dimension_list = self.variable_index["dimension"].unique().tolist()
            dimension_list = [
                dimension.split(";") if ";" in dimension else dimension
                for dimension in dimension_list
            ]
            dimension_list = [
                item
                for sublist in dimension_list
                for item in (sublist if isinstance(sublist, list) else [sublist])
            ]
            if "none" in dimension_list:
                dimension_list.remove("none")
            logger.info(
                f"Found {len(dimension_list)} unique dimensions: {dimension_list}"
            )

            dimensions_group = ncfile.createGroup("dimensions")
            dimensions_group.description = "Group for dataset dimensions"

            for dimension in dimension_list:
                if dimension not in list(dimensions_group.dimensions.keys()):
                    logger.info(f"Create dimension {dimension} in netCDF4 file.")
                    dimensions_group.createDimension(dimension, None)

            dimensions_group.createDimension("time", len(self.time_points))

            # Create time variable with proper unit
            time_dataset = self.create_variable_with_unit(
                dimensions_group,
                dimensions_group,
                "time_points",
                ("time",),
                "seconds",
                "Time points from ASTEC simulation",
                np.float32,
            )
            time_dataset[:] = self.time_points
            time_dataset.completed_index = -1
            time_dataset.domain = "global"
            time_dataset.variable_type = "data"

            # Create variables with proper unit handling
            for _, variable in self.variable_index.iterrows():
                var_name = variable["name"]

                if var_name in list(ncfile.variables.keys()):
                    logger.warning(
                        f"Variable {var_name} already exists in the netCDF4 file."
                    )
                    continue

                # Determine target group for this variable
                domain = variable["domain"]
                group_name, subgroup_name = self.get_group_name_from_domain(domain)

                # Get target group/subgroup or use root if no match
                target_location = self.get_target_location(
                    ncfile, group_name, subgroup_name
                )
                location_path = self.get_location_path(group_name, subgroup_name)

                logger.info(f"Create variable {var_name} in {location_path}")

                # Prepare dimensions
                dimensions = list(variable["dimension"].split(";"))
                dimensions.insert(0, "time")
                dimensions = [d for d in dimensions if d != "none"]

                logger.info(
                    f"Create variable dataset for {var_name} with "
                    f"dimensions {dimensions} in {location_path}"
                )

                # Create variable with proper unit handling
                try:
                    var_dataset = self.create_variable_with_unit(
                        target_location,
                        dimensions_group,
                        var_name,
                        tuple(dimensions),
                        variable["unit"],
                        variable["long_name"],
                    )

                    # Set additional ASTEC-specific attributes
                    var_dataset.domain = variable["domain"]
                    var_dataset.strategy = variable["strategy"]
                    var_dataset.variable_type = "data"

                    # Add group information
                    if group_name:
                        var_dataset.group_assignment = group_name
                        if subgroup_name:
                            var_dataset.subgroup_assignment = subgroup_name
                            var_dataset.full_group_path = (
                                f"{group_name}/{subgroup_name}"
                            )
                        else:
                            var_dataset.full_group_path = group_name
                    else:
                        var_dataset.group_assignment = "root"
                        var_dataset.full_group_path = "root"

                    # Add to tracking dictionary
                    variable_datasets[var_name] = {
                        "dataset": var_dataset,
                        "location": location_path,
                        "group": group_name,
                        "subgroup": subgroup_name,
                    }

                    logger.info(
                        f"Successfully created variable {var_name} in {location_path}."
                    )

                except Exception as e:
                    logger.error(
                        f"Failed to create variable {var_name} in {location_path}: {e}."
                    )
                    continue

            # Add global unit metadata
            self.add_unit_metadata_to_file(ncfile)

            # Log summary
            self.log_variable_assignment_summary(variable_datasets)

    def add_unit_metadata_to_file(self, ncfile: netCDF4.Dataset) -> None:
        """Add global unit metadata to the NetCDF4 file."""
        # Add unit registry info
        unique_unit = set()
        self.collect_unit_from_variables(ncfile, unique_unit)

        ncfile.setncattr("units_used", "; ".join(sorted(unique_unit)))
        logger.info(f"Added global unit metadata with {len(unique_unit)} unique units")

    def collect_unit_from_variables(
        self, ncfile: netCDF4.Dataset, unique_unit: set
    ) -> None:
        """Recursively collect unit from all variables in file and groups."""
        # Collect from root variables
        for var_name, var in ncfile.variables.items():
            if hasattr(var, "unit"):
                unique_unit.add(var.unit)

        # Collect from group variables
        for group_name, group in ncfile.groups.items():
            self.collect_unit_from_variables(group, unique_unit)

    def create_missing_group(
        self,
        ncfile: netCDF4.Dataset,
        group_name: str,
        subgroup_name: str = None,
    ) -> None:
        """Create missing group and subgroup.

        Args:
            ncfile: NetCDF4 dataset object
            group_name (str): Name of the group to create
            subgroup_name (str): Name of the subgroup to create

        """
        if group_name not in ncfile.groups:
            main_group = ncfile.createGroup(group_name)
            main_group.description = f"Auto-created group for {group_name}"
            main_group.auto_created = True
            logger.info(f"Auto-created group: {group_name}.")

            if subgroup_name:
                subgroup = main_group.createGroup(subgroup_name)
                subgroup.description = (
                    f"Auto-created subgroup: {group_name} - {subgroup_name}."
                )
                subgroup.auto_created = True
                logger.info(f"Auto-created subgroup: {group_name}/{subgroup_name}.")

    def get_location_path(
        self,
        group_name: str,
        subgroup_name: str = None,
    ) -> str:
        """Get a human-readable path for the target location.

        Args:
            group_name (str): Name of the group
            subgroup_name (str): Name of the subgroup

        Returns:
            str: Human-readable location path

        """
        if group_name:
            if subgroup_name:
                logger.debug(
                    f"Creating location path for group {group_name} "
                    f"and subgroup {subgroup_name}."
                )
                return f"group/{group_name}/{subgroup_name}"
            else:
                logger.debug(f"Creating location path for group {group_name}.")
                return f"group/{group_name}"
        else:
            return "root"

    def log_variable_assignment_summary(
        self,
        variable_datasets: dict,
    ) -> None:
        """Log a summary of variable assignments to groups.

        Args:
            variable_datasets (dict): Dictionary of variable assignments

        """
        # Count variables by location
        location_counts = {}
        for var_name, var_info in variable_datasets.items():
            location = var_info["location"]
            if location not in location_counts:
                location_counts[location] = []
            location_counts[location].append(var_name)

        logger.info("Variable assignment summary:")
        for location, var_list in location_counts.items():
            logger.info(f"{location}: {len(var_list)} variables")
            logger.debug(
                f"Variables: "
                f"{', '.join(var_list[:5])}{'...' if len(var_list) > 5 else ''}"
            )

        logger.info(f"Total variables created: {len(variable_datasets)}")

    def get_group_name_from_domain(self, domain_name: str) -> tuple:
        """Get the group name and subgroup name from a domain name.

        Args:
            domain_name (str): The domain name to look up

        Returns:
            tuple: (group_name, subgroup_name) where subgroup_name could be None
                Returns (None, None) if domain is not found

        """
        domain_lower = domain_name.lower().strip()

        # Search through DOMAIN_GROUP_CONFIG
        for group_name, config in DOMAIN_GROUP_CONFIG.items():
            # Check if domain matches the odessa_name exactly
            if config.get("odessa_name", "").lower() == domain_lower:
                return (group_name, None)

            # Check if domain is in the domains list
            if "domains" in config:
                for domain_item in config["domains"]:
                    if domain_item.lower() == domain_lower:
                        # Find the appropriate subgroup for this domain
                        target_subgroup = self._find_subgroup_for_domain(
                            group_name, domain_item
                        )
                        return (group_name, target_subgroup)

            # Check if domain starts with the group's odessa_name (partial match)
            odessa_name = config.get("odessa_name", "").lower()
            if odessa_name and domain_lower.startswith(odessa_name):
                return (group_name, None)

        # If no match found, return None
        return (None, None)

    def _find_subgroup_for_domain(self, group_name: str, domain: str) -> str:
        """Find the appropriate subgroup for a domain within a group.

        Args:
            group_name (str): The main group name
            domain (str): The domain to find subgroup for

        Returns:
            str: The subgroup name, or None if not found

        """
        if group_name not in DOMAIN_GROUP_CONFIG:
            return None

        group_config = DOMAIN_GROUP_CONFIG[group_name]
        if "subgroups" not in group_config:
            return None

        # Look through subgroups to find one that contains this domain
        for subgroup_name, subgroup_config in group_config["subgroups"].items():
            if subgroup_name == "metadata":  # Skip metadata subgroups
                continue

            if "domains" in subgroup_config:
                if domain in subgroup_config["domains"]:
                    return subgroup_name

        # If no specific subgroup found, return the first non-metadata subgroup
        for subgroup_name in group_config["subgroups"].keys():
            if subgroup_name != "metadata":
                return subgroup_name

        return None

    def create_groups_in_ncfile(self, ncfile: netCDF4.Dataset) -> None:
        """Create groups in ncfile based on DOMAIN_GROUP_CONFIG.

        Args:
            ncfile: NetCDF4 dataset object

        """
        groups = {}
        for group_name, config in DOMAIN_GROUP_CONFIG.items():
            # Skip global_metadata group if it has no odessa_name
            if config.get("odessa_name") is None and group_name == "global_metadata":
                continue

            groups[group_name] = ncfile.createGroup(group_name)
            groups[group_name].description = config["description"]

            if config.get("odessa_name"):
                groups[group_name].odessa_name = config["odessa_name"]

            logger.info(f"Created group: {group_name}")

            # Create subgroups based on the enhanced configuration
            if "subgroups" in config:
                for subgroup_name, subgroup_config in config["subgroups"].items():
                    subgroup = groups[group_name].createGroup(subgroup_name)
                    subgroup.description = subgroup_config["description"]

                    # Add subgroup-specific attributes
                    if "domains" in subgroup_config:
                        subgroup.applicable_domains = "; ".join(
                            subgroup_config["domains"]
                        )

                    if "metadata_vars" in subgroup_config:
                        subgroup.metadata_variables = "; ".join(
                            subgroup_config["metadata_vars"]
                        )
                        subgroup.group_type = (
                            "metadata" if subgroup_name == "metadata" else "data"
                        )

                    logger.info(f"Created subgroup: {group_name}/{subgroup_name}")

    def initialize_groups_in_netcdf4(self) -> None:
        """Initialize groups in the netCDF4 file using enhanced configuration.

        Returns:
            None

        """
        logger.info(
            f"Initialize groups in netCDF4 file with path {str(self.output_path)}."
        )

        with netCDF4.Dataset(f"{self.output_path}", "a", format="NETCDF4") as ncfile:
            self.create_groups_in_ncfile(ncfile)
            logger.info("Successfully created enhanced group structure")

    def get_target_location(
        self,
        ncfile: netCDF4.Dataset,
        group_name: str,
        subgroup_name: str = None,
    ) -> netCDF4.Group:
        """Get the target location (group/subgroup) for variable creation.

        Args:
            ncfile: NetCDF4 dataset object
            group_name (str): Name of the target group
            subgroup_name (str): Name of the target subgroup

        Returns:
            NetCDF4 group object or root dataset

        """
        if group_name and group_name in ncfile.groups:
            target_group = ncfile.groups[group_name]

            if subgroup_name and subgroup_name in target_group.groups:
                return target_group.groups[subgroup_name]
            else:
                # If no specific subgroup, try to find the best match
                best_subgroup = self.find_best_subgroup_for_variable(
                    group_name, target_group
                )
                if best_subgroup:
                    return best_subgroup
                return target_group
        else:
            # Create missing groups if needed
            if group_name:
                logger.warning(f"Group {group_name} not found, creating it.")
                self.create_missing_group_enhanced(ncfile, group_name, subgroup_name)
                return self.get_target_location(ncfile, group_name, subgroup_name)

            # Fall back to root level
            logger.info("No matching group found, placing variable at root level.")
            return ncfile

    def find_best_subgroup_for_variable(
        self,
        group_name: str,
        target_group: netCDF4.Group,
    ) -> netCDF4.Group:
        """Find the best subgroup for a variable based on the enhanced configuration.

        Args:
            group_name (str): Name of the main group
            target_group: NetCDF4 group object

        Returns:
            NetCDF4 group object or None

        """
        if group_name not in DOMAIN_GROUP_CONFIG:
            return None

        group_config = DOMAIN_GROUP_CONFIG[group_name]
        if "subgroups" not in group_config:
            return None

        # Find the first non-metadata subgroup
        for subgroup_name, subgroup_config in group_config["subgroups"].items():
            if subgroup_name != "metadata" and subgroup_name in target_group.groups:
                return target_group.groups[subgroup_name]

        return None

    def create_missing_group_enhanced(
        self,
        ncfile: netCDF4.Dataset,
        group_name: str,
        subgroup_name: str = None,
    ) -> None:
        """Create missing group and subgroup using enhanced configuration.

        Args:
            ncfile: NetCDF4 dataset object
            group_name (str): Name of the group to create
            subgroup_name (str): Name of the subgroup to create

        """
        if group_name not in ncfile.groups:
            # Check if group exists in configuration
            if group_name in DOMAIN_GROUP_CONFIG:
                config = DOMAIN_GROUP_CONFIG[group_name]
                main_group = ncfile.createGroup(group_name)
                main_group.description = config["description"]

                if config.get("odessa_name"):
                    main_group.odessa_name = config["odessa_name"]

                logger.info(f"Created group from config: {group_name}")

                # Create subgroups from configuration
                if "subgroups" in config:
                    for sg_name, sg_config in config["subgroups"].items():
                        subgroup = main_group.createGroup(sg_name)
                        subgroup.description = sg_config["description"]

                        if "domains" in sg_config:
                            subgroup.applicable_domains = "; ".join(
                                sg_config["domains"]
                            )

                        if "metadata_vars" in sg_config:
                            subgroup.metadata_variables = "; ".join(
                                sg_config["metadata_vars"]
                            )
                            subgroup.group_type = (
                                "metadata" if sg_name == "metadata" else "data"
                            )

                        logger.info(
                            f"Created subgroup from config: {group_name}/{sg_name}"
                        )
            else:
                # Fallback for unknown groups
                main_group = ncfile.createGroup(group_name)
                main_group.description = f"Auto-created group for {group_name}"
                main_group.auto_created = True
                logger.info(f"Auto-created group: {group_name}")

                if subgroup_name:
                    subgroup = main_group.createGroup(subgroup_name)
                    subgroup.description = (
                        f"Auto-created subgroup: {group_name} - {subgroup_name}"
                    )
                    subgroup.auto_created = True
                    logger.info(f"Auto-created subgroup: {group_name}/{subgroup_name}")

    def create_metadata_variables_in_groups(self) -> None:
        """Create metadata variables in their designated groups.

        This method uses the enhanced configuration to create metadata variables
        Create metadata variables in their designated groups based on enhanced config.
        """
        logger.info("Creating metadata variables in designated groups")

        with netCDF4.Dataset(f"{self.output_path}", "a", format="NETCDF4") as ncfile:
            for meta_var_name, meta_config in META_DATA_VAR_NAMES.items():
                target_group_path = meta_config.get(
                    "target_group", "global_metadata/simulation"
                )

                # Navigate to target group
                target_group = self.navigate_to_group(ncfile, target_group_path)

                if target_group is None:
                    logger.warning(
                        f"Target group {target_group_path} "
                        f"not found for {meta_var_name}. Skipping creation."
                    )
                    continue

                # Create metadata variable
                self.create_metadata_variable_enhanced(
                    target_group, meta_var_name, meta_config, target_group_path
                )

    def navigate_to_group(
        self, ncfile: netCDF4.Dataset, group_path: str
    ) -> netCDF4.Group:
        """Navigate to a nested group using path notation."""
        if not group_path or group_path == "root":
            return ncfile

        try:
            path_parts = group_path.split("/")
            current_location = ncfile

            for part in path_parts:
                if part in current_location.groups:
                    current_location = current_location.groups[part]
                else:
                    logger.warning(
                        f"Group part '{part}' not found in path '{group_path}'"
                    )
                    return None

            return current_location

        except Exception as e:
            logger.error(f"Error navigating to group {group_path}: {e}")
            return None

    def create_metadata_variable_enhanced(
        self,
        target_group: netCDF4.Group,
        meta_var_name: str,
        meta_config: dict,
        group_path: str,
    ) -> None:
        """Create a metadata variable in the target group with enhanced attributes."""
        try:
            # Determine dimensions for metadata variable
            attributes = meta_config["attribute"]
            if isinstance(attributes, list):
                max_attr_len = (
                    max(len(attr) for attr in attributes) if attributes else 50
                )
                dimensions = (f"{meta_var_name}_count", f"{meta_var_name}_string_len")

                # Create dimensions if they don't exist
                if f"{meta_var_name}_count" not in target_group.dimensions:
                    target_group.createDimension(f"{meta_var_name}_count", None)
                if f"{meta_var_name}_string_len" not in target_group.dimensions:
                    target_group.createDimension(
                        f"{meta_var_name}_string_len", max_attr_len
                    )
            else:
                dimensions = (f"{meta_var_name}_count", f"{meta_var_name}_string_len")

                if f"{meta_var_name}_count" not in target_group.dimensions:
                    target_group.createDimension(f"{meta_var_name}_count", None)
                if f"{meta_var_name}_string_len" not in target_group.dimensions:
                    target_group.createDimension(f"{meta_var_name}_string_len", 50)

            # Create the metadata variable
            meta_var = target_group.createVariable(
                meta_var_name,
                "S1",  # String type
                dimensions,
            )

            # Set metadata variable attributes using enhanced config
            meta_var.description = meta_config.get(
                "description", f"Metadata for {meta_var_name}"
            )
            meta_var.variable_type = "metadata"

            if meta_config.get("domain"):
                meta_var.source_domain = meta_config["domain"]
            meta_var.source_element = meta_config["element"]

            if isinstance(attributes, list):
                meta_var.attributes = "; ".join(attributes)
            else:
                meta_var.attributes = attributes

            meta_var.group_location = group_path
            meta_var.target_group = meta_config.get("target_group", "")

            logger.info(
                f"Created enhanced metadata variable {meta_var_name} in {group_path}"
            )

        except Exception as e:
            logger.error(f"Failed to create metadata variable {meta_var_name}: {e}")

    def populate_metadata_variables_in_domain_groups(self) -> None:
        """Populate the meta data for the meta data variables in the domain groups."""
        logger.info("Populating meta data variables in domain groups.")

        with netCDF4.Dataset(f"{self.output_path}", "a", format="NETCDF4") as ncfile:
            for group_name, group_config in DOMAIN_GROUP_CONFIG.items():
                logger.info(f"Processing group: {group_name}")
                if "subgroups" not in group_config:
                    continue
                for subgroup_name, subgroup_config in group_config["subgroups"].items():
                    if "metadata_vars" not in subgroup_config:
                        continue

                    # Get the group and subgroup objects
                    if group_name not in ncfile.groups:
                        logger.warning(f"Group {group_name} not found in file.")
                        continue
                    group = ncfile.groups[group_name]
                    if subgroup_name not in group.groups:
                        logger.warning(
                            f"Subgroup {subgroup_name} not found in group {group_name}."
                        )
                        continue
                    subgroup = group.groups[subgroup_name]

                    for meta_var_name in subgroup_config["metadata_vars"]:
                        if meta_var_name not in META_DATA_VAR_NAMES:
                            logger.warning(
                                f"Meta data variable {meta_var_name} "
                                f"not in META_DATA_VAR_NAMES."
                            )
                            continue
                        meta_config = META_DATA_VAR_NAMES[meta_var_name]

                        # Read meta data from Odessa
                        odessa_base = pyod.restore(str(self.input_path), 0)
                        meta_data = self.read_meta_data_from_odessa_base(
                            odessa_base,
                            domain=meta_config.get("domain"),
                            element=meta_config.get("element"),
                            attribute=meta_config.get("attribute"),
                        )

                        # Write meta data to the variable in the NetCDF4 file
                        if meta_var_name not in subgroup.variables:
                            logger.warning(
                                f"Meta data variable {meta_var_name} not "
                                f"found in {group_name}/{subgroup_name}."
                            )
                            continue

                        meta_var = subgroup.variables[meta_var_name]
                        # Store as JSON string attribute
                        meta_var.setncattr(
                            "meta_data", json.dumps({"meta_data": meta_data})
                        )
                        logger.info(
                            f"Populated meta data for {meta_var_name} in "
                            f"{group_name}/{subgroup_name}."
                        )

    def assign_variables_to_enhanced_groups(self) -> None:
        """Assign existing variables to enhanced groups.

        Assign data variables to appropriate groups and metadata variables to
        metadata groups.
        """
        logger.info("Assigning variables to enhanced group structure")

        with netCDF4.Dataset(f"{self.output_path}", "a", format="NETCDF4") as ncfile:
            # First, move data variables to data groups
            self.move_data_variables_to_groups(ncfile)

            # First, assign data variables to data groups
            self.assign_data_variables_enhanced(ncfile)

            # Then, assign metadata variables to metadata groups
            self.assign_metadata_variables_enhanced(ncfile)

            # Finally, create cross-references
            self.create_cross_references_enhanced(ncfile)

    def move_data_variables_to_groups(self, ncfile: netCDF4.Dataset) -> None:
        """Actually move data variables from root to appropriate groups."""
        variables_to_move = []
        variables_moved = 0
        variables_failed = 0

        # First pass: identify variables to move
        for _, variable in self.variable_index.iterrows():
            var_name = variable["name"]
            domain = variable["domain"]
            strategy = variable["strategy"]

            # Skip if variable doesn't exist at root
            if var_name not in ncfile.variables:
                continue

            # Skip time_points - keep it at root
            if var_name == "time_points":
                continue

            # Determine target group and subgroup
            group_name, subgroup_name = self.get_group_name_from_domain(domain)

            if group_name and group_name in ncfile.groups:
                target_group = ncfile.groups[group_name]

                # Determine specific subgroup
                target_subgroup = self.determine_data_subgroup_enhanced(
                    target_group, domain, strategy, group_name
                )

                if target_subgroup:
                    target_location = target_subgroup
                    target_path = f"{group_name}/{target_subgroup.name.split('/')[-1]}"
                else:
                    target_location = target_group
                    target_path = group_name

                variables_to_move.append(
                    {
                        "var_name": var_name,
                        "source_var": ncfile.variables[var_name],
                        "target_location": target_location,
                        "target_path": target_path,
                        "group_name": group_name,
                        "subgroup_name": subgroup_name,
                    }
                )

        logger.info(f"Identified {len(variables_to_move)} variables to move")

        # Second pass: actually move the variables
        for move_info in variables_to_move:
            try:
                success = self.move_single_variable_to_group(ncfile, move_info)
                if success:
                    variables_moved += 1
                else:
                    variables_failed += 1
            except Exception as e:
                logger.error(f"Failed to move variable {move_info['var_name']}: {e}")
                variables_failed += 1

        logger.info(
            f"Variable movement complete: {variables_moved} moved, "
            f"{variables_failed} failed"
        )

    def move_single_variable_to_group(
        self, ncfile: netCDF4.Dataset, move_info: dict
    ) -> bool:
        """Move a single variable from root to target group."""
        var_name = move_info["var_name"]
        source_var = move_info["source_var"]
        target_location = move_info["target_location"]
        target_path = move_info["target_path"]

        try:
            # Check if variable already exists in target location
            if var_name in target_location.variables:
                logger.warning(f"Variable {var_name} already exists in {target_path}")
                return False

            # Create dimensions in target location if they don't exist
            self.ensure_dimensions_exist(source_var, target_location, ncfile)

            # Create new variable in target location
            new_var = target_location.createVariable(
                varname=var_name,
                datatype=source_var.dtype,
                dimensions=source_var.dimensions,
                fill_value=getattr(source_var, "_FillValue", None),
            )

            # Copy all data
            new_var[:] = source_var[:]

            # Copy all attributes
            for attr_name in source_var.ncattrs():
                if attr_name not in ["_FillValue"]:  # Skip special attributes
                    attr_value = source_var.getncattr(attr_name)
                    new_var.setncattr(attr_name, attr_value)

            # Add movement tracking attributes
            new_var.setncattr("moved_from_root", 1)
            new_var.setncattr("moved_from_root_2", 88)
            new_var.setncattr("target_group_path", target_path)
            new_var.setncattr("enhanced_group_assignment", move_info["group_name"])
            if move_info["subgroup_name"]:
                new_var.setncattr(
                    "enhanced_subgroup_assignment", move_info["subgroup_name"]
                )

            # Mark original variable as moved (can't delete in NetCDF4)
            source_var.setncattr("moved_to_group", target_path)
            source_var.setncattr("deprecated", 1)
            source_var.setncattr("replacement_location", target_path)

            logger.info(f"Successfully moved variable {var_name} to {target_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to move variable {var_name} to {target_path}: {e}")
            return False

    def ensure_dimensions_exist(
        self,
        source_var: netCDF4.Variable,
        target_location: netCDF4.Group,
        ncfile: netCDF4.Dataset,
    ) -> None:
        """Ensure all required dimensions exist in target location."""
        for dim_name in source_var.dimensions:
            if dim_name not in target_location.dimensions:
                # Get dimension size from source
                if dim_name in ncfile.dimensions:
                    dim_size = (
                        len(ncfile.dimensions[dim_name])
                        if not ncfile.dimensions[dim_name].isunlimited()
                        else None
                    )
                else:
                    # Dimension might be in root, use None for unlimited
                    dim_size = None

                target_location.createDimension(dim_name, dim_size)
                logger.debug(f"Created dimension {dim_name} in target location")

    def assign_data_variables_enhanced(self, ncfile: netCDF4.Dataset) -> None:
        """Assign data variables to appropriate data subgroups using enhanced config."""
        for _, variable in self.variable_index.iterrows():
            var_name = variable["name"]
            domain = variable["domain"]
            strategy = variable["strategy"]

            # Determine target group and subgroup using enhanced logic
            group_name, subgroup_name = self.get_group_name_from_domain(domain)

            if group_name and group_name in ncfile.groups:
                target_group = ncfile.groups[group_name]

                # Determine specific subgroup based on enhanced configuration
                target_subgroup = self.determine_data_subgroup_enhanced(
                    target_group, domain, strategy, group_name
                )

                if target_subgroup and var_name in ncfile.variables:
                    # Add metadata about group assignment to existing variable
                    var = ncfile.variables[var_name]
                    var.enhanced_group_assignment = group_name
                    if hasattr(target_subgroup, "name"):
                        var.enhanced_subgroup_assignment = target_subgroup.name.split(
                            "/"
                        )[-1]
                        var.enhanced_full_path = (
                            f"{group_name}/{target_subgroup.name.split('/')[-1]}"
                        )
                    else:
                        var.enhanced_full_path = group_name

                    logger.info(
                        f"Enhanced assignment: {var_name} -> {var.enhanced_full_path}"
                    )

    def determine_data_subgroup_enhanced(
        self, target_group: netCDF4.Group, domain: str, strategy: str, group_name: str
    ) -> netCDF4.Group:
        """Determine the appropriate data subgroup using enhanced configuration."""
        if group_name in DOMAIN_GROUP_CONFIG:
            group_config = DOMAIN_GROUP_CONFIG[group_name]
            subgroups = group_config.get("subgroups", {})

            # First, try to find subgroup that explicitly contains this domain
            for subgroup_name, subgroup_config in subgroups.items():
                if subgroup_name == "metadata":  # Skip metadata subgroups
                    continue

                if "domains" in subgroup_config:
                    if domain in subgroup_config["domains"]:
                        if subgroup_name in target_group.groups:
                            return target_group.groups[subgroup_name]

        return target_group

    def assign_metadata_variables_enhanced(self, ncfile: netCDF4.Dataset) -> None:
        """Assign metadata variables to metadata subgroups using enhanced config."""
        for meta_var_name, meta_config in META_DATA_VAR_NAMES.items():
            target_group_path = meta_config.get(
                "target_group", "global_metadata/simulation"
            )

            # Check if metadata variable exists
            target_location = self.navigate_to_group(ncfile, target_group_path)

            if target_location and meta_var_name not in target_location.variables:
                # Create the metadata variable if it doesn't exist
                self.create_metadata_variable_enhanced(
                    target_location, meta_var_name, meta_config, target_group_path
                )

    def create_cross_references_enhanced(self, ncfile: netCDF4.Dataset) -> None:
        """Create enhanced cross-references between data and metadata variables."""
        # Add global attributes about enhanced structure
        metadata_groups = []
        data_groups = []
        self.collect_enhanced_groups(ncfile, metadata_groups, data_groups, "")

        ncfile.setncattr("metadata_groups", "; ".join(metadata_groups))
        ncfile.setncattr("data_groups", "; ".join(data_groups))
        ncfile.setncattr("structure_version", "enhanced_config_v1")
        ncfile.setncattr(
            "metadata_organization", "grouped by system component with subgroups"
        )

        logger.info(
            f"Created enhanced cross-references: {len(metadata_groups)} "
            f"metadata groups, {len(data_groups)} data groups"
        )

    def collect_enhanced_groups(
        self,
        location: netCDF4.Dataset,
        metadata_groups: list,
        data_groups: list,
        current_path: str,
    ) -> None:
        """Recursively collect all enhanced group paths."""
        for group_name, group in location.groups.items():
            group_path = f"{current_path}/{group_name}" if current_path else group_name

            # Check if this is a metadata group
            if hasattr(group, "group_type"):
                if group.group_type == "metadata":
                    metadata_groups.append(group_path)
                else:
                    data_groups.append(group_path)
            else:
                # Check by name convention
                if group_name == "metadata":
                    metadata_groups.append(group_path)
                else:
                    data_groups.append(group_path)

            # Recurse into subgroups
            self.collect_enhanced_groups(
                group, metadata_groups, data_groups, group_path
            )

    def verify_variable_movement(self) -> dict:
        """Verify that variables have been properly moved to groups."""
        verification = {
            "root_variables": [],
            "moved_variables": [],
            "group_variables": {},
            "deprecated_variables": [],
        }

        with netCDF4.Dataset(f"{self.output_path}", "r", format="NETCDF4") as ncfile:
            # Check root variables
            for var_name, var in ncfile.variables.items():
                if hasattr(var, "deprecated") and var.deprecated:
                    verification["deprecated_variables"].append(var_name)
                else:
                    verification["root_variables"].append(var_name)

            # Check group variables
            for group_name, group in ncfile.groups.items():
                verification["group_variables"][group_name] = []

                for var_name, var in group.variables.items():
                    verification["group_variables"][group_name].append(var_name)
                    if hasattr(var, "moved_from_root") and var.moved_from_root:
                        verification["moved_variables"].append(
                            f"{group_name}/{var_name}"
                        )

                # Check subgroups
                for subgroup_name, subgroup in group.groups.items():
                    subgroup_key = f"{group_name}/{subgroup_name}"
                    verification["group_variables"][subgroup_key] = []

                    for var_name, var in subgroup.variables.items():
                        verification["group_variables"][subgroup_key].append(var_name)
                        if hasattr(var, "moved_from_root") and var.moved_from_root:
                            verification["moved_variables"].append(
                                f"{subgroup_key}/{var_name}"
                            )

        return verification

    def get_root_dimensions_info(self) -> dict:
        """Get information about root-level dimensions using netCDF4."""
        dimensions_info = {
            "unlimited_dimensions": [],
            "fixed_dimensions": {},
            "dimension_variables": [],
        }

        with netCDF4.Dataset(self.output_path, "r") as ncfile:
            # Get root dimensions
            for dim_name, dim in ncfile.dimensions.items():
                if dim.isunlimited():
                    dimensions_info["unlimited_dimensions"].append(dim_name)
                else:
                    dimensions_info["fixed_dimensions"][dim_name] = len(dim)

                # Check if there's a corresponding dimension variable
                if dim_name in ncfile.variables:
                    dimensions_info["dimension_variables"].append(dim_name)

            logger.info(f"Root dimensions: {list(ncfile.dimensions.keys())}")
            logger.info(f"Root variables: {list(ncfile.variables.keys())}")

        return dimensions_info

    def migrate_to_clean_file_structure(self) -> dict:
        """Migrate to a cleaned file structure with preserved groups."""
        logger.info("Migrating to cleaned file structure with preserved groups")

        migration_summary = {
            "method": "migrate_to_clean_file",
            "success": False,
            "groups_preserved": [],
            "errors": [],
            "backup_path": None,
            "dimensions_moved": [],
            "dimensions_info": self.get_root_dimensions_info(),
        }

        try:
            backup_path = self.output_path.with_suffix(".backup_before_cleanup.nc")
            shutil.copy(self.output_path, backup_path)
            migration_summary["backup_path"] = str(backup_path)
            logger.info(f"Created backup: {backup_path}")

            # Create a cleaned root file
            cleaned_root_file = self.output_path.with_suffix(".migrated.nc")
            if cleaned_root_file.exists():
                cleaned_root_file.unlink()

            # Copy groups from original file to cleaned root file
            self.copy_content_to_cleaned_file(cleaned_root_file, migration_summary)

            # Replace original file with cleaned root file
            shutil.copy(cleaned_root_file, self.output_path)

            migration_summary["success"] = True
            logger.info("Successfully migrated to cleaned file structure")

        except Exception as e:
            error_msg = f"Failed to migrate to cleaned file structure: {e}"
            logger.error(error_msg)
            migration_summary["errors"].append(error_msg)

        return migration_summary

    def copy_content_to_cleaned_file(
        self, cleaned_root_file: Path, migration_summary: dict
    ) -> None:
        """Copy all groups from original file to the cleaned root file."""
        groups_copied = []

        try:
            # Open both files
            with netCDF4.Dataset(self.output_path, "r") as original_file:
                with netCDF4.Dataset(cleaned_root_file, "w") as cleaned_file:
                    # Step 1: Create dimensions group
                    dimensions_group = cleaned_file.createGroup("dimensions")
                    dimensions_group.description = "Central location for all dimensions"

                    # Step 2: Copy all dimensions to dimensions group
                    for dim_name, dim in original_file.dimensions.items():
                        logger.info(
                            f"Copying dimension: {dim_name} with size "
                            f"{len(dim) if not dim.isunlimited() else 'unlimited'}"
                        )
                        size = len(dim) if not dim.isunlimited() else None
                        dimensions_group.createDimension(dim_name, size)
                        migration_summary["dimensions_moved"].append(dim_name)
                        logger.info(
                            f"Moved dimension {dim_name} to dimensions group "
                            f"with the name: {dimensions_group.name}."
                        )

                    # Step 3: Copy global attributes
                    for attr_name in original_file.ncattrs():
                        logger.info(f"Copying global attribute: {attr_name}")
                        if (
                            attr_name != "_FillValue"
                        ):  # Avoid copying _FillValue globally
                            cleaned_file.setncattr(
                                attr_name, original_file.getncattr(attr_name)
                            )

                    # Step 4: Copy all groups recursively
                    for group_name, original_group in original_file.groups.items():
                        logger.info(f"Copying group: {group_name}")
                        try:
                            self.copy_single_group_recursive(
                                original_group, cleaned_file, group_name
                            )
                            groups_copied.append(group_name)
                            logger.info(f"Copied group: {group_name}")
                        except Exception as e:
                            logger.error(f"Failed to copy group {group_name}: {e}")
                            migration_summary["errors"].append(
                                f"Group copy failed: {group_name} - {e}"
                            )

            migration_summary["groups_preserved"] = groups_copied
            logger.info(
                f"Successfully copied {len(groups_copied)} groups to cleaned file."
            )

        except Exception as e:
            logger.error(f"Failed to copy groups: {e}")
            migration_summary["errors"].append(f"Group copying failed: {e}.")

    def copy_single_group_recursive(
        self,
        source_group: netCDF4.Group,
        target_location: netCDF4.Dataset,
        group_name: str,
    ) -> None:
        """Recursively copy a single group and all its contents."""
        # Create the group in target location
        target_group = target_location.createGroup(group_name)

        # Copy group attributes
        for attr_name in source_group.ncattrs():
            attr_value = source_group.getncattr(attr_name)
            target_group.setncattr(attr_name, attr_value)

        # Copy dimensions
        for dim_name, dim in source_group.dimensions.items():
            logger.info(
                f"Copying dimension: {dim_name} in {source_group.name} with "
                f"size {len(dim) if not dim.isunlimited() else 'unlimited'} "
                f"to target group {target_group.name}."
            )
            size = len(dim) if not dim.isunlimited() else None
            target_group.createDimension(dim_name, size)

        # Copy variables
        for var_name, source_var in source_group.variables.items():
            logger.info(
                f"Copying variable: {var_name} in {source_group.name} "
                f"to target group {target_group.name}."
            )
            # Create variable
            target_var = target_group.createVariable(
                var_name,
                source_var.dtype,
                source_var.dimensions,
                fill_value=getattr(source_var, "_FillValue", None),
            )

            # Copy data
            target_var[:] = source_var[:]

            # Copy attributes
            for attr_name in source_var.ncattrs():
                if attr_name != "_FillValue":
                    attr_value = source_var.getncattr(attr_name)
                    target_var.setncattr(attr_name, attr_value)

        # Recursively copy subgroups
        for subgroup_name, source_subgroup in source_group.groups.items():
            self.copy_single_group_recursive(
                source_subgroup, target_group, subgroup_name
            )
