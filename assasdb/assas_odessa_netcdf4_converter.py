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
import time
import netCDF4
import logging
import numpy as np
import pandas as pd
import pkg_resources

from tqdm import tqdm
from typing import List, Union
from pathlib import Path


logger = logging.getLogger("assas_app")

LOG_INTERVAL = 100
ASTEC_ROOT = os.environ.get("ASTEC_ROOT")
print(f"ASTEC_ROOT is {ASTEC_ROOT}")
ASTEC_TYPE = "linux_64"
ASTEC_PYTHON_ODESSA = os.path.join(
    ASTEC_ROOT, "odessa", "bin", ASTEC_TYPE + "-release", "wrap_python"
)

if ASTEC_PYTHON_ODESSA not in sys.path:
    logger.info(f"Append path to odessa to environment: {ASTEC_PYTHON_ODESSA}")
    sys.path.append(ASTEC_PYTHON_ODESSA)

import pyodessa as pyod  # noqa: E402


class AssasOdessaNetCDF4Converter:
    """Class to convert ASTEC binary archive to netCDF4 format.

    This class reads an ASTEC binary archive and converts it to a netCDF4 dataset.
    """

    def __init__(
        self,
        input_path: Union[str, Path],
        output_path: Union[str, Path],
    ) -> None:
        """Initialize AssasOdessaNetCDF4Converter class.

        Args:
            input_path (Union[str, Path]): Path to the ASTEC binary archive.
            output_path (Union[str, Path]): Path to the output netCDF4 file.

        Returns:
            None

        """
        self.input_path = Path(input_path)
        logger.info(f"Input path of ASTEC binary archive is {str(self.input_path)}.")

        self.output_path = Path(output_path)
        logger.info(f"Output path of hdf5 file is {str(self.output_path)}.")

        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        self.time_points = pyod.get_saving_times(str(self.input_path))
        logger.info(f"Read {len(self.time_points)} time points from ASTEC archive.")
        logger.debug(f"List of time points: {self.time_points}.")

        self.variable_index = self.read_astec_variable_index_files(report=True)

        self.magma_debris_ids = self.read_vessel_magma_debris_ids(
            resource_file="astec_config/inr/assas_variables_vessel_magma_debris_ids.csv"
        )
        self.fuel_ids = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_vessel_fuel_ids.csv"
        )
        self.clad_ids = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_vessel_clad_ids.csv"
        )
        self.component_states = self.read_csv_resource_file(
            resource_file="astec_config/inr/assas_variables_component_states.csv"
        )

        self.variable_strategy_mapping = {
            "primary_pipe_ther": (
                AssasOdessaNetCDF4Converter.parse_variable_from_primary_pipe_ther
            ),
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
            "secondar_pipe_ther": (
                AssasOdessaNetCDF4Converter.parse_variable_from_primary_pipe_ther
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
            "systems_valve": (
                AssasOdessaNetCDF4Converter.parse_variable_from_systems_valve
            ),
            "sensor": (AssasOdessaNetCDF4Converter.parse_variable_from_sensor),
            "containment_dome": (
                AssasOdessaNetCDF4Converter.parse_variable_from_containment_dome
            ),
            "containment_zone": (
                AssasOdessaNetCDF4Converter.parse_variable_from_containment_zone
            ),
            "containment_zone_ther": (
                AssasOdessaNetCDF4Converter.parse_variable_from_containment_zone_ther
            ),
            "containment_conn": (
                AssasOdessaNetCDF4Converter.parse_variable_from_containment_conn
            ),
            "containment_wall_temp": (
                AssasOdessaNetCDF4Converter.parse_variable_from_containment_wall_temp
            ),
            "containment_pool": (
                AssasOdessaNetCDF4Converter.parse_variable_from_containment_pool
            ),
            "connecti": (AssasOdessaNetCDF4Converter.parse_variable_from_connecti),
            "connecti_heat": (
                AssasOdessaNetCDF4Converter.parse_variable_from_connecti_heat
            ),
            "connecti_source": (
                AssasOdessaNetCDF4Converter.parse_variable_from_connecti_source
            ),
            "connecti_source_index": (
                AssasOdessaNetCDF4Converter.parse_variable_from_connecti_source_index
            ),
            "connecti_source_fp": (
                AssasOdessaNetCDF4Converter.parse_variable_from_connecti_source_fp
            ),
            "vessel_magma_debris": self.parse_variable_vessel_magma_debris,
            "vessel_clad": self.parse_variable_vessel_clad,
            "vessel_fuel": self.parse_variable_vessel_fuel,
            "vessel_clad_stat": self.parse_variable_vessel_clad_stat,
            "vessel_fuel_stat": self.parse_variable_vessel_fuel_stat,
            "vessel_trup": AssasOdessaNetCDF4Converter.parse_variable_vessel_trup,
            "private_assas_param": (
                AssasOdessaNetCDF4Converter.parse_variable_private_assas_param,
            ),
        }

    def get_time_points(self) -> List[int]:
        """Get the time points from the ASTEC archive.

        Returns:
            List[int]: A list of time points in seconds.

        """
        return self.time_points

    def get_odessa_base_from_index(self, index: int):
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

    def read_astec_variable_index_files(self, report: bool = False) -> pd.DataFrame:
        """Read the ASTEC variable index files.

        This method reads multiple CSV files containing ASTEC variable information
        and concatenates them into a single dataframe. The resulting dataframe
        contains the variable names, IDs, and other relevant information.

        Args:
            report (bool): If True, save the dataframe to a CSV file for
            reporting purposes.

        Returns:
            pd.DataFrame: A dataframe containing the ASTEC variable index.

        """
        file_list = [
            "astec_config/inr/assas_variables_cavity.csv",
            "astec_config/inr/assas_variables_containment.csv",
            "astec_config/inr/assas_variables_containment_conn.csv",
            "astec_config/inr/assas_variables_containment_dome_pool.csv",
            "astec_config/inr/assas_variables_containment_wall.csv",
            "astec_config/inr/assas_variables_containment_zone.csv",
            "astec_config/inr/assas_variables_lower_plenum.csv",
            "astec_config/inr/assas_variables_vessel.csv",
            "astec_config/inr/assas_variables_vessel_face_ther.csv",
            "astec_config/inr/assas_variables_vessel_general.csv",
            "astec_config/inr/assas_variables_vessel_mesh.csv",
            "astec_config/inr/assas_variables_primary_junction_ther.csv",
            "astec_config/inr/assas_variables_primary_pipe_ther.csv",
            "astec_config/inr/assas_variables_primary_volume_ther.csv",
            "astec_config/inr/assas_variables_primary_wall.csv",
            "astec_config/inr/assas_variables_primary_wall_ther.csv",
            "astec_config/inr/assas_variables_secondar_junction_ther.csv",
            "astec_config/inr/assas_variables_secondar_volume_ther.csv",
            "astec_config/inr/assas_variables_secondar_wall.csv",
            "astec_config/inr/assas_variables_secondar_wall_ther.csv",
            "astec_config/inr/assas_variables_connecti.csv",
            "astec_config/inr/assas_variables_connecti_source_fp.csv",
            "astec_config/inr/assas_variables_sequence.csv",
            "astec_config/inr/assas_variables_private_assas_param.csv",
        ]

        dataframe_list = []
        for file in file_list:
            with pkg_resources.resource_stream(__name__, file) as csv_file:
                dataframe = pd.read_csv(csv_file)
                dataframe_list.append(dataframe)

        dataframe = pd.concat(dataframe_list)
        logger.info(f"Shape of variable index is {dataframe.shape}.")

        if report:
            output_file = (
                os.path.dirname(os.path.realpath(__file__))
                + "/astec_config/assas_variables_wp2_report.csv"
            )
            dataframe.to_csv(output_file)

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
        with pkg_resources.resource_stream(__name__, resource_file) as csv_file:
            logger.info(f"Read csv resource file {csv_file}.")
            dataframe = pd.read_csv(csv_file)

        logger.debug(f"{dataframe}")

        return dataframe

    @staticmethod
    def check_if_odessa_path_exists(
        odessa_base,  # TODO: fix type hint
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

            if count == 1:  # Using initiale base argument
                len_odessa_base = odessa_base.len(name_stru.replace("'", ""))

                if len_odessa_base >= int(num_stru):
                    if count < nkeys:  # getting next structure
                        new_base = odessa_base.get(name_stru + " " + num_stru)
                else:
                    is_valid_path = False
                    break

            else:  # Using substructure
                len_odessa_base = new_base.len(name_stru.replace("'", ""))

                if len_odessa_base >= int(num_stru):
                    if count < nkeys:  # getting next structure
                        new_base = new_base.get(name_stru + " " + num_stru)
                else:
                    is_valid_path = False
                    break

        return is_valid_path

    @staticmethod
    def convert_odessa_structure_to_float(
        odessa_structure,
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
        odessa_base,  # TODO: fix type hint
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
        odessa_base,  # TODO: fix type hint
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

            odessa_path = f"VESSEL 1: COMP {int(comp_id)}: {variable_name} 1"

            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                odessa_base, odessa_path
            ):
                variable_structure = odessa_base.get(odessa_path)

                logger.debug(f"Collect variable structure {variable_structure}.")
                array[idx] = variable_structure

        return array

    def parse_variable_vessel_clad(
        self,
        odessa_base,  # TODO: fix type hint
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

            odessa_path = f"VESSEL 1: COMP {int(comp_id)}: {variable_name} 1"

            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                odessa_base, odessa_path
            ):
                variable_structure = odessa_base.get(odessa_path)

                logger.debug(f"Collect variable structure {variable_structure}.")
                array[idx] = variable_structure

        return array

    def parse_variable_vessel_fuel_stat(
        self,
        odessa_base,  # TODO: fix type hint
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

    def parse_variable_vessel_clad_stat(
        self,
        odessa_base,  # TODO: fix type hint
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
        odessa_base,  # TODO: fix type hint
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
        odessa_base,  # TODO: fix type hint
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
        odessa_base, variable_name: str
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
        odessa_base, variable_name: str
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
        odessa_base, variable_name: str
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
        odessa_base, variable_name: str
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
        odessa_base, variable_name: str
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
        odessa_base, variable_name: str
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
        odessa_base, variable_name: str
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
    def parse_variable_from_primary_pipe_ther(
        odessa_base, variable_name: str
    ) -> np.ndarray:
        """Parse ASTEC variable from primary pipe thermal data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type primary_pipe_ther.")

        primary_pipe_check_path = "PRIMARY 1: PIPE 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, primary_pipe_check_path
        ):
            primary = odessa_base.get("PRIMARY")
            number_of_pipes = primary.len("PIPE")

            logger.debug(f"Number of pipes in primary: {number_of_pipes}.")

            array = np.full((number_of_pipes), fill_value=np.nan)

            for idx, pipe_number in enumerate(range(1, number_of_pipes + 1)):
                odessa_path = (
                    f"PRIMARY 1: PIPE {pipe_number}: THER 1: {variable_name} 1"
                )

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure

        else:
            logger.debug(
                f"Path {primary_pipe_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_primary_pipe_geom(
        odessa_base, variable_name: str
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
        odessa_base, variable_name: str
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
        odessa_base, variable_name: str
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
        odessa_base, variable_name: str
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
        odessa_base,  # TODO: fix type hint
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
        odessa_base,  # TODO: fix type hint
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
        odessa_base,  # TODO: fix type hint
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
        odessa_base,  # TODO: fix type hint
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
        odessa_base,  # TODO: fix type hint
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
        odessa_base,  # TODO: fix type hint
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
        odessa_base,  # TODO: fix type hint
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
        odessa_base,  # TODO: fix type hint
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
        odessa_base,  # TODO: fix type hint
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
    def parse_variable_from_systems_valve(
        odessa_base,  # TODO: fix type hint
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
        odessa_base,  # TODO: fix type hint
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
    def parse_variable_from_containment_dome(
        odessa_base,  # TODO: fix type hint
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
        odessa_base,  # TODO: fix type hint
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
        odessa_base,  # TODO: fix type hint
        variable_name: str,
    ):
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
        odessa_base,  # TODO: fix type hint
        variable_name: str,
    ):
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
    def parse_variable_from_containment_conn(
        odessa_base,  # TODO: fix type hint
        variable_name: str,
    ):
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
                f"Number of connections in containment: {number_of_connections}."
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
        odessa_base,  # TODO: fix type hint
        variable_name: str,
    ):
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

            array = np.full((number_of_walls), fill_value=np.nan)

            for idx, wall_number in enumerate(range(1, number_of_walls + 1)):
                odessa_path = (
                    f"CONTAINM 1: WALL {wall_number}: SLAB 1: {variable_name} 1"
                )

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
    def parse_variable_from_connecti(
        odessa_base,  # TODO: fix type hint
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from connecti data.

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

            logger.debug(f"Number of valves in systems: {number_of_connectis}.")

            array = np.full((number_of_connectis), fill_value=np.nan)

            for idx, connecti_number in enumerate(range(1, number_of_connectis + 1)):
                odessa_path = f"CONNECTI {connecti_number}: {variable_name} 1"

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
                f"Path {connecti_check_path} nnot in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_connecti_heat(
        odessa_base,  # TODO: fix type hint
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from connecti heat data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type connecti_heat.")

        connecti_check_path = "CONNECTI 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, connecti_check_path
        ):
            number_of_connectis = odessa_base.len("CONNECTI")

            logger.debug(f"Number of valves in systems: {number_of_connectis}.")

            array = np.full((number_of_connectis), fill_value=np.nan)

            for idx, connecti_number in enumerate(range(1, number_of_connectis + 1)):
                odessa_path = f"CONNECTI {connecti_number}: HEAT 1: {variable_name} 1"

                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base, odessa_path
                ):
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f"Collect variable structure {variable_structure}.")
                    array[idx] = variable_structure[0]

        else:
            logger.debug(
                f"Path {connecti_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_connecti_source(
        odessa_base,  # TODO: fix type hint
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from connecti source data.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(f"Parse ASTEC variable {variable_name}, type connecti_source.")

        connecti_source_check_path = "CONNECTI 1: SOURCE 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, connecti_source_check_path
        ):
            number_of_connectis = odessa_base.len("CONNECTI")

            overall_shape = 0
            for connecti_number in range(1, number_of_connectis + 1):
                connecti_object = odessa_base.get(f"CONNECTI {connecti_number}")
                number_of_sources = connecti_object.len("SOURCE")
                for source_number in range(1, number_of_sources + 1):
                    overall_shape += 1

            logger.debug(
                f"Number of valves in systems: {number_of_connectis}."
                f" Complete shape {overall_shape}."
            )

            array = np.full((overall_shape), fill_value=np.nan)

            index = 0
            for _, connecti_number in enumerate(range(1, number_of_connectis + 1)):
                connecti_object = odessa_base.get(f"CONNECTI {connecti_number}")
                number_of_sources = connecti_object.len("SOURCE")

                for _, source_number in enumerate(range(1, number_of_sources + 1)):
                    odessa_path = f"CONNECTI {connecti_number}:"
                    odessa_path += f" SOURCE {source_number}: {variable_name} 1"

                    if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                        odessa_base, odessa_path
                    ):
                        variable_structure = odessa_base.get(odessa_path)
                        logger.debug(
                            f"Collect variable structure {variable_structure}."
                        )
                        array[index] = variable_structure

                    index += 1

        else:
            logger.debug(
                f"Path {connecti_source_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_connecti_source_index(
        odessa_base,
        variable_name: str,
        index: int,  # TODO: fix type hint
    ) -> np.ndarray:
        """Parse ASTEC variable from connecti source with index.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.
            index (int): Index of the source to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(
            f"Parse ASTEC variable {variable_name}, "
            f"type connecti_source_index. Index: {index}"
        )

        connecti_source_check_path = "CONNECTI 1: SOURCE 1"

        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            odessa_base, connecti_source_check_path
        ):
            number_of_connectis = odessa_base.len("CONNECTI")

            overall_shape = 0
            for connecti_number in range(1, number_of_connectis + 1):
                connecti_object = odessa_base.get(f"CONNECTI {connecti_number}")
                number_of_sources = connecti_object.len("SOURCE")
                for source_number in range(1, number_of_sources + 1):
                    overall_shape += 1

            logger.debug(
                f"Number of valves in systems: {number_of_connectis}. "
                f"Complete shape {overall_shape}."
            )

            array = np.full((overall_shape), fill_value=np.nan)

            index = 0
            for _, connecti_number in enumerate(range(1, number_of_connectis + 1)):
                connecti_object = odessa_base.get(f"CONNECTI {connecti_number}")
                number_of_sources = connecti_object.len("SOURCE")

                for _, source_number in enumerate(range(1, number_of_sources + 1)):
                    odessa_path = f"CONNECTI {connecti_number}:"
                    odessa_path += f" SOURCE {source_number}: {variable_name} 1"

                    if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                        odessa_base, odessa_path
                    ):
                        variable_structure = odessa_base.get(odessa_path)
                        logger.debug(
                            f"Collect variable structure {variable_structure}."
                        )
                        array[index] = variable_structure[index]

        else:
            logger.debug(
                f"Path {connecti_source_check_path} not in odessa base, "
                "fill array with np.nan."
            )
            array = np.full((1), fill_value=np.nan)

        return array

    @staticmethod
    def parse_variable_from_connecti_source_fp(
        odessa_base,  # TODO: fix type hint
        variable_name: str,
    ) -> np.ndarray:
        """Parse ASTEC variable from connecti source with fixed path.

        Args:
            odessa_base: The odessa base object.
            variable_name (str): Name of the variable to parse.

        Returns:
            np.ndarray: An array containing the parsed variable data.

        """
        logger.debug(
            f"Parse ASTEC variable from connecti source {variable_name}, "
            "type connecti_source_fp."
        )

        odessa_path = f"CONNECTI 1: SOURCE {variable_name}: QMAV 1"

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
    def parse_variable_vessel_trup(
        odessa_base,  # TODO: fix type hint
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
        odessa_base,  # TODO: fix type hint
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
        """Set general meta data for the netCDF4 file.

        Args:
            output_path (str): Path to the output netCDF4 file.
            archive_name (str): Name of the archive.
            archive_description (str): Description of the archive.

        Returns:
            None

        """
        output_path_object = Path(output_path)
        logger.info(
            f"Write general meta data to hdf5 file with path {str(output_path_object)}."
        )

        output_path_object.parent.mkdir(parents=True, exist_ok=True)

        with netCDF4.Dataset(f"{output_path_object}", "w", format="NETCDF4") as ncfile:
            ncfile.title = archive_name

            ncfile.setncattr("name", archive_name)
            ncfile.setncattr("description", archive_description)
            ncfile.setncattr("history", "created " + time.ctime(time.time()))

    @staticmethod
    def read_meta_values_from_netcdf4(
        netcdf4_file: str,
    ) -> List[dict]:
        """Read meta values from a netCDF4 file.

        Args:
            netcdf4_file (str): Path to the netCDF4 file.

        Returns:
            List[dict]: A list of dictionaries containing variable metadata.

        """
        result = []

        with netCDF4.Dataset(f"{netcdf4_file}", "r", format="NETCDF4") as ncfile:
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
            if "time_points" in list(ncfile.variables.keys()):
                completed_index = ncfile.variables["time_points"].getncattr(
                    "completed_index"
                )
                logger.info(f"Completed index is {completed_index}.")
            else:
                logger.warning("No time points found in the netCDF4 file.")

        return completed_index

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
                ncfile.createDimension("mesh", None)
                ncfile.createDimension("pipe", None)
                ncfile.createDimension("junction", None)
                ncfile.createDimension("volume", None)
                ncfile.createDimension("face", None)
                ncfile.createDimension("wall", None)
                ncfile.createDimension("connecti", None)
                ncfile.createDimension("component", None)

                time_dataset = ncfile.createVariable(
                    varname="time_points", datatype=np.float32, dimensions="time"
                )
                time_dataset[:] = self.time_points
                time_dataset.completed_index = 0

                for idx, variable in self.variable_index.iterrows():
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
                    variable_datasets[variable["name"]].units = variable["unit"]
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

            progress_bar = tqdm(time_points)
            for idx, time_point in enumerate(progress_bar):
                logger.info(f"Restore odessa base for time point {time_point}.")
                odessa_base = pyod.restore(str(self.input_path), time_point)

                for _, variable in self.variable_index.iterrows():
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
                            index=int(variable["index"]),
                        )

                    logger.debug(
                        f"Read data for {variable['name_odessa']} with "
                        f"shape {data_per_timestep.shape}. "
                        f"Odessa index {variable['index']}, "
                        f"isnan {np.isnan(variable['index'])}."
                    )

                    ncfile.variables[variable["name"]][start_index + idx] = (
                        data_per_timestep
                    )

                if progress_bar.n % LOG_INTERVAL == 0:
                    logger.info(str(progress_bar))

                ncfile.variables["time_points"].completed_index = start_index + idx
