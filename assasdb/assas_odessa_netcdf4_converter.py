#!/usr/bin/env python

from datetime import datetime
import sys
import os
import time
import netCDF4
import logging
import logging.handlers
import numpy as np
import pandas as pd
import shutil
import pkg_resources

from typing import List, Union
from os.path import join, dirname, abspath
from pathlib import Path

from assasdb.assas_utils import get_duration

logger = logging.getLogger('assas_app')

ROOT = '/root/astecV3.1.2'
COMPUTER = 'linux_64'

astec_python_location = os.path.join(ROOT, "odessa", "bin", COMPUTER + "-release", "wrap_python")

if astec_python_location not in sys.path:
    logger.info(f'Append path to odessa to environment: {astec_python_location}')
    sys.path.append(astec_python_location)

import pyodessa as pyod

class AssasOdessaNetCDF4Converter:
    
    def __init__(
        self,
        input_path: str,
        output_path: Union[str, Path],
    ) -> None:
        '''
        Initialize AssasOdessaNetCDF4Converter class.
        
        Parameters
        ----------
        input_path: str
            Input path of ASTEC binary archive to convert.
        output_path: str
            Output path of resulting netCDF4 dataset.
        astec_variable_index_file: str, optional
            CSV file containing hte information about the ASTEc varibales to extract.
        
        Returns
        ----------
        None
        '''

        self.input_path = input_path
        self.output_path = Path(output_path)
        logger.info(f'Output path of hdf5 file is {str(self.output_path)}.')

        self.output_path.parent.mkdir(parents = True, exist_ok = True)

        self.time_points = pyod.get_saving_times(input_path)
        logger.info(f'Read following time points from ASTEC archive: {self.time_points}.')

        self.variable_index = self.read_astec_variable_index_files(
            report = False
        )
        
        self.magma_debris_ids = self.read_vessel_magma_debris_ids(
            resource_file = 'data/inr/assas_variables_vessel_magma_debris_ids.csv'
        )
        self.fuel_ids = self.read_csv_resource_file(
            resource_file = 'data/inr/assas_variables_vessel_fuel_ids.csv'
        )
        self.clad_ids = self.read_csv_resource_file(
            resource_file = 'data/inr/assas_variables_vessel_clad_ids.csv'
        )
        self.component_states = self.read_csv_resource_file(
            resource_file = 'data/inr/assas_variables_component_states.csv'
        )

        self.variable_strategy_mapping = {
            'primary_pipe_ther': AssasOdessaNetCDF4Converter.parse_variable_from_primary_pipe_ther,
            'primary_pipe_geom': AssasOdessaNetCDF4Converter.parse_variable_from_primary_pipe_geom,
            'primary_volume_ther': AssasOdessaNetCDF4Converter.parse_variable_from_primary_volume_ther,
            'primary_volume_geom': AssasOdessaNetCDF4Converter.parse_variable_from_primary_volume_geom,
            'primary_junction_ther': AssasOdessaNetCDF4Converter.parse_variable_from_primary_junction_ther,
            'primary_junction_geom': AssasOdessaNetCDF4Converter.parse_variable_from_primary_junction_geom,
            'primary_wall': AssasOdessaNetCDF4Converter.parse_variable_from_primary_wall,
            'primary_wall_ther': AssasOdessaNetCDF4Converter.parse_variable_from_primary_wall_ther,
            'primary_wall_ther_2': AssasOdessaNetCDF4Converter.parse_variable_from_primary_wall_ther_2,
            'primary_wall_geom': AssasOdessaNetCDF4Converter.parse_variable_from_primary_wall_geom,
            'secondar_pipe_ther': AssasOdessaNetCDF4Converter.parse_variable_from_primary_pipe_ther,
            'secondar_pipe_geom': AssasOdessaNetCDF4Converter.parse_variable_from_primary_pipe_geom,
            'secondar_volume_ther': AssasOdessaNetCDF4Converter.parse_variable_from_secondar_volume_ther,
            'secondar_junction_ther': AssasOdessaNetCDF4Converter.parse_variable_from_secondar_junction_ther,
            'secondar_junction_geom': AssasOdessaNetCDF4Converter.parse_variable_from_secondar_junction_geom,
            'secondar_wall': AssasOdessaNetCDF4Converter.parse_variable_from_secondar_wall,
            'secondar_wall_ther': AssasOdessaNetCDF4Converter.parse_variable_from_secondar_wall_ther,
            'secondar_wall_ther_2': AssasOdessaNetCDF4Converter.parse_variable_from_secondar_wall_ther_2,
            'secondar_wall_geom': AssasOdessaNetCDF4Converter.parse_variable_from_secondar_wall_geom,
            'vessel_face_ther': AssasOdessaNetCDF4Converter.parse_variable_from_vessel_face_ther,
            'vessel_mesh_ther': AssasOdessaNetCDF4Converter.parse_variable_from_vessel_mesh_ther,
            'vessel_mesh': AssasOdessaNetCDF4Converter.parse_variable_from_vessel_mesh,
            'vessel_general': AssasOdessaNetCDF4Converter.parse_variable_from_vessel_general,
            'fp_heat_vessel': AssasOdessaNetCDF4Converter.parse_variable_from_fp_heat_vessel,
            'systems_pump': AssasOdessaNetCDF4Converter.parse_variable_from_systems_pump,
            'systems_valve': AssasOdessaNetCDF4Converter.parse_variable_from_systems_valve,
            'sensor': AssasOdessaNetCDF4Converter.parse_variable_from_sensor,
            'containment_dome': AssasOdessaNetCDF4Converter.parse_variable_from_containment_dome,
            'containment_pool': AssasOdessaNetCDF4Converter.parse_variable_from_containment_pool,
            'connecti': AssasOdessaNetCDF4Converter.parse_variable_from_connecti,
            'connecti_heat': AssasOdessaNetCDF4Converter.parse_variable_from_connecti_heat,
            'connecti_source': AssasOdessaNetCDF4Converter.parse_variable_from_connecti_source,
            'connecti_source_index': AssasOdessaNetCDF4Converter.parse_variable_from_connecti_source_index,
            'connecti_source_fp': AssasOdessaNetCDF4Converter.parse_variable_from_connecti_source_fp,
            'vessel_magma_debris': self.parse_variable_vessel_magma_debris,
            'vessel_clad': self.parse_variable_vessel_clad,
            'vessel_fuel': self.parse_variable_vessel_fuel,
            'vessel_clad_stat': self.parse_variable_vessel_clad_stat,
            'vessel_fuel_stat': self.parse_variable_vessel_fuel_stat,
        }
        
    def get_time_points(
        self
    ) -> List[int]:
        
        return self.time_points
    
    def get_odessa_base_from_index(
        self,
        index: int
    ):
        time_point = self.time_points[index]
        return pyod.restore(self.input_path, time_point)
    
    def get_variable_index(
        self
    ) -> pd.DataFrame:
        
        return self.variable_index

    def read_astec_variable_index_files(
        self,
        report: bool = False
    )-> pd.DataFrame:

        file_list = [
            'data/inr/assas_variables_cavity.csv',
            'data/inr/assas_variables_containment.csv',
            'data/inr/assas_variables_containment_dome_pool.csv',
            'data/inr/assas_variables_lower_plenum.csv',
            'data/inr/assas_variables_vessel.csv',
            'data/inr/assas_variables_vessel_face_ther.csv',
            'data/inr/assas_variables_vessel_general.csv',
            'data/inr/assas_variables_vessel_mesh.csv',
            'data/inr/assas_variables_primary_junction_ther.csv',
            'data/inr/assas_variables_primary_pipe_ther.csv',
            'data/inr/assas_variables_primary_volume_ther.csv',
            'data/inr/assas_variables_primary_wall.csv',
            'data/inr/assas_variables_primary_wall_ther.csv',
            'data/inr/assas_variables_secondar_junction_ther.csv',
            'data/inr/assas_variables_secondar_volume_ther.csv',
            'data/inr/assas_variables_secondar_wall.csv',
            'data/inr/assas_variables_secondar_wall_ther.csv',
            'data/inr/assas_variables_connecti.csv',
            'data/inr/assas_variables_connecti_source_fp.csv',
        ]
        
        dataframe_list = []
        for file in file_list:
            with pkg_resources.resource_stream(__name__, file) as csv_file:
                dataframe = pd.read_csv(csv_file)
                dataframe_list.append(dataframe)
        
        dataframe = pd.concat(dataframe_list)
        logger.info(f'Shape of variable index is {dataframe.shape}.')
        
        if report:
            output_file = os.path.dirname(os.path.realpath(__file__)) + '/assas_variables_wp2_report.csv'
            dataframe.to_csv(output_file)
        
        return dataframe
    
    def read_vessel_magma_debris_ids(
        self,
        resource_file: str,
    )-> pd.DataFrame:
        '''
        Read names of the ASTEC variables into a dataframe.
        
        Parameters
        ----------
        filename: str
            Name of the csv file containing the ASTEC variable names.
        
        Returns
        ----------
        List[str] 
            List of strings representing the ASTEC variable names.
        '''

        dataframe = self.read_csv_resource_file(
            resource_file = resource_file
        )
        dataframe.replace('nan', np.nan)
        
        return dataframe
    
    def read_csv_resource_file(
        self,
        resource_file: str,
    )-> pd.DataFrame:
        
        with pkg_resources.resource_stream(__name__, resource_file) as csv_file:
            
            logger.info(f'Read csv resource file {csv_file}')
            dataframe = pd.read_csv(csv_file)

        logger.debug(f'{dataframe}')
        
        return dataframe
    
    @staticmethod
    def check_if_odessa_path_exists(
        odessa_base,
        odessa_path: str,
    )-> bool:
        
        keys = odessa_path.split(":")
        nkeys = len(keys)
        is_valid_path = True
        
        logger.debug(f'Keys of odessa_path: {keys}. Depth of path: {nkeys}.')
        
        for count, var in enumerate(keys, start = 1):
            
            logger.debug('------------------------------------')
            var = var.strip()
            logger.debug(f'Handle key {var}.')
            num_stru = 1
            
            if " " in var:
                
                name_stru = var.split(" ")[0]
                num_stru = var.split(" ")[1]
            
            elif "[" in var:
                
                name_stru = var.split("[")[0]

            logger.debug(f'Name of the structure: {name_stru}.')
            logger.debug(f'Number of the structure: {num_stru}.')
            logger.debug(f'Structure index: {count}.')
            
            if count == 1: #Using initiale base argument
                
                len_odessa_base = odessa_base.len(name_stru.replace("'", ""))
                logger.debug(f'Length of odessa base element: {len_odessa_base}.')
                
                if len_odessa_base >= int(num_stru):
                    if count < nkeys: #getting next structure
                        new_base = odessa_base.get(name_stru + " "+num_stru)
                else:
                    is_valid_path = False
                    break
            
            else: #Using substructure
                
                len_odessa_base = new_base.len(name_stru.replace("'", ""))
                logger.debug(f'Length of odessa base element: {len_odessa_base}.')
                
                if len_odessa_base >= int(num_stru):
                    if count < nkeys: #getting next structure
                        new_base = new_base.get(name_stru+" "+num_stru)
                else:
                    is_valid_path = False
                    break
    
        return is_valid_path
    
    def parse_variable_vessel_magma_debris(
        self,
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:

        array = np.zeros((len(self.magma_debris_ids.index)))
        logger.debug(f'Initialized array with shape {array.shape}.')
        
        for _, dataframe_row in self.magma_debris_ids.iterrows():
            
            mesh_id = dataframe_row['mesh_id']
            variable_id = dataframe_row[variable_name]
            
            logger.debug(f'Handle mesh_id {mesh_id} and variable_id {variable_id}.')
            
            if np.isnan(variable_id):
                array[int(mesh_id)-1] = np.nan
            else:
                odessa_path = f'VESSEL 1: COMP {int(variable_id)}: M 1'
                variable_structure = odessa_base.get(odessa_path)
                
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[int(mesh_id)-1] = variable_structure
        
        return array

    def parse_variable_vessel_fuel(
        self,
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type vessel_fuel.')

        array = np.zeros((len(self.fuel_ids.index)))
        logger.debug(f'Initialized array with shape {array.shape}.')
        
        for idx, dataframe_row in self.fuel_ids.iterrows():
            
            comp_id = dataframe_row['fuel_id']
            
            logger.debug(f'Handle comp_id {comp_id}.')

            odessa_path = f'VESSEL 1: COMP {int(comp_id)}: {variable_name} 1'
            variable_structure = odessa_base.get(odessa_path)
                
            logger.debug(f'Collect variable structure {variable_structure}.')
            array[idx] = variable_structure
        
        return array
    
    def parse_variable_vessel_clad(
        self,
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type vessel_clad.')

        array = np.zeros((len(self.clad_ids.index)))
        logger.debug(f'Initialized array with shape {array.shape}.')
        
        for idx, dataframe_row in self.clad_ids.iterrows():
            
            comp_id = dataframe_row['clad_id']
            
            logger.debug(f'Handle comp_id {comp_id}.')

            odessa_path = f'VESSEL 1: COMP {int(comp_id)}: {variable_name} 1'
            variable_structure = odessa_base.get(odessa_path)
                
            logger.debug(f'Collect variable structure {variable_structure}.')
            array[idx] = variable_structure
        
        return array
    
    def parse_variable_vessel_fuel_stat(
        self,
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type vessel_fuel_stat.')

        array = np.zeros((len(self.fuel_ids.index)))
        logger.debug(f'Initialized array with shape {array.shape}.')
        
        for idx, dataframe_row in self.fuel_ids.iterrows():
            
            comp_id = dataframe_row['fuel_id']
            
            logger.debug(f'Handle comp_id {comp_id}.')

            odessa_path = f'VESSEL 1: COMP {int(comp_id)}: {variable_name} 1'
            variable_structure = odessa_base.get(odessa_path)
                
            component_state = self.component_states.loc[self.component_states['state'] == variable_structure]
            component_state_code = component_state['code']
            
            logger.debug(f'Collect variable structure string {variable_structure}, what corresponds to code {int(component_state_code.iloc[0])}.')
            array[idx] = int(component_state_code.iloc[0])
        
        return array
    
    def parse_variable_vessel_clad_stat(
        self,
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type vessel_clad_stat.')

        array = np.zeros((len(self.clad_ids.index)))
        logger.debug(f'Initialized array with shape {array.shape}.')
        
        for idx, dataframe_row in self.clad_ids.iterrows():
            
            comp_id = dataframe_row['clad_id']
            
            logger.debug(f'Handle comp_id {comp_id}.')

            odessa_path = f'VESSEL 1: COMP {int(comp_id)}: {variable_name} 1'
            variable_structure = odessa_base.get(odessa_path)
            
            component_state = self.component_states.loc[self.component_states['state'] == variable_structure]
            component_state_code = component_state['code']
            
            logger.debug(f'Collect variable structure string {variable_structure}, what corresponds to code {int(component_state_code.iloc[0])}.')
            array[idx] = int(component_state_code.iloc[0])
        
        return array
    
    @staticmethod
    def parse_variable_from_vessel_channel_mesh_ther(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        '''
        Parse the data for a ASTEC variable out of the odessa base.
        
        Parameters
        ----------
        odessa_base: pyod.lib.od_base
            Odessa base object considered for extraction.
        variable_name: str
            Name of the ASTEC variable.
        
        Returns
        ----------
        np.ndarray 
            Numpy array which contains the data for the ASTEC variable.
        '''

        logger.info(f'Parse ASTEC variable {variable_name}, type vessel_channel_mesh_ther.')

        vessel = odessa_base.get('VESSEL')
        number_of_channels = vessel.len('CHANNEL')
        channel = vessel.get(f'CHANNEL 0') # Take first channel to get dimensions
        number_of_meshes = channel.len('MESH')

        array = np.zeros((number_of_channels, number_of_meshes))
        logger.debug(f'Initialized array with shape {array.shape}.')

        for channel_number in range(1, number_of_channels):
            
            channel = vessel.get(f'CHANNEL {channel_number}')
            
            for mesh_number in range(1, channel.len('MESH')):
                
                logger.debug(f'Channel number {channel_number}, Mesh number {mesh_number}.')
                mesh_identifier = channel.get(f'MESH {mesh_number}')
                logger.debug(f'Read mesh identifier {mesh_identifier}.')

                odessa_path = f'VESSEL 1: MESH {mesh_identifier}: THER 1: {variable_name} 1'
                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
                ):  
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f'Collect variable structure {variable_structure}.')
                    array[channel_number][mesh_number] = variable_structure[0]
                else:
                    logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                    array[channel_number][mesh_number] = np.nan

        return array
    
    @staticmethod
    def parse_variable_from_vessel_mesh_ther(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        '''
        Parse the data for a ASTEC variable out of the odessa base.
        
        Parameters
        ----------
        odessa_base: pyod.lib.od_base
            Odessa base object considered for extraction.
        variable_name: str
            Name of the ASTEC variable.
        
        Returns
        ----------
        np.ndarray 
            Numpy array which contains the data for the ASTEC variable.
        '''

        logger.info(f'Parse ASTEC variable {variable_name}, type vessel_mesh_ther.')

        vessel = odessa_base.get('VESSEL')
        number_of_meshes = vessel.len('MESH')

        array = np.zeros((number_of_meshes))
        logger.debug(f'Initialized array with shape {array.shape}.')

        for mesh_number in range(1, number_of_meshes):
            
            logger.debug(f'Mesh number {mesh_number}.')

            odessa_path = f'VESSEL 1: MESH {mesh_number}: THER 1: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                odessa_base = odessa_base,
                odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[mesh_number] = variable_structure[0]
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[mesh_number] = np.nan

        return array
    
    @staticmethod
    def parse_variable_from_vessel_mesh(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        '''
        Parse the data for a ASTEC variable out of the odessa base.
        
        Parameters
        ----------
        odessa_base: pyod.lib.od_base
            Odessa base object considered for extraction.
        variable_name: str
            Name of the ASTEC variable.
        
        Returns
        ----------
        np.ndarray 
            Numpy array which contains the data for the ASTEC variable.
        '''

        logger.info(f'Parse ASTEC variable {variable_name}, type vessel_mesh.')

        vessel = odessa_base.get('VESSEL')
        number_of_meshes = vessel.len('MESH')

        array = np.zeros((number_of_meshes))
        logger.debug(f'Initialized array with shape {array.shape}.')

        for mesh_number in range(1, number_of_meshes):
            
            logger.debug(f'Mesh number {mesh_number}.')

            odessa_path = f'VESSEL 1: MESH {mesh_number}: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                odessa_base = odessa_base,
                odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[mesh_number] = variable_structure
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[mesh_number] = np.nan

        return array
    
    @staticmethod
    def parse_variable_from_vessel_face_ther(
        odessa_base,
        variable_name: str
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type vessel_face_ther.')

        vessel = odessa_base.get('VESSEL')
        number_of_faces = vessel.len('FACE')
        
        logger.debug(f'Number of faces in vessel: {number_of_faces}.')
        
        array = np.zeros((number_of_faces))
        
        for face_number in range(1, number_of_faces):
            
            odessa_path = f'VESSEL 1: FACE {face_number}: THER 1: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[face_number] = variable_structure[0]
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[face_number] = np.nan
            
        return array
    
    @staticmethod
    def convert_odessa_structure_to_array(
        odessa_structure,
    )-> np.ndarray:
        
        array = []
        typ = type(odessa_structure)

        if isinstance(odessa_structure, pyod.cls_rg.Rg):
            array = np.array([odessa_structure[k] for k in odessa_structure.keys()])
        elif isinstance(odessa_structure, pyod.R1):
            array = np.array([odessa_structure])
        elif isinstance(odessa_structure, float):
            array = np.array([odessa_structure])
        else:
            logger.warning('Unkown type')

        return array
    
    @staticmethod
    def parse_variable_from_vessel_general(
        odessa_base,
        variable_name: str
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type vessel_general.')

        odessa_path = f'VESSEL 1: GENERAL 1: {variable_name} 1'
        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):
            variable_structure = odessa_base.get(odessa_path)
            logger.debug(f'Collect variable structure {variable_structure}.')
            array = AssasOdessaNetCDF4Converter.convert_odessa_structure_to_array(
                odessa_structure = variable_structure
            )
        else:
            logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
            array = np.array([np.nan])

        return array
    
    @staticmethod
    def parse_variable_from_fp_heat_vessel(
        odessa_base,
        variable_name: str
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type fp_heat_vessel.')

        odessa_path = f'FP_HEAT 1: VESSEL 1: {variable_name} 1'
        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):
            variable_structure = odessa_base.get(odessa_path)
            logger.debug(f'Collect variable structure {variable_structure}.')
            array = np.array([variable_structure[0]])
        else:
            logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
            array = np.array([np.nan])

        return array
    
    @staticmethod
    def parse_variable_from_primary_junction_ther(
        odessa_base,
        variable_name: str
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type primary_junction_ther.')

        primary = odessa_base.get('PRIMARY')
        number_of_junctions = primary.len('JUNCTION')
        
        logger.debug(f'Number of junctions in primary: {number_of_junctions}.')
        
        array = np.zeros((number_of_junctions))
        
        for junction_number in range(1, number_of_junctions):
            
            odessa_path = f'PRIMARY 1: JUNCTION {junction_number}: THER 1: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[junction_number] = variable_structure[0]
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[junction_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_primary_junction_geom(
        odessa_base,
        variable_name: str
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type primary_junction_geom.')

        primary = odessa_base.get('PRIMARY')
        number_of_junctions = primary.len('JUNCTION')
        
        logger.debug(f'Number of junctions in primary: {number_of_junctions}.')
        
        array = np.zeros((number_of_junctions))
        
        for junction_number in range(1, number_of_junctions):
            
            odessa_path = f'PRIMARY 1: JUNCTION {junction_number}: GEOM 1: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path,
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[junction_number] = variable_structure[0]
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[junction_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_primary_volume_ther(
        odessa_base,
        variable_name: str
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type primary_volume_ther.')

        primary = odessa_base.get('PRIMARY')
        number_of_volumes = primary.len('VOLUME')
        
        logger.debug(f'Number of volumes in primary: {number_of_volumes}.')
        
        array = np.zeros((number_of_volumes))
        
        for volume_number in range(1, number_of_volumes):
            
            odessa_path = f'PRIMARY 1: VOLUME {volume_number}: THER 1: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[volume_number] = variable_structure[0]
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[volume_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_primary_volume_geom(
        odessa_base,
        variable_name: str
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type primary_volume_geom.')

        primary = odessa_base.get('PRIMARY')
        number_of_volumes = primary.len('VOLUME')
        
        logger.debug(f'Number of volumes in primary: {number_of_volumes}.')
        
        array = np.zeros((number_of_volumes))
        
        for volume_number in range(1, number_of_volumes):
            
            odessa_path = f'PRIMARY 1: VOLUME {volume_number}: GEOM 1: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[volume_number] = variable_structure[0]
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[volume_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_primary_pipe_ther(
        odessa_base,
        variable_name: str
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type primary_pipe_ther.')

        primary = odessa_base.get('PRIMARY')
        number_of_pipes = primary.len('PIPE')
        
        logger.debug(f'Number of pipes in primary: {number_of_pipes}.')
        
        array = np.zeros((number_of_pipes))
        
        for pipe_number in range(1, number_of_pipes):
            
            odessa_path = f'PRIMARY 1: PIPE {pipe_number}: THER 1: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[pipe_number] = variable_structure
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[pipe_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_primary_pipe_geom(
        odessa_base,
        variable_name: str
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type primary_pipe_geom.')

        primary = odessa_base.get('PRIMARY')
        number_of_pipes = primary.len('PIPE')
        variable_structure = primary.get(f'PIPE 1: GEOM 1: {variable_name} 1')
        
        logger.debug(f'Number of pipes in primary: {number_of_pipes}. Length of variable structure: {len(variable_structure)}.')
        
        array = np.zeros((number_of_pipes, len(variable_structure)))
        
        for pipe_number in range(1, number_of_pipes):
            
            odessa_path = f'PRIMARY 1: PIPE {pipe_number}: GEOM 1: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[pipe_number] = variable_structure[0]
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[pipe_number] = np.nan
            
        return array
        
    @staticmethod
    def parse_variable_from_secondar_junction_ther(
        odessa_base,
        variable_name: str
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type secondar_junction_ther.')

        secondar = odessa_base.get('SECONDAR')
        number_of_junctions = secondar.len('JUNCTION')
        
        logger.debug(f'Number of junctions in secondar: {number_of_junctions}.')
        
        array = np.zeros((number_of_junctions))
        
        for junction_number in range(1, number_of_junctions):
            
            odessa_path = f'SECONDAR 1: JUNCTION {junction_number}: THER 1: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[junction_number] = variable_structure[0]
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[junction_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_secondar_junction_geom(
        odessa_base,
        variable_name: str
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type secondar_junction_geom.')

        secondar = odessa_base.get('SECONDAR')
        number_of_junctions = secondar.len('JUNCTION')
        
        logger.debug(f'Number of junctions in secondar: {number_of_junctions}.')
        
        array = np.zeros((number_of_junctions))
        
        for junction_number in range(1, number_of_junctions):
            
            odessa_path = f'SECONDAR 1: JUNCTION {junction_number}: GEOM 1: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[junction_number] = variable_structure[0]
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[junction_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_secondar_volume_ther(
        odessa_base,
        variable_name: str
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type secondar_volume_ther.')

        secondar = odessa_base.get('SECONDAR')
        number_of_volumes = secondar.len('VOLUME')
        
        logger.debug(f'Number of volumes in secondar: {number_of_volumes}.')
        
        array = np.zeros((number_of_volumes))
        
        for volume_number in range(1, number_of_volumes):
            
            odessa_path = f'SECONDAR 1: VOLUME {volume_number}: THER 1: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[volume_number] = variable_structure[0]
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[volume_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_primary_wall(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type primary_wall.')

        primary = odessa_base.get('PRIMARY')
        number_of_walls = primary.len('WALL')
        
        logger.debug(f'Number of walls in primary: {number_of_walls}.')
        
        array = np.zeros((number_of_walls))
        
        for wall_number in range(1, number_of_walls):
            
            odessa_path = f'PRIMARY 1: WALL {wall_number}: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[wall_number] = variable_structure
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[wall_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_primary_wall_ther(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type primary_wall_ther.')

        primary = odessa_base.get('PRIMARY')
        number_of_walls = primary.len('WALL')
        
        logger.debug(f'Number of walls in primary: {number_of_walls}.')
        
        array = np.zeros((number_of_walls))
        
        for wall_number in range(1, number_of_walls):
            
            odessa_path = f'PRIMARY 1: WALL {wall_number}: THER 1: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[wall_number] = variable_structure[0]
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[wall_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_primary_wall_ther_2(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type primary_wall_ther_2.')

        primary = odessa_base.get('PRIMARY')
        number_of_walls = primary.len('WALL')
        
        logger.debug(f'Number of walls in primary: {number_of_walls}.')
        
        array = np.zeros((number_of_walls))
        
        for wall_number in range(1, number_of_walls):
            
            odessa_path = f'PRIMARY 1: WALL {wall_number}: THER 2: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[wall_number] = variable_structure[0]
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[wall_number] = np.nan
            
        return array

    @staticmethod
    def parse_variable_from_primary_wall_geom(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type primary_wall_geom.')

        primary = odessa_base.get('PRIMARY')
        number_of_walls = primary.len('WALL')
        
        logger.debug(f'Number of walls in primary: {number_of_walls}.')
        
        array = np.zeros((number_of_walls))
        
        for wall_number in range(1, number_of_walls):
            
            odessa_path = f'PRIMARY 1: WALL {wall_number}: GEOM 1: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[wall_number] = variable_structure[0]
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[wall_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_secondar_wall(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type secondar_wall.')

        secondar = odessa_base.get('SECONDAR')
        number_of_walls = secondar.len('WALL')
        
        logger.debug(f'Number of walls in secondar: {number_of_walls}.')
        
        array = np.zeros((number_of_walls))
        
        for wall_number in range(1, number_of_walls):
            
            odessa_path = f'SECONDAR 1: WALL {wall_number}: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[wall_number] = variable_structure
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[wall_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_secondar_wall_ther(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type secondar_wall_ther.')

        secondar = odessa_base.get('SECONDAR')
        number_of_walls = secondar.len('WALL')
        
        logger.debug(f'Number of walls in secondar: {number_of_walls}.')
        
        array = np.zeros((number_of_walls))
        
        for wall_number in range(1, number_of_walls):

            odessa_path = f'SECONDAR 1: WALL {wall_number}: THER 1: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[wall_number] = variable_structure[0]
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[wall_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_secondar_wall_ther_2(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type secondar_wall_ther.')

        secondar = odessa_base.get('SECONDAR')
        number_of_walls = secondar.len('WALL')
        
        logger.debug(f'Number of walls in secondar: {number_of_walls}.')
        
        array = np.zeros((number_of_walls))
        
        for wall_number in range(1, number_of_walls):

            odessa_path = f'SECONDAR 1: WALL {wall_number}: THER 2: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[wall_number] = variable_structure[0]
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[wall_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_secondar_wall_geom(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type secondar_wall_geom.')

        secondar = odessa_base.get('SECONDAR')
        number_of_walls = secondar.len('WALL')
        
        logger.debug(f'Number of walls in secondar: {number_of_walls}.')
        
        array = np.zeros((number_of_walls))
        
        for wall_number in range(1, number_of_walls):
            
            odessa_path = f'SECONDAR 1: WALL {wall_number}: GEOM 1: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[wall_number] = variable_structure[0]
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[wall_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_systems_pump(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type systems_pump.')

        systems = odessa_base.get('SYSTEMS')
        number_of_pumps = systems.len('PUMP')
        
        logger.info(f'Number of pumps in systems: {number_of_pumps}.')
        
        array = np.zeros((number_of_pumps))
        
        for pump_number in range(1, number_of_pumps):
            
            odessa_path = f'SYSTEMS 1: PUMP {pump_number}: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[pump_number] = variable_structure[0]
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[pump_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_systems_valve(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type systems_valve.')

        systems = odessa_base.get('SYSTEMS')
        number_of_valves = systems.len('VALVE')
        
        logger.info(f'Number of valves in systems: {number_of_valves}.')
        
        array = np.zeros((number_of_valves))
        
        for valve_number in range(1, number_of_valves):
            
            odessa_path = f'SYSTEMS 1: VALVE {valve_number}: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[valve_number] = variable_structure[0]
            else:
                logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[valve_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_sensor(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable from sensor {variable_name}, type sensor.')

        odessa_path = f'SENSOR {variable_name}: value 1'
        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):
            variable_structure = odessa_base.get(odessa_path)
            logger.debug(f'Collect variable structure {variable_structure}.')
            array = AssasOdessaNetCDF4Converter.convert_odessa_structure_to_array(
                odessa_structure = variable_structure
            )
        else:
            logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
            array = np.array([np.nan])
        
        return array
    
    @staticmethod
    def parse_variable_from_containment_dome(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable from sensor {variable_name}, type containment_dome.')

        odessa_path = f'CONTAINM 1: ZONE 10: THER 1: {variable_name} 1'
        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):
            variable_structure = odessa_base.get(odessa_path)
            logger.debug(f'Collect variable structure {variable_structure}.')
            array = AssasOdessaNetCDF4Converter.convert_odessa_structure_to_array(
                odessa_structure = variable_structure
            )
        else:
            logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
            array = np.array([np.nan])
        
        return array
    
    @staticmethod
    def parse_variable_from_containment_pool(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable from sensor {variable_name}, type containment_pool.')

        odessa_path = f'CONTAINM 1: ZONE 11: THER 1: {variable_name} 1'
        if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):
            variable_structure = odessa_base.get(odessa_path)
            logger.debug(f'Collect variable structure {variable_structure}.')
            array = AssasOdessaNetCDF4Converter.convert_odessa_structure_to_array(
                odessa_structure = variable_structure
            )
        else:
            logger.warning(f'Variable not in odessa base, fill datapoint with np.nan.')
            array = np.array([np.nan])
        
        return array
    
    @staticmethod
    def parse_variable_from_connecti(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type connecti.')

        number_of_connectis = odessa_base.len('CONNECTI')
        
        logger.info(f'Number of valves in systems: {number_of_connectis}.')
        
        array = np.zeros((number_of_connectis))
        
        for connecti_number in range(1, number_of_connectis):
            
            odessa_path = f'CONNECTI {connecti_number}: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[connecti_number] = variable_structure
            else:
                logger.debug(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[connecti_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_connecti_heat(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type connecti_heat.')

        number_of_connectis = odessa_base.len('CONNECTI')
        
        logger.info(f'Number of valves in systems: {number_of_connectis}.')
        
        array = np.zeros((number_of_connectis))
        
        for connecti_number in range(1, number_of_connectis):
            
            odessa_path = f'CONNECTI {connecti_number}: HEAT 1: {variable_name} 1'
            if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                    odessa_base = odessa_base,
                    odessa_path = odessa_path
            ):  
                variable_structure = odessa_base.get(odessa_path)
                logger.debug(f'Collect variable structure {variable_structure}.')
                array[connecti_number] = variable_structure[0]
            else:
                logger.debug(f'Variable not in odessa base, fill datapoint with np.nan.')
                array[connecti_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_connecti_source(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type connecti_source.')

        number_of_connectis = odessa_base.len('CONNECTI')
        
        overall_shape = 0
        for connecti_number in range(1, number_of_connectis):
            connecti_object = odessa_base.get(f'CONNECTI {connecti_number}')
            overall_shape += connecti_object.len('SOURCE')
            
        logger.info(f'Number of valves in systems: {number_of_connectis}. Complete shape {overall_shape}.')
        
        array = np.zeros((overall_shape))
        
        for connecti_number in range(1, number_of_connectis):
            
            connecti_object = odessa_base.get(f'CONNECTI {connecti_number}')
            number_of_sources = connecti_object.len('SOURCE')
            
            for source_number in range(1, number_of_sources):

                odessa_path = f'CONNECTI {connecti_number}: SOURCE {source_number}: {variable_name} 1'
                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                        odessa_base = odessa_base,
                        odessa_path = odessa_path
                ):  
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f'Collect variable structure {variable_structure}.')
                    array[connecti_number] = variable_structure
                else:
                    logger.debug(f'Variable not in odessa base, fill datapoint with np.nan.')
                    array[connecti_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_connecti_source_index(
        odessa_base,# TODO: fix type hint
        variable_name: str,
        index: int
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable {variable_name}, type connecti_source_index. Index: {index}')

        number_of_connectis = odessa_base.len('CONNECTI')
        
        overall_shape = 0
        for connecti_number in range(1, number_of_connectis):
            connecti_object = odessa_base.get(f'CONNECTI {connecti_number}')
            overall_shape += connecti_object.len('SOURCE')
            
        logger.info(f'Number of valves in systems: {number_of_connectis}. Complete shape {overall_shape}.')
        
        array = np.zeros((overall_shape))
        
        for connecti_number in range(1, number_of_connectis):
            
            connecti_object = odessa_base.get(f'CONNECTI {connecti_number}')
            number_of_sources = connecti_object.len('SOURCE')
            
            for source_number in range(1, number_of_sources):

                odessa_path = f'CONNECTI {connecti_number}: SOURCE {source_number}: {variable_name} 1'
                if AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                        odessa_base = odessa_base,
                        odessa_path = odessa_path
                ):  
                    variable_structure = odessa_base.get(odessa_path)
                    logger.debug(f'Collect variable structure {variable_structure}.')
                    array[connecti_number] = variable_structure[index]
                else:
                    logger.debug(f'Variable not in odessa base, fill datapoint with np.nan.')
                    array[connecti_number] = np.nan
            
        return array
    
    @staticmethod
    def parse_variable_from_connecti_source_fp(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable from connecti source {variable_name}, type connecti_source_fp.')

        odessa_path = f'CONNECTI 1: SOURCE {variable_name}: QMAV 1'
        
        variable_structure = odessa_base.get(odessa_path)
        logger.debug(f'Collect variable structure {variable_structure}.')
        
        array = AssasOdessaNetCDF4Converter.convert_odessa_structure_to_array(
                odessa_structure = variable_structure
        )
        
        return array
    
    @staticmethod
    def get_general_meta_data(
        netcdf4_file_path: str,
        attribute_name: str,
    ) -> str:
        
        netcdf4_path_object = Path(netcdf4_file_path)
        logger.info(f'Path of hdf5 file is {str(netcdf4_path_object)}.')
        
        value = None
        with netCDF4.Dataset(f'{netcdf4_path_object}', 'r', format='NETCDF4') as ncfile:
            value = ncfile.getncattr(attribute_name)
            
        return value

    @staticmethod
    def set_general_meta_data(
        output_path: str,
        archive_name: str,
        archive_description: str,
    ) -> None:
        
        output_path_object = Path(output_path)
        logger.info(f'Output path of hdf5 file is {str(output_path_object)}.')

        output_path_object.parent.mkdir(parents = True, exist_ok = True)
        
        with netCDF4.Dataset(f'{output_path_object}', 'w', format='NETCDF4') as ncfile:
            
            ncfile.title = archive_name
            
            ncfile.setncattr('name', archive_name)
            ncfile.setncattr('description', archive_description)
            ncfile.setncattr('history', 'created ' + time.ctime(time.time()))
    
    @staticmethod
    def read_meta_values_from_netcdf4(
        netcdf4_file: str,
    )-> List[dict]:

        result = []

        with netCDF4.Dataset(f'{netcdf4_file}', 'r', format='NETCDF4') as ncfile:

            for variable_name in ncfile.variables.keys():

                variable_dict = {}

                variable_dict['name'] = variable_name
                logger.info(f'Read variable {variable_name}.')

                dimensions = ncfile.variables[variable_name].dimensions
                
                variable_dict['dimensions'] = '(' + ', '.join(str(dimension) for dimension in dimensions) + ')'
                logger.debug(f"Dimension string is {variable_dict['dimensions']}.")
                
                shapes = ncfile.variables[variable_name].shape
                
                variable_dict['shape'] = '(' + ', '.join(str(shape) for shape in shapes) + ')'
                logger.debug(f"Shape string is {variable_dict['shape']}.")
                
                if variable_name == 'time_points':
                    domain = '-'
                else:
                    domain = ncfile.variables[variable_name].getncattr('domain')
                
                variable_dict['domain'] = domain
                logger.debug(f"Domain string is {variable_dict['domain']}.")
                
                for attr_name in ncfile.variables[variable_name].ncattrs():
                    logger.debug(f"Attribute name {attr_name}.")
                
                result.append(variable_dict)
        
        return result
    
    def convert_astec_variables_to_netcdf4(
        self,
        explicit_times: List[int] = None,
    ) -> None:
        '''
        Convert the data for given ASTEC variables from odessa into hdf5.
        
        Parameters
        ----------
        output_file : str, optional
            Name of hdf5 file. Default name is dataset.h5.
        Returns
        ----------
        None
        '''
    
        logger.info(f'Parse ASTEC data from binary with path {self.input_path}.')

        time_points = self.time_points
        if explicit_times is not None:
            time_points = time_points[explicit_times[0]:explicit_times[1]]

        logger.info(f'Parse following time points from ASTEC archive: {time_points}.')
        
        with netCDF4.Dataset(f'{self.output_path}', 'a', format='NETCDF4') as ncfile:

            variable_datasets = {}
            
            ncfile.createDimension('time', len(time_points))
            ncfile.createDimension('channel', None)
            ncfile.createDimension('mesh', None)
            ncfile.createDimension('pipe', None)
            ncfile.createDimension('junction', None)
            ncfile.createDimension('volume', None)
            ncfile.createDimension('face', None)
            ncfile.createDimension('wall', None)
            ncfile.createDimension('general', None)
            ncfile.createDimension('pump', None)
            ncfile.createDimension('sensor', None)
            ncfile.createDimension('connecti', None)
            ncfile.createDimension('component', None)

            time_dataset = ncfile.createVariable(
                varname = 'time_points',
                datatype = np.float32,
                dimensions = 'time'
            )
            time_dataset[:] = time_points

            for idx, variable in self.variable_index.iterrows():

                dimensions = list(variable['dimension'].split(';'))
                dimensions.insert(0, 'time')
                dimensions = [dimension for dimension in dimensions if dimension != 'none']
                
                logger.info(f'Use dimension: {dimensions}.')

                variable_datasets[variable['name']] = ncfile.createVariable(
                    varname = variable['name'],
                    datatype = np.float32,
                    dimensions = tuple(dimensions),
                )
                
                variable_datasets[variable['name']].long_name = variable['long_name']
                variable_datasets[variable['name']].units = variable['unit']
                variable_datasets[variable['name']].domain = variable['domain']
                variable_datasets[variable['name']].strategy = variable['strategy']

            for idx, time_point in enumerate(time_points):

                logger.info(f'Restore odessa base for time point {time_point}.')
                odessa_base = pyod.restore(self.input_path, time_point)

                for _, variable in self.variable_index.iterrows():
                    
                    strategy_function = self.variable_strategy_mapping[variable['strategy']]
                    
                    if np.isnan(variable['index']):
                        data_per_timestep = strategy_function(
                            odessa_base = odessa_base,
                            variable_name = variable['name_odessa']
                        )
                    else:
                        data_per_timestep = strategy_function(
                            odessa_base = odessa_base,
                            variable_name = variable['name_odessa'],
                            index = int(variable['index'])
                        )

                    logger.debug(f"Read data for {variable['name_odessa']} with shape {data_per_timestep.shape}. Odessa index {variable['index']} {np.isnan(variable['index'])}")

                    variable_datasets[variable['name']][idx] = data_per_timestep
