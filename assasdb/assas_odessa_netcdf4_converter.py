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
        astec_variable_index_file: str = 'data/astec_vessel_ther_variables_inr.csv',
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

        #self.variable_index = self.read_astec_variable_index(
        #    variable_index_file = astec_variable_index_file
        #)
        self.variable_index = self.read_astec_variable_index_files()

        self.variable_strategy_mapping = { # TODO: Implement all other types
            'primary_pipe_ther': AssasOdessaNetCDF4Converter.parse_variable_from_primary_pipe_ther,
            'primary_pipe_geom': AssasOdessaNetCDF4Converter.parse_variable_from_primary_pipe_geom,
            'primary_volume_ther': AssasOdessaNetCDF4Converter.parse_variable_from_primary_volume_ther,
            'primary_volume_geom': AssasOdessaNetCDF4Converter.parse_variable_from_primary_volume_geom,
            'primary_junction_ther': AssasOdessaNetCDF4Converter.parse_variable_from_primary_junction_ther,
            'primary_junction_geom': AssasOdessaNetCDF4Converter.parse_variable_from_primary_junction_geom,
            'primary_wall': AssasOdessaNetCDF4Converter.parse_variable_from_primary_wall,
            'primary_wall_ther': AssasOdessaNetCDF4Converter.parse_variable_from_primary_wall_ther,
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
            'systems_pump': AssasOdessaNetCDF4Converter.parse_variable_from_systems_pump,
            'systems_valve': AssasOdessaNetCDF4Converter.parse_variable_from_systems_valve,
            'sensor': AssasOdessaNetCDF4Converter.parse_variable_from_sensor,
            'containment_dome': AssasOdessaNetCDF4Converter.parse_variable_from_containment_dome,
            'containment_pool': AssasOdessaNetCDF4Converter.parse_variable_from_containment_pool,
            'connecti': AssasOdessaNetCDF4Converter.parse_variable_from_connecti,
        }
        
    def get_time_points(
        self
    ) -> List[int]:
        
        return self.time_points
    
    def get_variable_index(
        self
    ) -> pd.DataFrame:
        
        return self.variable_index

    @staticmethod
    def get_size_of_archive_in_giga_bytes(
        number_of_timesteps: int,
        size_of_saving: float = 1.68
    ) -> float:
        '''
        Function to estimate the size of the ASTEC archive.
        
        Parameters
        ----------
        number_of_timesteps: int
            Number of timesteps assumed in the ASTEC archive.
        
        Returns
        ----------
        float 
            Estimation of the size in giga bytes.
        '''
        
        size_in_giga_bytes = (number_of_timesteps * size_of_saving) / 1000.0
        
        return size_in_giga_bytes

    @staticmethod
    def get_lists_of_saving_times(
        archive_path_list: List[str],
    ) -> List[List[int]]:
        '''
        Get the time points for a list of ASTEC archives.
        
        Parameters
        ----------
        archive_path_list: List[str]
            List of the ASTEC binary archives.
        
        Returns
        ----------
        List[List[int]] 
            List of List of integers containing all time points 
            of the ASTEC archives.
        '''

        result_list = []

        for archive_path in archive_path_list:

            try:
                saved_instants = pyod.get_saving_times(archive_path)
                result_list.append(saved_instants)

            except:
                logger.error(f'Astec archive is not consistent {archive_path}')
                result_list.append([-1])

        return result_list
    
    def read_astec_variable_index_files(
        self
    )-> pd.DataFrame:

        file_list = [
            'data/inr/assas_variables_cavity.csv',
            'data/inr/assas_variables_containment.csv',
            'data/inr/assas_variables_lower_plenum.csv',
            'data/inr/assas_variables_vessel.csv',
            'data/inr/assas_variables_vessel_face_ther.csv',
            'data/inr/assas_variables_vessel_general.csv',
            'data/inr/assas_variables_vessel_mesh.csv',
            'data/inr/assas_variables_primary_junction_ther.csv',
            'data/inr/assas_variables_primary_pipe_ther.csv',
            'data/inr/assas_variables_primary_volume_ther.csv',
            'data/inr/assas_variables_primary_wall_ther.csv',
            'data/inr/assas_variables_secondar_junction_ther.csv',
            'data/inr/assas_variables_secondar_volume_ther.csv',
            'data/inr/assas_variables_secondar_wall.csv',
            'data/inr/assas_variables_secondar_wall_ther.csv',
        ]
        
        dataframe_list = []
        for file in file_list:
            csv_path = pkg_resources.resource_stream(__name__, file)
            dataframe = pd.read_csv(csv_path)
            dataframe_list.append(dataframe)
        
        return pd.concat(dataframe_list)
    
    def read_astec_variable_index(
        self,
        variable_index_file: str = 'data/astec_vessel_ther_variables_inr.csv'
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

        csv_path = pkg_resources.resource_stream(__name__, variable_index_file)
        logger.info(f'Read variable index file {csv_path}')
        
        dataframe = pd.read_csv(csv_path)

        logger.debug(f'Read ASTEC variables to process from file {variable_index_file}.')
        logger.debug(f'{dataframe}')

        return dataframe

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
        number_of_channels = vessel.len('CHANNEL')
        channel = vessel.get(f'CHANNEL 0') # Take first channel to get dimensions
        number_of_meshes = channel.len('MESH')

        array = np.zeros((number_of_channels, number_of_meshes))
        logger.debug(f'Initialized array with shape {array.shape}.')

        for channel_number in range(1, vessel.len('CHANNEL')):

            channel = vessel.get(f'CHANNEL {channel_number}')

            for mesh_number in range(1, channel.len('MESH')):

                logger.debug(f'Channel number {channel_number}, Mesh number {mesh_number}.')
                mesh_identifier = channel.get(f'MESH {mesh_number}')
                logger.debug(f'Read mesh identifier {mesh_identifier}.')

                mesh_object = vessel.get(f'MESH {mesh_identifier}')
                ther_object = mesh_object.get(f'THER')
                variable_structure = ther_object.get(f'{variable_name}')

                logger.debug(f'Collect variable structure {variable_structure}, extract data point: {variable_structure[2]}.')
                array[channel_number][mesh_number] = variable_structure[2]

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
        number_of_channels = vessel.len('CHANNEL')
        channel = vessel.get(f'CHANNEL 0') # Take first channel to get dimensions
        number_of_meshes = channel.len('MESH')

        array = np.zeros((number_of_channels, number_of_meshes))
        logger.debug(f'Initialized array with shape {array.shape}.')

        for channel_number in range(vessel.len('CHANNEL')):

            channel = vessel.get(f'CHANNEL {channel_number}')

            for mesh_number in range(channel.len('MESH')):

                logger.debug(f'Channel number {channel_number}, Mesh number {mesh_number}.')
                mesh_identifier = channel.get(f'MESH {mesh_number}')
                logger.debug(f'Read mesh identifier {mesh_identifier}.')

                mesh_object = vessel.get(f'MESH {mesh_identifier}')
                variable_structure = mesh_object.get(f'{variable_name}')

                logger.debug(f'Collect variable structure {variable_structure}.')
                array[channel_number][mesh_number] = variable_structure

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
            
            junction_object = vessel.get(f'FACE {face_number}')
            ther_object = junction_object.get(f'THER')
            variable_structure = ther_object.get(f'{variable_name}')
            
            logger.debug(f'Collect variable structure {variable_structure}, extract data point: {variable_structure[2]}.')
            array[face_number] = variable_structure[2]
            
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

        vessel = odessa_base.get('VESSEL')
        general_object = vessel.get('GENERAL')
        variable_structure = general_object.get(f'{variable_name}')
        
        logger.debug(f'Collect variable structure {variable_structure}.')

        array = AssasOdessaNetCDF4Converter.convert_odessa_structure_to_array(
            odessa_structure = variable_structure
        )

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
            
            junction_object = primary.get(f'JUNCTION {junction_number}')
            ther_object = junction_object.get(f'THER')
            variable_structure = ther_object.get(f'{variable_name}')
            
            logger.debug(f'Collect variable structure {variable_structure}, extract data point: {variable_structure[2]}.')
            array[junction_number] = variable_structure[2]
            
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
            
            junction_object = primary.get(f'JUNCTION {junction_number}')
            geom_object = junction_object.get(f'GEOM')
            variable_structure = geom_object.get(f'{variable_name}')
            
            logger.debug(f'Collect variable structure {variable_structure}.')
            array[junction_number] = variable_structure
            
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
            
            volume_object = primary.get(f'VOLUME {volume_number}')
            ther_object = volume_object.get(f'THER')
            variable_structure = ther_object.get(f'{variable_name}')
            
            logger.debug(f'Collect variable structure {variable_structure}, extract data point: {variable_structure[2]}.')
            array[volume_number] = variable_structure[2]
            
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
            
            volume_object = primary.get(f'VOLUME {volume_number}')
            geom_object = volume_object.get(f'GEOM')
            variable_structure = geom_object.get(f'{variable_name}')
            
            logger.debug(f'Collect variable structure {variable_structure} {volume_number}.')
            array[volume_number] = variable_structure
            
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
            
            pipe_object = primary.get(f'PIPE {pipe_number}')
            ther_object = pipe_object.get(f'THER')
            variable_structure = ther_object.get(f'{variable_name}')
            
            logger.debug(f'Collect variable structure {variable_structure}.')
            array[pipe_number] = variable_structure
            
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
        
        logger.debug(f'Number of pipes in primary: {number_of_pipes}. {len(variable_structure)} {variable_structure}')
        
        array = np.zeros((number_of_pipes, len(variable_structure)))
        
        for pipe_number in range(1, number_of_pipes):
            
            pipe_object = primary.get(f'PIPE {pipe_number}')
            geom_object = pipe_object.get(f'GEOM')
            variable_structure = primary.get(f'PIPE {pipe_number}: GEOM 1: {variable_name} 1')
            
            logger.debug(f'Collect variable structure {variable_structure}.')
            array[pipe_number] = variable_structure
            
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
            
            junction_object = secondar.get(f'JUNCTION {junction_number}')
            ther_object = junction_object.get(f'THER')
            variable_structure = ther_object.get(f'{variable_name}')
            
            logger.debug(f'Collect variable structure {variable_structure}, extract data point: {variable_structure[2]}.')
            array[junction_number] = variable_structure[2]
            
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
            
            junction_object = secondar.get(f'JUNCTION {junction_number}')
            geom_object = junction_object.get(f'GEOM')
            variable_structure = geom_object.get(f'{variable_name}')
            
            logger.debug(f'Collect variable structure {variable_structure}.')
            array[junction_number] = variable_structure
            
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
            
            volume_object = secondar.get(f'VOLUME {volume_number}')
            ther_object = volume_object.get(f'THER')
            variable_structure = ther_object.get(f'{variable_name}')
            
            logger.debug(f'Collect variable structure {variable_structure}, extract data point: {variable_structure[2]}.')
            array[volume_number] = variable_structure[2]
            
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
            
            wall_object = primary.get(f'WALL {wall_number}')
            variable_structure = wall_object.get(f'{variable_name}')
            
            logger.debug(f'Collect variable structure {variable_structure}.')
            array[wall_number] = variable_structure
            
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
            
            wall_object = primary.get(f'WALL {wall_number}')
            ther_object = wall_object.get(f'THER')
            variable_structure = ther_object.get(f'{variable_name}')
            
            logger.debug(f'Collect variable structure {variable_structure}, extract data point: {variable_structure[2]}.')
            array[wall_number] = variable_structure[2]
            
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
            
            wall_object = primary.get(f'WALL {wall_number}')
            geom_object = wall_object.get(f'GEOM')
            variable_structure = geom_object.get(f'{variable_name}')
            
            logger.debug(f'Collect variable structure {variable_structure}, extract data point: {variable_structure}.')
            array[wall_number] = variable_structure
            
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
            
            wall_object = secondar.get(f'WALL {wall_number}')
            variable_structure = wall_object.get(f'{variable_name}')
            
            logger.debug(f'Collect variable structure {variable_structure}.')
            array[wall_number] = variable_structure
            
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

            wall_object = secondar.get(f'WALL {wall_number}')
            ther_object = wall_object.get(f'THER 1')
            variable_structure = ther_object.get(f'{variable_name}')
            
            logger.debug(f'Collect variable structure {variable_structure}, extract data point: {variable_structure[2]}.')
            array[wall_number] = variable_structure[2]
            
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

            wall_object = secondar.get(f'WALL {wall_number}')
            ther_object = wall_object.get(f'THER 2')
            variable_structure = ther_object.get(f'{variable_name}')
            
            logger.debug(f'Collect variable structure {variable_structure}, extract data point: {variable_structure[2]}.')
            array[wall_number] = variable_structure[2]
            
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
            
            wall_object = secondar.get(f'WALL {wall_number}')
            geom_object = wall_object.get(f'GEOM')
            variable_structure = geom_object.get(f'{variable_name}')
            
            logger.debug(f'Collect variable structure {variable_structure}.')
            array[wall_number] = variable_structure
            
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
            
            pump_object = systems.get(f'PUMP {pump_number}')
            variable_structure = pump_object.get(f'{variable_name}')
            
            logger.debug(f'Collect variable structure {variable_structure}, extract data point: {variable_structure}.')
            array[pump_number] = variable_structure
            
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
            
            valve_object = systems.get(f'VALVE {valve_number}')
            variable_structure = valve_object.get(f'{variable_name}')
            
            logger.debug(f'Collect variable structure {variable_structure}, extract data point: {variable_structure}.')
            array[valve_number] = variable_structure
            
        return array
    
    @staticmethod
    def parse_variable_from_sensor(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable from sensor {variable_name}, type sensor.')

        sensor = odessa_base.get(f'SENSOR {variable_name}')
        sensor_value = sensor.get('value')
        
        logger.debug(f'Sensor value: {sensor_value}.')
        
        return np.asarray([sensor_value])
    
    @staticmethod
    def parse_variable_from_containment_dome(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable from sensor {variable_name}, type containment_dome.')

        containment = odessa_base.get('CONTAINM')
        zone_10 = containment.get('ZONE 10')
        variable_structure = zone_10.get(f'THER: {variable_name}')

        array = AssasOdessaNetCDF4Converter.convert_odessa_structure_to_array(
            variable_structure
        )
        
        return array
    
    @staticmethod
    def parse_variable_from_containment_pool(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.info(f'Parse ASTEC variable from sensor {variable_name}, type containment_pool.')

        containment = odessa_base.get('CONTAINM')
        zone_10 = containment.get('ZONE 11')
        variable_structure = zone_10.get(f'THER: {variable_name}')

        array = AssasOdessaNetCDF4Converter.convert_odessa_structure_to_array(
            variable_structure
        )
        
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
            
            connecti_object = odessa_base.get(f'CONNECTI {connecti_number}')
            variable_structure = connecti_object.get(f'{variable_name}')
            
            logger.debug(f'Collect variable structure {variable_structure}.')
            array[connecti_number] = variable_structure
            
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
                
                dimension_string = ' '.join(str(dimension) for dimension in dimensions)
                variable_dict['dimensions'] = ' '.join(str(dimension) for dimension in dimensions)
                logger.debug(f'Dimension string {dimension_string}.')
                
                shapes = ncfile.variables[variable_name].shape
                
                shape_string = ' '.join(str(shape) for shape in shapes)
                variable_dict['shape'] = ' '.join(str(shape) for shape in shapes)
                logger.debug(f'Shape string {shape_string}.')
                
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

            time_dataset = ncfile.createVariable(
                varname = 'time_points',
                datatype = np.float32,
                dimensions = 'time'
            )
            time_dataset[:] = time_points

            for idx, variable in self.variable_index.iterrows():

                dimensions = list(variable['dimension'].split(';'))
                dimensions.insert(0, 'time')
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
                    
                    data_per_timestep = strategy_function(
                        odessa_base = odessa_base,
                        variable_name = variable['name_odessa']
                    )

                    logger.debug(f"Read data for {variable['name_odessa']} with shape {data_per_timestep.shape}.")

                    variable_datasets[variable['name']][idx] = data_per_timestep
