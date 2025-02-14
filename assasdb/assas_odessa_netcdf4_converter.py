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

        self.variable_index = self.read_astec_variable_index(
            variable_index_file = astec_variable_index_file
        )

        self.variable_strategy_mapping = { # TODO: Implement all other types
            'vessel': AssasOdessaNetCDF4Converter.parse_variable_from_odessa_in_vessel,
            'other': AssasOdessaNetCDF4Converter.parse_variable_from_odessa_in_other,
        }

    @staticmethod
    def get_size_of_archive_in_giga_bytes(
        number_of_timesteps: int,
        size_of_saving: float = 1.68
    ) -> float:
        
        size_in_giga_bytes = (number_of_timesteps * size_of_saving) / 1000.0
        
        return size_in_giga_bytes

    @staticmethod
    def get_lists_of_saving_times(
        archive_path_list: List[str],
    ) -> List[List[str]]:

        result_list = []

        for archive_path in archive_path_list:

            try:
                saved_instants = pyod.get_saving_times(archive_path)
                result_list.append(saved_instants)

            except:
                logger.error(f'Astec archive is not consistent {archive_path}')
                result_list.append([-1])

        return result_list
    
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

        #csv_path = dirname(abspath(__file__))
        #csv_path = join(csv_path, filename)
        csv_path = pkg_resources.resource_stream(__name__, variable_index_file)
        logger.info(f'Read variable index file {csv_path}')
        
        dataframe = pd.read_csv(csv_path)

        logger.debug(f'Read ASTEC variables to process from file {variable_index_file}.')
        logger.debug(f'{dataframe}')

        return dataframe

    @staticmethod
    def parse_variable_from_odessa_in_vessel(
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

        logger.info(f'Parse data for ASTEC variable {variable_name}.')

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
                ther_object = mesh_object.get(f'THER')
                variable_structure = ther_object.get(f'{variable_name}')

                logger.debug(f'Collect variable structure {variable_structure}, extract data point: {variable_structure[2]}.')
                array[channel_number][mesh_number] = variable_structure[2]

        return array
    
    @staticmethod
    def parse_variable_from_odessa_in_other(
        odessa_base,# TODO: fix type hint
        variable_name: str,
    )-> np.ndarray:
        
        logger.warning(f'Not implemented yet')
        
        return np.zeros((2, 5))

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
    
    def convert_astec_variables_to_netcdf4(
        self,
        output_file: str = 'dataset.h5'
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
        logger.info(f'Read following time_points from ASTEC archive: {self.time_points}.')

        with netCDF4.Dataset(f'{self.output_path}', 'a', format='NETCDF4') as ncfile:

            variable_datasets = {}
            
            ncfile.createDimension('time', len(self.time_points))
            ncfile.createDimension('channel', None)
            ncfile.createDimension('mesh', None)

            for idx, variable in self.variable_index.iterrows():
                
                variable_datasets[variable['name']] = ncfile.createVariable(
                    varname = variable['name'],
                    datatype = np.float32,
                    dimensions = ('time', 'channel', 'mesh'),
                )
                variable_datasets[variable['name']].units = variable['unit']
            
            for idx, time_point in enumerate(self.time_points):

                logger.info(f'Restore odessa base for time point {time_point}.')
                odessa_base = pyod.restore(self.input_path, time_point)

                for _, variable in self.variable_index.iterrows():
                    
                    strategy_function = self.variable_strategy_mapping[variable['type']]
                    
                    data_per_timestep = strategy_function(
                        odessa_base = odessa_base,
                        variable_name = variable['name']
                    )

                    logger.debug(f"Read data for {variable['name']} with shape {data_per_timestep.shape}.")
                    logger.debug(f'Resize dataset to ({len(self.time_points)},{data_per_timestep.shape[0]},{data_per_timestep.shape[1]}).')

                    variable_datasets[variable['name']][idx,:,:] = data_per_timestep
