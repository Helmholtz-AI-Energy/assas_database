import h5py
import logging
import os 
import numpy as np

from .assas_database_dataset import AssasDataset
from .assas_database_handler import AssasDocumentFile

logger = logging.getLogger('assas_app')

class AssasHdf5DatasetHandler:
    
    def __init__(
        self,
        document: dict,
        dataset: AssasDataset
    )-> None:
        
        self.document = document
        self.dataset = dataset        
    
    @staticmethod
    def write_data_into_hdf5(
        file_path: str,
        dataset: AssasDataset
    )-> None:
        
        logger.info(f'Write data values into {file_path}')
        
        with h5py.File(file_path, 'a') as h5file:
            
            data_group = h5file.create_group('data')
                
            for variable in dataset.get_variables():
                
                group = data_group.create_group(variable)
                array = dataset.get_data_for_variable(variable)
                
                logger.info(f'Create dataset for variable {variable}')
                group.create_dataset(variable, data=array)

        #h5file.close()
        
    @staticmethod
    def get_variable_data_from_hdf5(
        file_path: str,
        variable: str
    ):
        
        logger.info(f'Read dataset from {file_path} for variable {variable}')
        
        with h5py.File(file_path, 'r') as h5file:            
            try:
                array = h5file['data'][variable][variable][:]
                logger.info(f'Shape of dataset {np.shape(array)}')
                
                return array
            except:
                logger.error(f'Wrong variable name {variable}')

    @staticmethod
    def write_meta_data_to_hdf5(
        document: AssasDocumentFile
    )-> None:
        
        hdf5_path = document.get_value('system_result')
        logger.info(f'Write meta data into {hdf5_path}')
        
        result_dir = os.path.dirname(hdf5_path)
        if not os.path.isdir(result_dir):
            os.makedirs(result_dir)
        
        with h5py.File(hdf5_path, 'w') as h5file:
            
            h5file.create_group('meta_data')
            
            h5file['meta_data'].attrs['uuid'] = document.get_value('system_uuid')
            h5file['meta_data'].attrs['name'] = document.get_value('meta_name')
            h5file['meta_data'].attrs['description'] = document.get_value('meta_description')
            
            h5file['meta_data'].attrs['variables'] = document.get_value('meta_data_variables')
            h5file['meta_data'].attrs['channels'] = document.get_value('meta_data_channels')
            h5file['meta_data'].attrs['meshes'] = document.get_value('meta_data_meshes')
            h5file['meta_data'].attrs['samples'] = document.get_value('meta_data_samples')
        
        #h5file.close()
        
    @staticmethod
    def read_meta_data_from_hdf5(
        document: AssasDocumentFile
    ) -> AssasDocumentFile:
        
        hdf5_path = document.get_value('system_result')
        logger.info(f'Read meta data from {hdf5_path}')
        
        with h5py.File(hdf5_path, 'r') as h5file:
            
            document.set_value('meta_name', h5file['meta_data'].attrs['name'])
            document.set_value('meta_description', h5file['meta_data'].attrs['description'])
            
            document.set_value('meta_data_variables', h5file['meta_data'].attrs['variables'])
            document.set_value('meta_data_channels', str(h5file['meta_data'].attrs['channels']))
            document.set_value('meta_data_meshes', str(h5file['meta_data'].attrs['meshes']))
            document.set_value('meta_data_samples', str(h5file['meta_data'].attrs['samples']))
            
        #h5file.close()
        
        return document
        
             