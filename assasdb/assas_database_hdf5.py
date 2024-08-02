import h5py
import logging

from .assas_database_dataset import AssasDataset
from .assas_database_handler import AssasDocumentFile

logger = logging.getLogger('assas_app')

class AssasDatasetHandler:
    
    def __init__(
        self,
        document: dict,
        dataset: AssasDataset
    )-> None:
        
        self.dataset = dataset
        self.document = document

    def create_hdf5(
        self
    )-> None:
        
        logger.info('create hdf5 file at %s' % self.document['system_result'])
        
        with h5py.File(self.document['system_result'],'w') as h5file:
        
            h5file.create_group('meta_data')
            
            h5file['meta_data'].attrs['uuid'] = self.document['system_uuid']
            h5file['meta_data'].attrs['name'] = self.document['meta_name']
            h5file['meta_data'].attrs['group'] = self.document['meta_group']
            h5file['meta_data'].attrs['date'] = self.document['meta_date']
            h5file['meta_data'].attrs['creator'] = self.document['meta_creator']
            
            h5file.create_group('input')
            h5file['input'].attrs['debris'] = 0

            data_group = h5file.create_group('data')
            
            for variable in self.dataset.get_variables():
                group = data_group.create_group(variable)
                array = self.dataset.get_data_for_variable(variable)
                group.create_dataset(variable, data = array)

        h5file.close()
        
    @staticmethod
    def update_meta_data(
        document: AssasDocumentFile
    ) -> AssasDocumentFile:
        
        hdf5_path = document.get_value('system_result')
        logger.info(f'update meta data from {hdf5_path}')
        
        with h5py.File(document.get_value('system_result'),'r') as h5file:
            
            document.set_value('meta_data_variables', h5file['meta_data'].attrs['variables'])
            document.set_value('meta_data_channels', str(h5file['meta_data'].attrs['channels']))
            document.set_value('meta_data_meshes', str(h5file['meta_data'].attrs['meshes']))
            document.set_value('meta_data_samples', str(h5file['meta_data'].attrs['samples']))
            
        h5file.close()
        
        return document
        
             