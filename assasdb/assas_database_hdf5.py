import h5py
import logging

from .assas_database_dataset import AssasDataset

logger = logging.getLogger('assas_app')

class AssasDatasetHandler:
    
    def __init__(self, document, dataset: AssasDataset):
        
        self.dataset = dataset
        self.document = document

    def create_hdf5(self):
        
        logger.info('create hdf5 file at %s' % self.document['system_path'])
        
        with h5py.File(self.document['system_path']+"dataset.h5","w") as h5file:
        
            h5file.create_group('metadata')
            
            h5file['metadata'].attrs['uuid'] = self.document['system_uuid']
            h5file['metadata'].attrs['name'] = self.document['meta_name']
            h5file['metadata'].attrs['group'] = self.document['meta_group']
            h5file['metadata'].attrs['date'] = self.document['meta_date']
            h5file['metadata'].attrs['creator'] = self.document['meta_creator']
            
            h5file.create_group('input')
            h5file['input'].attrs['debris'] = 0

            data_group = h5file.create_group('data')
            
            for variable in self.dataset.get_variables():
                group = data_group.create_group(variable)
                array = self.dataset.get_data_for_variable(variable)
                group.create_dataset(variable, data = array)

        h5file.close()       