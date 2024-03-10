import pandas
import logging
import numpy
import os
import shutil

from uuid import uuid4
from datetime import datetime
from .assas_database_handler import AssasDatabaseHandler
from .assas_database_storage import AssasStorageHandler
from .assas_astec_handler import AssasAstecHandler
from .assas_database_hdf5 import AssasDatasetHandler
from .assas_database_dataset import AssasDataset

logger = logging.getLogger('assas_app')

class AssasDatabaseManager:

    def __init__(self, local_share, lsdf_share):
        
        self.connectionstring = 'mongodb://localhost:27017/'
        self.database_handler = AssasDatabaseHandler(self.connectionstring)
        self.storage_handler = AssasStorageHandler(local_share, lsdf_share)
        self.astec_handler = AssasAstecHandler()
       
    def process_archive(self, archive: str):
        
        archive_dir = os.path.dirname(archive)
        #zipped_archive = get_astec_archive(archive_dir)
        
        logger.info(f'start conversion (archive: {archive}')
        
        self.astec_handler.unzip_archive(archive, archive_dir + '/archive')
        
        self.astec_handler.convert_archive(archive_dir)
    
    def synchronize_archive(self, system_uuid: str):
        
        self.storage_handler.store_archive_on_share(system_uuid)           
    
    def add_database_entry(self, system_uuid: str, system_path: str):
        
        dataset_file_document = AssasDatabaseHandler.get_test_document_file(system_uuid, system_path)
                                            
        self.database_handler.insert_file_document(dataset_file_document)
        
        logger.info(f'inserted document {dataset_file_document}')
        
    def get_database_entries(self):
        
        file_collection = self.database_handler.get_file_collection()
        
        data_frame = pandas.DataFrame(list(file_collection.find()))
        
        data_frame['system_index'] = range(1, len(data_frame) + 1)    
        data_frame['_id'] = data_frame['_id'].astype(str)
        
        logger.info(f'load data frame with shape {str(data_frame.shape)}')

        return data_frame
    
    def drop(self):
        
        self.database_handler.drop_file_collection()
        
    def get_database_entry(self, id):
        
        return self.database_handler.get_file_document(id)
    
