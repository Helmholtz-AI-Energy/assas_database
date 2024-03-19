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
from .assas_database_handler import AssasDocumentFile

logger = logging.getLogger('assas_app')

class AssasDatabaseManager:

    def __init__(self, config: dict) -> None:
        
        self.config = config
        
        self.connectionstring = 'mongodb://localhost:27017/'
        self.database_handler = AssasDatabaseHandler(self.connectionstring)
        self.storage_handler = AssasStorageHandler(config.LOCAL_ARCHIVE, config.LSDF_ARCHIVE)
        self.astec_handler = AssasAstecHandler(config.PYTHON_VERSION, config.ASTEC_ROOT, config.ASTEC_PARSER)
       
    def process_archive(self, zipped_archive_path: str) -> None:
        
        archive_dir = os.path.dirname(zipped_archive_path)
        logger.info(f'start processing archive {archive_dir}')
        
        self.astec_handler.unzip_archive(zipped_archive_path)
        
        self.astec_handler.convert_archive(archive_dir)
    
    def store_local_archive(self, uuid) -> None:
        
        logger.info("store dataset for uuid %s", uuid)
        
        archive_dir = self.storage_handler.local_archive + uuid + '/result/'
        self.storage_handler.create_dataset_archive(archive_dir)
        
        dataset_file_document = AssasDocumentFile.get_test_document_file(uuid, archive_dir)
        
        self.database_handler.insert_file_document(dataset_file_document)
        
        dataset = AssasDataset('test', 1000)
        
        dataset_handler = AssasDatasetHandler(dataset_file_document, dataset)
        dataset_handler.create_hdf5() 
    
    def synchronize_archive(self, system_uuid: str) -> None:
        
        self.storage_handler.store_archive_on_share(system_uuid)           
    
    def add_test_database_entry(self, system_uuid: str, system_path: str) -> None:
        
        dataset_file_document = AssasDocumentFile.get_test_document_file(system_uuid, system_path)
        
        logger.info(f'insert test document {dataset_file_document}')
                                                    
        self.database_handler.insert_file_document(dataset_file_document)
        
        logger.info(f'inserted test document {dataset_file_document}')
        
    def add_database_entry(self, document: str) -> None:
        
        logger.info(f'insert document {document}')
        
        self.database_handler.insert_file_document(document) 
        
    def get_database_entries(self) -> pandas.DataFrame:
        
        file_collection = self.database_handler.get_file_collection()
        
        data_frame = pandas.DataFrame(list(file_collection.find()))
        
        logger.info(f'load data frame with size {str(data_frame.size),str(data_frame.shape)}')
        
        if data_frame.size == 0:
            return data_frame
        
        data_frame['system_index'] = range(1, len(data_frame) + 1)    
        data_frame['_id'] = data_frame['_id'].astype(str)

        return data_frame
    
    def drop(self):
        
        self.database_handler.drop_file_collection()
        
    def get_database_entry(self, id):
        
        return self.database_handler.get_file_document(id)
    
    def get_database_entry_uuid(self, uuid):
        
        return self.database_handler.get_file_document_uuid(uuid)
    
