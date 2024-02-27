import pandas
import zipfile
import glob
import logging

from uuid import uuid4
from datetime import datetime
from .assas_database_handler import DatabaseHandler
from .assas_data_handler import AssasDataHandler
from .assas_database_storage import AssasStorageHandler
from .assas_astec_handler import convert_archive, unzip_archive, get_astec_archive
from .assas_database_hdf5 import AssasDatasetHandler
from .assas_database_dataset import AssasDataset

logger = logging.getLogger('assas_app')

class AssasDatabaseManager:

    def __init__(self):
        
        self.connectionstring = "mongodb://localhost:27017/"
        self.database_handler = DatabaseHandler(self.connectionstring)
        self.storage_handler = AssasStorageHandler()
        #self.storage_handler.create_lsdf_archive()
        
    def upload(self, uuid):
        
        logger.info("start upload for uuid %s", uuid)
        
        archive_dir = "/mnt/ASSAS/media/documents/" + uuid
        logger.info(archive_dir)
        
        zipped_archive = get_astec_archive(archive_dir)
        
        unzip_archive(zipped_archive, archive_dir + "/archive")
        
        #convert_archive(archive_dir)
                
        dataset_file_document = DatabaseHandler.get_test_document_file(uuid, archive_dir)
                                            
        self.database_handler.insert_file_document(dataset_file_document)
        
        logger.info("inserted %s", dataset_file_document)
        
    def store_dataset(self, uuid):
        
        logger.info("store dataset for uuid %s", uuid)
        
        archive_dir = self.storage_handler.get_path() + uuid
        self.storage_handler.create_dataset_archive(archive_dir)
        
        dataset_file_document = DatabaseHandler.get_test_document_file(uuid, archive_dir)
        
        self.database_handler.insert_file_document(dataset_file_document)
        
        dataset = AssasDataset('test',1000)
        
        dataset_handler = AssasDatasetHandler(dataset_file_document, dataset)
        dataset_handler.create_hdf5()
        
    def view(self):
        
        file_collection = self.database_handler.get_file_collection()
        
        #logger.info(list(file_collection.find()))

        return pandas.DataFrame(list(file_collection.find()))
    
    def drop(self):
        
        self.database_handler.drop_file_collection()
        
    def get_file_document(self, id):
        
        return self.database_handler.get_file_document(id)
    
