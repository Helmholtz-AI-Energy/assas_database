import pandas
import zipfile
import glob
from datetime import datetime

from .assas_database_handler import DatabaseHandler
from .assas_data_handler import AssasDataHandler
from .assas_database_storage import AssasStorageHandler
from .assas_astec_handler import convert_archive, unzip_archive, get_astec_archive

import logging

logger = logging.getLogger(__name__)

class AssasDatabaseManager:

    def __init__(self):
        
        self.connectionstring = "mongodb://localhost:27017/"
        self.database_handler = DatabaseHandler(self.connectionstring)
        #self.storage_handler = AssasStorageHandler()
        #self.storage_handler.create_lsdf_archive()
        
    def upload(self, uuid):
        
        logger.info("start upload for uuid %s", uuid)
        
        archive_dir = "/mnt/ASSAS/media/documents/" + uuid
        logger.info(archive_dir)
        
        zipped_archive = get_astec_archive(archive_dir)
        
        unzip_archive(zipped_archive, archive_dir + "/archive")
        
        #convert_archive(archive_dir)
                
        dataset_file_document = {
                                "uuid": uuid,
                                "file_name": "testname",
                                "file_date": datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
                                "file_path": archive_dir,
                                "file_size": "1MB",
                                "file_user": "test",
                                "file_download": "LINK",
                                "file_status": "complete",
                                "common_scenario": "scenario A",
                                "common_description": "'this is a test description!'",
                                "common_attribute_1": "attribute 1",
                                "common_attribute_2": "attribute 2",
                                "common_attribute_3": "attribute 3",
                                "data_variables": "['pressure', 'voidf', 'temp', 'sat_temp']",
                                "data_channels": "4",
                                "data_meshes": "16",
                                "data_timesteps": "1000"
                                }
        
        self.database_handler.insert_file_document(dataset_file_document)
        
        logger.info("inserted %s", dataset_file_document)
        
    def view(self):
        
        file_collection = self.database_handler.get_file_collection()
        logger.info(file_collection)

        for file in file_collection.find():
            logger.info(file)

        return pandas.DataFrame(list(file_collection.find()))
    
    def drop(self):
        
        self.database_handler.drop_file_collection()
        
    def get_file_document(self, id):
        
        return self.database_handler.get_file_document(id)