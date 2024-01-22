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
        
        dataset_file_document = {"uuid": uuid, "upload_time": datetime.now().strftime("%m/%d/%Y, %H:%M:%S"), "path": archive_dir}
        
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