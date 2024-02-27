import logging
 
from pymongo import MongoClient
from bson.objectid import ObjectId
from uuid import uuid4
from datetime import datetime

logger = logging.getLogger('assas_app')

class DatabaseHandler:

    def __init__(self, connectionstring):
        
        self.client = MongoClient(connectionstring)

        self.db_handle = self.client['assas']
        self.file_collection = self.db_handle['files']

    def get_db_handle(self):

        return self.db_handle

    def get_file_collection(self):
        
        return self.file_collection
    
    def insert_file_document(self, file):
        
        logger.info('insert %s' % file)
        
        self.file_collection.insert_one(file)

    def drop_file_collection(self):

        self.file_collection.drop()
        
    def get_file_document(self, id):
        
        return self.file_collection.find_one(ObjectId(id))
    
    @staticmethod
    def get_test_document_file(system_uuid=str(uuid4()),system_path="default path"):
        return {
                    "system_uuid": system_uuid,
                    "system_date": datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
                    "system_path": system_path,
                    "system_size": "1MB",
                    "system_user": "test",
                    "system_download": "LINK",
                    "system_status": "complete",
                    "meta_name": "Name of Simulation X",
                    "meta_group": "Group or Kind of Simulation",
                    "meta_date": datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
                    "meta_creator": "Test User",
                    "meta_description": "'this is a test description!'",
                    "meta_data_variables": "['pressure', 'voidf', 'temp', 'sat_temp']",
                    "meta_data_channels": "4",
                    "meta_data_meshes": "16",
                    "meta_data_timesteps": "1000"    
                }
        
        