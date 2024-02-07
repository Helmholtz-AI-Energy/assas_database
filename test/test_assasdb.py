import unittest
import uuid
from datetime import datetime

from assasdb import AssasDatabaseManager
from assasdb import AssasStorageHandler

class AssasStorageHandlerTest(unittest.TestCase):
    
    def setUp(self):
        
        self.storage_handler = AssasStorageHandler()
    
    def tearDown(self):
        
        self.test_storage_handler = None
    
    def test_storage_handler_get_path(self):
        
        self.storage_handler.get_path()
        
    def test_storage_handler_client_config(self):
        
        print(self.storage_handler.client_config())
        
    def test_storage_handler_register_session(self):
        
        print(self.storage_handler.register_session())
        
    def test_storage_handler_reset_connection_cache(self):
        
        print(self.storage_handler.reset_connection_cache())

class AssasDatabaseManagerTest(unittest.TestCase):
    
    def setUp(self):
        
        self.database_manager = AssasDatabaseManager()
        
    def tearDown(self):
        
        self.database_manager = None

    def test_database_manager_basic_upload(self):

        self.database_manager.upload()
        
    def test_database_manager_insert_50_entries(self):
        
        for i in range(0, 50):
            dataset_file_document = {
                                    "uuid": str(uuid.uuid4()),
                                    "file_name": "dataset",
                                    "file_date": datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
                                    "file_path": "test path",
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
            self.database_manager.database_handler.insert_file_document(dataset_file_document)
            
    def test_database_manager_insert_1000_entries(self):
        
        for i in range(0, 1000):
            dataset_file_document = {
                                    "uuid": str(uuid.uuid4()),
                                    "file_name": "dataset",
                                    "file_date": datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
                                    "file_path": "test path",
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
            self.database_manager.database_handler.insert_file_document(dataset_file_document)
        
    def test_database_manager_drop(self):

        self.database_manager.drop()
        
    def test_database_manager_basic_view(self):

        self.database_manager.view()
        
if __name__ == '__main__':
    unittest.main()