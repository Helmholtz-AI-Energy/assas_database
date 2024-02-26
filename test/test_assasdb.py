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
                                    "system_uuid": str(uuid.uuid4()),
                                    "system_date": datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
                                    "system_path": "test path",
                                    "system_size": "1MB",
                                    "system_user": "test",
                                    "system_download": "LINK",
                                    "system_status": "complete",
                                    "meta_name": "Name of Simulation X",
                                    "meta_group": "Group or Kind of Simulation",
                                    "meta_description": "'this is a test description!'",
                                    "meta_data_variables": "['pressure', 'voidf', 'temp', 'sat_temp']",
                                    "meta_data_channels": "4",
                                    "meta_data_meshes": "16",
                                    "meta_data_timesteps": "1000"    
                                 }
            self.database_manager.database_handler.insert_file_document(dataset_file_document)
            
    def test_database_manager_insert_1000_entries(self):
        
        for i in range(0, 1000):
            dataset_file_document = {
                                    "system_uuid": str(uuid.uuid4()),
                                    "system_date": datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
                                    "system_path": "test path",
                                    "system_size": "1MB",
                                    "system_user": "test",
                                    "system_download": "LINK",
                                    "system_status": "complete",
                                    "meta_name": "Name of Simulation X",
                                    "meta_group": "Group or Kind of Simulation",
                                    "meta_description": "'This is a test description!'",
                                    "meta_data_variables": "['pressure', 'voidf', 'temp', 'sat_temp']",
                                    "meta_data_channels": "4",
                                    "meta_data_meshes": "16",
                                    "meta_data_timesteps": "1000"    
                                 }
            self.database_manager.database_handler.insert_file_document(dataset_file_document)
        
    def test_database_manager_drop(self):

        self.database_manager.drop()
        
    def test_database_manager_basic_view(self):

        self.database_manager.view()
        
if __name__ == '__main__':
    unittest.main()