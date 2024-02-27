import unittest
import uuid

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
        
    def test_database_store_1000_datasets(self):
        
        self.database_manager.drop()
        
        for i in range(0, 1000):
            self.database_manager.store_dataset(str(uuid.uuid4()))  
        
    def test_database_manager_insert_50_entries(self):
        
        for i in range(0, 50):
            dataset_file_document = self.database_manager.database_handler.get_test_document_file()
            self.database_manager.database_handler.insert_file_document(dataset_file_document)
            
    def test_database_manager_insert_1000_entries(self):
        
        for i in range(0, 1000):
            dataset_file_document = self.database_manager.database_handler.get_test_document_file()
            self.database_manager.database_handler.insert_file_document(dataset_file_document)
        
    def test_database_manager_drop(self):

        self.database_manager.drop()
        
    def test_database_manager_basic_view(self):

        self.database_manager.view()
        
if __name__ == '__main__':
    unittest.main()