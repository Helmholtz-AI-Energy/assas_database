import unittest
import logging

from uuid import uuid4
from assasdb import AssasDatabaseManager
from assasdb import AssasStorageHandler

logger = logging.getLogger('assas_app')

class AssasStorageHandlerTest(unittest.TestCase):
    
    def setUp(self):
        
        self.storage_handler = AssasStorageHandler()
    
    def tearDown(self):
        
        self.test_storage_handler = None
    
    def test_storage_handler_get_archive_dir(self):
        
        self.storage_handler.get_archive_dir()
        
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

    def test_database_manager_drop(self):

        self.database_manager.drop()
        
    def test_database_manager_get_datasets(self):

        self.database_manager.get_database_entries()
        
if __name__ == '__main__':
    unittest.main()