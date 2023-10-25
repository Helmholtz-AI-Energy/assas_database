import unittest

from assasdb import AssasDatabaseManager
from assasdb import AssasStorageHandler

class AssasStorageHandlerTest(unittest.TestCase):
    
    def setup(self) -> None:
        
        self.storage_handler = AssasStorageHandler()
    
    def tearDown(self) -> None:
        
        self.test_storage_handler = None
    
    def test_storage_handler_get_path(self):
        
        self.storage_handler.get_path()

class AssasDatabaseManagerTest(unittest.TestCase):
    
    def setUp(self):
        
        self.database_manager = AssasDatabaseManager()
        
    def tearDown(self):
        
        self.database_manager = None

    def test_database_manager_basic_upload(self):

        self.database_manager.upload()
        
    def test_database_manager_basic_view(self):

        self.database_manager.view()
        
if __name__ == '__main__':
    unittest.main()