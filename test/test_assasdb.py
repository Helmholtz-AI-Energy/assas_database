import unittest
import logging
import os

from uuid import uuid4
from assasdb import AssasDatabaseManager
from assasdb import AssasStorageHandler
from assasdb import AssasDatabaseHandler
from assasdb import AssasDocumentFile

logger = logging.getLogger('assas_app')

class AssasStorageHandlerTest(unittest.TestCase):
    
    def setUp(self):
        
        self.storage_handler = AssasStorageHandler()
    
    def tearDown(self):
        
        self.test_storage_handler = None
    
    def test_storage_handler_get_local_archive_dir(self):
        
        self.storage_handler.get_local_archive_dir
        
    def test_storage_handler_get_lsdf_archive_dir(self):
        
        self.storage_handler.create_lsdf_archive
        
    def test_storage_handler_client_config(self):
        
        logger.info(self.storage_handler.client_config())
        
    def test_storage_handler_register_session(self):
        
        logger.info(self.storage_handler.register_session())
        
    def test_storage_handler_reset_connection_cache(self):
        
        logger.info(self.storage_handler.reset_connection_cache())


class TestConfig(object):
    
    DEBUG = True
    DEVELOPMENT = True
    LSDF_ARCHIVE = r'/mnt/ASSAS/upload/'
    LOCAL_ARCHIVE = r'/root/upload/'
    PYTHON_VERSION = r'/opt/python/3.11.8/bin/python3.11'
    ASTEC_ROOT = r'/root/astecV3.1.1_linux64/astecV3.1.1/code/proc/astec.py' 
    ASTEC_PARSER = r'/root/assas-data-hub/assas_database/assasdb/assas_astec_parser.py'    

class AssasDatabaseManagerTest(unittest.TestCase):
    
    def setUp(self):
        
        config = TestConfig()
        self.database_manager = AssasDatabaseManager(config)
        
    def tearDown(self):
        
        self.database_manager = None

    def test_database_manager_drop(self):

        self.database_manager.drop()
        
    def test_database_manager_get_datasets(self):

        self.database_manager.get_database_entries()
        
    def test_database_manager_basic_upload(self):

        test_archive = os.path.dirname(os.path.abspath(__file__)) \
            + '/data/PWR1300_LOCA_12P_CL_linux_64.bin.zip'
        
        logger.info(f'test_archive {test_archive}')
        
        self.database_manager.process_archive(test_archive)
        
    def test_database_store_100_datasets(self):
        
        self.database_manager.drop()
        
        for i in range(0, 100):
            self.database_manager.store_local_archive(str(uuid4()))


class AssasDatabaseHandlerTest(unittest.TestCase):
    
    def setUp(self):
        
        self.database_handler = AssasDatabaseHandler('mongodb://localhost:27017/')
        
    def tearDown(self):
        
        self.database_handler = None

    def test_database_handler_insert_dataset(self):
        
        document = AssasDocumentFile.get_test_document_file()
        self.database_handler.insert_file_document(document)
        
        
if __name__ == '__main__':
    unittest.main()