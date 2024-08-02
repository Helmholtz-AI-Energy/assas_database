import unittest
import logging
import os
import shutil

from uuid import uuid4
from assasdb import AssasDatabaseManager
from assasdb import AssasStorageHandler

logger = logging.getLogger('assas_app')

class TestConfig(object):
    
    DEBUG = True
    DEVELOPMENT = True
    LSDF_ARCHIVE = r'/mnt/ASSAS/upload_test/'
    LOCAL_ARCHIVE = r'/root/upload/'
    PYTHON_VERSION = r'/opt/python/3.11.8/bin/python3.11'
    ASTEC_ROOT = r'/root/astecV3.1.1_linux64/astecV3.1.1'
    ASTEC_COMPUTER = r'linux_64'
    ASTEC_COMPILER = r'release' 
    ASTEC_PARSER = r'/root/assas-data-hub/assas_database/assasdb/assas_astec_parser.py'
    CONNECTIONSTRING = r'mongodb://localhost:27017/'

class AssasStorageHandlerTest(unittest.TestCase):
    
    def setUp(self):
        
        self.config = TestConfig()
        self.storage_handler = AssasStorageHandler(self.config)
    
    def tearDown(self):
        
        self.storage_handler = None
    
    def test_storage_handler_get_local_archive_dir(self):
        
        self.assertEqual(
            self.storage_handler.get_local_archive_dir(),
            self.config.LOCAL_ARCHIVE
        )
        
    def test_storage_handler_get_lsdf_archive_dir(self):
        
        self.assertEqual(
            self.storage_handler.get_lsdf_archive_dir(),
            self.config.LSDF_ARCHIVE
        )    