import unittest
import logging
import os
import shutil
import uuid

from uuid import uuid4
from assasdb import AssasDatabaseHandler
from assasdb import AssasDocumentFile

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

class AssasDatabaseHandlerTest(unittest.TestCase):
    
    def setUp(self):
        
        config = TestConfig()
        self.database_handler = AssasDatabaseHandler(config)
        
        self.database_handler.drop_file_collection()
        
    def tearDown(self):
        
        self.database_handler = None

    def test_database_handler_insert_and_find(self):
        
        document = AssasDocumentFile.get_test_document_file()
        uuid = document['system_uuid']
        
        self.database_handler.insert_file_document(document)
        
        found_document = self.database_handler.get_file_document_by_uuid(uuid)
                
        self.assertEqual(document, found_document)
        
    def test_database_handler_insert_and_find_by_path(self):
        
        document = AssasDocumentFile.get_test_document_file()
        path = document['system_path']
        
        self.database_handler.insert_file_document(document)
        
        found_document = self.database_handler.get_file_document_by_path(path)
                
        self.assertEqual(document, found_document)
        
    def test_database_handler_insert_update_and_find(self):
        
        document = AssasDocumentFile.get_test_document_file()
        uuid = document['system_uuid']
        update = {'system_result':'testresultpath'}
        
        self.database_handler.insert_file_document(document)
        
        found_document = self.database_handler.get_file_document_by_uuid(uuid)
        self.assertEqual(document, found_document)        
        
        self.database_handler.update_file_document_by_uuid(uuid, update)
        
        found_document = self.database_handler.get_file_document_by_uuid(uuid)
                
        self.assertEqual(update['system_result'], found_document['system_result'])
        
    def test_database_handler_insert_update_by_path_and_find(self):
        
        document = AssasDocumentFile.get_test_document_file()
        path = document['system_path']
        update = {'system_result':'testresultpath'}
        
        self.database_handler.insert_file_document(document)
        
        found_document = self.database_handler.get_file_document_by_path(path)
        self.assertEqual(document, found_document)        
        
        self.database_handler.update_file_document_by_path(path, update)
        
        found_document = self.database_handler.get_file_document_by_path(path)
                
        self.assertEqual(update['system_result'], found_document['system_result'])
        
    def test_database_handler_insert_update_by_upload_uuid_and_find(self):
        
        new_upload_uuid = uuid4()
        document = AssasDocumentFile.get_test_document_file(system_upload_uuid=str(new_upload_uuid))
        print(document)
        update = {'system_user':'usertochange'}
        
        self.database_handler.insert_file_document(document)
        
        found_document = self.database_handler.get_file_document_by_upload_uuid(new_upload_uuid)
        self.assertEqual(document, found_document)        
        
        self.database_handler.update_file_document_by_upload_uuid(new_upload_uuid, update)
        
        found_document = self.database_handler.get_file_document_by_upload_uuid(new_upload_uuid)
                
        self.assertEqual(update['system_user'], found_document['system_user'])
        
    def test_database_handler_empty_database(self):
        
        self.database_handler.drop_file_collection()
        
    def test_database_handler_delete_one_by_upload_uuid(self):
        
        upload_uuid = uuid.UUID('b6279ecb-0580-4ee9-862a-6754c62ff89c')
        self.database_handler.delete_file_document_by_upload_uuid(upload_uuid)
        
        upload_uuid = uuid.UUID('66045178-aac4-414d-8353-3e3db44f36cc')
        self.database_handler.delete_file_document_by_upload_uuid(upload_uuid)
        
        
        
if __name__ == '__main__':
    unittest.main()