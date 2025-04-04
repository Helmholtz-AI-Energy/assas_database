import unittest
import logging
import os
import shutil
import uuid

from uuid import uuid4
from assasdb import AssasDatabaseHandler
from assasdb import AssasDocumentFile

logger = logging.getLogger('assas_app')

class AssasDatabaseHandlerTest(unittest.TestCase):
    
    def setUp(self):
        
        self.database_handler = AssasDatabaseHandler(
            connection_string = 'mongodb://localhost:27017/',
            database_name = 'test_assas',
            file_collection_name = 'test_files'
        )
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
        
    def test_database_handler_update_names_by_upload_uuid(self):
        
        self.database_handler = AssasDatabaseHandler(
            connection_string = 'mongodb://localhost:27017/',
            database_name = 'assas',
            file_collection_name = 'files'
        )
        list_to_update = [
            (uuid.UUID('3ec9fd4c-9a52-4675-bf6c-df38de42e9e9'),'SBO_KIT_init_sim_s2'),
            (uuid.UUID('9660f85e-ed19-400f-952f-0ee36d4c50c6'),'SBO_KIT_init_sim_s4'),
            (uuid.UUID('02db5e8a-b684-445d-96e9-382148ed2765'),'SBO_KIT_init_sim_s5'),
            (uuid.UUID('36ee904b-b98e-4385-809e-53a57d2c75f2'),'SBO_KIT_init_sim_s3'),
            (uuid.UUID('b44e548b-848d-424b-a056-0af9938fe3a7'),'SBO_KIT_init_sim_s1'),
        ]
        
        for updates in list_to_update:
            
            update = {'meta_name': updates[1]}
            self.database_handler.update_file_document_by_upload_uuid(updates[0], update)
            document = self.database_handler.get_file_document_by_upload_uuid(updates[0])
            self.assertEqual(document['meta_name'],update['meta_name'])
        
    '''
    Test case to delete database entries manually.
    def test_database_handler_delete_many_by_upload_uuid(self):
        
        
        self.database_handler = AssasDatabaseHandler(
            connection_string = 'mongodb://localhost:27017/',
            database_name = 'assas',
            file_collection_name = 'files'
        )
        list_to_delete = [
            uuid.UUID('4ae44354-325a-4dc1-a9c3-79a671a2feae'),
            uuid.UUID('5e5d7636-b24c-41cf-80f4-997e343c1da1'),
            uuid.UUID('309b2b56-eb03-4113-88be-6c08a696010a'),
            uuid.UUID('4fc93eb0-0c89-4fc1-820b-5f03698cf225'),
            uuid.UUID('3d2d481d-5364-4adf-88b1-2ea9ef3f644e'),
            uuid.UUID('31554b66-dd5e-4460-8a37-396f14d27362'),
        ]
        
        for upload_uuid in list_to_delete:
            self.database_handler.delete_file_documents_by_upload_uuid(
                upload_uuid = upload_uuid
            )
    '''


if __name__ == '__main__':
    unittest.main()