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
            backup_directory = '/root/backup_mongodb',
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
            backup_directory = '/mnt/ASSAS/backup_mongodb',
            database_name = 'assas',
            file_collection_name = 'files',
        )
        list_to_update = [
            #(uuid.UUID('3ec9fd4c-9a52-4675-bf6c-df38de42e9e9'), 'SBO_KIT_init_sim_s2'),
            #(uuid.UUID('9660f85e-ed19-400f-952f-0ee36d4c50c6'), 'SBO_KIT_init_sim_s4'),
            #(uuid.UUID('02db5e8a-b684-445d-96e9-382148ed2765'), 'SBO_KIT_init_sim_s5'),
            (uuid.UUID('f4361f9e-01b4-49cf-a9a5-b8bf82db7bf4'), 'CIEMAT-test_0'),
            (uuid.UUID('2a7e2707-a8c4-47c0-9eae-3bea6dd42cf3'), 'CIEMAT-test_1'),
        ]
        
        for updates in list_to_update:
            
            update = {'meta_name': updates[1]}
            self.database_handler.update_file_document_by_upload_uuid(updates[0], update)
            document = self.database_handler.get_file_document_by_upload_uuid(updates[0])
            
            self.assertEqual(document['meta_name'], update['meta_name'])
        
    
    #Test case to delete database entries manually.
    def test_database_handler_delete_many_by_upload_uuid(self):
        
        
        self.database_handler = AssasDatabaseHandler(
            connection_string = 'mongodb://localhost:27017/',
            backup_directory = '/mnt/ASSAS/backup_mongodb',
            database_name = 'assas',
            file_collection_name = 'files'
        )
        list_to_delete = [
            uuid.UUID('d6795e22-6563-4db0-b640-5e5f6d75c36f'),
            #uuid.UUID('c8f26900-c97e-4879-80ac-a5b1c2dee799'),
            #uuid.UUID('b306989c-2c7f-455c-975f-c43ae465422b'),
            #uuid.UUID('39a0140c-f309-4ab7-bada-0b0095d00b5f'),
        ]
        
        for upload_uuid in list_to_delete:
            self.database_handler.delete_file_documents_by_upload_uuid(
                upload_uuid = upload_uuid
            )
    
    def test_database_handler_unset_meta_data_variables(self):
        
        self.database_handler = AssasDatabaseHandler(
            connection_string = 'mongodb://localhost:27017/',
            backup_directory = '/mnt/ASSAS/backup_mongodb',
            database_name = 'assas',
            file_collection_name = 'files'
        )
        
        system_uuid = uuid.UUID('144f6875-b09e-45d5-9656-0cfbac61c7ab')
        self.database_handler.unset_meta_data_variables(system_uuid)

    def test_database_handler_get_documents_to_collect_meta(self):
        
        self.database_handler = AssasDatabaseHandler(
            connection_string = 'mongodb://localhost:27017/',
            backup_directory = '/mnt/ASSAS/backup_mongodb',
            database_name = 'assas',
            file_collection_name = 'files'
        )
        documents = self.database_handler.get_file_documents_to_collect_meta_data()
        document_files = [AssasDocumentFile(document) for document in documents]
        
        logger.info(f'Found documents: {len(document_files)}.')
        
        for document in document_files:
            print(f'{document}')


if __name__ == '__main__':
    unittest.main()