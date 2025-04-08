import unittest
import logging
import os
import shutil
import sys
import uuid
import pickle 

from abc import ABC, abstractmethod
from datetime import datetime
from uuid import uuid4
from typing import List, Tuple, Union

from assasdb import AssasDatabaseManager, AssasAstecArchive, AssasDocumentFileStatus

logger = logging.getLogger('assas_test')

logging.basicConfig(
    format = '%(asctime)s %(module)s %(levelname)s: %(message)s',
    level = logging.DEBUG,
    stream = sys.stdout)

class SBO_fb_test_samples:
    
    def __init__(
        self,
        config: dict,        
    ) -> None:
        
        self._config = config
        self._upload_uuids = [uuid.UUID('2bdd775d-442c-487f-a0a0-9aec7f47d796'),uuid.UUID('ce3b0594-c213-4339-a334-f4a099b17da9')]
        self._archive_list = [SBO_fb_test_samples.archive_factory(upload_uuid) for upload_uuid in self._upload_uuids]
        
    def get_archive_list(
        self
    ) -> List[AssasAstecArchive]:
        
        return self._archive_list
    
    @staticmethod
    def archive_factory(
        upload_uuid: uuid4
    )-> AssasAstecArchive:
        
        return AssasAstecArchive(
            upload_uuid=f'{str(upload_uuid)}',
            name='SBO fb',
            date='08/05/2024, 23:25:37',
            user='ke4920',
            description=f'Station blackout scenario number, with 2 parameters',
            archive_path=f'/mnt/ASSAS/upload_test/{str(upload_uuid)}/STUDY/TRANSIENT/BASE_SIMPLIFIED/SBO/SBO_feedbleed/SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS.bin',
            result_path=f'/mnt/ASSAS/upload_test/{str(upload_uuid)}/result/dataset.h5'
        )
    
class AssasDatabaseManagerTest(unittest.TestCase):
    
    def setUp(self):
        
        self.database_manager = AssasDatabaseManager()
        
    def tearDown(self):
        
        self.database_manager = None

    def test_database_manager_empty(self):

        self.database_manager.empty_internal_database()
        
    def test_database_manager_get_datasets(self):
        
        frames = self.database_manager.get_all_database_entries()
        print(set(frames['system_status']))
    ''' 
    def test_database_manager_SBO_fb_100_samples_register(self):
        
        self.database_manager.empty_internal_database()
        
        archives = SBO_fb_test_samples(self.config).get_archive_list()
        logger.info(len(archives))
        
        self.database_manager.register_archives(archives)
        
        entries = self.database_manager.get_all_database_entries()
        self.assertEqual(len(entries), len(archives))
        
    def test_database_manager_process_upload(self):
        
        upload_uuid = uuid.UUID('e406d8aa-f370-4b58-9da1-3a896cd04a87')
                
        self.database_manager.process_upload(upload_uuid)
        
        document = self.database_manager.get_database_entry_by_upload_uuid(upload_uuid)
        
        self.assertEqual(str(upload_uuid), document['system_upload_uuid'])
    '''    
    def test_database_manager_process_uploads(self):
        
        self.assertTrue(self.database_manager.process_uploads())
        
    def test_database_manager_process_uploads_reload(self):
        
        self.assertTrue(self.database_manager.process_uploads_with_reload_flag())
        
    #def test_database_manager_convert_archives_to_hdf5(self):
    #    
    #    self.assertTrue(self.database_manager.convert_archives_to_hdf5(number_of_archives_to_convert=1))
        
    def test_database_manager_get_size(self):
        
        size_bytes = AssasDatabaseManager.get_size_of_directory_in_bytes('/mnt/ASSAS/upload_test/0c65e12b-a75b-486b-b3ff-cc68fc89b78a/STUDY/TRANSIENT/BASE_SIMPLIFIED/SBO/SBO_feedbleed/SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS.bin')
        size = AssasDatabaseManager.convert_from_bytes(size_bytes)
        print(f'size {size}')
        
    def test_database_manager_file_size(self):
        
        size = AssasDatabaseManager.file_size('/mnt/ASSAS/upload_test/0c65e12b-a75b-486b-b3ff-cc68fc89b78a/result/dataset.h5')
        print(f'size {size}')
        
    def test_database_manager_update_archive_sizes(self):
        
        self.assertTrue(self.database_manager.update_archive_sizes(
            number_of_archives = 10
        ))

    def test_database_manager_set_status(self):
        
        document_uuid = uuid.UUID('46a980bc-0d2c-4ab9-9ced-4a5e11e8d1ef')
        status = AssasDocumentFileStatus.VALID
               
        self.database_manager.set_document_status_by_uuid(document_uuid, status)
        
    def test_database_manager_update_upload_information(self):

        document_uuid = uuid.UUID('b6279ecb-0580-4ee9-862a-6754c62ff89c')
        archive_paths = ['/all_samples/sample_1/STUDY/TRANSIENT/BASE_SIMPLIFIED/SBO/SBO_feedbleed/SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS.bin']
        
        self.database_manager.update_upload_info(document_uuid, 'archive_paths', archive_paths)
    
    '''
    def test_database_manager_set_to_converted(self):
        
        frames = self.database_manager.get_all_database_entries()
        print(set(frames['system_status']))
        
        for idx in frames.index:
            
            if frames['system_status'][idx] == AssasDocumentFileStatus.VALIDATED:
                
                status = AssasDocumentFileStatus.CONVERTED
                
                document_uuid_str = frames['system_uuid'][idx]
                document_uuid = uuid.UUID(document_uuid_str)
                
                self.database_manager.set_document_status_by_uuid(document_uuid, status)
             
    def test_database_manager_get_upload_uuids(self):
        
        AssasDatabaseManager.get_upload_uuids2('/mnt/ASSAS/upload_test')
    '''
    def test_database_manager_convert_next(self):
        
        self.database_manager.convert_next_validated_archive()
        
    def test_database_manager_collect_meta(self):
        
        self.database_manager.collect_meta_data_after_conversion()
        
    def test_database_manager_get_upload_time(self):
        
        directory = '/mnt/ASSAS/upload_test/defb5a82-edeb-4efb-b824-fd15d95317cf'
        self.database_manager.get_upload_time(directory)
        
    def test_database_manager_backup_internal_database(self):
        
        self.database_manager.backup_internal_database()
        
    def test_database_manager_reset_invalid_archives(self):
        
        self.database_manager.reset_invalid_archives()
        
    def test_database_manager_reset_result_files(self):
        
        self.database_manager.reset_all_result_files()
        

if __name__ == '__main__':
    unittest.main()