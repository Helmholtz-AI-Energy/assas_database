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
        
    def test_SBO_fb_100_samples_add_entries(self):
        
        number_of_samples = 35
        lsdf_dest_path = '/mnt/ASSAS/upload_horeka/results/SBO_fb_100_samples'
        lsdf_sub_dir = '/archive/SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS.bin'
        target_path_list = [AssasStorageHandler.create_lsdf_archive_path(lsdf_dest_path, lsdf_sub_dir, sample) for sample in range(1, number_of_samples + 1)]
        
        self.database_manager.drop()
        
        self.database_manager.add_archives_to_database(target_path_list)
            
        entries = self.database_manager.get_database_entries()
        
        self.assertEqual(len(entries), len(target_path_list))
        
    def test_SBO_fb_100_samples_convert_archives(self):
        
        test_archive_dir = '/mnt/ASSAS/upload_horeka/results/SBO_fb_100_samples/sample_32/archive/SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS.bin'
        test_archive_dir_2 = '/mnt/ASSAS/upload_horeka/results/SBO_fb_100_samples/sample_33/archive/SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS.bin'
        
        test_result_dir = '/mnt/ASSAS/upload_horeka/results/SBO_fb_100_samples/sample_32/result/dataset.h5'
        test_result_dir_2 = '/mnt/ASSAS/upload_horeka/results/SBO_fb_100_samples/sample_33/result/dataset.h5'
        
        self.assertTrue(self.database_manager.convert_archive(test_archive_dir, test_result_dir))
        
    def test_SBO_fb_100_samples_update_meta_data(self):
        
        test_archive_dir = '/mnt/ASSAS/upload_horeka/results/SBO_fb_100_samples/sample_32/archive/SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS.bin'
        test_result_dir = '/mnt/ASSAS/upload_horeka/results/SBO_fb_100_samples/sample_32/result/dataset.h5'
        
        self.database_manager.update_meta_data(test_archive_dir, test_result_dir)
        
    def test_database_manager_process_archive(self):

        test_archive = os.path.dirname(os.path.abspath(__file__)) \
            + '/data/PWR1300_LOCA_12P_CL_linux_64.bin.zip'
        
        logger.info(f'test_archive {test_archive}')
        
        self.database_manager.process_archive(test_archive)
        
    def test_database_manager_process_unzipped_archive(self):

        test_archive = '/mnt/ASSAS/upload_horeka/results/SBO_fb_100_samples/sample_32'
        
        logger.info(f'test_archive {test_archive}')
        
        self.database_manager.process_unzipped_archive(test_archive)
        
    def test_database_manager_add_archive_to_database(self):
        
        test_archive = '/mnt/ASSAS/upload_horeka/results/SBO_fb_100_samples/sample_1'
        
        logger.info(f'test_archive {test_archive}')
        
        self.database_manager.add_archive_to_database(test_archive)
        
    def test_database_store_100_datasets(self):        
        
        self.database_manager.drop()
        
        for i in range(0, 100):
            self.database_manager.store_local_archive(str(uuid4()))
        
    def test_database_manager_upload_process(self):
        
        test_uuid = str(uuid4())
        
        self.database_manager.drop()
        
        # fake upload through app
        logger.info(f'store test dataset for uuid {test_uuid}')
        
        local_archive_dir = self.database_manager.storage_handler.local_archive + test_uuid
        self.database_manager.storage_handler.create_dataset_archive(local_archive_dir)
        
        zipped_test_archive = os.path.dirname(os.path.abspath(__file__)) \
            + '/data/PWR1300_LOCA_12P_CL_linux_64.bin.zip'
        
        logger.info(f'copy {zipped_test_archive} to {local_archive_dir}')
        zipped_test_archive = shutil.copy2(zipped_test_archive, local_archive_dir)
        
        self.database_manager.process_archive(zipped_test_archive)       
        
        self.database_manager.synchronize_archive(test_uuid)

if __name__ == '__main__':
    unittest.main()