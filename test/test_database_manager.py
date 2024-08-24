import unittest
import logging
import os
import shutil
import sys

from abc import ABC, abstractmethod
from datetime import datetime
from uuid import uuid4
from typing import List, Tuple, Union

from assasdb import AssasDatabaseManager, AssasAstecArchive

logger = logging.getLogger('assas_test')

logging.basicConfig(
    format = '%(asctime)s %(module)s %(levelname)s: %(message)s',
    level = logging.INFO,
    stream = sys.stdout)

class SBO_fb_100_samples:
    
    def __init__(
        self,
        number_of_samples: int = 100
    ) -> None:
        
        self._archive_list = [SBO_fb_100_samples.archive_factory(sample) for sample in range(1, number_of_samples + 1)]
        
    def get_archive_list(
        self
    ) -> List[AssasAstecArchive]:
        
        return self._archive_list
    
    @staticmethod
    def archive_factory(
        number: int
    )-> AssasAstecArchive:
        
        return AssasAstecArchive(
            f'SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS_{number}',
            'SBO fb',
            '08/05/2024, 23:25:37',
            'Anastasia Stakhanova',
            f'Station blackout scenario number, with 2 parameters {number}',
            f'/mnt/ASSAS/upload_horeka/results/24b15f81-d4fd-4605-b324-0f85ab07917f/all_samples/sample_{number}/STUDY/TRANSIENT/BASE_SIMPLIFIED/SBO/SBO_feedbleed/SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS.bin',
            f'/mnt/ASSAS/upload_horeka/results/24b15f81-d4fd-4605-b324-0f85ab07917f/all_samples/sample_{number}/result/dataset.h5'
        )
    
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

    def test_database_manager_empty(self):

        self.database_manager.empty_internal_database()
        
    def test_database_manager_get_datasets(self):
        
        self.database_manager.get_all_database_entries()
        
    def test_database_manager_SBO_fb_100_samples_register(self):
        
        archives = SBO_fb_100_samples().get_archive_list()
        self.database_manager.empty_internal_database()
        
        self.database_manager.register_archives(archives)
        
        entries = self.database_manager.get_all_database_entries()
        self.assertEqual(len(entries), len(archives))
        
    def test_database_manager_SBO_fb_100_samples_convert_archive_to_hdf5(self):
        
        archive_list = SBO_fb_100_samples(2).get_archive_list()
        
        self.assertTrue(self.database_manager.convert_archives_to_hdf(archive_list[0]))
        
    def test_database_manager_SBO_fb_100_samples_convert_archives_to_hdf5(self):
        
        archive_list = SBO_fb_100_samples(2).get_archive_list()
        
        self.assertTrue(self.database_manager.convert_archives_to_hdf(archive_list))
        

if __name__ == '__main__':
    unittest.main()