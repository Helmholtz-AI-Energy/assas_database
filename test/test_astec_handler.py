import sys
import unittest
import logging 

from typing import List, Tuple, Union

from assasdb import AssasAstecHandler, AssasAstecArchive

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

class AssasAstecHandlerTest(unittest.TestCase):
    
    def setUp(self):
        
        self.config = TestConfig()
        self.astec_handler = AssasAstecHandler(self.config)        
        
    def tearDown(self):
        
        self.astec_handler = None
        
#    def test_astec_handler_read_binaries(self):
#        
#        archive_list = SBO_fb_100_samples(2).get_archive_list()
#        archive_path_list = [archive.archive_path for archive in archive_list]
#        
#        dataset_list = self.astec_handler.read_astec_archives(archive_path_list)
#        self.assertEqual(len(archive_list),len(dataset_list))

#    def test_astec_handler_get_list_of_saving_times(self):
#        
#        archive_list = SBO_fb_100_samples(2).get_archive_list()
#        archive_path_list = [archive.archive_path for archive in archive_list]
#        
#        lists_of_saving_time = self.astec_handler.get_lists_of_saving_times(archive_path_list)
#        
#        self.assertEqual(len(lists_of_saving_time[0]), 15927)      
#        self.assertEqual(len(lists_of_saving_time[1]), 66849)
        
    def test_astec_handler_get_size_of_archive_in_giga_bytes(self):
        
        size_in_giga_bytes = AssasAstecHandler.get_size_of_archive_in_giga_bytes(69941)        
        self.assertEqual(size_in_giga_bytes, 117.50088)

if __name__ == '__main__':
    unittest.main()        