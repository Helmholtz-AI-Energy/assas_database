import unittest

from assasdb import AssasAstecHandler

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
        
        self.test_archive_dir = '/mnt/ASSAS/upload_horeka/results/SBO_fb_100_samples/sample_32/archive/SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS.bin'
        self.test_archive_dir_2 = '/mnt/ASSAS/upload_horeka/results/SBO_fb_100_samples/sample_33/archive/SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS.bin'
        
        self.test_result_dir = '/mnt/ASSAS/upload_horeka/results/SBO_fb_100_samples/sample_32/result/dataset.h5'
        self.test_result_dir_2 = '/mnt/ASSAS/upload_horeka/results/SBO_fb_100_samples/sample_33/result/dataset.h5'
        
    def tearDown(self):
        
        self.astec_handler = None
        
    def test_astec_handler_get_list_of_saving_times(self):
        
        lists_of_saving_time = self.astec_handler.get_lists_of_saving_times([self.test_archive_dir, self.test_archive_dir_2])
        
        self.assertEqual(len(lists_of_saving_time[0]), 69941)       
        self.assertEqual(len(lists_of_saving_time[1]), 66242)
        
    def test_astec_handler_get_size_of_archive_in_giga_bytes(self):
        
        size_in_giga_bytes = AssasAstecHandler.get_size_of_archive_in_giga_bytes(69941)        
        self.assertEqual(size_in_giga_bytes, 117.50088)
        
    def test_astec_handler_convert_archives(self):
        
        self.assertTrue(self.astec_handler.convert_to_hdf5(self.test_archive_dir, self.test_result_dir))
        