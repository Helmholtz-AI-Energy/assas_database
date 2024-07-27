import unittest
import logging
import os
import shutil

from uuid import uuid4
from assasdb import AssasDatabaseManager
from assasdb import AssasStorageHandler
from assasdb import AssasDatabaseHandler
from assasdb import AssasDocumentFile

logger = logging.getLogger('assas_app')

class TestConfig(object):
    
    DEBUG = True
    DEVELOPMENT = True
    LSDF_ARCHIVE = r'/mnt/ASSAS/upload_test/'
    LOCAL_ARCHIVE = r'/root/upload/'
    PYTHON_VERSION = r'/opt/python/3.11.8/bin/python3.11'
    ASTEC_ROOT = r'/root/astecV3.1.1_linux64/astecV3.1.1/code/proc/astec.py' 
    ASTEC_PARSER = r'/root/assas-data-hub/assas_database/assasdb/assas_astec_parser.py'
    CONNECTIONSTRING = r'mongodb://localhost:27017/'

class AssasStorageHandlerTest(unittest.TestCase):
    
    def setUp(self):
        
        self.config = TestConfig()
        self.storage_handler = AssasStorageHandler(self.config)
    
    def tearDown(self):
        
        self.test_storage_handler = None
    
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


class AssasDatabaseManagerTest(unittest.TestCase):
    
    def setUp(self):
        
        config = TestConfig()
        self.database_manager = AssasDatabaseManager(config)
        
    def tearDown(self):
        
        self.database_manager = None

    def test_database_manager_drop(self):

        self.database_manager.drop()
        
    def test_database_manager_get_datasets(self):
        
        ASTECROOT = "/root/astecV3.1.1_linux64/astecV3.1.1" # root directory of your astec installation
        COMPUTER = "linux_64" # or "win64"
        COMPILER = "release" # or an other compiler, but it is release for official releases
        import numpy as np # It can be necessary to load numpy before pyastec, because numpy importing generate a FPE which can be caught by astec
        import os,sys
        sys.path.append( os.path.join( ASTECROOT, "code","proc") ) # where to find astec.py
        sys.path.append( os.path.join( ASTECROOT, "code","bin", COMPUTER + "-" + COMPILER, "wrap_python" ) ) # where to find pyastec. wrap_python can be replaced by wrap_python_batch for the batch version
        #import AstecParser # needed by astec (class from astec.py)
        #import astec # Contains definition of class Astec which will set all environment variables
        #AP = AstecParser.AstecParser() # initialize a parser of a fictive command line
        #AP.parsed_arguments.compiler=COMPILER # replace the default compiler by the one you want
        #A = astec.Astec(AP) # make an instance of Astec
        #A.set_environment() # initialize all environment variables
        astec_archive_dir = '/mnt/ASSAS/upload_horeka/results/SBO_fb_100_samples/sample_1/archive/SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS.bin'
        import pyastec as pa # pyastec can now be loadded
        #pa.astec_init() # pyastec initialization
        saved_instants = pa.tools.get_list_of_saving_time_from_path(astec_archive_dir)
        print(f'time list {saved_instants}')
        
        #self.database_manager.get_database_entries()
        
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


class AssasDatabaseHandlerTest(unittest.TestCase):
    
    def setUp(self):
        
        config = TestConfig()
        self.database_handler = AssasDatabaseHandler(config)
        
    def tearDown(self):
        
        self.database_handler = None

    def test_database_handler_insert_dataset(self):
        
        document = AssasDocumentFile.get_test_document_file()
        self.database_handler.insert_file_document(document)
        
        
if __name__ == '__main__':
    unittest.main()