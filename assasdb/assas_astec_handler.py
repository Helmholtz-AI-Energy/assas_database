import os
import sys
import zipfile
import glob
import logging
import subprocess
import numpy

from typing import List, Tuple, Union

logger = logging.getLogger('assas_app')

class AssasAstecHandler:
    
    def __init__(
        self,
        config: dict
    ) -> None:
        
        self.command = [f'{config.PYTHON_VERSION}', f'{config.ASTEC_ROOT}', f'{config.ASTEC_PARSER}']
        self.config = config
        
        astec_python_interface_location = os.path.join(config.ASTEC_ROOT, 'code', 'proc')
        if astec_python_interface_location not in sys.path:
            sys.path.append(astec_python_interface_location)
        
        astec_python_location = os.path.join(config.ASTEC_ROOT, 'code', 'bin', config.ASTEC_COMPUTER + '-' + config.ASTEC_COMPILER, 'wrap_python')
        if astec_python_location not in sys.path:
            sys.path.append(astec_python_location)
            
        import AstecParser # needed by astec (class from astec.py)
        import astec # contains definition of class Astec which will set all environment variables
        
        AP = AstecParser.AstecParser() # initialize a parser of a fictive command line
        AP.parsed_arguments.compiler=config.ASTEC_COMPILER # replace the default compiler by the one you want
        A = astec.Astec(AP) # make an instance of Astec
        A.set_environment() # initialize all environment variables
    
    def get_lists_of_saving_times(
        self,
        archive_dir_list: List[str],
    ) -> List[List[str]]:
        
        result_list = []

        import pyastec as pa # pyastec can now be loadded
        pa.astec_init()
        
        for archive_dir in archive_dir_list:
            try:
                saved_instants = pa.tools.get_list_of_saving_time_from_path(archive_dir)
                result_list.append(saved_instants)
            except:
                result_list.append([-1])
       
        pa.end()        
        
        return result_list
    
    @staticmethod
    def get_size_of_archive_in_giga_bytes(        
        number_of_timesteps: int,
        size_of_saving: float = 1.68
    ) -> float:
        
        size_in_giga_bytes = (number_of_timesteps * size_of_saving) / 1000.0
        
        return size_in_giga_bytes

    def convert_archive(
        self,
        archive_dir: str
    ) -> bool:
        
        try:
            current_dir = os.getcwd()
            logger.info(f'current working directory is {current_dir}')

            logger.info(f'change to archive directory {archive_dir}')
            os.chdir(archive_dir + '/archive')
            
            print(f'run assas astec parser with command {self.command}')
            with subprocess.Popen(self.command) as process: process.wait()
                            
            logger.info(f'changed back to current_dir: {current_dir}')
            os.chdir(current_dir)
        except:
            return False
        
        return True
        
    @staticmethod
    def unzip_archive(
        zipped_archive_path: str
    ) -> bool:
        
        unzipped_archive_dir = os.path.dirname(zipped_archive_path) + '/archive'
        try:
            with zipfile.ZipFile(zipped_archive_path, 'r') as zip:
                
                zip.extractall(unzipped_archive_dir)
        except:
            return False
        
        return True
            
    @staticmethod
    def get_astec_archive(
        archive_dir: str
    )-> str:
        
        logger.info(f'archive directory: {archive_dir}')
        
        zipped_dir = glob.glob(archive_dir + '/*.zip')
        
        logger.info(f'archive directory: {zipped_dir}')
        
        if len(zipped_dir) != 1:
            raise ValueError('no or more than one archive present')
            return
        
        return zipped_dir[0]