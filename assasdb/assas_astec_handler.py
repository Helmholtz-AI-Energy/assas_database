import os
import zipfile
import glob
import logging
import subprocess

logger = logging.getLogger('assas_app')

class AssasAstecHandler:
    
    def __init__(
        self,
        config: dict
    ) -> None:
        
        self.command = [f'{config.PYTHON_VERSION}', f'{config.ASTEC_ROOT}', f'{config.ASTEC_PARSER}']

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