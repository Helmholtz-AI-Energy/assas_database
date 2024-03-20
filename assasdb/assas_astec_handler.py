import os
import zipfile
import glob
import logging

logger = logging.getLogger('assas_app')

class AssasAstecHandler:
    
    def __init__(self, python_version, astec_root, astec_parser):
        
        self.command = f'{python_version} {astec_root} {astec_parser}'

    def convert_archive(self, archive_dir: str) -> bool:
        
        current_dir = os.getcwd()
        logger.info(f'current working directory is {current_dir}')

        logger.info(f'change to archive directory {archive_dir}')
        os.chdir(archive_dir + '/archive')
        
        logger.info(f'run assas astec parser with command {self.command}')
        os.system(self.command)
            
        logger.info(f'changed back to current_dir: {current_dir}')
        os.chdir(current_dir)
        
    @staticmethod
    def unzip_archive(zipped_archive_path: str) -> bool:
        
        unzipped_archive_dir = os.path.dirname(zipped_archive_path) + '/archive'
        try:
            with zipfile.ZipFile(zipped_archive_path, 'r') as zip:
                
                zip.extractall(unzipped_archive_dir)
        except:
            return False
        
        return True
            
    @staticmethod
    def get_astec_archive(archive_dir: str):
        
        logger.info(f'archive directory: {archive_dir}')
        
        zipped_dir = glob.glob(archive_dir + '/*.zip')
        
        logger.info(f'archive directory: {zipped_dir}')
        
        if len(zipped_dir) != 1:
            raise ValueError('no or more than one archive present')
            return
        
        return zipped_dir[0]