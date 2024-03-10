import os
import zipfile
import glob
import logging

logger = logging.getLogger('assas_app')

class AssasAstecHandler:
    
    def __init__(self):
        
        self.python_version = 'python3'
        self.python_interface = '~/astecV3.1/code/proc/astec.py'
        self.astec_parser = '~/assas_app/assas_database/assasdb/assas_astec_parser.py'
        self.space = " "
        self.command = self.python_version + self.space + self.python_interface + self.space + self.astec_parser

    @staticmethod
    def unzip_archive(dir: str, target_dir: str):
            
        with zipfile.ZipFile(dir, 'r') as zip:
            zip.extractall(target_dir)
    
    @staticmethod
    def get_astec_archive(archive_dir: str):
        
        logger.info(f'archive directory: {archive_dir}')
        
        zip = glob.glob(dir + '/*.zip')
        
        logger.info(f'archive directory: {zip}')
        
        if len(zip) != 1:
            raise ValueError('no or more than one archive present')
            return
        return zip[0]

    def convert_archive(self, archive_dir: str):
        
        current_dir = os.getcwd()
        logger.info(f'current working directory: {current_dir}')

        os.chdir(archive_dir)

        logger.info(f'changed to archive directory: {current_dir}')
       
        logger.info(f'execute command: {self.command}')
        
        os.system(self.command)
            
        os.chdir(archive_dir)
        logger.info(f'changed back to archive_dir: {archive_dir}')