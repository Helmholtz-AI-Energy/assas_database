import os
import zipfile
import glob
import logging

logger = logging.getLogger('assas_app')

def unzip_archive(dir, target_dir):
        
    with zipfile.ZipFile(dir, "r") as zip:
        zip.extractall(target_dir)
        
def get_astec_archive(archive_dir):
    
    logger.info(f'archive directory: {archive_dir}')
    
    zip = glob.glob(dir + "/*.zip")
    
    logger.info(f'archive directory: {zip}')
    
    if len(zip) != 1:
        raise ValueError("no or more than one archive present")
        return
    return zip[0]

def convert_archive(archive_dir):
    
    current_dir = os.getcwd()
    logger.info("current working directory: {0}".format(current_dir))

    os.chdir(archive_dir)

    logger.info("changed to archive directory: {0}".format(os.getcwd()))
    
    python_interface = "~/astecV3.1/code/proc/astec.py"
    astec_parser = "~/assas_app/assas_database/assasdb/assas_astec_parser.py"
    space = " "
    
    command = "python3" + space + python_interface + space + astec_parser
    
    logger.info(command)
    
    os.system(command)
        
    os.chdir(archive_dir)