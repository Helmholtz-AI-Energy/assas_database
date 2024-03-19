import os
import smbclient
import logging
import shutil
import errno

logger = logging.getLogger('assas_app')

class AssasStorageHandler:
    
    def __init__(self, local_archive, lsdf_archive):
        
        self.local_archive = local_archive
        self.lsdf_archive = lsdf_archive
        
        self.create_local_archive()
        self.create_lsdf_archive()
        
    def store_archive_on_share(self, system_uuid: str):
        
        logger.info(f'copy archive to share (uuid: {system_uuid})')
        
        try:
            shutil.copytree(self.local_archive + system_uuid, self.lsdf_archive + system_uuid)
        except:    
        #except OSError as exc:
            logger.warning(f'exception during copy process occured')            
            #if exc.errno in (errno.ENOTDIR, errno.EINVAL):
            #    shutil.copy(self.local_archive + system_uuid, self.lsdf_archive + system_uuid)
        
        logger.info(f'copied archive to share {system_uuid}')
        
    def create_lsdf_archive(self):

        logger.info(f'create lsdf archive {self.lsdf_archive}')
        
        if not os.path.isdir(self.lsdf_archive):
            os.makedirs(self.lsdf_archive)
        else:
            logger.warning('lsdf archive already exists')
            
    def create_local_archive(self):

        logger.info(f'create local archive {self.local_archive}')
        
        if not os.path.isdir(self.local_archive):
            os.makedirs(self.local_archive)
        else:
            logger.warning(f'local archive already exists {self.local_archive}')
            
    def create_dataset_archive(self, path):
        
        logger.info(f'create dataset archive {path}')
        
        if not os.path.isdir(path):
            os.makedirs(path)
        else:
            logger.warning(f'dataset archive already exists {path}')
            
    def get_local_archive_dir(self):
        
        return self.local_archive
    
    def get_lsdf_archive_dir(self):
        
        return self.lsdf_archive
    
