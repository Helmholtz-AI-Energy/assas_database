import os
import smbclient

class AssasStorageHandler:
    
    def __init__(self, mount_point="/mnt/ASSAS/", archive_dir="/test/", tmp_dir="./tmp/"):
        
        self.mount_point = mount_point
        self.archive_dir = archive_dir
        self.tmp_dir = tmp_dir

        self.path = self.mount_point + self.archive_dir
        
        self.user = 'ke4920'
        self.password = 'R.adio_!1234'
        self.server = 'os.lsdf.kit.edu'
        self.share = 'kit\scc\projects\ASSAS'
        
        self.smbclient_config = self.client_config()
        self.session = self.register_session()
        
    def create_lsdf_archive(self):

        if not os.path.isdir(self.path):
            os.mkdir(self.path)
            
    def create_dataset_archive(self, path):
        
        if not os.path.isdir(path):
            os.mkdir(path)
            
    def get_path(self):
        
        return self.path
    
    def register_session(self):
        
        return smbclient.register_session(self.server, username=self.user, password=self.password)
        
    def client_config(self):
        
        return smbclient.ClientConfig(username=self.user, password=self.password)
    
    def reset_connection_cache():
        
        return smbclient.reset_coonection_cache()
