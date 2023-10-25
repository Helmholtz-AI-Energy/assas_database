import os

class AssasStorageHandler:
    
    def __init__(self, mount_point="/mnt/ASSAS/", archive_dir="/test/", tmp_dir="./tmp/"):
        
        self.mount_point = mount_point
        self.archive_dir = archive_dir
        self.tmp_dir = tmp_dir
        self.path = self.mount_point + self.archive_dir
        
    def create_lsdf_archive(self):

        if not os.path.isdir(self.mount_point + self.archive_dir):
            os.mkdir(self.mount_point + self.archive_dir)
        
        if not os.path.isdir(self.path):
            os.mkdir(self.path)
            
    def create_dataset_archive(self, path):
        
        if not os.path.isdir(path):
            os.mkdir(path)
            
    def get_path(self):
        
        return self.path