
import numpy

from .assas_database_file import AssasFileHandler, AssasHdf5DataFileHandler
from .assas_database_dataset import AssasDataset

class AssasDataHandler:
    
    fileHandler: AssasFileHandler
    
    def __init__(self, archive_dir):
        
        self.archive_path = archive_dir               
        self.dataset = self.load_data_from_archive(archive_dir)        
        self.fileHandler = self.load_file_handler(self.dataset)
    
    def load_file_handler(self, dataset: AssasDataset):
    
        return AssasHdf5DataFileHandler(dataset)
    
    def get_dataset_data(self):
        
        return self.dataset.get_data()
                
    def get_dataset(self):

        return self.dataset
    
    def load_data_from_archive(self, archive_dir) -> AssasDataset:
        
        print("load %s" % (archive_dir))
        
        dataset = self.get_test_dataset()
        
        return dataset
    
    def get_document_file(self):
        
        return self.fileHandler.get_document_file()
    
    def generate_file(self):
        
        return self.fileHandler.generate_file()
    
    def get_archive_path(self):
        
        return self.fileHandler.get_archive_path()
    
    @staticmethod
    def get_test_dataset():
        
        name = "test"
        variables = ["Var1","Var2"]
        channels = 4
        meshes = 16
        samples = 1000        
        
        return AssasDataset(name, variables, channels, meshes, samples)