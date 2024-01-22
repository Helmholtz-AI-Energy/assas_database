import uuid
from datetime import datetime
import h5py

from abc import ABC, abstractmethod

from .assas_database_storage import AssasStorageHandler
from .assas_database_dataset import AssasDataset

class AssasFileHandler(ABC):
    
    @abstractmethod
    def generate_file(self) -> str:
        pass
    
    @abstractmethod
    def get_document_file(self) -> str:
        pass
    
    @abstractmethod
    def get_archive_path(self) -> str:
        pass
    
class AssasHdf5DataFileHandler(AssasFileHandler):
    
    def __init__(self, dataset: AssasDataset) -> None:
        
        self.assas_dataset = dataset
        self.storage_handler = AssasStorageHandler()
        
        self.name = self.assas_dataset.name
        self.scenario = "blco"
        
        self.filename = "dataset_%s.h5" % (self.assas_dataset.name)
        self.uuid = str(uuid.uuid4())
        self.upload_time = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        self.creation_time = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        self.path = self.storage_handler.get_path() + str(self.uuid) + "/"
        self.data_link = ""
        
    def generate_file(self) -> str:
        
        with h5py.File(self.path + self.filename, 'w') as h5f:

            # metadata
            h5f.create_group('metadata')
            h5f['metadata'].attrs['name'] = self.name
            h5f['metadata'].attrs['scenario'] = self.scenario
            h5f['metadata'].attrs['filename'] = self.filename
            h5f['metadata'].attrs['uuid'] = self.uuid
            h5f['metadata'].attrs['upload_time'] = self.upload_time
            h5f['metadata'].attrs['creation_time'] = self.creation_time
            h5f['metadata'].attrs['path'] = self.path
            h5f['metadata'].attrs['data_link'] = self.data_link
            
            h5f.create_group('input')
            h5f['input'].attrs['debris'] = 0

            data_group = h5f.create_group('data')
            
            for variable in self.assas_dataset.get_variables():
            
                group = data_group.create_group(variable)
                array = self.assas_dataset.get_data_for_variable(variable)
                group.create_dataset(variable, data = array)

        h5f.close()
        
        return self.path + self.filename
        
    def get_document_file(self) -> str:
        
        return {"uuid": self.uuid, "name": self.name, "scenario": self.scenario, "filename": self.filename, \
            "upload_time": self.upload_time, "creation_time": self.creation_time, \
            "path": self.path, "data_link": self.data_link}
    
    def get_archive_path(self) -> str:
        return self.path
    
    
        
      
