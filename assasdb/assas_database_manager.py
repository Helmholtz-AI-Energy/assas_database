from .assas_database_handler import DatabaseHandler
from .assas_dataset import AssasDataset

import pandas

class DatabaseManager:

    def __init__(self):
        
        self.connectionstring = "mongodb://localhost:27017/"
        self.mountpoint = "/mnt/ASSAS/"
        self.archive_dir = "test/"
        self.tmp_dir = "./tmp"

        self.database_handler = DatabaseHandler(self.connectionstring)

    def upload(self):

        assas_dataset = AssasDataset(self.mountpoint, self.archive_dir, self.tmp_dir)
       
        assas_dataset.read_binary()

        assas_dataset.create_lsdf_archive()
        
        assas_dataset.create_hdf5()

        dataset_file_document = assas_dataset.get_file_document()
        print(dataset_file_document)

        self.database_handler.insert_file_document(dataset_file_document)
        
    def view(self):
        
        file_collection = self.database_handler.get_file_collection()
        print(file_collection)

        for file in file_collection.find():
            print(file)

        return pandas.DataFrame(list(file_collection.find()))