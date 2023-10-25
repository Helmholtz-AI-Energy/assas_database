import pandas

from .assas_database_handler import DatabaseHandler
from .assas_data_handler import AssasDataHandler
from .assas_database_storage import AssasStorageHandler

class AssasDatabaseManager:

    def __init__(self):
        
        self.connectionstring = "mongodb://localhost:27017/"
        self.database_handler = DatabaseHandler(self.connectionstring)
        self.storage_handler = AssasStorageHandler()
        self.storage_handler.create_lsdf_archive()

    def upload(self):
        
        archive_dir = "./data/"
        assas_datahandler = AssasDataHandler(archive_dir)
        
        archive_path = self.storage_handler.create_dataset_archive(assas_datahandler.get_archive_path())
        print("created dataset archive: %s" % (archive_path))
        
        file_path = assas_datahandler.generate_file()
        print("created dataset file: %s" % (file_path))
        
        dataset_file_document = assas_datahandler.get_document_file()
        print(dataset_file_document)

        self.database_handler.insert_file_document(dataset_file_document)
        
    def view(self):
        
        file_collection = self.database_handler.get_file_collection()
        print(file_collection)

        for file in file_collection.find():
            print(file)

        return pandas.DataFrame(list(file_collection.find()))