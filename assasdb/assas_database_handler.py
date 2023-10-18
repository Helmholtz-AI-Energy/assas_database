from pymongo import MongoClient

class DatabaseHandler:

    def __init__(self, connectionstring):
        
        self.client = MongoClient(connectionstring)

        self.db_handle = self.client["assas"]
        self.file_collection = self.db_handle["files"]

    def get_db_handle(self):

        return self.db_handle

    def get_file_collection(self):
        
        return self.file_collection
    
    def insert_file_document(self, file):

        self.file_collection.insert_one(file)

    def drop_file_collection(self):

        self.file_collection.drop()