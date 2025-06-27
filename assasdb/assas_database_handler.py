import os
import logging
import bson

from pymongo import MongoClient
from bson.objectid import ObjectId
from uuid import uuid4
from pathlib import Path

from assasdb.assas_document_file import AssasDocumentFileStatus

logger = logging.getLogger("assas_app")


class AssasDatabaseHandler:
    """
    Handles interactions with the internal MongoDB client, including dumping and
    restoring collections.

    Attributes:
        client (MongoClient): The MongoDB client instance.
        backup_directory (Path): The directory for storing backup files.
        db_handle (Database): The database handle.
        file_collection (Collection): The file collection handle.
    """

    def __init__(
        self,
        client: MongoClient,
        backup_directory: str,
        database_name: str = "assas",
        file_collection_name: str = "files",
    ) -> None:
        """
        Initializes the AssasDatabaseHandler instance.

        Args:
            connection_string (str): The MongoDB connection string.
            backup_directory (str): The path to the backup directory.
            database_name (str, optional): The name of the database.
            Defaults to "assas".
            file_collection_name (str, optional):
            The name of the file collection. Defaults to "files".

        Raises:
            ValueError: If the backup directory does not exist.
        """
        if not Path(backup_directory).exists():
            raise ValueError(f"Backup directory '{backup_directory}' does not exist.")

        self.client = client

        self.backup_directory = Path(backup_directory)
        self.db_handle = self.client[database_name]
        self.file_collection = self.db_handle[file_collection_name]

    def dump_collections(
        self,
        collection_names,
    ) -> None:
        """
        -----------
        Dumps specified collections into backup files in the backup directory.
        Args:
            collection_names (List[str]): A list of collection names to dump.
        Returns:
            None
        -----------
        Example:
            AssasDatabaseHandler.dump_collections(["collection1", "collection2"])
        -----------
        """
        for collection_name in collection_names:
            logger.info(f"Dump collection {collection_name} into a backup file.")

            with open(
                Path.joinpath(self.backup_directory, f"{collection_name}.bson"), "wb+"
            ) as f:
                for doc in self.db_handle[collection_name].find():
                    f.write(bson.BSON.encode(doc))

    def restore_collections(
        self,
    ) -> None:
        """
        -----------
        Restores collections from backup files in the backup directory.
        Args:
            None
        Returns:
            None
        -----------
        Example:
            AssasDatabaseHandler.restore_collections()
        -----------
        """
        for collection in os.listdir(self.backup_directory):
            if collection.endswith(".bson"):
                with open(Path.joinpath(self.backup_directory, collection), "rb+") as f:
                    self.db_handle[collection.split(".")[0]].insert_many(
                        bson.decode_all(f.read())
                    )

    def read_collection_from_backup(self, collection_file="files.bson") -> None:
        """
        -----------
        Reads a collection from a backup file.
        Args:
            collection_file (str): The name of the backup file to read.
        Returns:
            collection: The collection read from the backup file.
        -----------
        Example:
            collection = AssasDatabaseHandler.read_collection_from_backup("files.bson")
        -----------
        """
        collection = []

        with open(Path.joinpath(self.backup_directory, collection_file), "rb+") as f:
            collection = bson.decode_all(f.read())

        return collection

    def get_db_handle(self):
        """
        -----------
        Returns the database handle.
        Args:
            None
        Returns:
            db_handle: The database handle.
        -----------
        Example:
            db_handle = AssasDatabaseHandler.get_db_handle()
        -----------
        """
        return self.db_handle

    def get_file_collection(self):
        """
        -----------
        Returns the file collection.
        Args:
            None
        Returns:
            file_collection: The file collection.
        -----------
        Example:
            file_collection = AssasDatabaseHandler.get_file_collection()
        -----------
        """
        if not self.file_collection:
            logger.warning("File collection is not initialized. Returning None.")
            return None

        logger.info("Returning file collection.")
        return self.file_collection

    def get_all_file_documents(self):
        """
        -----------
        Returns all file documents in the file collection.
        Args:
            None
        Returns:
            A cursor to the file documents.
        -----------
        Example:
            file_documents = AssasDatabaseHandler.get_all_file_documents()
        -----------
        """
        if not self.file_collection:
            logger.warning("File collection is not initialized. Returning None.")
            return None

        logger.info("Returning all file documents.")
        return self.file_collection.find()

    def insert_file_document(self, file: dict):
        """
        -----------
        Inserts a file document into the file collection.
        Args:
            file (dict): The file document to insert.
        Returns:
            None
        -----------
        Example:
            AssasDatabaseHandler.insert_file_document(file)
        -----------
        """

        logger.info(f"Insert file document: {file}.")
        self.file_collection.insert_one(file)

    def drop_file_collection(self):
        """
        -----------
        Drops the file collection.
        Args:
            None
        Returns:
            None
        -----------
        Example:
            AssasDatabaseHandler.drop_file_collection()
        -----------
        """

        logger.info("Dropping file collection.")
        self.file_collection.drop()

    def get_file_document(self, id: str):
        """
        -----------
        Returns a file document by its ID.
        Args:
            id (str): The ID of the file document.
        Returns:
            The file document with the given ID.
        -----------
        Example:
            file_document = AssasDatabaseHandler.get_file_document(id)
        -----------
        """

        return self.file_collection.find_one(ObjectId(id))

    def get_file_document_by_uuid(self, uuid: uuid4):
        """
        -----------
        Returns a file document by its UUID.
        Args:
            uuid (uuid4): The UUID of the file document.
        Returns:
            The file document with the given UUID.
        -----------
        Example:
            file_document = AssasDatabaseHandler.get_file_document_by_uuid(uuid)
        -----------
        """

        return self.file_collection.find_one({"system_uuid": str(uuid)})

    def get_file_document_by_upload_uuid(
        self,
        upload_uuid: uuid4,
    ):
        """
        -----------
        Returns a file document by its upload UUID.
        Args:
            upload_uuid (uuid4): The upload UUID of the file document.
        Returns:
            The file document with the given upload UUID.
        -----------
        Example:
            file_document =
            AssasDatabaseHandler.get_file_document_by_upload_uuid(upload_uuid)
        -----------
        """

        return self.file_collection.find_one({"system_upload_uuid": str(upload_uuid)})

    def get_file_documents_by_upload_uuid(
        self,
        upload_uuid: uuid4,
    ):
        """
        -----------
        Returns file documents by their upload UUID.
        Args:
            upload_uuid (uuid4): The upload UUID of the file documents.
        Returns:
            A cursor to the file documents with the given upload UUID.
        -----------
        Example:
            file_documents =
            AssasDatabaseHandler.get_file_documents_by_upload_uuid(upload_uuid)
        -----------
        """

        return self.file_collection.find({"system_upload_uuid": str(upload_uuid)})

    def get_file_document_by_path(self, path: str):
        """
        -----------
        Returns a file document by its system path.
        Args:
            path (str): The system path of the file document.
        Returns:
            The file document with the given system path.
        -----------
        Example:
            file_document = AssasDatabaseHandler.get_file_document_by_path(path)
        -----------
        """

        return self.file_collection.find_one({"system_path": path})

    def get_file_documents_by_status(self, status: str):
        """
        -----------
        Returns file documents by their system status.
        Args:
            status (str): The system status of the file documents.
        Returns:
            A cursor to the file documents with the given system status.
        -----------
        Example:
            file_documents = AssasDatabaseHandler.get_file_documents_by_status(status)
        -----------
        """

        return self.file_collection.find({"system_status": status})

    def get_file_documents_to_update_size(
        self,
        update_key: str = "...",
    ):
        """
        -----------
        Returns file documents that need their system size updated.
        Args:
            update_key (str): The key to filter the documents by system size.
        Returns:
            A cursor to the file documents that need their system size updated.
        -----------
        Example:
            file_documents =
            AssasDatabaseHandler.get_file_documents_to_update_size(update_key)
        -----------
        """

        return self.file_collection.find({"system_size": update_key})

    def get_file_documents_to_collect_number_of_samples(self, system_status: str):
        """
        -----------
        Returns file documents that need their number of samples collected.
        Args:
            system_status (str): The system status of the file documents.
        Returns:
            A cursor to the file documents that need their number of samples collected.
        -----------
        Example:
            file_documents =
            AssasDatabaseHandler.
            get_file_documents_to_collect_number_of_samples(system_status)
        -----------
        """

        return self.file_collection.find(
            {
                "$and": [
                    {"system_number_of_samples": {"$exists": False}},
                    {"system_status": system_status},
                ]
            }
        )

    def get_file_documents_to_collect_meta_data(
        self,
    ):
        """
        -----------
        Returns file documents that need their meta data collected.
        Args:
            None
        Returns:
            A cursor to the file documents that need their meta data collected.
        -----------
        Example:
            file_documents =
            AssasDatabaseHandler.get_file_documents_to_collect_meta_data()
        -----------
        """

        return self.file_collection.find(
            {
                "$and": [
                    {"meta_data_variables": {"$exists": False}},
                    {"system_status": AssasDocumentFileStatus.VALID},
                ]
            }
        )

    def update_file_document_by_uuid(self, uuid: uuid4, update: dict):
        """
        -----------
        Updates a file document by its UUID.
        Args:
            uuid (uuid4): The UUID of the file document.
            update (dict): The update to apply to the file document.
        Returns:
            The result of the update operation.
        -----------
        Example:
            result = AssasDatabaseHandler.update_file_document_by_uuid(uuid, update)
        -----------
        """

        post = {"$set": update}
        return self.file_collection.update_one({"system_uuid": str(uuid)}, post)

    def update_file_document_by_path(self, path: str, update: dict):
        """
        -----------
        Updates a file document by its system path.
        Args:
            path (str): The system path of the file document.
            update (dict): The update to apply to the file document.
        Returns:
            The result of the update operation.
        -----------
        Example:
            result = AssasDatabaseHandler.update_file_document_by_path(path, update)
        -----------
        """

        post = {"$set": update}
        return self.file_collection.update_one({"system_path": path}, post)

    def update_file_document_by_upload_uuid(self, upload_uuid: uuid4, update: dict):
        """
        -----------
        Updates a file document by its upload UUID.
        Args:
            upload_uuid (uuid4): The upload UUID of the file document.
            update (dict): The update to apply to the file document.
        Returns:
            The result of the update operation.
        -----------
        Example:
            result =
            AssasDatabaseHandler.
            update_file_document_by_upload_uuid(upload_uuid, update)
        -----------
        """

        post = {"$set": update}
        return self.file_collection.update_one(
            {"system_upload_uuid": str(upload_uuid)}, post
        )

    def unset_meta_data_variables(
        self,
        system_uuid: uuid4,
    ):
        """
        -----------
        Unsets the meta data variables for a file document by its system UUID.
        Args:
            system_uuid (uuid4): The system UUID of the file document.
        Returns:
            The result of the unset operation.
        -----------
        Example:
            result = AssasDatabaseHandler.unset_meta_data_variables(system_uuid)
        -----------
        """

        self.file_collection.update_one(
            {"system_uuid": str(system_uuid)}, {"$unset": {"meta_data_variables": ""}}
        )

    def delete_file_document(self, id: str):
        """
        -----------
        Deletes a file document by its ID.
        Args:
            id (str): The ID of the file document.
        Returns:
            The result of the delete operation.
        -----------
        Example:
            result = AssasDatabaseHandler.delete_file_document(id)
        -----------
        """

        return self.file_collection.delete_one({"_id": ObjectId(id)})

    def delete_file_document_by_uuid(self, uuid: uuid4):
        """
        -----------
        Deletes a file document by its UUID.
        Args:
            uuid (uuid4): The UUID of the file document.
        Returns:
            The result of the delete operation.
        -----------
        Example:
            result = AssasDatabaseHandler.delete_file_document_by_uuid(uuid)
        -----------
        """

        return self.file_collection.delete_one({"system_uuid": str(uuid)})

    def delete_file_document_by_upload_uuid(self, upload_uuid: uuid4):
        """
        -----------
        Deletes a file document by its upload UUID.
        Args:
            upload_uuid (uuid4): The upload UUID of the file document.
        Returns:
            The result of the delete operation.
        -----------
        Example:
            result =
            AssasDatabaseHandler.delete_file_document_by_upload_uuid(upload_uuid)
        -----------
        """
        return self.file_collection.delete_one({"system_upload_uuid": str(upload_uuid)})

    def delete_file_documents_by_upload_uuid(self, upload_uuid: uuid4):
        """
        -----------
        Deletes all file documents by their upload UUID.
        Args:
            upload_uuid (uuid4): The upload UUID of the file documents.
        Returns:
            The result of the delete operation.
        -----------
        Example:
            result =
            AssasDatabaseHandler.delete_file_documents_by_upload_uuid(upload_uuid)
        -----------
        """
        return self.file_collection.delete_many(
            {"system_upload_uuid": str(upload_uuid)}
        )
