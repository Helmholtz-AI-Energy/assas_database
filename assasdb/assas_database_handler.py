"""Database handler for ASSAS application.

This module provides the `AssasDatabaseHandler` class, which manages interactions
with the internal MongoDB client. It includes methods for dumping and restoring
collections, reading and writing file documents, and managing the file collection.
"""

import os
import logging
import bson

from typing import List, Optional
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError
from pymongo.database import Database
from pymongo.cursor import Cursor
from pymongo.results import InsertOneResult, UpdateResult, DeleteResult
from bson.objectid import ObjectId
from uuid import uuid4
from pathlib import Path

from assasdb.assas_document_file import AssasDocumentFileStatus

logger = logging.getLogger(__name__)


class AssasDatabaseHandler:
    """AssasDatabaseHandler class.

    Handle interactions with the internal MongoDB client, including dumping and
    restoring collections.
    Provide methods for reading and writing file documents, as well as managing
    the file collection.
    """

    def __init__(
        self,
        client: Optional[MongoClient] = None,
        connection_string: Optional[str] = None,
        database_name: Optional[str] = None,
        backup_directory: Optional[str] = None,
        file_collection_name: str = "files",
        user_collection_name: str = "users",
        restore_from_backup: bool = False,
    ) -> None:
        """Initialize the AssasDatabaseHandler instance.

        Args:
            client (MongoClient): The MongoDB client to use for database operations.
            connection_string (str): The connection string for the MongoDB client.
            database_name (str): The name of the database to connect to.
            backup_directory (str): The directory where backup files will be stored.
            file_collection_name (str): The name of the file collection.
            user_collection_name (str): The name of the user collection.
            restore_from_backup (bool): Whether to restore collections from backup.

        Returns:
            None

        Example:
            AssasDatabaseHandler(
                client=MongoClient("mongodb://localhost:27017/"),
                backup_directory="/mnt/ASSAS/backup_mongodb",
                database_name="assas",
                file_collection_name="files"
            )

        """
        if client is not None:
            self.client = client
            self._owns_client = False
        else:
            uri = (
                connection_string
                or os.getenv("CONNECTIONSTRING")
                or "mongodb://localhost:27017"
            )
            self.client = MongoClient(uri)
            self._owns_client = True

        self.db_name = database_name or os.getenv("MONGO_DB_NAME") or "assas"
        self.db_handle: Database = self.client[self.db_name]
        self.backup_directory = backup_directory or os.getenv("BACKUP_DIRECTORY")

        self.file_collection_name = file_collection_name
        self.user_collection_name = user_collection_name
        self.file_collection: Collection = self.db_handle[self.file_collection_name]
        self.user_collection: Collection = self.db_handle[self.user_collection_name]

        logger.info(
            f"Connected to MongoDB database '{self.db_name}' "
            f"and collection '{self.file_collection_name}'."
        )

        if restore_from_backup:
            logger.info("Restoring collections from backup files.")
            self.restore_collections()

    def close(self) -> None:
        """Close DB resources (always close the MongoClient if present)."""
        try:
            client = getattr(self, "client", None)
            if client is not None:
                client.close()
                # Optional: prevent double-close usage later
                self.client = None
        except Exception:
            logger.exception("Failed to close MongoClient")

    def dump_collections(
        self,
        collection_names: List[str],
    ) -> None:
        """Dump specified collections into backup files in the backup directory.

        Args:
            collection_names (List[str]): A list of collection names to dump.

        Returns:
            None

        Example:
            AssasDatabaseHandler.dump_collections(["collection1", "collection2"])

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
        """Restore collections from backup files in the backup directory.

        Args:
            None

        Returns:
            None

        Example:
            AssasDatabaseHandler.restore_collections()

        """
        for collection in os.listdir(self.backup_directory):
            if collection.endswith(".bson"):
                with open(Path.joinpath(self.backup_directory, collection), "rb+") as f:
                    documents = bson.decode_all(f.read())
                    logger.debug(
                        f"Restoring collection {collection.split('.')[0]} "
                        f"with {len(documents)} documents."
                    )
                    if not documents:
                        logger.warning(
                            "No documents found in collection "
                            f"{collection.split('.')[0]}."
                        )
                        continue
                    for doc in documents:
                        try:
                            # Use upsert to handle duplicates
                            self.file_collection.replace_one(
                                {"_id": doc["_id"]}, doc, upsert=True
                            )
                        except KeyError:
                            logger.error(f"Document missing '_id': {doc}")
                        except Exception as e:
                            logger.error(f"Error restoring document: {e}")
                        except BulkWriteError as e:
                            logger.error(f"BulkWriteError occurred: {e.details}")

            logger.info(
                f"Restored collection {collection.split('.')[0]} from backup file."
            )

    def read_collection_from_backup(
        self,
        collection_file: str = "files.bson",
    ) -> List[dict]:
        """Read a collection from a backup file.

        Args:
            collection_file (str): The name of the backup file to read.

        Returns:
            collection: The collection read from the backup file.

        Example:
            collection = AssasDatabaseHandler.read_collection_from_backup("files.bson")

        """
        if not getattr(self, "backup_directory", None):
            raise RuntimeError("backup_directory is not set on AssasDatabaseHandler.")

        backup_dir = Path(self.backup_directory)  # works for str or Path
        file_path = backup_dir / collection_file

        if not file_path.exists():
            raise FileNotFoundError(f"Backup collection file not found: {file_path}")

        file_collection: List[dict] = []

        with open(file_path, "rb") as f:
            file_collection = bson.decode_all(f.read())

        return file_collection

    def list_database_names(self) -> List[str]:
        """List all database names in the MongoDB client.

        Args:
            None

        Returns:
            List[str]: A list of database names.

        Example:
            db_names = AssasDatabaseHandler.list_database_names()

        """
        if self.client is None:
            logger.warning("MongoDB client is not initialized. Returning empty list.")
            return []

        db_names = self.client.list_database_names()
        logger.info(f"Listing database names: {db_names}")
        return db_names

    def get_db_handle(self) -> Database:
        """Return the database handle.

        Args:
            None

        Returns:
            db_handle: The database handle.

        Example:
            db_handle = AssasDatabaseHandler.get_db_handle()

        """
        return self.db_handle

    def get_file_collection(self) -> Collection | None:
        """Return the file collection.

        Args:
            None

        Returns:
            file_collection: The file collection.

        Example:
            file_collection = AssasDatabaseHandler.get_file_collection()

        """
        if self.file_collection is None:
            logger.warning("File collection is not initialized. Returning None.")
            return None

        logger.info("Returning file collection.")
        return self.file_collection

    def get_user_collection(self) -> Collection | None:
        """Return the user collection.

        Args:
            None

        Returns:
            user_collection: The user collection.

        Example:
            user_collection = AssasDatabaseHandler.get_user_collection()

        """
        if self.user_collection is None:
            logger.warning("User collection is not initialized. Returning None.")
            return None

        logger.info("Returning user collection.")
        return self.user_collection

    def get_all_user_documents(self) -> Cursor | None:
        """Return all user documents in the user collection.

        Args:
            None

        Returns:
            A cursor to the user documents.

        Example:
            user_documents = AssasDatabaseHandler.get_all_user_documents()

        """
        if self.user_collection is None:
            logger.warning("User collection is not initialized. Returning None.")
            return None

        logger.info("Returning all user documents.")
        return self.user_collection.find()

    def get_all_file_documents(self) -> Cursor | None:
        """Return all file documents in the file collection.

        Args:
            None

        Returns:
            A cursor to the file documents.

        Example:
            file_documents = AssasDatabaseHandler.get_all_file_documents()

        """
        if self.file_collection is None:
            logger.warning("File collection is not initialized. Returning None.")
            return None

        logger.info("Returning all file documents.")
        return self.file_collection.find()

    def insert_file_document(self, file: dict) -> InsertOneResult:
        """Insert a file document into the file collection.

        Args:
            file (dict): The file document to insert.

        Returns:
            None

        Example:
            AssasDatabaseHandler.insert_file_document(file)

        """
        logger.info(f"Insert file document: {file}.")
        return self.file_collection.insert_one(file)

    def drop_file_collection(self) -> None:
        """Drop the file collection.

        Args:
            None

        Returns:
            None

        Example:
            AssasDatabaseHandler.drop_file_collection()

        """
        logger.info("Dropping file collection.")
        self.file_collection.drop()

    def get_file_document(self, id: str) -> dict | None:
        """Return a file document by its ID.

        Args:
            id (str): The ID of the file document.

        Returns:
            The file document with the given ID.

        Example:
            file_document = AssasDatabaseHandler.get_file_document(id)

        """
        return self.file_collection.find_one(ObjectId(id))

    def get_file_document_by_uuid(self, uuid: uuid4) -> dict | None:
        """Return a file document by its UUID.

        Args:
            uuid (uuid4): The UUID of the file document.

        Returns:
            The file document with the given UUID.

        Example:
            file_document = AssasDatabaseHandler.get_file_document_by_uuid(uuid)

        """
        return self.file_collection.find_one({"system_uuid": str(uuid)})

    def get_file_document_by_upload_uuid(
        self,
        upload_uuid: uuid4,
    ) -> dict | None:
        """Return a file document by its upload UUID.

        Args:
            upload_uuid (uuid4): The upload UUID of the file document.

        Returns:
            The file document with the given upload UUID.

        Example:
            file_document =
            AssasDatabaseHandler.get_file_document_by_upload_uuid(upload_uuid)

        """
        return self.file_collection.find_one({"system_upload_uuid": str(upload_uuid)})

    def get_file_documents_by_upload_uuid(
        self,
        upload_uuid: uuid4,
    ) -> dict | None:
        """Return file documents by their upload UUID.

        Args:
            upload_uuid (uuid4): The upload UUID of the file documents.

        Returns:
            A cursor to the file documents with the given upload UUID.

        Example:
            file_documents =
            AssasDatabaseHandler.get_file_documents_by_upload_uuid(upload_uuid)

        """
        return self.file_collection.find({"system_upload_uuid": str(upload_uuid)})

    def get_file_document_by_path(self, path: str) -> dict | None:
        """Return a file document by its system path.

        Args:
            path (str): The system path of the file document.

        Returns:
            The file document with the given system path.

        Example:
            file_document = AssasDatabaseHandler.get_file_document_by_path(path)

        """
        return self.file_collection.find_one({"system_path": path})

    def get_file_documents_by_status(self, status: str) -> Cursor:
        """Return file documents by their system status.

        Args:
            status (str): The system status of the file documents.

        Returns:
            A cursor to the file documents with the given system status.

        Example:
            file_documents = AssasDatabaseHandler.get_file_documents_by_status(status)

        """
        return self.file_collection.find({"system_status": status})

    def get_file_documents_to_update_size(
        self,
        update_key: str = "...",
    ) -> Cursor:
        """Return file documents that need their system size updated.

        Args:
            update_key (str): The key to filter the documents by system size.

        Returns:
            A cursor to the file documents that need their system size updated.

        Example:
            file_documents =
            AssasDatabaseHandler.get_file_documents_to_update_size(update_key)

        """
        return self.file_collection.find({"system_size": update_key})

    def get_file_documents_to_collect_number_of_samples(
        self, system_status: str
    ) -> Cursor:
        """Return file documents that need their number of samples collected.

        Args:
            system_status (str): The system status of the file documents.

        Returns:
            A cursor to the file documents that need their number of samples collected.

        Example:
            file_documents =
            AssasDatabaseHandler.
            get_file_documents_to_collect_number_of_samples(system_status)

        """
        return self.file_collection.find(
            {
                "$and": [
                    {"system_number_of_samples": {"$exists": False}},
                    {"system_status": system_status},
                ]
            }
        )

    def get_file_documents_to_collect_completed_number_of_samples(
        self, system_status: str
    ) -> Cursor:
        """Return file documents that need their number of samples collected.

        Args:
            system_status (str): The system status of the file documents.

        Returns:
            A cursor to the file documents that need their number of samples collected.

        Example:
            file_documents =
            AssasDatabaseHandler.
            get_file_documents_to_collect_number_of_samples(system_status)

        """
        return self.file_collection.find(
            {
                "$and": [
                    {"system_number_of_samples_completed": {"$exists": False}},
                    {"system_status": system_status},
                ]
            }
        )

    def delete_metadata_variables(
        self,
        system_uuid: uuid4,
    ) -> UpdateResult:
        """Delete the meta data variables for a file document by its system UUID.

        Args:
            system_uuid (uuid4): The system UUID of the file document.

        Returns:
            The result of the delete operation.

        Example:
            result = AssasDatabaseHandler.delete_metadata_variables(system_uuid)

        """
        return self.file_collection.update_one(
            {"system_uuid": str(system_uuid)}, {"$unset": {"meta_data_variables": ""}}
        )

    def get_file_documents_to_collect_meta_data(
        self,
    ) -> Cursor:
        """Return file documents that need their meta data collected.

        Args:
            None

        Returns:
            A cursor to the file documents that need their meta data collected.

        Example:
            file_documents =
            AssasDatabaseHandler.get_file_documents_to_collect_meta_data()

        """
        return self.file_collection.find(
            {
                "$and": [
                    {"meta_data_variables": {"$exists": False}},
                    {"system_status": AssasDocumentFileStatus.VALID.value},
                ]
            }
        )

    def update_file_document_by_uuid(self, uuid: uuid4, update: dict) -> UpdateResult:
        """Update a file document by its UUID.

        Args:
            uuid (uuid4): The UUID of the file document.
            update (dict): The update to apply to the file document.

        Returns:
            The result of the update operation.

        Example:
            result = AssasDatabaseHandler.update_file_document_by_uuid(uuid, update)

        """
        post = {"$set": update}
        return self.file_collection.update_one({"system_uuid": str(uuid)}, post)

    def update_file_document_by_path(self, path: str, update: dict) -> UpdateResult:
        """Update a file document by its system path.

        Args:
            path (str): The system path of the file document.
            update (dict): The update to apply to the file document.

        Returns:
            The result of the update operation.

        Example:
            result = AssasDatabaseHandler.update_file_document_by_path(path, update)

        """
        # Ensure we don't attempt to modify the immutable '_id'.
        # If `post` is a full document, use $set with all keys except '_id'.
        post_copy = {k: v for k, v in update.items() if k != "_id"}
        if not post_copy:
            # nothing to update
            return None

        return self.file_collection.update_one(
            {"system_path": path}, {"$set": post_copy}
        )

    def update_file_document_by_upload_uuid(
        self, upload_uuid: uuid4, update: dict
    ) -> UpdateResult:
        """Update a file document by its upload UUID.

        Args:
            upload_uuid (uuid4): The upload UUID of the file document.
            update (dict): The update to apply to the file document.

        Returns:
            The result of the update operation.

        Example:
            result =
            AssasDatabaseHandler.
            update_file_document_by_upload_uuid(upload_uuid, update)

        """
        post = {"$set": update}
        return self.file_collection.update_one(
            {"system_upload_uuid": str(upload_uuid)}, post
        )

    def unset_meta_data_variables(
        self,
        system_uuid: uuid4,
    ) -> UpdateResult:
        """Unset the meta data variables for a file document by its system UUID.

        Args:
            system_uuid (uuid4): The system UUID of the file document.

        Returns:
            The result of the unset operation.

        Example:
            result = AssasDatabaseHandler.unset_meta_data_variables(system_uuid)

        """
        self.file_collection.update_one(
            {"system_uuid": str(system_uuid)}, {"$unset": {"meta_data_variables": ""}}
        )

    def delete_file_document(self, id: str) -> DeleteResult:
        """Delete a file document by its ID.

        Args:
            id (str): The ID of the file document.

        Returns:
            The result of the delete operation.

        Example:
            result = AssasDatabaseHandler.delete_file_document(id)

        """
        return self.file_collection.delete_one({"_id": ObjectId(id)})

    def delete_file_document_by_uuid(self, uuid: uuid4) -> DeleteResult:
        """Delete a file document by its UUID.

        Args:
            uuid (uuid4): The UUID of the file document.

        Returns:
            The result of the delete operation.

        Example:
            result = AssasDatabaseHandler.delete_file_document_by_uuid(uuid)

        """
        return self.file_collection.delete_one({"system_uuid": str(uuid)})

    def delete_file_document_by_upload_uuid(self, upload_uuid: uuid4) -> DeleteResult:
        """Delete a file document by its upload UUID.

        Args:
            upload_uuid (uuid4): The upload UUID of the file document.

        Returns:
            The result of the delete operation.

        Example:
            result =
            AssasDatabaseHandler.delete_file_document_by_upload_uuid(upload_uuid)

        """
        return self.file_collection.delete_one({"system_upload_uuid": str(upload_uuid)})

    def delete_file_documents_by_upload_uuid(self, upload_uuid: uuid4) -> DeleteResult:
        """Delete all file documents by their upload UUID.

        Args:
            upload_uuid (uuid4): The upload UUID of the file documents.

        Returns:
            The result of the delete operation.

        Example:
            result =
            AssasDatabaseHandler.delete_file_documents_by_upload_uuid(upload_uuid)

        """
        return self.file_collection.delete_many(
            {"system_upload_uuid": str(upload_uuid)}
        )

    def update_md5_for_system_uuid(self, system_uuid: str, md5: str) -> bool:
        """Update the MD5 checksum for a given system_uuid in the MongoDB collection."""
        result = self.file_collection.update_one(
            {"system_uuid": system_uuid}, {"$set": {"system_md5": md5}}
        )
        return result.modified_count > 0
