"""AssasDatabaseManager class.

This module provides the AssasDatabaseManager class, which manages the interaction
between the ASSAS application and the NoSql database.
"""

import csv
import hashlib
import os
import sys
import re
import math
import pandas as pd
import logging
import uuid
import pickle
import subprocess

from uuid import uuid4
from datetime import datetime
from pathlib import Path
from typing import Any, List, Mapping, Optional, Tuple
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed

from assasdb.assas_astec_archive import AssasAstecArchive
from assasdb.assas_database_handler import AssasDatabaseHandler
from assasdb.assas_document_file import AssasDocumentFile, AssasDocumentFileStatus
from assasdb.assas_odessa_netcdf4_converter import AssasOdessaNetCDF4Converter
from assasdb.assas_mongodb_backup_handler import AssasMongodbBackupHandler

logger = logging.getLogger("assas_app")


class AssasDatabaseManager:
    """Class to manage the interaction with the ASSAS database.

    It provides methods to retrieve, update, and manage database entries related
    to ASTEC archives.
    It also handles the conversion of ASTEC archives to NetCDF4 format and
    manages the status of the archives in the database.
    """

    def __init__(
        self,
        database_handler: AssasDatabaseHandler,
        upload_directory: str = "/mnt/ASSAS/upload_datahub",
        backup_handler: AssasMongodbBackupHandler | None = None,
    ) -> None:
        """Initialize the AssasDatabaseManager instance.

        Args:
            database_handler (AssasDatabaseHandler): An instance of the database handler
            upload_directory (str): Directory where uploaded archives are stored.
            backup_handler (AssasMongodbBackupHandler | None): Optional backup handler.

        Returns:
            None

        """
        self.database_handler = database_handler
        self.backup_handler = backup_handler or AssasMongodbBackupHandler()

        logger.info(
            f"Initialize AssasDatabaseManager with database handler {database_handler},"
            f" backup handler {self.backup_handler}, "
            f"and upload directory {upload_directory}."
        )

        if not isinstance(self.database_handler, AssasDatabaseHandler):
            logger.warning(
                "The provided database_handler is not an instance of "
                "AssasDatabaseHandler. Please check your setup."
            )

        self.upload_directory = Path(upload_directory)
        if not self.upload_directory.exists():
            logger.warning(
                f"Upload directory {self.upload_directory} does not exist. "
                "Create it and check your setup."
            )

    def close_resources(self) -> None:
        """Close resources used by the handler."""
        self.database_handler.close()

    @staticmethod
    def create_database_handler(
        connection_string: str,
        backup_directory: str,
        database_name: str,
        client_config: Optional[dict] = None,
    ) -> AssasDatabaseHandler:
        """Create a new AssasDatabaseHandler instance.

        Returns:
            AssasDatabaseHandler: A new instance of the database handler.

        """
        logger.info("Create new AssasDatabaseHandler instance.")
        client: MongoClient = MongoClient(
            connection_string,
            **(client_config or {}),
        )
        return AssasDatabaseHandler(
            client=client,
            backup_directory=backup_directory,
            database_name=database_name,
        )

    def get_database_entry_by_upload_uuid(self, upload_uuid: uuid4) -> dict:
        """Retrieve a database entry by its upload UUID.

        Args:
            upload_uuid (uuid4): The UUID of the upload.

        Returns:
            The database entry corresponding to the upload UUID.

        """
        logger.info(f"Get database entry by upload uuid {upload_uuid}.")
        return self.database_handler.get_file_document_by_upload_uuid(upload_uuid)

    def get_database_entry_by_id(self, id: str) -> dict:
        """Retrieve a database entry by its ID.

        Args:
            id (str): The ID of the database entry.

        Returns:
            The database entry corresponding to the ID.

        """
        logger.info(f"Get database entry by id {id}.")
        return self.database_handler.get_file_document(id)

    def get_database_entry_by_uuid(self, uuid: uuid4) -> dict:
        """Retrieve a database entry by its UUID.

        Args:
            uuid (uuid4): The UUID of the database entry.

        Returns:
            The database entry corresponding to the UUID.

        """
        logger.info(f"Get database entry by uuid {uuid}.")
        return self.database_handler.get_file_document_by_uuid(uuid)

    def get_database_entry_by_path(self, path: str) -> dict:
        """Retrieve a database entry by its file path.

        Args:
            path (str): The file path of the database entry.

        Returns:
            The database entry corresponding to the file path.

        """
        logger.info(f"Get database entry by path {path}.")
        return self.database_handler.get_file_document_by_path(path)

    def get_all_database_entries_safe(
        self,
        projection: dict | None = None,
        limit: int | None = None,
        batch_size: int = 500,
        max_time_ms: int | None = None,
    ) -> pd.DataFrame:
        """Retrieve entries from the internal database.

        Args:
            projection: Mongo projection (e.g. {"big_field": 0} to exclude).
            limit: Optional maximum number of documents.
            batch_size: Cursor batch size.
            max_time_ms: Optional server-side time limit for the query.

        """
        file_collection = self.database_handler.get_file_collection()

        cursor = file_collection.find({}, projection=projection).batch_size(batch_size)
        if max_time_ms is not None:
            cursor = cursor.max_time_ms(max_time_ms)
        if limit is not None:
            cursor = cursor.limit(limit)

        data_frame = pd.DataFrame(list(cursor))
        logger.info(
            f"Load data frame with size {str(data_frame.size), str(data_frame.shape)}"
        )

        if data_frame.size == 0:
            return data_frame

        if "system_date" in data_frame.columns:
            data_frame["system_datetime"] = pd.to_datetime(
                arg=data_frame["system_date"], errors="coerce"
            )
            data_frame = data_frame.dropna(subset=["system_datetime"])
            data_frame = data_frame.sort_values(by="system_datetime", ascending=True)

        data_frame["system_index"] = range(1, len(data_frame) + 1)
        if "_id" in data_frame.columns:
            data_frame["_id"] = data_frame["_id"].astype(str)

        return data_frame

    def get_all_database_entries(self) -> pd.DataFrame:
        """Retrieve all entries from the internal database.

        This function fetches all documents from the 'files' collection in the
        internal database and returns them as a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing all entries from the internal database.

        """
        file_collection = self.database_handler.get_file_collection()

        data_frame = pd.DataFrame(list(file_collection.find()))
        logger.info(
            f"Load data frame with size {str(data_frame.size), str(data_frame.shape)}"
        )

        if data_frame.size == 0:
            return data_frame

        if "system_date" in data_frame.columns:
            data_frame["system_datetime"] = pd.to_datetime(
                arg=data_frame["system_date"], errors="coerce"
            )

            # Drop rows with invalid dates (optional)
            data_frame = data_frame.dropna(subset=["system_datetime"])

            # Sort the DataFrame by the 'date' column
            data_frame = data_frame.sort_values(by="system_datetime", ascending=True)

        data_frame["system_index"] = range(1, len(data_frame) + 1)
        data_frame["_id"] = data_frame["_id"].astype(str)

        return data_frame

    def get_all_database_entries_from_backup(self) -> pd.DataFrame:
        """Retrieve all entries from the internal database backup.

        This function reads the 'files' collection from the backup directory and
        returns them as a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing all entries from the internal
            database backup.

        """
        logger.info("Get all database entries from backup.")
        file_collection = self.database_handler.read_collection_from_backup()

        data_frame = pd.DataFrame(file_collection)

        logger.info(
            f"Load data frame with size {str(data_frame.size), str(data_frame.shape)}."
        )

        return data_frame

    def backup_internal_database(
        self, collections: Optional[List[str]] = None, verbose: bool = False
    ) -> None:
        """Backup the internal database.

        Args:
            collections: Optional list of collection names to dump. If None, dumps all.
            verbose: If True/int, enables/increases mongodump verbosity.

        """
        logger.info("Backup internal database.")
        self.backup_handler.backup_with_mongodump(
            collections=collections, verbose=verbose
        )

    def restore_internal_database(
        self,
        collections: Optional[List[str]] = None,
        drop: bool = True,
        verbose: bool = False,
    ) -> None:
        """Restore the internal database from backup.

        Args:
            collections: Optional[List[str]] = None,
                If None, restores all.
            drop: If True, drops the collections before restoring.
            verbose: If True/int, enables/increases mongorestore verbosity.

        Returns:
            None

        """
        logger.info("Restore internal database from backup.")
        self.backup_handler.restore_latest_backup(
            collections=collections, drop=drop, verbose=verbose
        )

    def set_document_status_by_uuid(
        self, uuid: uuid4, status: AssasDocumentFileStatus
    ) -> None:
        """Update the status of a file document in the database by its UUID.

        Args:
            uuid (uuid4): The UUID of the file document.
            status (AssasDocumentFileStatus): The new status to set for the document.

        Returns:
            None

        """
        update = {"system_status": f"{str(status.value)}"}
        logger.info(
            f"Update file document with uuid {uuid} with update string {update}."
        )

        document = self.database_handler.get_file_document_by_uuid(uuid)
        system_uuid = document["system_uuid"]
        logger.info(f"Found document with uuid {system_uuid}.")

        self.database_handler.update_file_document_by_uuid(uuid, update)
        logger.info(
            f"Update file document with uuid {uuid} and set status to {status.value}."
        )

    def set_hdf5_size_by_uuid(
        self,
        uuid: uuid4,
        size: str,
    ) -> None:
        """Update the size of the HDF5 file in the database by its UUID.

        Args:
            uuid (uuid4): The UUID of the file document.
            size (str): The size of the HDF5 file to set.

        Returns:
            None

        """
        update = {"system_size_hdf5": f"{str(size)}"}
        logger.info(
            f"Update file document with uuid {uuid} with update string {update}."
        )

        document = self.database_handler.get_file_document_by_uuid(uuid)
        system_uuid = document["system_uuid"]
        logger.info(f"Found document with uuid {system_uuid}.")

        self.database_handler.update_file_document_by_uuid(uuid, update)
        logger.info(f"Update file document with uuid {uuid} and set size to {size}.")

    def add_internal_database_entry(self, document: dict) -> None:
        """Insert a document into the internal database.

        Args:
            document (dict): The document to insert into the database.

        """
        logger.info(f"Insert document {document}.")
        self.database_handler.insert_file_document(document)

    def empty_internal_database(self) -> None:
        """Drop the 'files' collection in the internal database.

        This will remove all entries from the database.

        Returns:
            None

        """
        self.database_handler.drop_file_collection()

    def collect_number_of_samples_safely(self, document_file: AssasDocumentFile) -> int:
        """Run potentially segfault-prone code in a separate process."""
        try:
            script_content = f"""
import sys
sys.path.append('/root/assas-data-hub/assas_database')
from assasdb.assas_odessa_netcdf4_converter import AssasOdessaNetCDF4Converter

try:
    converter = AssasOdessaNetCDF4Converter(
        input_path="{document_file.get_value("system_path")}",
        output_path="{document_file.get_value("system_result")}"
    )
    number_of_samples = len(converter.get_time_points())
    print(number_of_samples)
except Exception as e:
    print(f"ERROR: {{e}}")
    sys.exit(1)
"""

            result = subprocess.run(
                [sys.executable, "-c", script_content],
                capture_output=True,
                text=True,
                timeout=30,  # 30 second timeout
            )

            if result.returncode == 0:
                return int(result.stdout.strip())
            else:
                logger.error(f"Subprocess failed: {result.stderr}")
                return -1

        except subprocess.TimeoutExpired:
            logger.error("Subprocess timed out - likely segfault or infinite loop")
            return -1
        except Exception as e:
            logger.error(f"Subprocess error: {e}")
            return -1

    def collect_number_of_samples(self, max_documents: int | None = None) -> None:
        """Collect the number of samples from all archives.

        This function retrieves all file documents from the database,
        converts them to AssasDocumentFile instances, and collects the
        number of samples from each archive using the
        AssasOdessaNetCDF4Converter. The results are stored back in the
        database.

        Args:
            max_documents (int | None): The maximum number of documents to process.

        """
        documents = self.database_handler.get_all_file_documents()
        document_files = [AssasDocumentFile(document) for document in documents]

        if max_documents is not None:
            document_files = document_files[:max_documents]

        for document_file in document_files:
            try:
                logger.debug(f"Archive path: {document_file.get_value('system_path')}")
                logger.debug(f"Result path: {document_file.get_value('system_result')}")
                logger.debug(
                    f"Upload_uuid: {document_file.get_value('system_upload_uuid')}"
                )

                # Use safe subprocess method
                number_of_samples = self.collect_number_of_samples_safely(document_file)

                logger.debug(f"Number of samples collected: {number_of_samples}")

            except Exception as exception:
                logger.error(
                    f"Error when collecting number of samples from archive "
                    f"{document_file.get_value('system_path')} "
                    f"with {document_file.get_value('system_upload_uuid')}, "
                    f"exception: {exception}."
                )
                number_of_samples = -1

            logger.info(
                f"Archive {document_file.get_value('system_path')} "
                f"has {number_of_samples} samples."
            )
            document_file.set_value("system_number_of_samples", str(number_of_samples))

            self.database_handler.update_file_document_by_path(
                document_file.get_value("system_path"), document_file.get_document()
            )

    def collect_number_of_samples_of_uploaded_archives(self) -> None:
        """Collect the number of samples from all uploaded archives.

        This function retrieves all uploaded and valid documents from the database,
        converts them to AssasDocumentFile instances, and collects the number of samples
        from each archive using the AssasOdessaNetCDF4Converter.
        The results are stored back in the database.

        Returns:
            None

        """
        documents_uploaded = (
            self.database_handler.get_file_documents_to_collect_number_of_samples(
                system_status=AssasDocumentFileStatus.UPLOADED.value
            )
        )
        document_files_uploaded = [
            AssasDocumentFile(document) for document in documents_uploaded
        ]
        documents_valid = (
            self.database_handler.get_file_documents_to_collect_number_of_samples(
                system_status=AssasDocumentFileStatus.VALID.value
            )
        )

        document_files_valid = [
            AssasDocumentFile(document) for document in documents_valid
        ]
        document_files = document_files_uploaded + document_files_valid
        logger.info(
            f"Collect number of samples of {len(document_files)} uploaded archives."
        )

        for document_file in document_files:
            try:
                converter = AssasOdessaNetCDF4Converter(
                    input_path=document_file.get_value("system_path"),
                    output_path=document_file.get_value("system_result"),
                )

                number_of_samples = len(converter.get_time_points())

            except Exception as exception:
                logger.error(
                    f"Error when collecting number of samples from archive "
                    f"{document_file.get_value('system_path')}: {exception}."
                )
                number_of_samples = -1

            logger.info(
                f"Archive {document_file.get_value('system_path')} "
                f"has {number_of_samples} samples."
            )
            document_file.set_value("system_number_of_samples", str(number_of_samples))

            self.database_handler.update_file_document_by_path(
                document_file.get_value("system_path"), document_file.get_document()
            )

    def get_overall_database_size(self) -> str:
        """Calculate the overall size of the database.

        This function retrieves all database entries, converts the 'system_size' field
        to bytes, sums them up, and converts the total size back to a human-readable
        format (e.g., '10.5 GB').

        Returns:
            str: The total size of the database in a human-readable format.

        """
        logger.info("Get overall size of database.")
        dataframe = self.get_all_database_entries()

        dataframe["system_size_bytes"] = dataframe["system_size"].apply(
            self.convert_to_bytes_2
        )

        total_size_bytes = dataframe["system_size_bytes"].sum()
        logger.info(f"Total size of database in bytes: {total_size_bytes}.")

        size = self.convert_from_bytes(total_size_bytes)
        logger.info(f"Total size of database in converted format: {size}.")

        return size

    @staticmethod
    def get_size_of_database_files_after_status(
        dataframe: pd.DataFrame,
        key: str = "system_size",
    ) -> Tuple[str, float]:
        """Get the size of the internal database.

        This function retrieves the size of the 'files' collection in the internal
        database and returns it in a human-readable format.

        Args:
            dataframe (pd.DataFrame): The DataFrame containing the database entries.
            status (AssasDocumentFileStatus): The status to filter the entries by.
            key (str): The key in the DataFrame that contains the size information.

        Returns:
            str: The size of the internal database in a human-readable format.

        """
        logger.info("Get size of internal database.")

        if dataframe.empty:
            return "0 B"

        dataframe = dataframe.copy()
        dataframe["system_size_bytes"] = dataframe[key].apply(
            AssasDatabaseManager.convert_to_bytes_2
        )

        total_size_bytes = dataframe["system_size_bytes"].sum()
        logger.info(f"Total size of database in bytes: {total_size_bytes}.")

        size = AssasDatabaseManager.convert_from_bytes(total_size_bytes)
        logger.info(f"Total size of database in converted format: {size}.")

        return size, total_size_bytes

    @staticmethod
    def calc_compression_rate(dataframes: pd.DataFrame) -> Tuple[float, float]:
        """Calculate the compression rate of the internal database.

        This function calculates the compression rate by dividing the size of the
        original files by the size of the compressed files.

        Args:
            dataframes (pd.DataFrame): The DataFrame containing the database entries.

        Returns:
            str: The compression rate in a human-readable format.

        """
        logger.info("Calculate compression rate of internal database.")

        if dataframes.empty:
            return (0.0, 0.0)

        dataframes = dataframes.copy()
        dataframes["system_size_bytes"] = dataframes["system_size"].apply(
            AssasDatabaseManager.convert_to_bytes_2
        )
        dataframes["system_size_hdf5_bytes"] = dataframes["system_size_hdf5"].apply(
            AssasDatabaseManager.convert_to_bytes_2
        )

        dataframes = dataframes[
            (dataframes["system_size_bytes"] > 0)
            & (dataframes["system_size_hdf5_bytes"] > 0)
        ].copy()

        dataframes["compression"] = (
            dataframes["system_size_bytes"] / dataframes["system_size_hdf5_bytes"]
        )

        dataframes["compression_rate"] = (
            (dataframes["system_size_bytes"] - dataframes["system_size_hdf5_bytes"])
            / dataframes["system_size_bytes"]
            * 100
        )

        dataframes = dataframes[(dataframes["compression"] < 100)].copy()

        return (dataframes["compression"].mean(), dataframes["compression_rate"].mean())

    @staticmethod
    def convert_to_bytes_old(size_str: str) -> int:
        """Convert a size string (e.g., '10 GB', '500 MB', '20 KB') into bytes.

        Args:
            size_str (str): The size string to convert.

        Returns:
            int: The size in bytes.

        """
        size_str = size_str.strip()
        if size_str.endswith("GB"):
            return int(float(size_str.replace("GB", "").strip()) * 1024**3)
        elif size_str.endswith("MB"):
            return int(float(size_str.replace("MB", "").strip()) * 1024**2)
        elif size_str.endswith("KB"):
            return int(float(size_str.replace("KB", "").strip()) * 1024)
        elif size_str.endswith("B"):
            return int(size_str.replace("B", "").strip())
        elif size_str == "..." or size_str == "....":
            logger.warning(
                "Received size string with '...' or '....'. "
                "Assuming size is not set and returning 0 bytes."
            )
            return 0
        else:
            logger.error(f"Unrecognized size format: {size_str}.")
            raise ValueError(f"Unrecognized size format: {size_str}")

    def update_dataset_attributes_old(self, uuid: str, update_data: dict) -> bool:
        """Update dataset attributes in MongoDB.

        Args:
            uuid: The UUID of the dataset to update
            update_data: Dictionary containing fields to update

        Returns:
            bool: True if update was successful, False otherwise

        """
        try:
            result = self.database_handler.file_collection.update_one(
                {"uuid": uuid}, {"$set": update_data}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Error updating dataset {uuid}: {str(e)}")
            return False

    @staticmethod
    def convert_to_bytes(size_str: str) -> int:
        """Convert a size string (e.g., '10 GB', '500 MB', '20 KB') into bytes.

        Args:
            size_str (str): The size string to convert.

        Returns:
            int: The size in bytes.

        """
        if size_str is None:
            return 0
        s = str(size_str).strip()
        if s in ("", "...", "...."):
            return 0
        import re

        m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([KMGTPE]?B)?\s*$", s, re.IGNORECASE)
        if not m:
            raise ValueError(f"Unrecognized size format: {size_str}")
        number = float(m.group(1))
        unit = (m.group(2) or "B").upper()
        multipliers = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
        return int(number * multipliers.get(unit, 1))

    @staticmethod
    def convert_to_bytes_2(size_str: str) -> int:
        """Convert size values to integer bytes.

        Accepts:
        - int/float (e.g., 0, 0.0, 1234.0)
        - strings like "123", "123B", "0.0", "0.0B", "1.5 KB", "2MB", "3.1 GB"
        Returns:
        - int bytes (floored via int())
        """
        if size_str is None:
            return 0

        # numeric inputs
        if isinstance(size_str, (int,)):
            return int(size_str)
        if isinstance(size_str, (float,)):
            if math.isnan(size_str):
                return 0
            return int(size_str)

        s = str(size_str).strip()
        if not s or s.lower() in {"nan", "none", "null"}:
            return 0

        # normalize
        s = s.replace(",", "").strip()

        # match number + optional unit
        m = re.match(
            r"^\s*([0-9]*\.?[0-9]+)\s*([kmgtp]?b)?\s*$", s, flags=re.IGNORECASE
        )
        if not m:
            raise ValueError(f"Unsupported size format: {size_str!r}")

        value = float(m.group(1))
        unit = (m.group(2) or "B").upper()

        multipliers = {
            "B": 1,
            "KB": 1024,
            "MB": 1024**2,
            "GB": 1024**3,
            "TB": 1024**4,
            "PB": 1024**5,
        }

        if unit not in multipliers:
            raise ValueError(f"Unsupported unit in size format: {size_str!r}")

        return int(value * multipliers[unit])

    @staticmethod
    def convert_from_bytes(number_of_bytes: float, blocksize: float = 1024.0) -> str:
        """Convert Bytes to kB, MB, GB, and TB.

        Args:
            number_of_bytes (float): The number of bytes to convert.
            blocksize (float): The block size for conversion, default is 1024.0.

        Returns:
            str: The converted size in a human-readable format (e.g., '10.5 MB').

        """
        logger.info(f"Number of bytes {number_of_bytes}.")

        for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
            if number_of_bytes < blocksize:
                return f"{round(number_of_bytes, 2)} {unit}"

            number_of_bytes /= blocksize

    @staticmethod
    def get_upload_time(directory: str) -> str:
        """Get the creation time of the archive directory.

        Args:
            directory (str): The path to the archive directory.

        Returns:
            str: The creation time of the archive in the format "MM/DD/YYYY, HH:MM:SS".

        """
        logger.info(f"Get creation time of archive {directory}.")
        archive_path = Path(directory)
        timestamp = os.path.getctime(archive_path)
        creation_time = datetime.fromtimestamp(timestamp).strftime("%m/%d/%Y, %H:%M:%S")

        logger.info(f"Creation time of archive {archive_path} is {creation_time}.")

        return creation_time

    @staticmethod
    def get_size_of_directory_in_bytes(directory: str) -> float:
        """Get the size of a directory in bytes.

        Args:
            directory (str): The path to the directory.

        Returns:
            float: The size of the directory in bytes.

        """
        logger.info(f"Get size of {directory}")
        return float(subprocess.check_output(["du", "-sb", directory]).split()[0])

    def reset_archive_sizes(self) -> None:
        """Reset the sizes of all archives in the database to '....'.

        This function retrieves all file documents from the database and sets their
        'system_size' field to '...'.

        Returns:
            None

        """
        logger.info("Reset archive sizes in the database.")
        documents = self.database_handler.get_file_documents_to_update_size(
            update_key="...."
        )

        document_file_list = [AssasDocumentFile(document) for document in documents]
        for document_file in document_file_list:
            document_file.set_value("system_size", "...")
            self.database_handler.update_file_document_by_path(
                document_file.get_value("system_path"), document_file.get_document()
            )

    def update_archive_sizes(
        self,
        number_of_archives: int | None = None,
    ) -> bool:
        """Update the sizes of archives in the database.

        This function retrieves all archives in the UPLOADED state without updated
        binary sizes, calculates their sizes, and updates the database entries.

        Args:
            number_of_archives (int | None): Optional limit on the number of archives
            to process.

        Returns:
            bool: True if the update was successful, False otherwise.

        """
        logger.info("Update archive sizes in the database.")
        success = False

        documents = self.database_handler.get_file_documents_to_update_size()
        document_file_list = [AssasDocumentFile(document) for document in documents]

        if len(document_file_list) == 0:
            logger.info(
                "No archives in state UPLOADED without updated binary size present."
            )

        if number_of_archives is not None:
            logger.info(f"Handle first {number_of_archives} archives.")
            document_file_list = document_file_list[0:number_of_archives]

        try:
            for document_file in document_file_list:
                document_file.set_value("system_size", "....")
                self.database_handler.update_file_document_by_path(
                    document_file.get_value("system_path"), document_file.get_document()
                )

            for document_file in document_file_list:
                system_path = document_file.get_value("system_path")

                archive_size = AssasDatabaseManager.get_size_of_directory_in_bytes(
                    system_path
                )
                converted_size = AssasDatabaseManager.convert_from_bytes(archive_size)

                document_file.set_value("system_size", converted_size)
                self.database_handler.update_file_document_by_path(
                    system_path, document_file.get_document()
                )

            success = True

        except Exception as exception:
            logger.error(f"Error during update of archive sizes occured: {exception}.")

        return success

    def get_new_upload_uuids_to_process(
        self,
    ) -> List[uuid4]:
        """Get a list of new upload UUIDs to process.

        It will check for the existence of the upload_info.pickle file in each
        archive directory.

        Returns:
            List[uuid4]: A list of UUIDs of new uploads to process.

        """
        logger.info("Get new upload uuids to process.")
        upload_uuid_list = []

        for directory in os.listdir(self.upload_directory):
            if os.path.isdir(Path.joinpath(self.upload_directory, directory)):
                if os.path.isfile(
                    Path.joinpath(self.upload_directory, directory, directory)
                ):
                    logger.debug(
                        f"Detected complete uploaded archive "
                        f"{Path.joinpath(self.upload_directory, directory)}."
                    )

                    try:
                        upload_uuid_list.append(uuid.UUID(directory))

                    except ValueError:
                        logger.error("Received univalid uuid.")

        logger.debug(
            f"Read {len(upload_uuid_list)} "
            f"upload uuids {upload_uuid_list} in {self.upload_directory}."
        )

        return upload_uuid_list

    def update_status_of_archives(self) -> None:
        """Update the status of archives in the database.

        It will set the status of all archives in state UPLOADED to CONVERTING and
        all archives in state CONVERTING to VALID.

        Returns:
            None

        """
        converting_archives = self.get_upload_uuids_of_converting_archives()
        logger.info(
            f"Found {len(converting_archives)} archives with file flag _converting."
        )

        for upload_uuid in converting_archives:
            documents = self.database_handler.get_file_documents_by_upload_uuid(
                upload_uuid=upload_uuid
            )

            document_files = [AssasDocumentFile(document) for document in documents]
            document_files = [
                document_file
                for document_file in document_files
                if document_file.get_value("system_status")
                == AssasDocumentFileStatus.UPLOADED.value
            ]

            for document_file in document_files:
                logger.info(
                    f"Update status of archive "
                    f"{document_file.get_value('system_path')} to CONVERTING."
                )

                document_file.set_value(
                    key="system_status", value=AssasDocumentFileStatus.CONVERTING.value
                )
                self.database_handler.update_file_document_by_path(
                    document_file.get_value("system_path"), document_file.get_document()
                )

        valid_archives = self.get_upload_uuids_of_valid_archives()
        logger.info(f"Found {len(valid_archives)} archives with file flag _valid.")

        for upload_uuid in valid_archives:
            documents = self.database_handler.get_file_documents_by_upload_uuid(
                upload_uuid=upload_uuid
            )

            logger.debug(f"Handle documents with upload uuid {upload_uuid}.")

            document_files = [AssasDocumentFile(document) for document in documents]

            logger.debug(f"Handle {len(document_files)} documents.")

            document_files = [
                document_file
                for document_file in document_files
                if document_file.get_value("system_status")
                == AssasDocumentFileStatus.CONVERTING.value
                if int(document_file.get_value("system_number_of_samples"))
                == int(document_file.get_value("system_number_of_samples_completed"))
            ]

            logger.debug(
                f"Found {len(document_files)} documents "
                f"with upload uuid {upload_uuid} "
                f"that are converting and have completed all samples."
            )

            for document_file in document_files:
                logger.info(
                    f"Update status of archive "
                    f"{document_file.get_value('system_path')} to VALID."
                )

                document_file.set_value(
                    key="system_status", value=AssasDocumentFileStatus.VALID.value
                )
                self.database_handler.update_file_document_by_path(
                    document_file.get_value("system_path"), document_file.get_document()
                )

    def get_upload_uuids_of_valid_archives(
        self,
    ) -> List[uuid4]:
        """Get a list of upload UUIDs of valid archives.

        It will check for the existence of the upload_uuid_valid file in each
        archive directory.

        Returns:
            List[uuid4]: A list of UUIDs of valid uploads.

        """
        upload_uuid_list = []

        for directory in os.listdir(self.upload_directory):
            if os.path.isdir(Path.joinpath(self.upload_directory, directory)):
                if os.path.isfile(
                    Path.joinpath(
                        self.upload_directory, directory, directory + "_valid"
                    )
                ):
                    logger.debug(
                        f"Detected valid archive "
                        f"{Path.joinpath(self.upload_directory, directory)}."
                    )

                    try:
                        upload_uuid_list.append(uuid.UUID(directory))

                    except ValueError:
                        logger.error("Received univalid uuid.")

        logger.debug(
            f"Read {len(upload_uuid_list)} upload uuids "
            f"{upload_uuid_list} in {self.upload_directory} of valid archives."
        )

        return upload_uuid_list

    def get_upload_uuids_of_converting_archives(
        self,
    ) -> List[uuid4]:
        """Get a list of upload UUIDs of archives that are currently being converted.

        It will check for the existence of the upload_uuid_converting file in each
        archive directory.

        Returns:
            List[uuid4]: A list of UUIDs of archives that are currently being converted.

        """
        logger.info("Get upload uuids of converting archives.")
        upload_uuid_list = []

        for directory in os.listdir(self.upload_directory):
            if os.path.isdir(Path.joinpath(self.upload_directory, directory)):
                if os.path.isfile(
                    Path.joinpath(
                        self.upload_directory, directory, directory + "_converting"
                    )
                ):
                    logger.debug(
                        f"Detected valid archive: "
                        f"{Path.joinpath(self.upload_directory, directory)}."
                    )

                    try:
                        upload_uuid_list.append(uuid.UUID(directory))

                    except ValueError:
                        logger.error("Received univalid uuid.")

        logger.debug(
            f"Read {len(upload_uuid_list)} upload uuids {upload_uuid_list} "
            f"in {self.upload_directory} of valid archives."
        )

        return upload_uuid_list

    def get_upload_uuids_to_reload(
        self,
    ) -> List[uuid4]:
        """Get a list of upload UUIDs of archives that have a reload flag.

        It will check for the existence of the upload_uuid_reload file in each
        archive directory.

        Returns:
            List[uuid4]: A list of UUIDs of archives to reload.

        """
        logger.info("Get upload uuids to reload.")
        upload_uuid_list = []

        for directory in os.listdir(self.upload_directory):
            if os.path.isdir(Path.joinpath(self.upload_directory, directory)):
                reload_file = Path.joinpath(
                    self.upload_directory, directory, directory + "_reload"
                )

                if os.path.isfile(reload_file):
                    logger.debug(
                        f"Detected reload file "
                        f"in archive {Path.joinpath(self.upload_directory, directory)}."
                    )

                    try:
                        upload_uuid_list.append(uuid.UUID(directory))
                        logger.info(f"Remove file with path {reload_file}.")
                        os.remove(reload_file)

                    except ValueError:
                        logger.error("Received univalid uuid.")

        logger.debug(
            f"Read {len(upload_uuid_list)} upload uuids "
            f"{upload_uuid_list} in {self.upload_directory} to reload."
        )

        return upload_uuid_list

    @staticmethod
    def get_file_size(file_path: str) -> str:
        """Get the size of a file or directory in a human-readable format.

        Args:
            file_path (str): The path to the file or directory.

        Returns:
            str: The size of the file or directory in a human-readable format.

        """
        if os.path.isfile(file_path):
            file_info = os.stat(file_path)
            logger.info(
                f"File size: {file_info.st_size} "
                f"Os.path.getsize: {os.path.getsize(file_path)}."
            )

            converted_in_bytes = AssasDatabaseManager.convert_from_bytes(
                file_info.st_size
            )
            logger.info(f"Converted in bytes {converted_in_bytes}.")

            return converted_in_bytes

        if os.path.isdir(file_path):
            raise NotImplementedError(f"Path {file_path} points to a directory.")

    def get_uploaded_archives_to_process(
        self,
    ) -> List[AssasAstecArchive]:
        """Get a list of ASTEC archives that are not processed.

        It will check for the existence of the upload_info.pickle file in each
        archive directory and retrieve the upload UUIDs of new uploads.

        Returns:
            List[AssasAstecArchive]: A list of ASTEC archives to process.

        """
        logger.info("Get uploaded archives to process.")
        uploaded_archives_to_process = []

        upload_uuid_list = self.get_new_upload_uuids_to_process()

        for upload_uuid in upload_uuid_list:
            documents = self.database_handler.get_file_document_by_upload_uuid(
                upload_uuid
            )

            if documents is None:
                logger.info(f"Detect new upload with upload_uuid {str(upload_uuid)}.")

                archive_list = self.read_upload_info(upload_uuid)
                uploaded_archives_to_process.extend(archive_list)

            else:
                logger.info(f"Upload_uuid is already processed {str(upload_uuid)}.")

        return uploaded_archives_to_process

    def get_uploaded_archives_to_reload(
        self,
    ) -> List[AssasAstecArchive]:
        """Get a list of ASTEC archives that have a reload flag.

        The method check for the existence of the upload_info.pickle file in each
        archive directory and retrieve the upload UUIDs of archives to reload.

        Returns:
            List[AssasAstecArchive]: A list of ASTEC archives to reload.

        """
        uploaded_archives_to_reload = []

        upload_uuid_list = self.get_upload_uuids_to_reload()

        for upload_uuid in upload_uuid_list:
            self.database_handler.delete_file_documents_by_upload_uuid(upload_uuid)

            logger.info(
                f"Delete existing archives archive with upoad_uuid {str(upload_uuid)}."
            )

            document = self.database_handler.get_file_document_by_upload_uuid(
                upload_uuid
            )

            if document is None:
                logger.info(
                    f"""Read upload info of existing archive
                    with upload_uuid {str(upload_uuid)}."""
                )

                archive_list = self.read_upload_info(upload_uuid)
                uploaded_archives_to_reload.extend(archive_list)

            else:
                logger.info(
                    f"Deleting the old archive was not succesful {str(upload_uuid)}."
                )

        return uploaded_archives_to_reload

    def process_uploads(
        self,
    ) -> bool:
        """Process all uploaded archives that are not yet processed.

        This function retrieves the list of uploaded archives to process,
        registers them in the database, and updates their status.

        Returns:
            bool: True if the processing was successful, False otherwise.

        """
        logger.info("Process uploads.")
        success = False

        try:
            archive_list = self.get_uploaded_archives_to_process()

            if len(archive_list) == 0:
                logger.info("No new archives present.")

            else:
                self.register_archives(archive_list)

            success = True

        except Exception as exception:
            logger.error(f"Error when processing uploads occured: {exception}.")

        return success

    def process_uploads_with_reload_flag(
        self,
    ) -> bool:
        """Process all uploaded archives that have a reload flag.

        This function retrieves the list of uploaded archives to reload,
        registers them in the database, and updates their status.

        Returns:
            bool: True if the processing was successful, False otherwise.

        """
        logger.info("Process uploads with reload flag.")
        success = False

        try:
            archive_list = self.get_uploaded_archives_to_reload()

            if len(archive_list) == 0:
                logger.info("No new archives present.")
            else:
                self.register_archives(archive_list)

            success = True

        except Exception as exception:
            logger.error(f"Error when processing uploads occured: {exception}.")

        return success

    def update_upload_info(
        self,
        upload_uuid: uuid4,
        key: str,
        value_list: List[str],
        upload_info_file_name: str = "upload_info.pickle",
    ) -> bool:
        """Update the upload information in the upload_info.pickle file.

        Args:
            upload_uuid (uuid4): The UUID of the upload.
            key (str): The key to update in the upload information.
            value_list (List[str]): The list of values to set for the key.
            upload_info_file_name (str): The name of the upload info file.

        Returns:
            bool: True if the update was successful, False otherwise.

        """
        success = False

        try:
            upload_info = {}
            upload_info_file = Path.joinpath(
                self.upload_directory, str(upload_uuid), upload_info_file_name
            )
            logger.info(f"Update upload info from file {str(upload_info_file)}")

            with open(upload_info_file, "rb") as file:
                upload_info = pickle.load(file)

            logger.info("Upload information:")
            for key, value in upload_info.items():
                logger.info(f"{key}: {value}")

            logger.info(f"Update key {key} with value {value}")
            upload_info[key] = value_list

            with open(upload_info_file, "wb+") as file:
                pickle.dump(upload_info, file)

            logger.info("Updated upload information:")
            for key, value in upload_info.items():
                logger.info(f"{key}: {value}")

            success = True

        except Exception as exception:
            logger.error(
                f"Error when updating upload information in "
                f"in file {str(upload_info_file)}, message: {exception}."
            )

        return success

    @staticmethod
    def remove_lead_slash_from_path_string(path: str) -> str:
        """Remove the leading slash from a path string if it exists.

        Args:
            path (str): The path string to process.

        Returns:
            str: The path string without the leading slash.

        """
        logger.info(f"Remove leading slash from path {path}.")
        if path.startswith("/"):
            path = path[1:]

        return path

    def read_upload_info(
        self, upload_uuid: uuid4, upload_info_file_name: str = "upload_info.pickle"
    ) -> List[AssasAstecArchive]:
        """Read the upload information from the upload_info.pickle file.

        Args:
            upload_uuid (uuid4): The UUID of the upload.
            upload_info_file_name (str): The name of the upload info file.

        Returns:
            List[AssasAstecArchive]: A list of AssasAstecArchive instances
            representing the uploaded archives.

        """
        archive_list = []

        upload_info = {}
        upload_info_file = Path.joinpath(
            self.upload_directory, str(upload_uuid), upload_info_file_name
        )
        logger.info(f"Read upload info from file {str(upload_info_file)}.")

        with open(upload_info_file, "rb") as file:
            upload_info = pickle.load(file)

        name = upload_info["name"]
        archive_path = Path.joinpath(self.upload_directory, str(upload_uuid))
        upload_time = AssasDatabaseManager.get_upload_time(directory=str(archive_path))

        logger.info(f"Path of database entry is {str(archive_path)}.")

        if len(upload_info["archive_paths"]) == 1:
            archive_sub_path = upload_info["archive_paths"][0]
            archive_sub_path = self.remove_lead_slash_from_path_string(archive_sub_path)
            logger.info(f"Sub path of ASTEC archive is {archive_sub_path}.")

            final_archive_path = Path.joinpath(archive_path, archive_sub_path)
            logger.info(f"Final path of ASTEC archive is {str(final_archive_path)}.")

            archive_list.append(
                AssasAstecArchive(
                    upload_uuid=str(upload_uuid),
                    name=name,
                    date=upload_time,
                    user=upload_info["user"],
                    description=upload_info["description"],
                    archive_path=str(final_archive_path),
                    result_path=str(final_archive_path)
                    + "/../result/dataset.h5",  # Put result next to binary
                )
            )
        else:
            for idx, archive_sub_path in enumerate(upload_info["archive_paths"]):
                archive_sub_path = self.remove_lead_slash_from_path_string(
                    archive_sub_path
                )
                logger.info(f"Sub path of ASTEC archive is {archive_sub_path}.")

                final_archive_path = Path.joinpath(archive_path, archive_sub_path)
                logger.info(
                    f"Final path of ASTEC archive is {str(final_archive_path)}."
                )

                archive_list.append(
                    AssasAstecArchive(
                        upload_uuid=str(upload_uuid),
                        name=f"{name}_{idx}",
                        date=upload_time,
                        user=upload_info["user"],
                        description=upload_info["description"],
                        archive_path=str(final_archive_path),
                        result_path=str(final_archive_path)
                        + "/../result/dataset.h5",  # Put result next to binary
                    )
                )

        return archive_list

    def register_archives(self, archive_list: List[AssasAstecArchive]) -> None:
        """Register a list of ASTEC archives in the internal database.

        Args:
            archive_list (List[AssasAstecArchive]): A list of ASTEC archives to
            register.

        Returns:
            None

        """
        logger.info(f"Start registering {len(archive_list)} archives.")

        for _, archive in enumerate(archive_list):
            logger.info(f"Set status of archive to UPLOADED {archive.archive_path}.")
            system_status = AssasDocumentFileStatus.UPLOADED.value

            document_file = AssasDocumentFile()

            document_file.set_system_values(
                system_uuid=str(uuid.uuid4()),
                system_upload_uuid=archive.upload_uuid,
                system_date=archive.date,
                system_path=archive.archive_path,
                system_result=archive.result_path,
                system_size="...",
                system_user=archive.user,
                system_download="hdf5 file",
                system_status=system_status,
            )

            document_file.set_general_meta_values(
                meta_name=archive.name, meta_description=archive.description
            )

            AssasOdessaNetCDF4Converter.set_general_meta_data(
                output_path=archive.result_path,
                archive_name=archive.name,
                archive_description=archive.description,
            )

            document_file.set_value(
                "system_size_hdf5",
                AssasDatabaseManager.get_file_size(archive.result_path),
            )
            self.add_internal_database_entry(document_file.get_document())

    def postpone_conversion(
        self,
        maximum_conversions: int = 5,
    ) -> bool:
        """Check the number of currently converting archives.

        This function retrieves all documents that are currently in the CONVERTING
        state from the database, converts them to AssasDocumentFile instances,
        and checks if the number of converting archives exceeds the specified maximum.

        Args:
            maximum_conversions (int): The maximum number of allowed conversions.

        Returns:
            bool: True if the number of converting archives exceeds the maximum,
            False otherwise.

        """
        documents = self.database_handler.get_file_documents_by_status(
            AssasDocumentFileStatus.CONVERTING.value
        )
        document_files = [AssasDocumentFile(document) for document in documents]

        return len(document_files) > maximum_conversions

    def convert_next_validated_archive(
        self,
        explicit_times: List[int] = None,
    ) -> None:
        """Convert the next archive that is in the UPLOADED state to NetCDF4 format.

        Args:
            explicit_times (List[int]): Optional list of explicit time points to use
            for conversion.

        """
        logger.info("Convert next validated archive to NetCDF4 format.")
        documents = self.database_handler.get_file_documents_by_status(
            AssasDocumentFileStatus.UPLOADED.value
        )
        document_files = [AssasDocumentFile(document) for document in documents]

        if len(document_files) == 0:
            logger.info("Found no new archive to convert.")
            return

        if self.postpone_conversion(maximum_conversions=1):
            logger.info("Too may conversions started. Skip this conversion.")
            return

        document_file = document_files[0]  # take first in list

        document_file.set_value(
            key="system_status", value=AssasDocumentFileStatus.CONVERTING.value
        )
        self.database_handler.update_file_document_by_path(
            document_file.get_value("system_path"), document_file.get_document()
        )

        try:
            AssasOdessaNetCDF4Converter(
                input_path=document_file.get_value("system_path"),
                output_path=document_file.get_value("system_result"),
            ).convert_astec_variables_to_netcdf4(explicit_times=explicit_times)

            document_file.set_value(
                key="system_status", value=AssasDocumentFileStatus.VALID.value
            )
            document_file.set_value(
                key="system_size_hdf5",
                value=AssasDatabaseManager.get_file_size(
                    document_file.get_value("system_result")
                ),
            )

            self.database_handler.update_file_document_by_path(
                document_file.get_value("system_path"), document_file.get_document()
            )

        except Exception as exception:
            logger.error(f"Update status to INVALID due to exception: {exception}.")

            document_file.set_value(
                key="system_status", value=AssasDocumentFileStatus.INVALID.value
            )
            self.database_handler.update_file_document_by_path(
                document_file.get_value("system_path"), document_file.get_document()
            )

    def reset_invalid_archives(self) -> None:
        """Reset the status of all invalid archives to UPLOADED.

        This function retrieves all file documents that are in the INVALID state,
        converts them to AssasDocumentFile instances, and updates their status
        to UPLOADED in the database.

        Returns:
            None

        """
        documents = self.database_handler.get_file_documents_by_status(
            AssasDocumentFileStatus.INVALID
        )
        document_files = [AssasDocumentFile(document) for document in documents]

        for document in document_files:
            document.set_value(
                key="system_status", value=AssasDocumentFileStatus.UPLOADED
            )
            self.database_handler.update_file_document_by_path(
                document.get_value("system_path"), document.get_document()
            )

    def reset_converting_archives(self) -> None:
        """Reset the status of all converting archives to UPLOADED.

        This function retrieves all file documents that are in the CONVERTING state,
        converts them to AssasDocumentFile instances, and updates their status
        to UPLOADED in the database.

        Returns:
            None

        """
        logger.info("Reset status of all converting archives to UPLOADED.")
        documents = self.database_handler.get_file_documents_by_status(
            AssasDocumentFileStatus.CONVERTING.value
        )
        document_files = [AssasDocumentFile(document) for document in documents]

        for document in document_files:
            document.set_value(
                key="system_status", value=AssasDocumentFileStatus.UPLOADED.value
            )
            self.database_handler.update_file_document_by_path(
                document.get_value("system_path"), document.get_document()
            )

    def reset_valid_archives(self) -> None:
        """Reset the status of all valid archives to UPLOADED.

        This function retrieves all file documents that are in the VALID state,
        converts them to AssasDocumentFile instances, and updates their status
        to UPLOADED in the database.

        Returns:
            None

        """
        logger.info("Reset status of all valid archives to UPLOADED.")
        documents = self.database_handler.get_file_documents_by_status(
            AssasDocumentFileStatus.VALID.value
        )
        document_files = [AssasDocumentFile(document) for document in documents]

        for document in document_files:
            document.set_value(
                key="system_status", value=AssasDocumentFileStatus.UPLOADED.value
            )
            self.database_handler.update_file_document_by_path(
                document.get_value("system_path"), document.get_document()
            )

    def reset_all_result_files(self) -> None:
        """Reset the result files of all archives in the database.

        Returns:
            None

        """
        documents = self.database_handler.get_all_file_documents()
        document_files = [AssasDocumentFile(document) for document in documents]

        for document in document_files:
            AssasOdessaNetCDF4Converter.set_general_meta_data(
                output_path=document.get_value("system_result"),
                archive_name=document.get_value("meta_name"),
                archive_description=document.get_value("meta_description"),
            )

    def reset_result_file_by_uuid(
        self,
        system_uuid: uuid4,
    ) -> None:
        """Reset the result file of a specific archive by its UUID.

        Args:
            system_uuid (uuid4): The UUID of the archive to reset.

        Returns:
            None

        """
        logger.info(f"Reset result file of archive with uuid {system_uuid}.")
        document = self.database_handler.get_file_document_by_uuid(uuid=system_uuid)

        document_file = AssasDocumentFile(document)

        AssasOdessaNetCDF4Converter.set_general_meta_data(
            output_path=document_file.get_value("system_result"),
            archive_name=document_file.get_value("meta_name"),
            archive_description=document_file.get_value("meta_description"),
        )

    def delete_status_files_by_uuid(self, upload_uuid: uuid4) -> None:
        """Delete the status files in the upload directory.

        Args:
            upload_uuid (uuid4): The UUID of whose status files should be deleted.

        Returns:
            None

        """
        upload_dir = self.upload_directory / str(upload_uuid)
        files_to_delete = [
            upload_dir / f"{upload_uuid}_valid",
            upload_dir / f"{upload_uuid}_converting",
        ]
        deleted_files = []

        for file_path in files_to_delete:
            try:
                if file_path.exists():
                    file_path.unlink()
                    logger.info(f"Deleted status file: {file_path}")
                    deleted_files.append(str(file_path))
                else:
                    logger.info(f"Status file does not exist: {file_path}")
            except Exception as e:
                logger.error(f"Failed to delete {file_path}: {e}")

        logger.info(
            f"Deleted {len(deleted_files)} status files for upload_uuid {upload_uuid}."
        )

    def reset_result_directories(self, status: AssasDocumentFileStatus) -> None:
        """Reset the result directory of all archives in the database.

        Returns:
            None

        """
        logger.info(f"Reset result directory of archives in state {status.value}.")
        documents = self.database_handler.get_file_documents_by_status(status.value)
        document_files = [AssasDocumentFile(document) for document in documents]

        for document in document_files:
            document.set_value(
                key="system_status", value=AssasDocumentFileStatus.UPLOADED.value
            )
            self.database_handler.update_file_document_by_path(
                document.get_value("system_path"), document.get_document()
            )

            self.delete_status_files_by_uuid(
                upload_uuid=uuid.UUID(document.get_value("system_upload_uuid"))
            )

    def reset_metadata_of_valid_archives(
        self, number_of_archives: Optional[int] = None
    ) -> None:
        """Reset the metadata of all valid archives in the database.

        This function retrieves all file documents that are in the VALID state,
        converts them to AssasDocumentFile instances, and resets their metadata
        to default values. The results are stored back in the database.

        Args:
            number_of_archives (Optional[int]): Optional limit on the number of
            archives to process.

        Returns:
            None

        """
        logger.info("Reset meta data of all valid archives in the database.")
        documents = self.database_handler.get_file_documents_by_status(
            AssasDocumentFileStatus.VALID.value
        )
        document_files = [AssasDocumentFile(document) for document in documents]

        if len(document_files) == 0:
            logger.info("Found no new archive to reset meta data.")
            return

        if number_of_archives is not None:
            logger.info(f"Handle first {number_of_archives} archives.")
            document_files = document_files[0:number_of_archives]

        try:
            for document_file in document_files:
                logger.info(
                    f"Reset meta info from file, "
                    f"filename is {document_file.get_value('system_result')}."
                )

                self.database_handler.delete_metadata_variables(
                    system_uuid=document_file.get_value("system_uuid")
                )

        except Exception as exception:
            logger.error(f"Reset meta info failed due to exception: {exception}.")

    def update_metadata_of_valid_archives(
        self, number_of_archives: Optional[int] = None
    ) -> None:
        """Collect meta data from all valid archives in the database.

        This function retrieves all file documents that are in the VALID state,
        converts them to AssasDocumentFile instances, and collects meta data
        from each archive using the AssasOdessaNetCDF4Converter.
        The results are stored back in the database.

        Returns:
            None

        """
        logger.info("Collect meta data from all valid archives in the database.")
        documents = self.database_handler.get_file_documents_to_collect_meta_data()
        document_files = [AssasDocumentFile(document) for document in documents]

        if len(document_files) == 0:
            logger.info("Found no new archive to collect meta data.")
            return

        if number_of_archives is not None:
            logger.info(f"Handle first {number_of_archives} archives.")
            document_files = document_files[0:number_of_archives]

        try:
            for document_file in document_files:
                logger.info(
                    f"Collect meta info from file, "
                    f"filename is {document_file.get_value('system_result')}."
                )

                meta_info = (
                    AssasOdessaNetCDF4Converter.read_metadata_from_variables_in_netcdf4(
                        netcdf4_file=document_file.get_value("system_result")
                    )
                )

                document_file.set_meta_data_values(meta_data_variables=meta_info)

                document_file.set_value(
                    "system_size_hdf5",
                    AssasDatabaseManager.get_file_size(
                        document_file.get_value("system_result")
                    ),
                )

                self.database_handler.update_file_document_by_path(
                    document_file.get_value("system_path"), document_file.get_document()
                )

        except Exception as exception:
            logger.error(f"Update meta info failed due to exception: {exception}.")

    def update_meta_data(
        self,
        uuid: uuid4,
    ) -> None:
        """Update the metadata of a specific archive by its UUID.

        Args:
            uuid (uuid4): The UUID of the archive to update.

        Returns:
            None

        """
        logger.info(f"Update meta data of archive with uuid {uuid}.")
        document = self.database_handler.get_file_document_by_uuid(uuid=uuid)
        document_file = AssasDocumentFile(document)

        if document_file is None:
            logger.info("Found no new archive to collect meta data.")
            return

        try:
            logger.info(
                f"Collect meta info from file: "
                f"{document_file.get_value('system_result')}."
            )

            meta_info = (
                AssasOdessaNetCDF4Converter.read_metadata_from_variables_in_netcdf4(
                    netcdf4_file=document_file.get_value("system_result")
                )
            )

            document_file.set_meta_data_values(meta_data_variables=meta_info)

            self.database_handler.update_file_document_by_path(
                document_file.get_value("system_path"), document_file.get_document()
            )

        except Exception as exception:
            logger.error(f"Update meta info failed due to exception: {exception}.")

    def update_maximum_index_value_from_valid_archives(self) -> None:
        """Update the maximum index value from all valid archives in the database.

        This function retrieves all file documents that are in the VALID state,
        converts them to AssasDocumentFile instances, and updates the maximum index
        value for each archive using the AssasOdessaNetCDF4Converter.
        The results are stored back in the database.

        Returns:
            None

        """
        logger.info(
            "Update maximum index value from all valid archives in the database."
        )
        documents = self.database_handler.get_file_documents_by_status(
            status=AssasDocumentFileStatus.VALID.value
        )
        document_files = [AssasDocumentFile(document) for document in documents]

        if len(document_files) == 0:
            logger.info("Found no new archive to collect maximum index value.")
            return

        try:
            for document_file in document_files:
                logger.info(
                    f"Collect maximum index value from file, "
                    f"filename is {document_file.get_value('system_result')}."
                )

                max_index = (
                    AssasOdessaNetCDF4Converter.get_completed_index_from_netcdf4_file(
                        netcdf4_file=document_file.get_value("system_result")
                    )
                )

                document_file.set_value(
                    key="system_number_of_samples_completed", value=str(max_index + 1)
                )

                self.database_handler.update_file_document_by_path(
                    document_file.get_value("system_path"), document_file.get_document()
                )

        except Exception as exception:
            logger.error(
                f"Update maximum index value failed due to exception: {exception}."
            )

    def collect_maximum_index_value_from_valid_archives(self) -> None:
        """Update the maximum index value from all valid archives in the database.

        This function retrieves all file documents that are in the VALID state,
        converts them to AssasDocumentFile instances, and updates the maximum index
        value for each archive using the AssasOdessaNetCDF4Converter.
        The results are stored back in the database.

        Returns:
            None

        """
        logger.info(
            "Update maximum index value from all converting archives in the database."
        )
        handler = self.database_handler
        documents_converting = handler.get_file_documents_by_status(
            status=AssasDocumentFileStatus.CONVERTING.value
        )
        document_files = [
            AssasDocumentFile(document) for document in documents_converting
        ]

        if len(document_files) == 0:
            logger.info("Found no new archive to collect maximum index value.")
            return

        for document_file in document_files:
            logger.info(
                f"Collect maximum index value from file, "
                f"filename is {document_file.get_value('system_result')}."
            )
            try:
                actual_max_index = document_file.get_value(
                    "system_number_of_samples_completed"
                )
            except KeyError:
                actual_max_index = None

            if actual_max_index is None:
                actual_max_index = -1
            else:
                actual_max_index = int(actual_max_index)

            logger.info(f"Actual maximum index value is {actual_max_index}.")

            try:
                max_index = (
                    AssasOdessaNetCDF4Converter.get_completed_index_from_netcdf4_file(
                        netcdf4_file=document_file.get_value("system_result")
                    )
                )

            except Exception as exception:
                logger.error(
                    f"Update maximum index value failed due to exception: {exception}."
                )
                max_index = -1

            if (max_index + 1) == actual_max_index:
                logger.info(
                    "Maximum index value is already up to date, "
                    "skip update of maximum index value."
                )
                continue

            logger.info(
                f"Update maximum index value from {actual_max_index} to {max_index}."
            )

            document_file.set_value(
                key="system_number_of_samples_completed", value=str(max_index + 1)
            )

            handler.update_file_document_by_path(
                path=document_file.get_value("system_path"),
                update=document_file.get_document(),
            )

    def update_dataset_attributes(
        self, uuid: str, update_data: Mapping[str, Any]
    ) -> bool:
        """Update dataset attributes ONLY on the currently configured DB."""
        if not update_data:
            logger.warning(
                "update_dataset_attributes called with empty update_data (uuid=%s).",
                uuid,
            )
            return False

        update_dict = dict(update_data)

        # Wrap plain updates in $set (avoid accidental full-document replacement)
        has_operator = any(str(k).startswith("$") for k in update_dict.keys())
        update_doc = update_dict if has_operator else {"$set": update_dict}

        # IMPORTANT: documents are identified by system_uuid
        filter_doc = {"system_uuid": str(uuid)}

        try:
            result = self.database_handler.file_collection.update_one(
                filter_doc, update_doc
            )
            logger.info(
                (
                    "update_dataset_attributes (local only) "
                    "system_uuid=%s matched=%d modified=%d"
                ),
                str(uuid),
                result.matched_count,
                result.modified_count,
            )
            # matched_count is the important signal;
            # modified_count can be 0 if values are unchanged
            return result.matched_count > 0
        except Exception as e:
            logger.error(
                "Error updating dataset %s on local DB: %s", uuid, e, exc_info=True
            )
            return False

    def link_files_to_batch_user(self) -> None:
        """Link files to batch user (local DB only)."""
        logger.info("Link all files without user to the batch user (local only).")

        file_collection = self.database_handler.get_file_collection()

        users = list(self.database_handler.get_all_user_documents())
        batch_to_user: dict[str, dict] = {}
        for user in users:
            batch = user.get("batch")
            if batch:
                batch_to_user[str(batch)] = user

        projection = {"system_uuid": 1, "system_user": 1}
        cursor = file_collection.find({}, projection=projection).batch_size(500)

        updated = 0
        skipped = 0

        for file_doc in cursor:
            system_uuid = str(file_doc.get("system_uuid", "")).strip()
            system_user = file_doc.get("system_user")

            if not system_uuid or system_user is None:
                skipped += 1
                continue

            batch_user = batch_to_user.get(str(system_user))
            if not batch_user:
                skipped += 1
                continue

            ok = self.update_dataset_attributes(
                system_uuid, {"system_user_info": batch_user}
            )
            if ok:
                updated += 1
            else:
                skipped += 1

        logger.info(
            "link_files_to_batch_user done: updated=%d skipped=%d", updated, skipped
        )

    def list_users(self, role: str = "curator") -> None:
        """List all users with a specific role in the database."""
        logger.info(f"List all users with role {role} in the database.")

        user_documents = self.database_handler.get_all_user_documents()
        for user in user_documents:
            roles = user.get("roles", [])
            if role in roles:
                logger.info(f"{role}s: {user}")

    def reset_in_progress_archive_sizes(
        self,
        from_placeholder: str = "....",
        to_placeholder: str = "...",
        *,
        only_status: str | None = None,
    ) -> tuple[int, int]:
        """Reset stuck/in-progress size placeholders.

        Some routines temporarily set system_size to "...." while calculating.
        If a run crashes, documents may remain stuck at "....".
        This function sets those back to "...", so they can be picked up again.

        Args:
            from_placeholder: Value to reset from (default: "....")
            to_placeholder: Value to reset to (default: "...")
            only_status: Optional filter by system_status (e.g. "UPLOADED")

        Returns:
            (matched_count, modified_count)

        """
        file_collection = self.database_handler.get_file_collection()

        filter_doc: dict[str, Any] = {"system_size": from_placeholder}
        if only_status is not None:
            filter_doc["system_status"] = only_status

        result = file_collection.update_many(
            filter_doc, {"$set": {"system_size": to_placeholder}}
        )

        logger.info(
            (
                "reset_in_progress_archive_sizes: "
                "from=%s to=%s status=%s matched=%d modified=%d"
            ),
            from_placeholder,
            to_placeholder,
            only_status,
            result.matched_count,
            result.modified_count,
        )
        return result.matched_count, result.modified_count

    @staticmethod
    def _convert_from_bytes_quiet(
        number_of_bytes: float, blocksize: float = 1024.0
    ) -> str:
        """Convert bytes to a human-readable string.

        Note: But without logging (keeps bulk runs fast/quiet).

        Args:
            number_of_bytes: The size in bytes to convert.
            blocksize: The block size to use for conversion (default: 1024).

        """
        x = float(number_of_bytes or 0.0)
        for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
            if x < blocksize:
                return f"{round(x, 2)} {unit}"
            x /= blocksize
        return f"{round(x, 2)} PB"

    def recalc_hdf5_sizes_fast(
        self,
        *,
        only_missing_or_zero: bool = True,
        max_documents: int | None = None,
        workers: int = 16,
        dry_run: bool = True,
        set_bytes_field: bool = True,
    ) -> dict:
        """Recalculate system_size_hdf5 from the actual file size.

        Fast: uses os.stat only; does NOT open HDF5.

        Updates fields:
          - system_size_hdf5 (human-readable, e.g. "12.34 MB")
          - optionally system_size_hdf5_bytes (int)

        Returns summary dict.
        """
        file_collection = self.database_handler.get_file_collection()

        projection = {"system_uuid": 1, "system_result": 1, "system_size_hdf5": 1}

        filter_doc: dict[str, Any] = {}
        if only_missing_or_zero:
            filter_doc = {
                "$or": [
                    {"system_size_hdf5": {"$exists": False}},
                    {"system_size_hdf5": None},
                    {
                        "system_size_hdf5": {
                            "$in": ["", "0 B", "0.0 B", "...", "....", "nan", "NaN"]
                        }
                    },
                ]
            }

        cursor = file_collection.find(filter_doc, projection=projection).batch_size(500)
        if max_documents is not None:
            cursor = cursor.limit(int(max_documents))

        docs = list(cursor)
        logger.info(
            "recalc_hdf5_sizes_fast: loaded %d docs (dry_run=%s)", len(docs), dry_run
        )

        def stat_one(doc: dict) -> tuple[str, str, int | None, str | None]:
            """Calculate the size of the HDF5 file for one document.

            Returns:
                (system_uuid, system_result, size_bytes, error)

            """
            system_uuid = str(doc.get("system_uuid") or "").strip()
            system_result = str(doc.get("system_result") or "").strip()
            if not system_uuid or not system_result:
                return (
                    system_uuid,
                    system_result,
                    None,
                    "missing_system_uuid_or_system_result",
                )
            try:
                size_bytes = os.path.getsize(system_result)
                return system_uuid, system_result, int(size_bytes), None
            except FileNotFoundError:
                return system_uuid, system_result, None, "file_not_found"
            except Exception as e:
                return system_uuid, system_result, None, f"stat_error:{e}"

        updated = 0
        missing = 0
        errors = 0
        unchanged = 0

        with ThreadPoolExecutor(max_workers=max(1, int(workers))) as ex:
            futs = [ex.submit(stat_one, d) for d in docs]
            for fut in as_completed(futs):
                system_uuid, system_result, size_bytes, err = fut.result()

                if err is not None:
                    if err == "file_not_found":
                        missing += 1
                    else:
                        errors += 1
                        logger.warning(
                            "HDF5 size stat failed: uuid=%s result=%s err=%s",
                            system_uuid,
                            system_result,
                            err,
                        )
                    continue

                new_human = self._convert_from_bytes_quiet(size_bytes)

                if dry_run:
                    updated += 1
                    continue

                update_doc: dict[str, Any] = {"system_size_hdf5": new_human}
                if set_bytes_field:
                    update_doc["system_size_hdf5_bytes"] = int(size_bytes)

                # Update by system_uuid (this is the canonical identifier in your code)
                res = file_collection.update_one(
                    {"system_uuid": system_uuid}, {"$set": update_doc}
                )
                if res.matched_count == 0:
                    errors += 1
                    logger.warning("No doc matched for system_uuid=%s", system_uuid)
                elif res.modified_count == 0:
                    unchanged += 1
                else:
                    updated += 1

        summary = {
            "docs_loaded": len(docs),
            "updated_or_would_update": updated,
            "unchanged": unchanged,
            "missing_files": missing,
            "errors": errors,
            "dry_run": dry_run,
        }
        logger.info("recalc_hdf5_sizes_fast summary: %s", summary)
        return summary

    def update_paths_in_database(
        self,
        old_dir: str = "/mnt/ASSAS/upload_test",
        new_dir: str = "/mnt/ASSAS/upload_datahub",
        dry_run: bool = True,
    ) -> dict:
        """Update the leading directory in 'system_path' and 'system_result' fields.

        Args:
            old_dir (str): The old directory prefix to replace.
            new_dir (str): The new directory prefix to use.
            dry_run (bool): If True, only logs what would be changed.

        Returns:
            dict: Summary of updates.

        """
        file_collection = self.database_handler.get_file_collection()
        filter_doc = {
            "$or": [
                {"system_path": {"$regex": f"^{re.escape(old_dir)}"}},
                {"system_result": {"$regex": f"^{re.escape(old_dir)}"}},
            ]
        }
        cursor = file_collection.find(
            filter_doc, {"system_uuid": 1, "system_path": 1, "system_result": 1}
        )
        updated = 0
        unchanged = 0
        errors = 0

        def replace_prefix(path: str, old: str, new: str) -> str:
            return re.sub(f"^{re.escape(old)}", new, path)

        for doc in cursor:
            system_uuid = doc.get("system_uuid")
            system_path = doc.get("system_path", "")
            system_result = doc.get("system_result", "")
            new_path = replace_prefix(system_path, old_dir, new_dir)
            new_result = replace_prefix(system_result, old_dir, new_dir)
            if new_path == system_path and new_result == system_result:
                unchanged += 1
                continue
            if dry_run:
                logger.info(
                    f"Would update uuid={system_uuid}: path={system_path} "
                    f"-> {new_path}, result={system_result} -> {new_result}"
                )
                updated += 1
                continue
            try:
                update_doc = {"system_path": new_path, "system_result": new_result}
                file_collection.update_one(
                    {"system_uuid": system_uuid}, {"$set": update_doc}
                )
                logger.info(
                    f"Updated uuid={system_uuid}: path={system_path} "
                    f"-> {new_path}, result={system_result} -> {new_result}"
                )
                updated += 1
            except Exception as e:
                logger.error(f"Failed to update uuid={system_uuid}: {e}")
                errors += 1

        summary = {
            "updated_or_would_update": updated,
            "unchanged": unchanged,
            "errors": errors,
            "dry_run": dry_run,
            "old_dir": old_dir,
            "new_dir": new_dir,
        }
        logger.info(f"update_paths_in_database summary: {summary}")
        return summary

    def generate_radar_import_csv(self, limit: int | None = None) -> None:
        """Generate a CSV file with the results for RADAR.

        Uses the dataframe returned by `get_all_database_entries()` and only includes
        rows that are:
        - VALID (system_status == AssasDocumentFileStatus.VALID.value)
        - have a non-empty `radar_dataset_id`

        Args:
            limit: Optional maximum number of rows to write.

        Returns:
            None

        """
        logger.info("Generate result CSV file for radar.")
        dataframe = self.get_all_database_entries()

        if dataframe is None or dataframe.empty:
            logger.info("No database entries found; not writing radar_import.csv")
            return

        if "system_status" not in dataframe.columns:
            logger.warning(
                "generate_radar_import_csv: "
                "missing column 'system_status'; cannot filter VALID"
            )
            return

        # Filter VALID
        df_valid = dataframe[
            dataframe["system_status"] == AssasDocumentFileStatus.VALID.value
        ]

        csv_path = self.upload_directory / "radar_import.csv"
        written = 0
        skipped_missing_dataset_id = 0
        skipped_missing_upload_info = 0
        skipped_missing_archive_paths = 0

        with open(csv_path, mode="w", newline="") as csv_file:
            fieldnames = [
                "system_uuid",
                "upload_uuid",
                "radar_dataset_id",
                "upload_root",
                "rel_tar_path",
                "tar_md5",
            ]
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()

            for _, row in df_valid.iterrows():
                if limit is not None and written >= int(limit):
                    break

                radar_dataset_id = row.get("radar_dataset_id", None)

                # require dataset id
                if radar_dataset_id is None or pd.isna(radar_dataset_id):
                    skipped_missing_dataset_id += 1
                    continue

                radar_dataset_id = str(radar_dataset_id).strip()
                if not radar_dataset_id or radar_dataset_id.lower() in {
                    "nan",
                    "none",
                    "null",
                }:
                    skipped_missing_dataset_id += 1
                    continue

                upload_uuid = str(row.get("system_upload_uuid", "") or "").strip()
                if not upload_uuid:
                    # if upload uuid is missing, we also cannot build upload_root
                    skipped_missing_upload_info += 1
                    continue

                upload_info = row.get("upload_info", None)
                if not isinstance(upload_info, dict):
                    skipped_missing_upload_info += 1
                    continue

                rel_tar_paths = upload_info.get("archive_paths", None)
                if not isinstance(rel_tar_paths, list) or len(rel_tar_paths) == 0:
                    skipped_missing_archive_paths += 1
                    continue

                rel_tar_path = Path(rel_tar_paths[0])
                rel_tar_path_str = rel_tar_path.with_name(
                    rel_tar_path.name + ".tar"
                ).as_posix()

                writer.writerow(
                    {
                        "system_uuid": str(row.get("system_uuid", "") or "").strip(),
                        "upload_uuid": upload_uuid,
                        "radar_dataset_id": radar_dataset_id,
                        "upload_root": f"{self.upload_directory}/{upload_uuid}",
                        "rel_tar_path": rel_tar_path_str,
                        "tar_md5": row.get("system_md5", ""),
                    }
                )
                written += 1

        logger.info(
            (
                "Generated result CSV file at %s "
                "(written=%d, skipped_missing_dataset_id=%d, "
                "skipped_missing_upload_info=%d, skipped_missing_archive_paths=%d)."
            ),
            csv_path,
            written,
            skipped_missing_dataset_id,
            skipped_missing_upload_info,
            skipped_missing_archive_paths,
        )

    def _md5_file(self, path: Path, *, bufsize: int = 8 * 1024 * 1024) -> str:
        """Calculate MD5 checksum of a file.

        Args:
            path: Path to the file
            bufsize: Buffer size for reading the file

        Returns:
            MD5 checksum as a hexadecimal string

        """
        h = hashlib.md5()  # noqa: S324 (md5 requested for checksum output)
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(bufsize), b""):
                h.update(chunk)
        return h.hexdigest()

    def _has_existing_md5(self, v: object) -> bool:
        """Check if the given value is a valid existing MD5 checksum."""
        if v is None:
            return False
        try:
            if pd.isna(v):
                return False
        except Exception:
            pass
        s = str(v).strip()
        if not s or s.lower() in {"nan", "none", "null", "...", "...."}:
            return False
        # md5 hex length check (best-effort)
        if re.fullmatch(r"[a-fA-F0-9]{32}", s) is None:
            return False
        return True

    def calculate_md5_for_tar(
        self, limit: int | None = None, force: bool = False
    ) -> None:
        """Calculate the MD5 checksum for the tar files of all valid archives.

        Only calculates and writes MD5 if no MD5 is already present in MongoDB,
        unless force=True, in which case all existing MD5 values are recalculated.
        """
        logger.info(
            "Calculate MD5 checksums for tar files of all valid archives (force=%s).",
            force,
        )
        dataframe = self.get_all_database_entries()

        df_valid = dataframe[
            dataframe["system_status"] == AssasDocumentFileStatus.VALID.value
        ]

        written = 0
        skipped_missing_dataset_id = 0
        skipped_existing_md5 = 0

        for _, row in df_valid.iterrows():
            if limit is not None and written >= int(limit):
                break

            radar_dataset_id = row.get("radar_dataset_id", None)

            if radar_dataset_id is None or pd.isna(radar_dataset_id):
                skipped_missing_dataset_id += 1
                continue

            radar_dataset_id = str(radar_dataset_id).strip()
            if not radar_dataset_id or radar_dataset_id.lower() in {
                "nan",
                "none",
                "null",
            }:
                skipped_missing_dataset_id += 1
                continue

            # Skip if md5 already exists, unless force=True
            if not force:
                existing_md5 = row.get("system_md5", None)
                if existing_md5 is None:
                    existing_md5 = row.get("tar_md5", None)
                if self._has_existing_md5(existing_md5):
                    skipped_existing_md5 += 1
                    continue

            system_uuid = str(row.get("system_uuid", "") or "").strip()
            upload_uuid = str(row.get("system_upload_uuid", "") or "").strip()
            upload_info = row.get("upload_info", None)
            system_path = str(row.get("system_path", "") or "").strip()

            if system_path.startswith("/mnt/ASSAS"):
                system_path = system_path.replace(
                    "/mnt/ASSAS", "/lsdf/kit/scc/projects/ASSAS", 1
                )

            if not system_uuid or not upload_uuid or not isinstance(upload_info, dict):
                continue

            tar_path = Path(system_path)
            tar_path = tar_path.with_name(tar_path.name + ".tar")
            if not tar_path.exists():
                logger.warning(
                    f"Tar file does not exist for uuid={system_uuid}: {tar_path}"
                )
                continue

            try:
                logger.info(
                    f"Calculating MD5 for uuid={system_uuid} at path {tar_path}."
                )

                md5_checksum = self._md5_file(tar_path)
                logger.info(f"Calculated MD5 for uuid={system_uuid}: {md5_checksum}")

                # Update the database with the calculated MD5
                result = self.database_handler.file_collection.update_one(
                    {"system_uuid": system_uuid}, {"$set": {"system_md5": md5_checksum}}
                )

                logger.info(
                    f"Updated MD5 in database for uuid={system_uuid}: "
                    f"matched={result.matched_count} modified={result.modified_count}"
                )
                written += 1

            except Exception as e:
                logger.error(
                    f"Failed to calculate MD5 for uuid={system_uuid}: {e}",
                    exc_info=True,
                )

        logger.info(
            f"calculate_md5_for_tar summary: written={written} "
            f"skipped_existing_md5={skipped_existing_md5} "
            f"skipped_missing_dataset_id={skipped_missing_dataset_id}"
        )
