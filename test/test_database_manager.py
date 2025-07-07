"""Test suite for AssasDatabaseManager."""

import os
import pickle
import unittest
import tempfile
import shutil
import pandas as pd
import logging
import HtmlTestRunner

from typing import List
from uuid import uuid4
from pathlib import Path
from logging.handlers import RotatingFileHandler
from unittest.mock import MagicMock, patch

from assasdb import AssasDatabaseManager
from assasdb import AssasDocumentFileStatus
from assasdb import AssasDatabaseHandler
from assasdb import AssasAstecArchive

# Configure rotating file logging
log_dir = Path(__file__).parent / "log"
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / (Path(__file__).stem + ".log")
log_handler = RotatingFileHandler(
    log_file,
    maxBytes=1024 * 1024,
    backupCount=3,  # 1MB per file, 3 backups
)
log_format = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
log_handler.setFormatter(log_format)
logging.basicConfig(
    level=logging.DEBUG,
    handlers=[log_handler, logging.StreamHandler()],  # Log to file and console
)


def create_fake_upload_archives(base_dir: str, num_archives: int = 3) -> None:
    """Create fake upload archive folders with upload_info.pickle files inside.

    Each folder is named after a UUID.
    """
    os.makedirs(base_dir, exist_ok=True)
    for _ in range(num_archives):
        upload_uuid = str(uuid4())
        archive_dir = os.path.join(base_dir, upload_uuid)
        os.makedirs(archive_dir, exist_ok=True)
        upload_info = {
            "name": f"archive_{upload_uuid}",
            "archive_paths": [f"{archive_dir}/archive_test.bin"],
            "user": "test_user",
            "description": "Fake upload archive for testing",
        }
        info_path = os.path.join(archive_dir, "upload_info.pickle")
        with open(info_path, "wb") as f:
            pickle.dump(upload_info, f)
        print(f"Created {info_path}")


class FakeMongoCollection:
    """A fake MongoDB collection for testing purposes."""

    def __init__(self, docs: List = None) -> None:
        """Initialize the fake collection with optional documents."""
        self.docs = docs or []

    def find(self) -> List:
        """Return all documents in the collection."""
        return self.docs

    def insert_one(self, doc: dict) -> None:
        """Insert a single document into the collection."""
        self.docs.append(doc)

    def drop(self) -> None:
        """Drop the collection by clearing its documents."""
        self.docs = []


class FakeDatabaseHandler:
    """A fake database handler for testing purposes."""

    def __init__(
        self,
        client: MagicMock,
        backup_directory: str = None,
    )-> None:
        """Initialize the fake database handler with a client and backup directory."""
        self._collection = FakeMongoCollection()
        self._backup = []

    def get_file_collection(self) -> FakeMongoCollection:
        """Return the fake collection."""
        return self._collection

    def get_file_document_by_upload_uuid(self, upload_uuid: str) -> dict:
        """Return a fake file document by upload UUID."""
        return {"upload_uuid": upload_uuid}

    def get_file_document(self, id: str) -> dict:
        """Return a fake file document by ID."""
        return {"_id": id}

    def get_file_document_by_uuid(self, uuid: uuid4) -> dict:
        """Return a fake file document by UUID."""
        return {"uuid": uuid, "system_uuid": uuid}

    def get_file_document_by_path(self, path: str) -> dict:
        """Return a fake file document by system path."""
        return {"system_path": path}

    def read_collection_from_backup(self) -> list:
        """Return the backup collection."""
        return self._backup

    def dump_collections(self, collection_names: List = None) -> None:
        """Dump the current collection to the backup."""
        self._backup = list(self._collection.find())

    def insert_file_document(self, doc: dict) -> None:
        """Insert a document into the fake collection."""
        self._collection.insert_one(doc)

    def drop_file_collection(self) -> None:
        """Drop the fake collection."""
        self._collection.drop()

    def update_file_document_by_uuid(self, uuid: uuid4, update: dict) -> None:
        """Update a file document by UUID."""
        pass

    def update_file_document_by_path(self, path: str, doc: dict) -> None:
        """Update a file document by system path."""
        pass

    def close(self) -> None:
        """Close the database connection."""
        pass


class AssasDatabaseManagerIntegrationTest(unittest.TestCase):
    """Integration test for AssasDatabaseManager."""

    def setUp(self) -> None:
        """Set up the test environment."""
        # Create fake directories
        self.upload_dir = tempfile.mkdtemp()
        self.backup_dir = tempfile.mkdtemp()

        # Create a mock AssasDatabaseHandler
        self.mock_handler = MagicMock(spec=AssasDatabaseHandler)

        # Mock the methods of AssasDatabaseHandler
        self.fake_handler = FakeDatabaseHandler(
            client=MagicMock(),  # Mock MongoClient
            backup_directory=self.backup_dir,
        )

        self.manager = AssasDatabaseManager(
            database_handler=self.mock_handler,
            upload_directory=self.upload_dir,
        )
        self.manager_faked = AssasDatabaseManager(
            database_handler=self.fake_handler,
            upload_directory=self.upload_dir,
        )

    def tearDown(self) -> None:
        """Clean up the test environment."""
        shutil.rmtree(self.upload_dir)
        shutil.rmtree(self.backup_dir)

        self.manager.close_resources()
        self.manager_faked.close_resources()

    def test_get_all_database_entries_empty(self) -> None:
        """Test getting all database entries when the collection is empty.

        This should return an empty DataFrame.
        """
        df = self.manager.get_all_database_entries()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 0)

    def test_add_and_get_database_entry(self) -> None:
        """Test adding a document and retrieving it from the database.

        This should ensure that the document is correctly added and can be retrieved.
        """
        doc = {"_id": "1", "system_path": "/fake/path"}
        self.manager_faked.add_internal_database_entry(doc)
        df = self.manager_faked.get_all_database_entries()
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["system_path"], "/fake/path")

    def test_backup_and_restore(self) -> None:
        """Test backing up the database and restoring from backup.

        This should ensure that the data persists across backup and restore operations.
        """
        doc = {"_id": "2", "system_path": "/another/path"}
        self.manager_faked.add_internal_database_entry(doc)
        self.manager_faked.backup_internal_database()
        # Simulate clearing the collection
        self.manager_faked.empty_internal_database()
        df_empty = self.manager_faked.get_all_database_entries()
        self.assertEqual(len(df_empty), 0)
        # Restore from backup
        df_backup = self.manager_faked.get_all_database_entries_from_backup()
        self.assertEqual(len(df_backup), 1)
        self.assertEqual(df_backup.iloc[0]["system_path"], "/another/path")

    def test_get_database_entry_by_upload_uuid(self) -> None:
        """Test getting a database entry by upload UUID."""
        self.mock_handler.get_file_document_by_upload_uuid.return_value = {
            "upload_uuid": "uuid"
        }
        result = self.manager.get_database_entry_by_upload_uuid("uuid")
        self.assertEqual(result, {"upload_uuid": "uuid"})

    def test_get_database_entry_by_id(self) -> None:
        """Test getting a database entry by ID."""
        self.mock_handler.get_file_document.return_value = {"_id": "id"}
        result = self.manager.get_database_entry_by_id("id")
        self.assertEqual(result, {"_id": "id"})

    def test_get_database_entry_by_uuid(self) -> None:
        """Test getting a database entry by UUID."""
        uuid = uuid4()
        self.mock_handler.get_file_document_by_uuid.return_value = {
            "uuid": uuid,
            "system_uuid": uuid,
        }
        result = self.manager.get_database_entry_by_uuid(uuid=uuid)
        self.assertEqual(result, {"uuid": uuid, "system_uuid": uuid})

    def test_get_database_entry_by_path(self) -> None:
        """Test getting a database entry by system path."""
        self.mock_handler.get_file_document_by_path.return_value = {
            "system_path": "path"
        }
        result = self.manager.get_database_entry_by_path("path")
        self.assertEqual(result, {"system_path": "path"})

    def test_get_all_database_entries(self) -> None:
        """Test getting all database entries from the database."""
        mock_collection = MagicMock()
        mock_collection.find.return_value = []
        self.mock_handler.get_file_collection.return_value = mock_collection
        df = self.manager.get_all_database_entries()
        self.assertIsInstance(df, pd.DataFrame)

    def test_get_all_database_entries_from_backup(self) -> None:
        """Test getting all database entries from the backup."""
        self.mock_handler.read_collection_from_backup.return_value = []
        df = self.manager.get_all_database_entries_from_backup()
        self.assertIsInstance(df, pd.DataFrame)

    def test_backup_internal_database(self) -> None:
        """Test backing up the internal database."""
        self.manager.backup_internal_database()
        self.mock_handler.dump_collections.assert_called()

    def test_set_document_status_by_uuid(self) -> None:
        """Test setting the status of a document by UUID."""
        self.mock_handler.get_file_document_by_uuid.return_value = {
            "system_uuid": "uuid"
        }
        self.manager.set_document_status_by_uuid(
            "uuid", AssasDocumentFileStatus.UPLOADED
        )
        self.mock_handler.update_file_document_by_uuid.assert_called()

    def test_set_hdf5_size_by_uuid(self) -> None:
        """Test setting the HDF5 size of a document by UUID."""
        self.mock_handler.get_file_document_by_uuid.return_value = {
            "system_uuid": "uuid"
        }
        self.manager.set_hdf5_size_by_uuid("uuid", "100MB")
        self.mock_handler.update_file_document_by_uuid.assert_called()

    def test_add_internal_database_entry(self) -> None:
        """Test adding an internal database entry."""
        self.manager.add_internal_database_entry({"_id": "id"})
        self.mock_handler.insert_file_document.assert_called()

    def test_empty_internal_database(self) -> None:
        """Test emptying the internal database."""
        self.manager.empty_internal_database()
        self.mock_handler.drop_file_collection.assert_called()

    def test_collect_number_of_samples_of_uploaded_archives(self) -> None:
        """Test collecting the number of samples from uploaded archives."""
        method = self.mock_handler.get_file_documents_to_collect_number_of_samples
        method.return_value = []
        # Should not raise
        self.manager.collect_number_of_samples_of_uploaded_archives()

    def test_get_overall_database_size(self) -> None:
        """Test getting the overall database size."""
        with patch.object(
            self.manager,
            "get_all_database_entries",
            return_value=pd.DataFrame({"system_size": ["1 KB", "2 KB"]}),
        ):
            size = self.manager.get_overall_database_size()
            self.assertIsInstance(size, str)

    def test_convert_to_bytes(self) -> None:
        """Test converting human-readable sizes to bytes."""
        self.assertEqual(self.manager.convert_to_bytes("1 GB"), 1024**3)
        self.assertEqual(self.manager.convert_to_bytes("1 MB"), 1024**2)
        self.assertEqual(self.manager.convert_to_bytes("1 KB"), 1024)
        self.assertEqual(self.manager.convert_to_bytes("1 B"), 1)

    def test_convert_from_bytes(self) -> None:
        """Test converting bytes to human-readable sizes."""
        self.assertEqual(self.manager.convert_from_bytes(1024), "1.0 KB")

    def test_get_upload_time(self) -> None:
        """Test getting the upload time of a directory."""
        with patch("os.path.getctime", return_value=0):
            with patch("datetime.datetime") as mock_datetime:
                mock_datetime.fromtimestamp.return_value.strftime.return_value = (
                    "01/01/1970, 00:00:00"
                )
                result = self.manager.get_upload_time("test_dir")
                self.assertIsInstance(result, str)

    def test_get_size_of_directory_in_bytes(self) -> None:
        """Test getting the size of a directory in bytes."""
        with patch("subprocess.check_output", return_value=b"1234\ttest_dir"):
            size = self.manager.get_size_of_directory_in_bytes("test_dir")
            self.assertEqual(size, 1234.0)

    def test_update_archive_sizes(self) -> None:
        """Test updating the sizes of archives."""
        self.mock_handler.get_file_documents_to_update_size.return_value = []
        result = self.manager.update_archive_sizes()
        self.assertIsInstance(result, bool)

    def test_get_new_upload_uuids_to_process(self) -> None:
        """Test getting new upload UUIDs to process."""
        with patch("os.listdir", return_value=[]):
            result = self.manager.get_new_upload_uuids_to_process()
            self.assertIsInstance(result, list)

    def test_update_status_of_archives(self) -> None:
        """Test updating the status of archives."""
        with (
            patch.object(
                self.manager, "get_upload_uuids_of_converting_archives", return_value=[]
            ),
            patch.object(
                self.manager, "get_upload_uuids_of_valid_archives", return_value=[]
            ),
        ):
            self.manager.update_status_of_archives()

    def test_get_upload_uuids_of_valid_archives(self) -> None:
        """Test getting upload UUIDs of valid archives."""
        with patch("os.listdir", return_value=[]):
            result = self.manager.get_upload_uuids_of_valid_archives()
            self.assertIsInstance(result, list)

    def test_get_upload_uuids_of_converting_archives(self) -> None:
        """Test getting upload UUIDs of converting archives."""
        with patch("os.listdir", return_value=[]):
            result = self.manager.get_upload_uuids_of_converting_archives()
            self.assertIsInstance(result, list)

    def test_get_upload_uuids_to_reload(self) -> None:
        """Test getting upload UUIDs to reload."""
        with patch("os.listdir", return_value=[]):
            result = self.manager.get_upload_uuids_to_reload()
            self.assertIsInstance(result, list)

    def test_get_file_size(self) -> None:
        """Test getting the file size of a file."""
        with patch("os.path.isfile", return_value=True), patch("os.stat") as mock_stat:
            mock_stat.return_value.st_size = 1024
            with patch.object(
                self.manager, "convert_from_bytes", return_value="1.0 KB"
            ):
                size = self.manager.get_file_size("test_file")
                self.assertEqual(size, "1.0 KB")

    def test_get_uploaded_archives_to_process(self) -> None:
        """Test getting uploaded archives to process."""
        with patch.object(
            self.manager, "get_new_upload_uuids_to_process", return_value=[]
        ):
            result = self.manager.get_uploaded_archives_to_process()
            self.assertIsInstance(result, list)

    def test_get_uploaded_archives_to_reload(self) -> None:
        """Test getting uploaded archives to reload."""
        with patch.object(self.manager, "get_upload_uuids_to_reload", return_value=[]):
            result = self.manager.get_uploaded_archives_to_reload()
            self.assertIsInstance(result, list)

    def test_process_uploads(self) -> None:
        """Test processing uploads."""
        with (
            patch.object(
                self.manager, "get_uploaded_archives_to_process", return_value=[]
            ),
            patch.object(self.manager, "register_archives"),
        ):
            result = self.manager.process_uploads()
            self.assertIsInstance(result, bool)

    def test_process_uploads_with_reload_flag(self) -> None:
        """Test processing uploads with the reload flag."""
        with (
            patch.object(
                self.manager, "get_uploaded_archives_to_reload", return_value=[]
            ),
            patch.object(self.manager, "register_archives"),
        ):
            result = self.manager.process_uploads_with_reload_flag()
            self.assertIsInstance(result, bool)

    def test_update_upload_info(self) -> None:
        """Test updating upload information."""
        with (
            patch("builtins.open", unittest.mock.mock_open(read_data=b"")),
            patch(
                "pickle.load",
                return_value={
                    "name": "test",
                    "archive_paths": ["/a"],
                    "user": "u",
                    "description": "d",
                },
            ),
            patch("pickle.dump"),
        ):
            result = self.manager.update_upload_info("uuid", "key", ["value"])
            self.assertIsInstance(result, bool)

    def test_remove_lead_slash_from_path_string(self) -> None:
        """Test removing leading slashes from a path string."""
        self.assertEqual(
            self.manager.remove_lead_slash_from_path_string("/test"), "test"
        )
        self.assertEqual(
            self.manager.remove_lead_slash_from_path_string("test"), "test"
        )

    def test_read_upload_info_from_fake_archives(self) -> None:
        """Test reading upload information from fake archives."""
        # Create 2 fake upload archives
        create_fake_upload_archives(self.upload_dir, num_archives=2)
        # List all created UUID folders
        archive_uuids = os.listdir(self.upload_dir)
        self.assertGreaterEqual(len(archive_uuids), 2)
        for uuid in archive_uuids:
            # Should not raise and should return a list with expected keys
            results = self.manager_faked.read_upload_info(uuid)
            self.assertIsInstance(results, list)
            for result in results:
                self.assertIsInstance(result, AssasAstecArchive)
                self.assertEqual(f"archive_{uuid}", result.name)

            # self.assertGreaterEqual(len(result), 1)
            # self.assertIn("name", result[0])
            # self.assertIn("archive_paths", result[0])
            # self.assertIn("user", result[0])
            # self.assertIn("description", result[0])

    def test_register_archives(self) -> None:
        """Test registering archives with the database manager."""
        with (
            patch("assasdb.assas_database_manager.AssasDocumentFile"),
            patch("assasdb.assas_database_manager.AssasOdessaNetCDF4Converter"),
        ):
            mock_archive = MagicMock()
            mock_archive.archive_path = "ap"
            mock_archive.result_path = "rp"
            mock_archive.name = "n"
            mock_archive.description = "d"
            mock_archive.upload_uuid = "u"
            mock_archive.date = "date"
            mock_archive.user = "user"
            self.manager.register_archives([mock_archive])
            self.mock_handler.insert_file_document.assert_called()

    def test_postpone_conversion(self) -> None:
        """Test postponing conversion of archives."""
        self.mock_handler.get_file_documents_by_status.return_value = [{}] * 6
        self.assertTrue(self.manager.postpone_conversion(maximum_conversions=5))

    def test_convert_next_validated_archive(self) -> None:
        """Test converting the next validated archive."""
        self.mock_handler.get_file_documents_by_status.return_value = []
        # Should not raise
        self.manager.convert_next_validated_archive()

    def test_reset_invalid_archives(self) -> None:
        """Test resetting invalid archives."""
        self.mock_handler.get_file_documents_by_status.return_value = []
        self.manager.reset_invalid_archives()

    def test_reset_converting_archives(self) -> None:
        """Test resetting converting archives."""
        self.mock_handler.get_file_documents_by_status.return_value = []
        self.manager.reset_converting_archives()

    def test_reset_valid_archives(self) -> None:
        """Test resetting valid archives."""
        self.mock_handler.get_file_documents_by_status.return_value = []
        self.manager.reset_valid_archives()

    def test_reset_all_result_files(self) -> None:
        """Test resetting all result files."""
        self.mock_handler.get_all_file_documents.return_value = []
        with patch("assasdb.assas_database_manager.AssasOdessaNetCDF4Converter"):
            self.manager.reset_all_result_files()

    def test_reset_result_file_by_uuid(self) -> None:
        """Test resetting a result file by UUID."""
        uuid = uuid4()
        self.mock_handler.get_file_document_by_uuid.return_value = {
            "uuid": uuid,
            "system_result": "result_path",
            "meta_name": "meta_name",
            "meta_description": "meta_description",
        }
        # self.mock_handler.get_file_document_by_uuid.return_value = {}
        with patch("assasdb.assas_database_manager.AssasOdessaNetCDF4Converter"):
            self.manager.reset_result_file_by_uuid(uuid)

    def test_update_meta_data_of_valid_archives(self) -> None:
        """Test updating metadata of valid archives."""
        self.mock_handler.get_file_documents_to_collect_meta_data.return_value = []
        with patch(
            "assasdb.assas_database_manager.AssasOdessaNetCDF4Converter.read_meta_values_from_netcdf4",
            return_value={},
        ):
            self.manager.update_meta_data_of_valid_archives()

    def test_update_meta_data(self) -> None:
        """Test updating metadata for a specific UUID."""
        self.mock_handler.get_file_document_by_uuid.return_value = {}
        with patch(
            "assasdb.assas_database_manager.AssasOdessaNetCDF4Converter.read_meta_values_from_netcdf4",
            return_value={},
        ):
            self.manager.update_meta_data("uuid")


if __name__ == "__main__":
    unittest.main(
        testRunner=HtmlTestRunner.HTMLTestRunner(
            output="test_reports",  # Directory for HTML reports
            report_title="AssasDatabaseManager Test Report",
            descriptions=True,
        )
    )
