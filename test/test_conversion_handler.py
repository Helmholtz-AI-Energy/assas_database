"""Test module with unit tests for the AssasConversionHandler class."""

import unittest
import os
import logging
import HtmlTestRunner
import tempfile
import bson
import shutil

from logging.handlers import RotatingFileHandler
from unittest.mock import patch
from pathlib import Path
from unittest.mock import MagicMock

from assasdb.tools.assas_conversion_handler import AssasConversionHandler

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


class AssasConversionHandlerTest(unittest.TestCase):
    """Unit tests for the AssasConversionHandler class.

    This class tests the methods responsible for handling ASSAS data conversion,
    including setup, file copying, synchronization, and conversion handling.
    It uses mocking to simulate file system operations and external dependencies.
    """

    @patch("assasdb.tools.assas_conversion_handler.LSDF_BACKUP_DIR", new="")
    def setUp(self) -> None:
        """Set up the test environment with fake directories and mocked dependencies."""
        self.fake_lsdf_data_dir = tempfile.mkdtemp()
        self.fake_tmp_dir = tempfile.mkdtemp()
        # Mock environment variables
        os.environ["LSDFPROJECTS"] = self.fake_lsdf_data_dir
        os.environ["TMPDIR"] = self.fake_tmp_dir
        self.fake_upload_uuid = "123e4567-e89b-12d3-a456-426614174000"

        self.fake_input_path = Path(
            f"{self.fake_lsdf_data_dir}/{self.fake_upload_uuid}"
        )
        self.fake_tmp_path = Path(f"{self.fake_tmp_dir}/{self.fake_upload_uuid}")
        self.fake_output_path = Path(
            f"{self.fake_lsdf_data_dir}/{self.fake_upload_uuid}/result/dataset.h5"
        )

        # Create a fake MongoDB backup directory with a fake .bson file
        self.fake_mongodb_backup_dir = self.fake_lsdf_data_dir
        fake_bson_file = Path(self.fake_mongodb_backup_dir) / "files.bson"
        fake_bson_entry = {
            "system_upload_uuid": self.fake_upload_uuid,
            "meta_name": "fake_dataset",
            "meta_description": "This is a fake BSON entry for testing.",
            "system_status": "UPLOADED",
            "system_path": str(self.fake_input_path),
            "system_result": str(self.fake_output_path),
        }

        with open(fake_bson_file, "wb") as f:
            f.write(bson.encode(fake_bson_entry))

        if not self.fake_output_path.exists():
            self.fake_output_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.fake_input_path.exists():
            self.fake_input_path.mkdir(parents=True, exist_ok=True)
        if not self.fake_tmp_path.exists():
            self.fake_tmp_path.mkdir(parents=True, exist_ok=True)

        self.test_file = self.fake_input_path / "file1.txt"
        with open(self.test_file, "w") as f:
            f.write("This is a test file.")

        # self.test_tmp_file = self.fake_tmp_path / "file1.txt"
        # with open(self.test_tmp_file, "w") as f:
        #    f.write("This is a test file.")

        self.test_file = self.fake_input_path / "file2.txt"
        with open(self.test_file, "w") as f:
            f.write("This is a test file.")

        # self.test_tmp_file = self.fake_tmp_path / "file2.txt"
        # with open(self.test_tmp_file, "w") as f:
        #    f.write("This is a test file.")

        # Create a fake AssasConversionHandler instance
        self.handler = AssasConversionHandler(
            upload_uuid=self.fake_upload_uuid,
            new=True,
            time=None,
            debug=True,
            lsdf_data_dir=self.fake_lsdf_data_dir,
            lsdf_backup_dir=self.fake_mongodb_backup_dir,
        )

    def tearDown(self) -> None:
        """Clean up temporary directories and files."""
        # Call the handler's cleanup method if it exists

        self.handler.close_resources()

        # Delete the handler instance
        del self.handler

        # Remove temporary directories
        shutil.rmtree(self.fake_lsdf_data_dir, ignore_errors=True)
        shutil.rmtree(self.fake_tmp_dir, ignore_errors=True)
        shutil.rmtree(self.fake_mongodb_backup_dir, ignore_errors=True)

    def test_lsdf_backup_dir_override(self) -> None:
        """Test that LSDF_BACKUP_DIR is correctly overridden."""
        # self.assertEqual(os.environ["LSDF_BACKUP_DIR"], self.fake_mongodb_backup_dir)
        self.assertEqual(self.handler.lsdf_backup_dir, self.fake_mongodb_backup_dir)

    @patch("os.makedirs")
    def test_setup_logging(self, mock_makedirs: MagicMock) -> None:
        """Test the setup_logging method."""
        self.handler.setup_logging(custom_level=logging.DEBUG)

        # Assert that the log directory was created
        mock_makedirs.assert_called_once()

    @patch("shutil.copy2")
    @patch("os.walk")
    def test_copytree_verbose_to_tmp_with_process(
        self,
        mock_os_walk: MagicMock,
        mock_copy2: MagicMock,
    ) -> None:
        """Test the copytree_verbose_to_tmp_with_process method."""
        file_list = [
            "file1.txt",
            "file2.txt",
        ]
        # Mock the os.walk output
        mock_os_walk.return_value = [(str(self.fake_input_path), [], file_list)]

        # Call the method
        tmp_path = self.handler.copytree_verbose_to_tmp_with_process(
            input_path=str(self.fake_input_path),
            tmp_path=str(self.fake_tmp_path),
        )

        mock_copy2.call_count = len(file_list)

        # Assert the returned path
        self.assertEqual(tmp_path, str(self.fake_tmp_path))

    @patch("assasdb.tools.assas_conversion_handler.copy2")
    def test_copy2_verbose(self, mock_copy2: MagicMock) -> None:
        """Test the copy2_verbose method."""
        source = str(self.fake_input_path / "file1.txt")
        destination = str(self.fake_tmp_path / "file1.txt")

        # Call the method
        self.handler.copy2_verbose(source=source, destination=destination)

        # Assert that the file was copied
        mock_copy2.assert_called_once_with(source, destination)

    @patch("assasdb.tools.assas_conversion_handler.sync")
    def test_sync_imput_and_tmp(self, mock_sync: MagicMock) -> None:
        """Test the sync_imput_and_tmp method."""
        # Call the method
        self.handler.sync_imput_and_tmp(
            input_path=str(self.fake_input_path),
            tmp_path=str(self.fake_tmp_path),
        )

        # Assert that the sync function was called
        mock_sync.assert_called_once_with(
            str(self.fake_input_path), str(self.fake_tmp_path), verbose=True
        )

    @patch("os.system")
    def test_remove_tmp(self, mock_os_system: MagicMock) -> None:
        """Test the remove_tmp method."""
        # Call the method
        self.handler.remove_tmp(tmp_path=str(self.fake_tmp_path))

        # Assert that the remove command was executed
        mock_os_system.assert_called_once_with(f"rm -rf {self.fake_tmp_path}")

    @unittest.skip("Skipping test for handle_conversion method")
    @patch.object(AssasConversionHandler, "notify_invalid_conversion")
    @patch.object(AssasConversionHandler, "notify_conversion_start")
    @patch("assasdb.AssasOdessaNetCDF4Converter")
    @patch.object(AssasConversionHandler, "remove_tmp")
    def test_handle_conversion(
        self,
        mock_remove_tmp: MagicMock,
        mock_odessa_converter: MagicMock,
        mock_notify_start: MagicMock,
        notify_invalid_conversion: MagicMock,
    ) -> None:
        """Test the handle_conversion method with mocked AssasOdessaNetCDF4Converter."""
        # Mock the Odessa converter
        mock_converter_instance = mock_odessa_converter.return_value
        mock_converter_instance.get_time_points.return_value = [1, 2, 3]
        mock_converter_instance.convert_astec_variables_to_netcdf4.return_value = None

        # Call the method
        self.handler.handle_conversion()
        mock_notify_start.assert_called_once()
        mock_remove_tmp.assert_called_once()
        notify_invalid_conversion.assert_called_once()

        # mock_remove_tmp.assert_called_once()
        # mock_converter_instance.get_time_points.assert_called_once()
        # mehtod = mock_converter_instance.convert_astec_variables_to_netcdf4
        # method.assert_called_once()

    @patch("os.system")
    def test_notify_conversion_start(self, mock_os_system: MagicMock) -> None:
        """Test the notify_valid_conversion method."""
        upload_directory = str(self.fake_lsdf_data_dir)
        upload_uuid = self.fake_upload_uuid

        # Call the method
        self.handler.notify_conversion_start(
            upload_uuid=upload_uuid, upload_directory=upload_directory
        )

        # Assert that the touch command was executed
        mock_os_system.assert_called_once_with(
            f"touch {upload_directory}/{upload_uuid}/{upload_uuid}_converting"
        )

    @patch("os.system")
    def test_notify_valid_conversion(self, mock_os_system: MagicMock) -> None:
        """Test the notify_valid_conversion method."""
        upload_directory = str(self.fake_lsdf_data_dir)
        upload_uuid = self.fake_upload_uuid

        # Call the method
        self.handler.notify_valid_conversion(
            upload_uuid=upload_uuid, upload_directory=upload_directory
        )

        # Assert that the touch command was executed
        mock_os_system.assert_called_once_with(
            f"touch {upload_directory}/{upload_uuid}/{upload_uuid}_valid"
        )

    @patch("os.system")
    def test_notify_invalid_conversion(self, mock_os_system: MagicMock) -> None:
        """Test the notify_invalid_conversion method."""
        upload_directory = str(self.fake_lsdf_data_dir)
        upload_uuid = self.fake_upload_uuid

        # Call the method
        self.handler.notify_invalid_conversion(
            upload_uuid=upload_uuid, upload_directory=upload_directory
        )

        # Assert that the touch command was executed
        mock_os_system.assert_called_once_with(
            f"touch {upload_directory}/{upload_uuid}/{upload_uuid}_invalid"
        )


if __name__ == "__main__":
    unittest.main(
        testRunner=HtmlTestRunner.HTMLTestRunner(
            output="test_reports",  # Directory for HTML reports
            report_title="AssasConversionHandler Test Report",
            descriptions=True,
        )
    )
