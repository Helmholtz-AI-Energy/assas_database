"""Test suite for AssasOdessaNetCDF4Converter class.

This module tests the conversion of ASTEC archives to NetCDF4 format using
the AssasOdessaNetCDF4Converter class.
"""

import unittest
import logging
import shutil
import tempfile
import HtmlTestRunner

from pathlib import Path
from logging.handlers import RotatingFileHandler

from assasdb import AssasOdessaNetCDF4Converter

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
    level=logging.INFO,
    handlers=[log_handler, logging.StreamHandler()],
    force=True  # Log to file and console
)


class AssasOdessaNetCDF4ConverterTest(unittest.TestCase):
    """Test suite for AssasOdessaNetCDF4Converter class.

    This class tests the conversion of ASTEC archives to NetCDF4 format.
    """

    def setUp(self) -> None:
        """Set up the test environment."""
        # Path to the test ASTEC archive

        test_path = Path(__file__).parent
        if not test_path.exists():
            self.fail(f"Test path does not exist: {test_path}")
        self.test_archive_path = (
            test_path / "test_data" / "archive" / "LOCA_12P_CL_1300_LIKE.bin"
        )

        # Ensure the test archive exists
        if not self.test_archive_path.exists():
            self.fail(f"Test archive not found: {self.test_archive_path}")

        # Create temporary directories for input and output
        self.fake_tmp_dir = tempfile.mkdtemp()
        self.fake_output_path = Path(self.fake_tmp_dir) / "output.nc"

        # Copy the test archive to the temporary directory
        self.fake_input_path = Path(self.fake_tmp_dir) / "test_archive.bin"
        shutil.copytree(self.test_archive_path, self.fake_input_path)

        # Initialize the AssasOdessaNetCDF4Converter
        self.converter = AssasOdessaNetCDF4Converter(
            input_path=self.fake_input_path,
            output_path=self.fake_output_path,
        )

    def tearDown(self) -> None:
        """Clean up temporary directories and files."""
        shutil.rmtree(self.fake_tmp_dir, ignore_errors=True)

    def test_convert_astec_archive(self) -> None:
        """Test converting the ASTEC archive to NetCDF4 format."""
        # Ensure the input file exists
        self.assertTrue(
            self.fake_input_path.exists(), "Input archive file does not exist."
        )

        # Call the conversion method
        try:
            self.converter.convert_astec_variables_to_netcdf4()
        except Exception as e:
            self.fail(f"Conversion failed with exception: {e}.")

        # Verify that the output file is created
        self.assertTrue(
            self.fake_output_path.exists(), "Output NetCDF4 file was not created."
        )

        # Additional checks can be added here to verify the content of the output file
        variable_index = self.converter.get_variable_index()
        meta_data_list = self.converter.read_meta_values_from_netcdf4(
            self.fake_output_path
        )

        variables_from_meta_data = [meta_data["name"] for meta_data in meta_data_list]
        variables_from_meta_data.remove("time_points")
        variables_from_index = variable_index["name"].tolist()

        self.assertEqual(set(variables_from_meta_data), set(variables_from_index))


if __name__ == "__main__":
    unittest.main(
        testRunner=HtmlTestRunner.HTMLTestRunner(
            output="test_reports",  # Directory for HTML reports
            report_title="AssasOdessaNetCDF4Converter Test Report",
            descriptions=True,
        )
    )
