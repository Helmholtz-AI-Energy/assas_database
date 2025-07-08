"""Test class for AssasAstecArchive.

This module contains unit tests for the AssasAstecArchive class,
which is part of the AssasDB package.
"""

import unittest
import logging
import HtmlTestRunner

from pathlib import Path
from logging.handlers import RotatingFileHandler
from uuid import uuid4
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


class AssasAstecArchiveTest(unittest.TestCase):
    """Unit tests for the AssasAstecArchive class."""

    def setUp(self) -> None:
        """Set up the test environment for AssasAstecArchive."""
        self.upload_uuid = uuid4()
        self.name = "Test Archive"
        self.date = "2025-06-27"
        self.user = "test_user"
        self.description = "Test description"
        self.archive_path = "/path/to/archive.zip"
        self.result_path = "/path/to/result.txt"
        self.archive = AssasAstecArchive(
            self.upload_uuid,
            self.name,
            self.date,
            self.user,
            self.description,
            self.archive_path,
            self.result_path,
        )

    def test_upload_uuid(self) -> None:
        """Test the upload_uuid property."""
        self.assertEqual(self.archive.upload_uuid, self.upload_uuid)

    def test_name(self) -> None:
        """Test the name property."""
        self.assertEqual(self.archive.name, self.name)

    def test_date(self) -> None:
        """Test the date property."""
        self.assertEqual(self.archive.date, self.date)

    def test_user(self) -> None:
        """Test the user property."""
        self.assertEqual(self.archive.user, self.user)

    def test_description(self) -> None:
        """Test the description property."""
        self.assertEqual(self.archive.description, self.description)

    def test_archive_path(self) -> None:
        """Test the archive_path property."""
        self.assertEqual(self.archive.archive_path, self.archive_path)

    def test_result_path(self) -> None:
        """Test the result_path property."""
        self.assertEqual(self.archive.result_path, self.result_path)


if __name__ == "__main__":
    unittest.main(
        testRunner=HtmlTestRunner.HTMLTestRunner(
            output="test_reports",  # Directory for HTML reports
            report_title="AssasAstecArchive Test Report",
            descriptions=True,
        )
    )
