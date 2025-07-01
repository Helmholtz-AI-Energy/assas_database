"""Test class for AssasDocumentFile.

This module contains unit tests for the AssasDocumentFile class,
which is part of the AssasDB package.
"""

import unittest
import logging
import HtmlTestRunner

from pathlib import Path
from logging.handlers import RotatingFileHandler
from assasdb import AssasDocumentFile

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


class AssasDocumentFileTest(unittest.TestCase):
    """Unit tests for the AssasDocumentFile class."""

    def setUp(self):
        """Set up the test environment for AssasDocumentFile."""
        self.initial_document = {"key1": "value1", "key2": "value2"}
        self.assas_document = AssasDocumentFile(self.initial_document)

    def test_get_document(self):
        """Test the get_document method."""
        document = self.assas_document.get_document()
        self.assertEqual(document, self.initial_document)
        self.assertIsInstance(document, dict)

    def test_set_document(self):
        """Test the set_document method."""
        new_document = {"key3": "value3", "key4": "value4"}
        self.assas_document.set_document(new_document)
        self.assertEqual(self.assas_document.get_document(), new_document)

    def test_set_document_invalid_type(self):
        """Test the set_document method with an invalid type."""
        with self.assertRaises(TypeError):
            self.assas_document.set_document(["invalid", "type"])

    def test_extend_document(self):
        """Test the extend_document method."""
        additional_data = {"key3": "value3", "key4": "value4"}
        self.assas_document.extend_document(additional_data)
        expected_document = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3",
            "key4": "value4",
        }
        self.assertEqual(self.assas_document.get_document(), expected_document)

    def test_extend_document_invalid_type(self):
        """Test the extend_document method with an invalid type."""
        with self.assertRaises(TypeError):
            self.assas_document.extend_document(["invalid", "type"])

    def test_set_general_meta_values(self):
        """Test the set_general_meta_values method."""
        meta_values = {"meta_name": "meta_value1", "meta_description": "meta_value2"}
        self.assas_document.set_general_meta_values(
            meta_name=meta_values["meta_name"],
            meta_description=meta_values["meta_description"],
        )
        expected_document = {
            "key1": "value1",
            "key2": "value2",
            "meta_name": "meta_value1",
            "meta_description": "meta_value2",
        }
        self.assertEqual(self.assas_document.get_document(), expected_document)

    def test_set_general_meta_values_invalid_type(self):
        """Test the set_general_meta_values method with an invalid type."""
        with self.assertRaises(TypeError):
            self.assas_document.set_general_meta_values(["invalid", "type"])


if __name__ == "__main__":
    unittest.main(
        testRunner=HtmlTestRunner.HTMLTestRunner(
            output="test_reports",  # Directory for HTML reports
            report_title="AssasDocumentFile Test Report",
            descriptions=True,
        )
    )
