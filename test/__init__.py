"""Assas Database Test Suite.

This module imports and aggregates all test cases for the Assas Database package.
"""

from .test_database_manager import AssasDatabaseManagerIntegrationTest
from .test_database_handler import AssasDatabaseHandlerTest
from .test_astec_archive import AssasAstecArchiveTest

# from .test_odessa_netcdf4_converter import AssasOdessaNetCDF4ConverterTest
from .test_document_file import AssasDocumentFileTest

__all__ = [
    "AssasDatabaseManagerIntegrationTest",
    "AssasDatabaseHandlerTest",
    "AssasAstecArchiveTest",
    # "AssasOdessaNetCDF4ConverterTest",
    "AssasDocumentFileTest",
]
