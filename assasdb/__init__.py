"""ASSAS Database Module.

This module provides classes and functions for managing the ASSAS database,
including handling ASTEC archives, converting data formats, and managing
document files. It includes the following components:
- `AssasAstecArchive`: Class for managing ASTEC archives.
- `AssasDatabaseManager`: Class for managing the ASSAS database.
- `AssasDatabaseHandler`: Class for handling database operations.
- `AssasDocumentFile`: Class for managing document files in the ASSAS database.
- `AssasDocumentFileStatus`: Enum for document file statuses.
- `AssasOdessaNetCDF4Converter`: Class for converting ASTEC binary archives to
    netCDF4 format.
- `get_duration`: Utility function to calculate the duration of a process.
"""

from .assas_astec_archive import AssasAstecArchive
from .assas_database_manager import AssasDatabaseManager
from .assas_database_handler import AssasDatabaseHandler
from .assas_document_file import AssasDocumentFileStatus, AssasDocumentFile
from .assas_odessa_netcdf4_converter import AssasOdessaNetCDF4Converter
from .assas_utils import get_duration
from .assas_netcdf4_meta_config import META_DATA_VAR_NAMES
# from .tools import assas_job_generator, assas_single_converter

__all__ = [
    "AssasDatabaseManager",
    "AssasAstecArchive",
    "AssasDatabaseHandler",
    "AssasDocumentFile",
    "AssasDocumentFileStatus",
    "AssasOdessaNetCDF4Converter",
    "get_duration",
    "META_DATA_VAR_NAMES",
    # "assas_job_generator",
    # "assas_single_converter",
]
