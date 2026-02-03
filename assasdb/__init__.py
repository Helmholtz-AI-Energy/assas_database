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

from pathlib import Path
from dotenv import load_dotenv

base_dir = Path(__file__).resolve().parent.parent
env_path = base_dir / ".env"
if env_path.exists():
    load_dotenv(env_path, override=False)

# Need to import after loading .env to ensure any env vars are available
from .assas_astec_archive import AssasAstecArchive  # noqa: E402
from .assas_database_manager import AssasDatabaseManager  # noqa: E402
from .assas_database_handler import AssasDatabaseHandler  # noqa: E402
from .assas_document_file import (  # noqa: E402
    AssasDocumentFileStatus,  # noqa: E402
    AssasDocumentFile,  # noqa: E402
)  # noqa: E402
from .assas_odessa_netcdf4_converter import AssasOdessaNetCDF4Converter  # noqa: E402
from .assas_utils import get_duration  # noqa: E402
from .assas_netcdf4_meta_config import (  # noqa: E402
    META_DATA_VAR_NAMES,  # noqa: E402
    DOMAIN_GROUP_CONFIG,  # noqa: E402
)  # noqa: E402
from .assas_unit_manager import AssasUnitManager  # noqa: E402
from .assas_netcdf4_variable_handler import AssasNetCDF4VariableHandler  # noqa: E402
from .assas_mongodb_backup_handler import AssasMongodbBackupHandler  # noqa: E402

__all__ = [
    "AssasDatabaseManager",
    "AssasAstecArchive",
    "AssasDatabaseHandler",
    "AssasDocumentFile",
    "AssasDocumentFileStatus",
    "AssasOdessaNetCDF4Converter",
    "get_duration",
    "META_DATA_VAR_NAMES",
    "DOMAIN_GROUP_CONFIG",
    "AssasUnitManager",
    "AssasNetCDF4VariableHandler",
    "AssasMongodbBackupHandler",
]
