# ASSAS database

ASSAS database (assasdb) is a Python package designed to manage and process large-scale
SLURM job submissions, database handling, and data conversion tasks. It provides
utilities for managing SLURM jobs, converting NetCDF4 files, and interacting with
MongoDB database used for the data handling of the flask application.

---

## Features

- **SLURM Job Management**:
  - Submit, cancel, and monitor SLURM jobs.
  - Generate SLURM job scripts dynamically.
  
- **Database Handling**:
  - Manage MongoDB database.
  - Query and process database entries efficiently.

- **NetCDF4 Conversion**:
  - Convert NetCDF4 files to HDF5 format.
  - Handle large-scale scientific data processing.

- **Utilities**:
  - Tools for logging, environment variable management, and more.

---

## Installation

### Prerequisites

- Python 3.11 or higher
- MongoDB (optional, for database-related features)
- SLURM (optional, for job submission features)

### Install via `pip`

```bash
git clone https://github.com/ke4920/assas_database
cd assas_database
pip install .

