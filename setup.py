"""Setup script to use the ASSAS database Python module as package.

It includes metadata such as the name, version, description, author,
author email, license, and the packages to be included in the distribution.
It also specifies that the package is not zip-safe and includes package data
from the `astec_config/inr` directory.
It uses setuptools to handle the packaging and distribution of the module.
"""

from setuptools import setup

setup(
    name="assasdb",
    version="0.1",
    description="python module to access the ASSAS database",
    url="https://github.com/ke4920/assas_database",
    author="Jonas Dressner",
    author_email="jonas.dressner@kit.edu",
    license="MIT",
    packages=["assasdb", "assasdb.astec_config", "assasdb.astec_config.inr"],
    zip_safe=False,
    include_package_data=True,
    package_data={"assasdb": ["astec_config/inr/*.csv"]},
)
"""
This script is used to package the ASSAS database Python module.
Args:
    name (str): The name of the package.
    version (str): The version of the package.
    description (str): A short description of the package.
    url (str): The URL for the package's repository.
    author (str): The author's name.
    author_email (str): The author's email address.
    license (str): The license under which the package is distributed.
    packages (list): A list of packages to include in the distribution.
    zip_safe (bool): Whether the package can be safely used in a zip file.
    include_package_data (bool): Whether to include additional package data.
    package_data (dict): A dictionary specifying additional data files to
    include in the package.
"""
