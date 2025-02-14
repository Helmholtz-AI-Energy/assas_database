from setuptools import setup

setup(
      name='assasdb',
      version='0.1',
      description='python module to access the ASSAS database',
      url='https://github.com/ke4920/assas_database',
      author='Jonas Dressner',
      author_email='jonas.dressner@kit.edu',
      license='MIT',
      packages = ['assasdb'],
      zip_safe = False,
      include_package_data = True,
      package_data = {'assasdb': ['data/*.csv']},
)