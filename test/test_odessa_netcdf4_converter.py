import unittest
import logging
import sys

logger = logging.getLogger('assas_app')

from assasdb import AssasOdessaNetCDF4Converter

logger = logging.getLogger('assas_test')

logging.basicConfig(
    format = '%(asctime)s %(module)s %(levelname)s: %(message)s',
    level = logging.DEBUG,
    stream = sys.stdout)

class AssasOdessaNetCDF4ConverterTest(unittest.TestCase):
    
    def setUp(self):
        
        self.input_path = '/root/assas-data-hub/assas_database/test/data/archive/LOCA_12P_CL_1300_LIKE.bin'
        self.output_path = '/root/assas-data-hub/assas_database/test/data/result/loca_12p_cl_1300_like.h5'
        
        self.odessa_converter = AssasOdessaNetCDF4Converter(
            input_path = self.input_path,
            output_path = self.output_path,
        )

    def tearDown(self):
        
        self.odessa_converter = None

    def test_initialize_file(self):
        
        self.odessa_converter.set_general_meta_data(
            output_path = self.output_path,
            archive_name = 'test',
            archive_description = 'test', 
        )
    
    def test_convert_netcdf4(self):
        
        self.odessa_converter.convert_astec_variables_to_netcdf4()
        
    def test_get_meta_info(self):
        
        result = AssasOdessaNetCDF4Converter.read_meta_values_from_netcdf4(
            netcdf4_file = self.output_path
        )
        print(f'result {result}')


if __name__ == '__main__':
    unittest.main()