import unittest
import logging

logger = logging.getLogger('assas_app')

from assasdb import AssasOdessaNetCDF4Converter

class AssasOdessaNetCDF4ConverterTest(unittest.TestCase):
    
    def setUp(self):
        
        self.odessa_converter = AssasOdessaNetCDF4Converter(
            input_path = '/root/assas-data-hub/assas_database/test/data/archive/LOCA_12P_CL_1300_LIKE.bin',
            output_path = '/root/assas-data-hub/assas_database/test/data/result/loca_12p_cl_1300_like.h5'
        )

    def tearDown(self):
        
        self.odessa_converter = None

    def test_convert_netcdf4(self):
        
        self.odessa_converter.convert_astec_variables_to_netcdf4()


if __name__ == '__main__':
    unittest.main()