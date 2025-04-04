import unittest
import logging
import sys
import os

from assasdb import AssasOdessaNetCDF4Converter

logger = logging.getLogger('assas_test')

logging.basicConfig(
    format = '%(asctime)s %(module)s %(levelname)s: %(message)s',
    level = logging.INFO,
    stream = sys.stdout)

class AssasOdessaNetCDF4ConverterTest(unittest.TestCase):
    
    #def setUp(self):
        
        #self.input_path = '/root/assas-data-hub/assas_database/test/data/archive/LOCA_12P_CL_1300_LIKE.bin'
        #self.output_path = '/root/assas-data-hub/assas_database/test/data/result/loca_12p_cl_1300_like.h5'
        #if os.path.exists(self.output_path):
        #    os.remove(self.output_path)

        #self.odessa_converter = AssasOdessaNetCDF4Converter(
        #    input_path = self.input_path,
        #    output_path = self.output_path,
        #    astec_variable_index_file = 'data/assas_astec_variables_wp2_report.csv'
        #)

    #def tearDown(self):

        #self.odessa_converter = None

    def test_initialize_file(self):

        input_path = '/root/assas-data-hub/assas_database/test/data/archive/LOCA_12P_CL_1300_LIKE.bin'
        output_path = '/root/assas-data-hub/assas_database/test/data/result/set_meta_data_test.h5'
        if os.path.exists(output_path):
            os.remove(output_path)
            
        test_name = 'test'
        test_description = 'test'
        
        AssasOdessaNetCDF4Converter.set_general_meta_data(
            output_path = output_path,
            archive_name = test_name,
            archive_description = test_description, 
        )
        
        self.assertEqual(AssasOdessaNetCDF4Converter.get_general_meta_data(output_path, 'name'), test_name)
        self.assertEqual(AssasOdessaNetCDF4Converter.get_general_meta_data(output_path, 'description'), test_description)

    def test_convert_netcdf4(self):

        input_path = '/root/assas-data-hub/assas_database/test/data/archive/LOCA_12P_CL_1300_LIKE.bin'
        output_path = '/root/assas-data-hub/assas_database/test/data/result/loca_12p_cl_1300_like.h5'
        if os.path.exists(output_path):
            os.remove(output_path)

        odessa_converter = AssasOdessaNetCDF4Converter(
            input_path = input_path,
            output_path = output_path,
        )

        odessa_converter.convert_astec_variables_to_netcdf4()

        variable_index = odessa_converter.get_variable_index()
        meta_data_list = odessa_converter.read_meta_values_from_netcdf4(output_path)

        variables_from_meta_data = [meta_data['name'] for meta_data in meta_data_list]
        variables_from_meta_data.remove('time_points')
        variables_from_index = variable_index['name'].tolist()

        self.assertEqual(set(variables_from_meta_data), set(variables_from_index))

    def test_on_lsdf(self):
        
        input_path = '/mnt/ASSAS/upload_test/f626e095-08dc-4154-82c9-22db4ca0e21b/STUDY/TRANSIENT/BASE_SIMPLIFIED/SBO/SBO_feedbleed/SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS_FILT.bin'
        output_path = '/mnt/ASSAS/upload_test/f626e095-08dc-4154-82c9-22db4ca0e21b/STUDY/TRANSIENT/BASE_SIMPLIFIED/SBO/SBO_feedbleed/SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS_FILT.h5'
        if os.path.exists(output_path):
            os.remove(output_path)

        odessa_converter = AssasOdessaNetCDF4Converter(
            input_path = input_path,
            output_path = output_path,
        )
        
        odessa_converter.convert_astec_variables_to_netcdf4(
            explicit_times = [0, 2]
        )

        variable_index = odessa_converter.get_variable_index()
        meta_data_list = odessa_converter.read_meta_values_from_netcdf4(output_path)

        variables_from_meta_data = [meta_data['name'] for meta_data in meta_data_list]
        variables_from_meta_data.remove('time_points')
        variables_from_index = variable_index['name'].tolist()

        self.assertEqual(set(variables_from_meta_data), set(variables_from_index))

    def test_on_demo_archive(self):
        
        input_path = '/root/assas-data-hub/assas_database/test/data/archive/LOCA_12P_CL_1300_LIKE.bin'
        output_path = '/root/assas-data-hub/assas_database/test/data/result/loca_12p_cl_1300_like.h5'
        if os.path.exists(output_path):
            os.remove(output_path)

        odessa_converter = AssasOdessaNetCDF4Converter(
            input_path = input_path,
            output_path = output_path,
        )
        
        odessa_converter.convert_astec_variables_to_netcdf4(
            explicit_times = [0, 2]
        )

        variable_index = odessa_converter.get_variable_index()
        meta_data_list = odessa_converter.read_meta_values_from_netcdf4(output_path)

        variables_from_meta_data = [meta_data['name'] for meta_data in meta_data_list]
        variables_from_meta_data.remove('time_points')
        variables_from_index = variable_index['name'].tolist()

        self.assertEqual(set(variables_from_meta_data), set(variables_from_index))
    
    def test_get_meta_info(self):
        
        output_path = '/root/assas-data-hub/assas_database/test/data/result/loca_12p_cl_1300_like.h5'
        result = AssasOdessaNetCDF4Converter.read_meta_values_from_netcdf4(
            netcdf4_file = output_path
        )
        print(f'result {result}')
        
    def test_check_if_odessa_path_exists(self):
        
        input_path = '/root/assas-data-hub/assas_database/test/data/archive/LOCA_12P_CL_1300_LIKE.bin'
        output_path = '/root/assas-data-hub/assas_database/test/data/result/loca_12p_cl_1300_like.h5'

        odessa_converter = AssasOdessaNetCDF4Converter(
            input_path = input_path,
            output_path = output_path,
        )

        test_base = odessa_converter.get_odessa_base_from_index(0)
        
        self.assertTrue(AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            test_base,
            "VESSEL 1: MESH 74: THER 1: P 1",
        ))
        self.assertFalse(AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            test_base,
            "VESSEL 1: MESH 74: THER 1: NOTEXISITNG 1",
        ))
        self.assertFalse(AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
            test_base,
            "NOTEXISITNG 1: MESH 74: THER 1: P 1",
        ))
        
        
        
    


if __name__ == '__main__':
    unittest.main()