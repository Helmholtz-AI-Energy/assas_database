import unittest
import logging
import sys
import os

from pathlib import Path

from assasdb import AssasOdessaNetCDF4Converter

logger = logging.getLogger("assas_app")

logging.basicConfig(
    format="%(asctime)s %(module)s %(levelname)s: %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)


class AssasOdessaNetCDF4ConverterTest(unittest.TestCase):
    def test_initialize_file(self):
        output_path = (
            "/root/assas-data-hub/assas_database/test/data/result/set_meta_data_test.h5"
        )
        if os.path.exists(output_path):
            os.remove(output_path)

        test_name = "test"
        test_description = "test"

        AssasOdessaNetCDF4Converter.set_general_meta_data(
            output_path=output_path,
            archive_name=test_name,
            archive_description=test_description,
        )

        self.assertEqual(
            AssasOdessaNetCDF4Converter.get_general_meta_data(output_path, "name"),
            test_name,
        )
        self.assertEqual(
            AssasOdessaNetCDF4Converter.get_general_meta_data(
                output_path, "description"
            ),
            test_description,
        )

    def test_initialize_file_on_assas_data_hub_with_remote_data(self):
        output_path = "/mnt/ASSAS/upload_test/32118491-31c1-47fa-870f-1f750f6cc8ea/STUD"
        output_path += "Y/TRANSIENT/BASE_SIMPLIFIED/SBO/SBO_feedbleed/result/dataset.h5"
        if os.path.exists(output_path):
            os.remove(output_path)

        test_name = "SBO_KIT_init_sim_s83"
        test_description = "SBO_KIT_init_sim_s83"

        AssasOdessaNetCDF4Converter.set_general_meta_data(
            output_path=output_path,
            archive_name=test_name,
            archive_description=test_description,
        )

        self.assertEqual(
            AssasOdessaNetCDF4Converter.get_general_meta_data(output_path, "name"),
            test_name,
        )
        self.assertEqual(
            AssasOdessaNetCDF4Converter.get_general_meta_data(
                output_path, "description"
            ),
            test_description,
        )

    def test_convert_netcdf4_with_local_data(self):
        current_dir = Path(__file__).parent
        input_path = current_dir / "data/archive/LOCA_12P_CL_1300_LIKE.bin"
        input_path = str(input_path.resolve())
        output_path = current_dir / "data/result/loca_12p_cl_1300_like.h5"
        output_path = str(output_path.resolve())

        if os.path.exists(output_path):
            os.remove(output_path)

        odessa_converter = AssasOdessaNetCDF4Converter(
            input_path=input_path,
            output_path=output_path,
        )

        odessa_converter.convert_astec_variables_to_netcdf4()

        variable_index = odessa_converter.get_variable_index()
        meta_data_list = odessa_converter.read_meta_values_from_netcdf4(output_path)

        variables_from_meta_data = [meta_data["name"] for meta_data in meta_data_list]
        variables_from_meta_data.remove("time_points")
        variables_from_index = variable_index["name"].tolist()

        self.assertEqual(set(variables_from_meta_data), set(variables_from_index))

    def test_convert_netcdf4_on_assas_data_hub_with_remote_data(self):
        input_path = (
            "/mnt/ASSAS/upload_test/32118491-31c1-47fa-870f-1f750f6cc8ea/STUDY/"
            "TRANSIENT/BASE_SIMPLIFIED/SBO/SBO_feedbleed/"
            "SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS_FILT.bin"
        )
        output_path = (
            "/mnt/ASSAS/upload_test/32118491-31c1-47fa-870f-1f750f6cc8ea/STUDY/"
            "TRANSIENT/BASE_SIMPLIFIED/SBO/SBO_feedbleed/result/dataset.h5"
        )

        if os.path.exists(output_path):
            os.remove(output_path)

        test_name = "SBO_KIT_init_sim_s83"
        test_description = "SBO_KIT_init_sim_s83"

        AssasOdessaNetCDF4Converter.set_general_meta_data(
            output_path=output_path,
            archive_name=test_name,
            archive_description=test_description,
        )

        self.assertEqual(
            AssasOdessaNetCDF4Converter.get_general_meta_data(output_path, "name"),
            test_name,
        )
        self.assertEqual(
            AssasOdessaNetCDF4Converter.get_general_meta_data(
                output_path, "description"
            ),
            test_description,
        )

        odessa_converter = AssasOdessaNetCDF4Converter(
            input_path=input_path,
            output_path=output_path,
        )

        odessa_converter.convert_astec_variables_to_netcdf4(maximum_index=5)

        variable_index = odessa_converter.get_variable_index()
        meta_data_list = odessa_converter.read_meta_values_from_netcdf4(output_path)

        variables_from_meta_data = [meta_data["name"] for meta_data in meta_data_list]
        variables_from_meta_data.remove("time_points")
        variables_from_index = variable_index["name"].tolist()

        self.assertEqual(set(variables_from_meta_data), set(variables_from_index))

    def test_convert_netcdf4_on_horeka_with_remote_data(self):
        input_path = (
            "/lsdf/kit/scc/projects/ASSAS/upload_test/"
            "32118491-31c1-47fa-870f-1f750f6cc8ea/STUDY/"
            "TRANSIENT/BASE_SIMPLIFIED/SBO/SBO_feedbleed/"
            "SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS_FILT.bin"
        )
        output_path = (
            "/lsdf/kit/scc/projects/ASSAS/upload_test/"
            "32118491-31c1-47fa-870f-1f750f6cc8ea/STUDY/"
            "TRANSIENT/BASE_SIMPLIFIED/SBO/SBO_feedbleed/result/dataset.h5"
        )

        if os.path.exists(output_path):
            os.remove(output_path)

        test_name = "SBO_KIT_init_sim_s83"
        test_description = "SBO_KIT_init_sim_s83"

        AssasOdessaNetCDF4Converter.set_general_meta_data(
            output_path=output_path,
            archive_name=test_name,
            archive_description=test_description,
        )

        self.assertEqual(
            AssasOdessaNetCDF4Converter.get_general_meta_data(output_path, "name"),
            test_name,
        )
        self.assertEqual(
            AssasOdessaNetCDF4Converter.get_general_meta_data(
                output_path, "description"
            ),
            test_description,
        )

        odessa_converter = AssasOdessaNetCDF4Converter(
            input_path=input_path,
            output_path=output_path,
        )

        odessa_converter.convert_astec_variables_to_netcdf4(maximum_index=5)

        variable_index = odessa_converter.get_variable_index()
        meta_data_list = odessa_converter.read_meta_values_from_netcdf4(output_path)

        variables_from_meta_data = [meta_data["name"] for meta_data in meta_data_list]
        variables_from_meta_data.remove("time_points")
        variables_from_index = variable_index["name"].tolist()

        self.assertEqual(set(variables_from_meta_data), set(variables_from_index))

    def test_on_demo_archive(self):
        input_path = (
            "/mnt/ASSAS/upload_test/0c65e12b-a75b-486b-b3ff-cc68fc89b78a/"
            "STUDY/TRANSIENT/BASE_SIMPLIFIED/SBO/SBO_feedbleed/"
            "SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS.bin"
        )
        output_path = (
            "/mnt/ASSAS/upload_test/0c65e12b-a75b-486b-b3ff-cc68fc89b78a/"
            "STUDY/TRANSIENT/BASE_SIMPLIFIED/SBO/SBO_feedbleed/result/dataset.h5"
        )

        if os.path.exists(output_path):
            os.remove(output_path)

        odessa_converter = AssasOdessaNetCDF4Converter(
            input_path=input_path,
            output_path=output_path,
        )

        odessa_converter.convert_astec_variables_to_netcdf4(explicit_times=[0, 2])

        variable_index = odessa_converter.get_variable_index()
        meta_data_list = odessa_converter.read_meta_values_from_netcdf4(output_path)

        variables_from_meta_data = [meta_data["name"] for meta_data in meta_data_list]
        variables_from_meta_data.remove("time_points")
        variables_from_index = variable_index["name"].tolist()

        self.assertEqual(set(variables_from_meta_data), set(variables_from_index))

    def test_get_meta_info(self):
        current_dir = Path(__file__).parent
        output_path = current_dir / "data/result/loca_12p_cl_1300_like.h5"
        output_path = str(output_path.resolve())

        result = AssasOdessaNetCDF4Converter.read_meta_values_from_netcdf4(
            netcdf4_file=output_path
        )
        print(f"result {result}")

    def test_check_if_odessa_path_exists(self):
        input_path = (
            "/mnt/ASSAS/upload_test/0c65e12b-a75b-486b-b3ff-cc68fc89b78a/"
            "STUDY/TRANSIENT/BASE_SIMPLIFIED/SBO/SBO_feedbleed/"
            "SBO_fb_1300_LIKE_SIMPLIFIED_ASSAS.bin"
        )
        output_path = (
            "/mnt/ASSAS/upload_test/0c65e12b-a75b-486b-b3ff-cc68fc89b78a/"
            "STUDY/TRANSIENT/BASE_SIMPLIFIED/SBO/SBO_feedbleed/result/dataset.h5"
        )

        odessa_converter = AssasOdessaNetCDF4Converter(
            input_path=input_path,
            output_path=output_path,
        )

        test_base = odessa_converter.get_odessa_base_from_index(0)

        self.assertTrue(
            AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                test_base,
                "VESSEL 1: MESH 74: THER 1: P 1",
            )
        )
        self.assertFalse(
            AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                test_base,
                "VESSEL 1: MESH 74: THER 1: NOTEXISITNG 1",
            )
        )
        self.assertFalse(
            AssasOdessaNetCDF4Converter.check_if_odessa_path_exists(
                test_base,
                "NOTEXISITNG 1: MESH 74: THER 1: P 1",
            )
        )


if __name__ == "__main__":
    unittest.main()
