"""Test suite for AssasOdessaNetCDF4Converter class.

This module tests the conversion of ASTEC archives to NetCDF4 format using
the AssasOdessaNetCDF4Converter class.
"""

import os
import unittest
import logging
import shutil
import tempfile
import HtmlTestRunner

from pathlib import Path
from logging.handlers import RotatingFileHandler
import netCDF4

from assasdb import (
    AssasOdessaNetCDF4Converter,
    META_DATA_VAR_NAMES,
    DOMAIN_GROUP_CONFIG,
)

# Configure rotating file logging
log_dir = Path(__file__).parent / "log"
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / (Path(__file__).stem + ".log")
log_handler = RotatingFileHandler(
    log_file,
    maxBytes=1024 * 1024,
    backupCount=3,  # 1MB per file, 3 backups
)
log_format = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
log_handler.setFormatter(log_format)

# Create test-specific logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Add handlers if not already present
if not logger.handlers:
    logger.addHandler(log_handler)
    # Also add console handler for immediate feedback during tests
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

# Configure root logging for other modules
logging.basicConfig(
    level=logging.INFO,
    handlers=[log_handler, logging.StreamHandler()],
    force=True,
)


class AssasOdessaNetCDF4ConverterTest(unittest.TestCase):
    """Test suite for AssasOdessaNetCDF4Converter class.

    This class tests the conversion of ASTEC archives to NetCDF4 format.
    """

    @classmethod
    def setUpClass(cls: "type[AssasOdessaNetCDF4ConverterTest]") -> None:
        """Set up class-level test environment."""
        cls.logger = logging.getLogger(f"{__name__}.{cls.__name__}")
        cls.logger.info("=" * 80)
        cls.logger.info(f"Starting test suite: {cls.__name__}")
        cls.logger.info("=" * 80)

    @classmethod
    def tearDownClass(cls: "type[AssasOdessaNetCDF4ConverterTest]") -> None:
        """Clean up class-level test environment."""
        cls.logger.info("=" * 80)
        cls.logger.info(f"Finished test suite: {cls.__name__}")
        cls.logger.info("=" * 80)

    def setUp(self) -> None:
        """Set up the test environment."""
        # Create test-specific logger
        self.test_logger = logging.getLogger(f"{__name__}.{self._testMethodName}")
        self.test_logger.info(f"Starting test: {self._testMethodName}")

        # Path to the test ASTEC archive
        test_path = Path(__file__).parent
        if not test_path.exists():
            self.test_logger.error(f"Test path does not exist: {test_path}")
            self.fail(f"Test path does not exist: {test_path}")

        self.test_archive_path = (
            test_path / "test_data" / "archive" / "LOCA_12P_CL_1300_LIKE.bin"
        )

        # Ensure the test archive exists
        if not self.test_archive_path.exists():
            self.test_logger.error(f"Test archive not found: {self.test_archive_path}")
            self.fail(f"Test archive not found: {self.test_archive_path}")

        # Create temporary directories for input and output
        self.fake_tmp_dir = tempfile.mkdtemp()
        self.fake_output_path = Path(self.fake_tmp_dir) / "output.nc"

        self.test_logger.info(f"Created temporary directory: {self.fake_tmp_dir}")
        self.test_logger.info(f"Output path: {self.fake_output_path}")

        # Copy the test archive to the temporary directory
        self.fake_input_path = Path(self.fake_tmp_dir) / "test_archive.bin"
        shutil.copytree(self.test_archive_path, self.fake_input_path)
        self.test_logger.info(f"Copied test archive to: {self.fake_input_path}")

        # Initialize the AssasOdessaNetCDF4Converter
        self.converter = AssasOdessaNetCDF4Converter(
            input_path=self.fake_input_path,
            output_path=self.fake_output_path,
        )
        self.test_logger.info("Initialized AssasOdessaNetCDF4Converter")

    def tearDown(self) -> None:
        """Clean up temporary directories and files."""
        try:
            shutil.rmtree(self.fake_tmp_dir, ignore_errors=True)
            self.test_logger.info(
                f"Cleaned up temporary directory: {self.fake_tmp_dir}"
            )
        except Exception as e:
            self.test_logger.warning(f"Failed to clean up temporary directory: {e}")

        self.test_logger.info(f"Finished test: {self._testMethodName}")
        self.test_logger.info("-" * 60)

    def test_convert_astec_archive(self) -> None:
        """Test converting the ASTEC archive to NetCDF4 format."""
        self.test_logger.info("Testing basic ASTEC archive conversion")

        # Ensure the input file exists
        self.assertTrue(
            self.fake_input_path.exists(), "Input archive file does not exist."
        )
        self.test_logger.info("Input file verification passed")

        # Call the conversion method
        try:
            self.test_logger.info("Starting conversion...")
            self.converter.convert_astec_variables_to_netcdf4()
            self.test_logger.info("Conversion completed successfully")
        except Exception as e:
            self.test_logger.error(f"Conversion failed: {e}")
            self.fail(f"Conversion failed with exception: {e}.")

        # Verify that the output file is created
        self.assertTrue(
            self.fake_output_path.exists(), "Output NetCDF4 file was not created."
        )
        self.test_logger.info("Output file creation verified")

        # Additional checks can be added here to verify the content of the output file
        variable_index = self.converter.get_variable_index()
        meta_data_list = self.converter.read_meta_values_from_netcdf4(
            self.fake_output_path
        )

        variables_from_meta_data = [meta_data["name"] for meta_data in meta_data_list]
        variables_from_meta_data.remove("time_points")
        variables_from_index = variable_index["name"].tolist()

        self.assertEqual(set(variables_from_meta_data), set(variables_from_index))
        self.test_logger.info(
            f"Variable verification passed: {len(variables_from_index)} variables"
        )

    def test_convert_astec_archive_with_groups(self) -> None:
        """Test converting the ASTEC archive with groups to NetCDF4 format."""
        self.test_logger.info("Testing ASTEC archive conversion with enhanced groups")

        # Ensure the input file exists
        self.assertTrue(
            self.fake_input_path.exists(), "Input archive file does not exist."
        )
        self.test_logger.info("Input file verification passed")

        # Call the conversion method
        try:
            self.test_logger.info("Step 1: Initialize groups in netCDF4")
            self.converter.initialize_groups_in_netcdf4()

            self.test_logger.info(
                "Step 2: Initialize data variables with units "
                "and enhanced group assignment"
            )
            self.converter.intialize_astec_variables_in_netcdf4()

            self.test_logger.info(
                "Step 3: Create metadata variables in designated groups"
            )
            self.converter.create_metadata_variables_in_groups()

            self.test_logger.info("Step 4: Convert metadata from Odessa")
            self.converter.convert_meta_data_from_odessa_to_netcdf4()

            self.test_logger.info(
                "Step 5: Assign variables to enhanced group structure"
            )
            self.converter.assign_variables_to_enhanced_groups()

            self.test_logger.info("Step 6: Convert data")
            self.converter.populate_data_from_groups_to_netcdf4()

            self.test_logger.info("All conversion steps completed successfully")
        except Exception as e:
            self.test_logger.error(f"Conversion with groups failed: {e}")
            self.fail(f"Conversion with groups failed with exception: {e}.")

        # Verify that the output file is created
        self.assertTrue(
            self.fake_output_path.exists(), "Output NetCDF4 file was not created."
        )
        self.test_logger.info("Output file creation verified")

        test_file_location = os.path.dirname(os.path.abspath(__file__))
        copied_file_path = os.path.join(test_file_location, "copied_output.nc")
        try:
            shutil.copy(self.fake_output_path, copied_file_path)
            self.assertTrue(
                os.path.exists(copied_file_path),
                f"Failed to copy the output file to {copied_file_path}.",
            )
            self.test_logger.info(f"Output file copied to: {copied_file_path}")
        except Exception as e:
            self.test_logger.error(f"Failed to copy output file: {e}")
            self.fail(f"Failed to copy the output file with exception: {e}.")

    def test_convert_astec_archive_meta(self) -> None:
        """Test converting the ASTEC archive metadata to NetCDF4 format."""
        self.test_logger.info("Testing ASTEC archive metadata conversion")

        # Ensure the input file exists
        self.assertTrue(
            self.fake_input_path.exists(), "Input archive file does not exist."
        )
        self.test_logger.info("Input file verification passed")

        expected_meta_data_var_names = META_DATA_VAR_NAMES
        self.test_logger.info(
            f"Expected {len(expected_meta_data_var_names)} metadata variables"
        )

        # Call the conversion method
        try:
            self.test_logger.info("Starting metadata conversion...")
            self.converter.convert_meta_data_from_odessa_to_netcdf4()
            self.test_logger.info("Metadata conversion completed successfully")
        except Exception as e:
            self.test_logger.error(f"Metadata conversion failed: {e}")
            self.fail(f"Conversion of meta data failed with exception: {e}.")

        # Verify that the output file is created
        self.assertTrue(
            self.fake_output_path.exists(), "Output NetCDF4 file was not created."
        )
        self.test_logger.info("Output file creation verified")

        test_file_location = os.path.dirname(os.path.abspath(__file__))
        copied_file_path = os.path.join(test_file_location, "copied_meta_output.nc")
        try:
            shutil.copy(self.fake_output_path, copied_file_path)
            self.assertTrue(
                os.path.exists(copied_file_path),
                f"Failed to copy the output file to {copied_file_path}.",
            )
            self.test_logger.info(f"Metadata output file copied to: {copied_file_path}")
        except Exception as e:
            self.test_logger.error(f"Failed to copy metadata output file: {e}")
            self.fail(f"Failed to copy the output file with exception: {e}.")

        meta_data_list = self.converter.read_meta_values_from_netcdf4(
            self.fake_output_path
        )
        variables_from_meta_data = [meta_data["name"] for meta_data in meta_data_list]

        self.assertEqual(
            set(variables_from_meta_data), set(expected_meta_data_var_names.keys())
        )
        self.test_logger.info(
            f"Metadata variable verification passed: "
            f"{len(variables_from_meta_data)} variables."
        )

        for meta_data in meta_data_list:
            var_name = meta_data["name"]
            expected_meta = expected_meta_data_var_names.get(var_name)
            if expected_meta:
                domain_value = expected_meta_data_var_names[var_name]["domain"]
                if domain_value is None:
                    domain_value = "None"
                self.assertEqual(meta_data["domain"], domain_value)

        var_names = list(expected_meta_data_var_names.keys())
        meta_data = self.converter.read_meta_data_from_netcdf4(var_names[0])
        self.assertIsNotNone(meta_data, "Meta data should not be None.")
        self.test_logger.info("Individual metadata reading verification passed")

    def test_migrate_variables_from_old_to_new_structure(self) -> None:
        """Test to migrating variables from old structure without groups.

        Test migrating variables from old structure without groups to new
        enhanced group structure.
        """
        self.test_logger.info(
            "Testing migration from old structure to new enhanced group structure"
        )

        # Ensure the input file exists
        self.assertTrue(
            self.fake_input_path.exists(), "Input archive file does not exist."
        )
        self.test_logger.info("Input file verification passed")

        # Step 1: Create the old structure (without groups) first
        try:
            self.test_logger.info("Step 1: Creating old structure (without groups)")
            # Create old structure with variables at root level
            self.converter.convert_astec_variables_to_netcdf4()

            # Verify old structure exists
            self.assertTrue(
                self.fake_output_path.exists(),
                "Old structure NetCDF4 file was not created.",
            )

            # Verify variables are at root level in old structure
            with netCDF4.Dataset(self.fake_output_path, "r") as old_ncfile:
                old_variables = list(old_ncfile.variables.keys())
                old_groups = list(old_ncfile.groups.keys())

                # Should have variables at root and no groups (except maybe time_points)
                self.assertGreater(
                    len(old_variables), 1, "Should have variables in old structure"
                )
                self.assertEqual(
                    len(old_groups), 0, "Old structure should have no groups"
                )

                self.test_logger.info(
                    f"Old structure has {len(old_variables)} variables at root level"
                )

            # Save old structure for comparison
            old_structure_path = Path(self.fake_tmp_dir) / "old_structure.nc"
            shutil.copy(self.fake_output_path, old_structure_path)
            self.test_logger.info(
                f"Old structure saved for comparison: {old_structure_path}"
            )

        except Exception as e:
            self.test_logger.error(f"Failed to create old structure: {e}")
            self.fail(f"Failed to create old structure: {e}")

        # Step 2: Migrate to new enhanced group structure
        try:
            self.test_logger.info("Step 2: Migrating to new enhanced group structure")

            # Initialize enhanced group structure
            self.test_logger.info("  - Initializing enhanced group structure")
            self.converter.initialize_groups_in_netcdf4()

            # Create metadata variables in designated groups
            self.test_logger.info(
                "  - Creating metadata variables in designated groups"
            )
            self.converter.create_metadata_variables_in_groups()

            # Convert metadata from Odessa
            self.test_logger.info("  - Converting metadata from Odessa")
            self.converter.convert_meta_data_from_odessa_to_netcdf4()

            # Assign existing variables to enhanced group structure
            self.test_logger.info(
                "  - Assigning existing variables to enhanced group structure"
            )
            self.converter.assign_variables_to_enhanced_groups()

            self.test_logger.info("Successfully migrated to enhanced group structure")

        except Exception as e:
            self.test_logger.error(f"Migration to enhanced structure failed: {e}")
            self.fail(f"Migration to enhanced structure failed: {e}")

        # Step 3: Verify the new enhanced structure
        try:
            self.test_logger.info("Step 3: Verifying new enhanced structure")

            with netCDF4.Dataset(self.fake_output_path, "r") as new_ncfile:
                new_groups = list(new_ncfile.groups.keys())
                new_root_variables = list(new_ncfile.variables.keys())

                # Should have groups now
                self.assertGreater(
                    len(new_groups), 0, "New structure should have groups"
                )
                self.test_logger.info(
                    f"New structure has {len(new_groups)} main groups: {new_groups}"
                )

                # Should have variables at root level
                self.assertGreater(
                    len(new_root_variables),
                    1,
                    "New structure should have variables at root level",
                )

                # Verify specific groups exist based on DOMAIN_GROUP_CONFIG
                expected_groups = [
                    group
                    for group in DOMAIN_GROUP_CONFIG.keys()
                    if DOMAIN_GROUP_CONFIG[group].get("odessa_name") is not None
                ]

                for expected_group in expected_groups:
                    if (
                        expected_group != "global_metadata"
                    ):  # Skip global_metadata for this test
                        self.assertIn(
                            expected_group,
                            new_groups,
                            f"Expected group '{expected_group}' not found",
                        )

                # Verify subgroups exist
                total_subgroups = 0
                variables_in_groups = 0

                for group_name, group in new_ncfile.groups.items():
                    subgroups = list(group.groups.keys())
                    total_subgroups += len(subgroups)
                    self.test_logger.info(
                        f"Group '{group_name}' has {len(subgroups)} "
                        f"subgroups: {subgroups}"
                    )

                    # Count variables in subgroups
                    for subgroup_name, subgroup in group.groups.items():
                        subgroup_variables = list(subgroup.variables.keys())
                        variables_in_groups += len(subgroup_variables)

                        if subgroup_variables:
                            self.test_logger.info(
                                f"Subgroup '{group_name}/{subgroup_name}' "
                                f"has variables: {subgroup_variables}."
                            )

                self.assertGreater(
                    total_subgroups, 0, "Should have subgroups in new structure"
                )
                self.test_logger.info(f"Total subgroups created: {total_subgroups}")
                self.test_logger.info(
                    f"Variables found in groups: {variables_in_groups}"
                )

                # Verify that variables have enhanced group assignments
                variables_with_assignments = 0
                for var_name, var in new_ncfile.variables.items():
                    if hasattr(var, "enhanced_group_assignment"):
                        variables_with_assignments += 1
                        self.test_logger.info(
                            f"Variable '{var_name}' assigned to: "
                            f"{var.enhanced_group_assignment}."
                        )

                        if hasattr(var, "enhanced_full_path"):
                            self.test_logger.info(
                                f"  Full path: {var.enhanced_full_path}"
                            )

                self.test_logger.info(
                    f"Variables with enhanced assignments: {variables_with_assignments}"
                )

        except Exception as e:
            self.test_logger.error(f"Failed to verify new structure: {e}")
            self.fail(f"Failed to verify new structure: {e}")

        # Step 4: Copy files for inspection
        self.test_logger.info("Step 4: Copying files for inspection")
        test_file_location = os.path.dirname(os.path.abspath(__file__))

        # Copy old structure
        old_copied_path = os.path.join(test_file_location, "migration_old_structure.nc")
        try:
            shutil.copy(old_structure_path, old_copied_path)
            self.assertTrue(
                os.path.exists(old_copied_path),
                f"Failed to copy old structure to {old_copied_path}",
            )
            self.test_logger.info(f"Old structure copied to: {old_copied_path}")
        except Exception as e:
            self.test_logger.error(f"Failed to copy old structure file: {e}")
            self.fail(f"Failed to copy old structure file: {e}")

        # Copy new structure
        new_copied_path = os.path.join(test_file_location, "migration_new_structure.nc")
        try:
            shutil.copy(self.fake_output_path, new_copied_path)
            self.assertTrue(
                os.path.exists(new_copied_path),
                f"Failed to copy new structure to {new_copied_path}",
            )
            self.test_logger.info(f"New structure copied to: {new_copied_path}")
        except Exception as e:
            self.test_logger.error(f"Failed to copy new structure file: {e}")
            self.fail(f"Failed to copy new structure file: {e}")

    def test_variable_assignment_to_correct_groups(self) -> None:
        """Test variable assignment to correct groups.

        Test that variables are assigned to the correct groups based on their domain.
        This test assumes that the old structure has been created first.
        """
        self.test_logger.info("Testing variable assignment to correct groups")

        # Ensure the input file exists
        self.assertTrue(
            self.fake_input_path.exists(), "Input archive file does not exist."
        )
        self.test_logger.info("Input file verification passed")

        # Create old structure first
        try:
            self.test_logger.info("Creating old structure as starting point")
            self.converter.convert_astec_variables_to_netcdf4()
        except Exception as e:
            self.test_logger.error(f"Failed to create old structure: {e}")
            self.fail(f"Failed to create old structure: {e}")

        # Get variable information before migration
        variable_index = self.converter.get_variable_index()

        # Create mapping of expected assignments
        expected_assignments = {}
        for _, variable in variable_index.iterrows():
            var_name = variable["name"]
            domain = variable["domain"]
            group_name, subgroup_name = self.converter.get_group_name_from_domain(
                domain
            )

            if group_name:
                expected_path = (
                    f"{group_name}/{subgroup_name}" if subgroup_name else group_name
                )
                expected_assignments[var_name] = {
                    "group": group_name,
                    "subgroup": subgroup_name,
                    "full_path": expected_path,
                    "domain": domain,
                }

        self.test_logger.info(
            f"Expected assignments for {len(expected_assignments)} variables"
        )

        # Migrate to enhanced structure
        try:
            self.test_logger.info("Migrating to enhanced structure")
            self.converter.initialize_groups_in_netcdf4()
            self.converter.create_metadata_variables_in_groups()
            self.converter.convert_meta_data_from_odessa_to_netcdf4()
            self.converter.assign_variables_to_enhanced_groups()
            self.test_logger.info("Migration completed")
        except Exception as e:
            self.test_logger.error(f"Migration failed: {e}")
            self.fail(f"Migration failed: {e}")

        # Verify assignments
        with netCDF4.Dataset(self.fake_output_path, "r") as ncfile:
            correct_assignments = 0
            incorrect_assignments = 0
            missing_assignments = 0

            for var_name in expected_assignments:
                expected = expected_assignments[var_name]

                if var_name in ncfile.variables:
                    var = ncfile.variables[var_name]

                    if hasattr(var, "enhanced_group_assignment"):
                        actual_group = var.enhanced_group_assignment

                        if actual_group == expected["group"]:
                            correct_assignments += 1
                            self.test_logger.info(
                                f"✓ {var_name}: correctly assigned to {actual_group}"
                            )
                        else:
                            incorrect_assignments += 1
                            self.test_logger.warning(
                                f"✗ {var_name}: expected {expected['group']}, "
                                f"got {actual_group}"
                            )
                    else:
                        missing_assignments += 1
                        self.test_logger.warning(
                            f"? {var_name}: no enhanced_group_assignment attribute"
                        )
                else:
                    self.test_logger.error(
                        f"Variable {var_name} not found in NetCDF4 file"
                    )

            self.test_logger.info(
                f"Assignment results: {correct_assignments} correct, "
                f"{incorrect_assignments} incorrect, {missing_assignments} missing"
            )

            # At least 80% should be correctly assigned
            total_checked = (
                correct_assignments + incorrect_assignments + missing_assignments
            )
            if total_checked > 0:
                success_rate = correct_assignments / total_checked
                self.assertGreater(
                    success_rate,
                    0.8,
                    f"Assignment success rate {success_rate:.2%} is too low",
                )
                self.test_logger.info(f"Assignment success rate: {success_rate:.2%}")

    def test_metadata_variable_placement(self) -> None:
        """Test that metadata variables are placed in the correct metadata subgroups.

        This test assumes that the old structure has been created first.
        """
        self.test_logger.info("Testing metadata variable placement")

        # Ensure the input file exists
        self.assertTrue(
            self.fake_input_path.exists(), "Input archive file does not exist."
        )
        self.test_logger.info("Input file verification passed")

        # Create enhanced structure
        try:
            self.test_logger.info("Creating enhanced structure with metadata")
            self.converter.initialize_groups_in_netcdf4()
            self.converter.create_metadata_variables_in_groups()
            self.converter.convert_meta_data_from_odessa_to_netcdf4()
            self.test_logger.info("Enhanced structure with metadata created")
        except Exception as e:
            self.test_logger.error(
                f"Failed to create enhanced structure with metadata: {e}"
            )
            self.fail(f"Failed to create enhanced structure with metadata: {e}")

        # Verify metadata variable placement
        with netCDF4.Dataset(self.fake_output_path, "r") as ncfile:
            metadata_variables_found = {}

            # Recursively search for metadata variables
            def find_metadata_variables(
                location: netCDF4.Dataset,
                path: str = "",
            ) -> None:
                for var_name, var in location.variables.items():
                    if (
                        hasattr(var, "variable_type")
                        and var.variable_type == "metadata"
                    ):
                        metadata_variables_found[var_name] = {
                            "location": path,
                            "target_group": getattr(var, "target_group", "unknown"),
                        }

                for group_name, group in location.groups.items():
                    group_path = f"{path}/{group_name}" if path else group_name
                    find_metadata_variables(group, group_path)

            find_metadata_variables(ncfile)

            self.test_logger.info(
                f"Found {len(metadata_variables_found)} metadata variables"
            )

            # Verify each metadata variable is in correct location
            correct_placements = 0
            for meta_var_name, meta_info in metadata_variables_found.items():
                actual_location = meta_info["location"]
                expected_location = META_DATA_VAR_NAMES.get(meta_var_name, {}).get(
                    "target_group", ""
                )

                if actual_location == expected_location:
                    correct_placements += 1
                    self.test_logger.info(
                        f"✓ {meta_var_name}: correctly placed in {actual_location}"
                    )
                else:
                    self.test_logger.warning(
                        f"✗ {meta_var_name}: expected {expected_location}, "
                        f"found in {actual_location}"
                    )

            # Should have some metadata variables correctly placed
            self.assertGreater(
                correct_placements,
                0,
                "Should have some correctly placed metadata variables",
            )

            if len(metadata_variables_found) > 0:
                placement_rate = correct_placements / len(metadata_variables_found)
                self.test_logger.info(
                    f"Metadata placement success rate: {placement_rate:.2%}"
                )

    def test_group_structure_integrity(self) -> None:
        """Test that the group structure matches the DOMAIN_GROUP_CONFIG."""
        self.test_logger.info("Testing group structure integrity")

        # Ensure the input file exists
        self.assertTrue(
            self.fake_input_path.exists(), "Input archive file does not exist."
        )
        self.test_logger.info("Input file verification passed")

        # Create enhanced structure
        try:
            self.test_logger.info("Creating enhanced structure")
            self.converter.initialize_groups_in_netcdf4()
            self.converter.create_metadata_variables_in_groups()
            self.test_logger.info("Enhanced structure created")
        except Exception as e:
            self.test_logger.error(f"Failed to create enhanced structure: {e}")
            self.fail(f"Failed to create enhanced structure: {e}")

        # Verify group structure integrity
        with netCDF4.Dataset(self.fake_output_path, "r") as ncfile:
            # Check main groups
            for group_name, config in DOMAIN_GROUP_CONFIG.items():
                # Skip global_metadata for this test
                if config.get("odessa_name") is None:
                    continue

                self.assertIn(
                    group_name, ncfile.groups, f"Main group '{group_name}' should exist"
                )

                main_group = ncfile.groups[group_name]

                # Verify group attributes
                self.assertEqual(
                    main_group.description,
                    config["description"],
                    f"Group description mismatch for {group_name}",
                )

                if config.get("odessa_name"):
                    self.assertEqual(
                        main_group.odessa_name,
                        config["odessa_name"],
                        f"Odessa name mismatch for {group_name}",
                    )

                # Check subgroups
                if "subgroups" in config:
                    for subgroup_name, subgroup_config in config["subgroups"].items():
                        self.assertIn(
                            subgroup_name,
                            main_group.groups,
                            f"Subgroup '{group_name}/{subgroup_name}' should exist",
                        )

                        subgroup = main_group.groups[subgroup_name]

                        # Verify subgroup attributes
                        self.assertEqual(
                            subgroup.description,
                            subgroup_config["description"],
                            f"Subgroup description mismatch for "
                            f"{group_name}/{subgroup_name}",
                        )

                        if "domains" in subgroup_config:
                            self.assertTrue(
                                hasattr(subgroup, "applicable_domains"),
                                f"Subgroup {group_name}/{subgroup_name} should have "
                                f"applicable_domains",
                            )

                        if "metadata_vars" in subgroup_config:
                            self.assertTrue(
                                hasattr(subgroup, "metadata_variables"),
                                f"Subgroup {group_name}/{subgroup_name} should "
                                f"have metadata_variables",
                            )

            self.test_logger.info("Group structure integrity verified successfully")

    def test_comprehensive_migration_workflow(self) -> None:
        """Test the complete migration workflow from old to new structure."""
        self.test_logger.info("Testing comprehensive migration workflow")

        # Ensure the input file exists
        self.assertTrue(
            self.fake_input_path.exists(), "Input archive file does not exist."
        )
        self.test_logger.info("Input file verification passed")

        workflow_steps = [
            (
                "Create old structure",
                lambda: self.converter.convert_astec_variables_to_netcdf4(),
            ),
            (
                "Initialize groups",
                lambda: self.converter.initialize_groups_in_netcdf4(),
            ),
            (
                "Initialize variables",
                lambda: self.converter.intialize_astec_variables_in_netcdf4(),
            ),
            (
                "Create metadata variables",
                lambda: self.converter.create_metadata_variables_in_groups(),
            ),
            (
                "Convert metadata",
                lambda: self.converter.convert_meta_data_from_odessa_to_netcdf4(),
            ),
            (
                "Assign to groups",
                lambda: self.converter.assign_variables_to_enhanced_groups(),
            ),
            (
                "Populate data",
                lambda: self.converter.populate_data_from_groups_to_netcdf4(),
            ),
        ]

        completed_steps = []

        for step_name, step_function in workflow_steps:
            try:
                self.test_logger.info(f"Executing workflow step: {step_name}")
                step_function()
                completed_steps.append(step_name)
                self.test_logger.info(f"✓ Completed: {step_name}")

                # Verify file exists after each step
                self.assertTrue(
                    self.fake_output_path.exists(),
                    f"Output file should exist after '{step_name}'",
                )

            except Exception as e:
                self.test_logger.error(f"Workflow failed at step '{step_name}': {e}")
                self.fail(f"Workflow failed at step '{step_name}': {e}")

        # Verify all steps completed
        self.assertEqual(
            len(completed_steps),
            len(workflow_steps),
            f"Expected {len(workflow_steps)} steps, completed {len(completed_steps)}",
        )

        # Final verification
        with netCDF4.Dataset(self.fake_output_path, "r") as ncfile:
            # Should have groups
            groups = list(ncfile.groups.keys())
            self.assertGreater(len(groups), 0, "Should have groups after migration")

            # Should have variables with data
            total_variables = len(ncfile.variables)
            for group_name, group in ncfile.groups.items():
                for subgroup_name, subgroup in group.groups.items():
                    total_variables += len(subgroup.variables)

            self.assertGreater(
                total_variables, 1, "Should have variables after migration"
            )

            # Should have time dimension
            self.assertIn("time", ncfile.dimensions, "Should have time dimension")

            # Should have completed_index attribute if time_points exists
            if "time_points" in ncfile.variables:
                time_var = ncfile.variables["time_points"]
                self.assertTrue(
                    hasattr(time_var, "completed_index"),
                    "time_points should have completed_index attribute",
                )

        self.test_logger.info("Comprehensive migration workflow completed successfully")
        self.test_logger.info(
            f"Final structure has {len(groups)} main groups and "
            f"{total_variables} total variables"
        )

        # Copy final result for inspection
        test_file_location = os.path.dirname(os.path.abspath(__file__))
        final_output_path = os.path.join(
            test_file_location, "comprehensive_migration_result.nc"
        )
        try:
            shutil.copy(self.fake_output_path, final_output_path)
            self.test_logger.info(f"Final result saved to: {final_output_path}")
        except Exception as e:
            self.test_logger.warning(f"Failed to copy final result: {e}")


if __name__ == "__main__":
    unittest.main(
        testRunner=HtmlTestRunner.HTMLTestRunner(
            output="test_reports",  # Directory for HTML reports
            report_title="AssasOdessaNetCDF4Converter Test Report",
            descriptions=True,
        )
    )
