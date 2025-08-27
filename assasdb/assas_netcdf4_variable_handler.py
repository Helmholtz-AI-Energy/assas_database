"""AssasNetCDF4VariableHandler class.

This module provides the AssasNetCDF4VariableHandler class for reading and managing
NetCDF4 variables, including support for groups and hierarchical data structures.
"""

import netCDF4 as nc
import numpy as np
import logging
from uuid import uuid4
from pathlib import Path
from typing import List, Dict, Optional, Union, Any, Tuple
from assasdb import AssasDatabaseHandler

logger = logging.getLogger("assas_app")

NumericType = Union[
    int,
    float,
    np.integer,
    np.floating,
    np.number,
    np.ndarray,  # for 0-dimensional arrays
    None,
]


class AssasNetCDF4VariableHandler:
    """Handler for reading and managing variables from NetCDF4 files.

    This class provides methods to read variables from NetCDF4 files with support
    for groups, filtering, and data subsetting. It integrates with the ASSAS database
    system to provide variable data for specific datasets.
    """

    def __init__(self, database_handler: AssasDatabaseHandler = None) -> None:
        """Initialize the NetCDF4 variable handler.

        Args:
            database_handler: Optional database handler for retrieving file paths.

        """
        self.database_handler = database_handler
        logger.info("Initialized AssasNetCDF4VariableHandler")

    def read_variables_from_netcdf4(
        self,
        netcdf4_file_path: str,
        variable_names: List[str],
        time_indices: Optional[List[int]] = None,
        spatial_indices: Optional[Dict[str, Union[slice, List[int]]]] = None,
        groups: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Read data for specified variables from the NetCDF4 file, including groups.

        Args:
            netcdf4_file_path (str): Path to the NetCDF4 file.
            variable_names (List[str]): List of variable names to read.
                Can include group paths like 'group/variable' or just 'variable'.
            time_indices (Optional[List[int]]):
                Specific time indices to read. If None, reads all times.
            spatial_indices (Optional[Dict[str, Union[slice, List[int]]]]):
                Spatial indices for each dimension.
            groups (Optional[List[str]]): Specific groups to search for variables.
                If None, searches root and all groups.

        Returns:
            Dict[str, Any]:
                Dictionary containing the requested variable data and metadata.

        Example:
            >>> handler = AssasNetCDF4VariableHandler()
            >>> data = handler.read_variables_from_netcdf4(
            ...     netcdf4_file_path='/path/to/dataset.h5',
            ...     variable_names=['temperature', 'physics/pressure', 'geometry/mesh'],
            ...     groups=['physics', 'geometry']
            ... )

        """
        try:
            if not Path(netcdf4_file_path).exists():
                raise FileNotFoundError(f"NetCDF4 file not found: {netcdf4_file_path}")

            logger.info(
                f"Reading variables {variable_names} "
                f"from NetCDF4 file: {netcdf4_file_path}"
            )

            result = {
                "file_path": netcdf4_file_path,
                "variables": {},
                "dimensions": {},
                "groups": {},
                "global_attributes": {},
                "time_indices": time_indices,
                "spatial_indices": spatial_indices,
                "requested_groups": groups,
            }

            with nc.Dataset(netcdf4_file_path, "r") as dataset:
                # Read global attributes
                result["global_attributes"] = {
                    attr: dataset.getncattr(attr) for attr in dataset.ncattrs()
                }

                # Read root dimensions
                result["dimensions"]["root"] = {}
                for dim_name, dimension in dataset.dimensions.items():
                    result["dimensions"]["root"][dim_name] = {
                        "size": len(dimension),
                        "unlimited": dimension.isunlimited(),
                    }

                # Read group structure and dimensions
                result["groups"] = self._read_group_structure(dataset)

                # Process each requested variable
                for var_name in variable_names:
                    result["variables"][var_name] = self._read_variable_from_groups(
                        dataset, var_name, time_indices, spatial_indices, groups
                    )

            logger.info(
                f"Successfully read {len(variable_names)} variables from NetCDF4 file"
            )
            return result

        except Exception as e:
            logger.error(f"Error reading variables from NetCDF4 file: {e}")
            raise

    def read_variables_by_uuid(
        self,
        system_uuid: uuid4,
        variable_names: List[str],
        time_indices: Optional[List[int]] = None,
        spatial_indices: Optional[Dict[str, Union[slice, List[int]]]] = None,
        groups: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Read variables from NetCDF4 file using system UUID to get file path.

        Args:
            system_uuid (uuid4): The UUID of the database entry.
            variable_names (List[str]): List of variable names to read.
            time_indices (Optional[List[int]]): Specific time indices to read.
            spatial_indices (Optional[Dict[str, Union[slice, List[int]]]]):
                Spatial indices.
            groups (Optional[List[str]]): Specific groups to search for variables.

        Returns:
            Dict[str, Any]:
                Dictionary containing the requested variable data and metadata.

        """
        if not self.database_handler:
            raise ValueError("Database handler is required for UUID-based operations")

        # Get database document
        document = self.database_handler.get_file_document_by_uuid(system_uuid)
        if not document:
            raise ValueError(f"No document found for UUID {system_uuid}")

        netcdf4_file_path = document.get("system_result")
        if not netcdf4_file_path:
            raise ValueError("No NetCDF4 file path found in document")

        return self.read_variables_from_netcdf4(
            netcdf4_file_path=netcdf4_file_path,
            variable_names=variable_names,
            time_indices=time_indices,
            spatial_indices=spatial_indices,
            groups=groups,
        )

    def explore_group(self, group: nc.Group, path: str = "") -> Dict[str, Any]:
        """Explore a NetCDF4 group and extract its structure."""
        group_info = {
            "path": path,
            "dimensions": {},
            "variables": {},
            "attributes": {},
            "subgroups": {},
        }

        # Read group attributes
        group_info["attributes"] = {
            attr: group.getncattr(attr) for attr in group.ncattrs()
        }

        # Read group dimensions
        for dim_name, dimension in group.dimensions.items():
            group_info["dimensions"][dim_name] = {
                "size": len(dimension),
                "unlimited": dimension.isunlimited(),
            }

        # Read group variables (just metadata, not data)
        for var_name, variable in group.variables.items():
            group_info["variables"][var_name] = {
                "dimensions": variable.dimensions,
                "shape": variable.shape,
                "dtype": str(variable.dtype),
                "attributes": {
                    attr: variable.getncattr(attr) for attr in variable.ncattrs()
                },
            }

        # Recursively explore subgroups
        for subgroup_name, subgroup in group.groups.items():
            subgroup_path = f"{path}/{subgroup_name}" if path else subgroup_name
            group_info["subgroups"][subgroup_name] = self.explore_group(
                subgroup, subgroup_path
            )

        return group_info

    def _read_group_structure(self, dataset: nc.Dataset) -> Dict[str, Any]:
        """Read the complete group structure from the NetCDF4 file.

        Args:
            dataset (nc.Dataset): The NetCDF4 dataset.

        Returns:
            Dict[str, Any]: Dictionary containing group structure information.

        """
        return self.explore_group(dataset)

    def _read_variable_from_groups(
        self,
        dataset: nc.Dataset,
        var_name: str,
        time_indices: Optional[List[int]] = None,
        spatial_indices: Optional[Dict[str, Union[slice, List[int]]]] = None,
        target_groups: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Read a variable from the dataset, searching through groups.

        Args:
            dataset (nc.Dataset): The NetCDF4 dataset.
            var_name (str): Variable name, can include group path like 'group/variable'.
            time_indices (Optional[List[int]]): Time indices to read.
            spatial_indices (Optional[Dict[str, Union[slice, List[int]]]]):
                Spatial indices.
            target_groups (Optional[List[str]]): Groups to search in.

        Returns:
            Dict[str, Any]: Variable data and metadata.

        """
        # Check if variable name includes a group path
        if "/" in var_name:
            group_path, actual_var_name = var_name.rsplit("/", 1)
            try:
                group = dataset[group_path]
                if actual_var_name in group.variables:
                    return self._extract_variable_data(
                        group.variables[actual_var_name],
                        actual_var_name,
                        time_indices,
                        spatial_indices,
                        group_path=group_path,
                    )
            except (KeyError, IndexError):
                pass
        else:
            actual_var_name = var_name
            group_path = None

        # Search for the variable in different locations
        search_locations = []

        # 1. Check root dataset first
        search_locations.append((dataset, "root"))

        # 2. Check specific target groups if provided
        if target_groups:
            for group_name in target_groups:
                try:
                    group = dataset[group_name]
                    search_locations.append((group, group_name))
                except KeyError:
                    logger.warning(f"Group '{group_name}' not found in dataset")
        else:
            # 3. Search all groups if no specific groups provided
            search_locations.extend(self._get_all_groups(dataset))

        # Search for the variable
        for location, location_path in search_locations:
            if actual_var_name in location.variables:
                logger.info(
                    f"Found variable '{actual_var_name}' in location '{location_path}'"
                )
                return self._extract_variable_data(
                    location.variables[actual_var_name],
                    actual_var_name,
                    time_indices,
                    spatial_indices,
                    group_path=location_path if location_path != "root" else None,
                )

        # Variable not found
        logger.warning(f"Variable '{var_name}' not found in any searched location")
        return {
            "error": f"Variable '{var_name}' not found",
            "searched_locations": [path for _, path in search_locations],
        }

    def collect_groups(
        self,
        group: nc.Group,
        path: str = "",
        groups_list: Optional[List[Tuple[nc.Group, str]]] = None,
    ) -> List[Tuple[nc.Group, str]]:
        """Collect all subgroups of a given netCDF4 group.

        Args:
            group: The netCDF4 group to collect subgroups from.
            path: The current path within the group.
            groups_list: The list to store collected groups.

        Returns:
            List[Tuple[nc.Group, str]]:
                A list of tuples containing subgroups and their paths.

        """
        if groups_list is None:
            groups_list = []

        for subgroup_name, subgroup in group.groups.items():
            subgroup_path = f"{path}/{subgroup_name}" if path else subgroup_name
            groups_list.append((subgroup, subgroup_path))
            self.collect_groups(subgroup, subgroup_path, groups_list)

        return groups_list

    def _get_all_groups(self, dataset: nc.Dataset) -> List[Tuple]:
        """Get all groups in the dataset recursively.

        Args:
            dataset (nc.Dataset): The NetCDF4 dataset.

        Returns:
            List[Tuple]: List of (group, path) tuples.

        """
        return self.collect_groups(dataset)

    def iterate_recursive_over_groups(
        self, group: nc.Group, result: list[dict], path_prefix: str = ""
    ) -> None:
        """Recursively iterate through the netCDF4 groups and variables.

        Args:
            group: The current netCDF4 group.
            result: The list to store variable metadata.
            path_prefix: The prefix for the current path.

        """
        logger.debug(f"Iterating through group: {group.name}")

        for var_name, var in group.variables.items():
            full_path = f"{path_prefix}/{var_name}" if path_prefix else var_name
            logger.info(
                f"Variable at {full_path}: shape={var.shape}, dtype={var.dtype}"
            )

            # Extract subgroup information from path_prefix
            if path_prefix:
                path_parts = path_prefix.split("/")
                if len(path_parts) > 1:
                    # First part is main group, rest is subgroup
                    main_group = path_parts[0]
                    subgroup = "/".join(path_parts[1:])
                else:
                    main_group = path_parts[0]
                    subgroup = "-"
            else:
                main_group = "root"
                subgroup = "-"

            if var_name == "time_points":
                variable_dict = {
                    "name": var_name,
                    "dimensions": "(" + ", ".join(str(d) for d in var.dimensions) + ")",
                    "shape": "(" + ", ".join(str(s) for s in var.shape) + ")",
                    "domain": "-",
                    "group": main_group,
                    "subgroup": subgroup,
                    "long_name": (
                        var.getncattr("long_name")
                        if "long_name" in var.ncattrs()
                        else "-"
                    ),
                    "strategy": (
                        var.getncattr("strategy")
                        if "strategy" in var.ncattrs()
                        else "-"
                    ),
                    "unit": (var.getncattr("unit") if "unit" in var.ncattrs() else "-"),
                }
                result.append(variable_dict)
                continue

            # Check if variable has variable_type attribute
            variable_type = (
                var.getncattr("variable_type")
                if "variable_type" in var.ncattrs()
                else "unknown"
            )

            if variable_type == "data":
                # Check for subgroup attribute in the variable itself
                var_subgroup = (
                    var.getncattr("subgroup")
                    if "subgroup" in var.ncattrs()
                    else subgroup
                )

                variable_dict = {
                    "name": var.name,
                    "dimensions": "(" + ", ".join(str(d) for d in var.dimensions) + ")",
                    "shape": "(" + ", ".join(str(s) for s in var.shape) + ")",
                    "domain": (
                        var.getncattr("domain") if "domain" in var.ncattrs() else "-"
                    ),
                    "group": main_group,
                    "subgroup": var_subgroup if var_subgroup != "-" else subgroup,
                    "long_name": (
                        var.getncattr("long_name")
                        if "long_name" in var.ncattrs()
                        else "-"
                    ),
                    "strategy": (
                        var.getncattr("strategy")
                        if "strategy" in var.ncattrs()
                        else "-"
                    ),
                    "unit": (var.getncattr("unit") if "unit" in var.ncattrs() else "-"),
                }
                result.append(variable_dict)

        # Continue recursion through subgroups
        for subgroup_name, subgroup in group.groups.items():
            new_prefix = (
                f"{path_prefix}/{subgroup_name}" if path_prefix else subgroup_name
            )
            self.iterate_recursive_over_groups(
                group=subgroup,
                result=result,
                path_prefix=new_prefix,
            )

    def read_metadata_for_variables(self, netcdf4_file: str) -> List[dict]:
        """Read metadata from all variables in the NetCDF4 file.

        Returns:
            List[dict]: A list of dictionaries containing variable metadata.

        """
        result = []
        logger.info(f"Reading metadata from netCDF4 file {netcdf4_file}.")

        with nc.Dataset(netcdf4_file, "r") as ncfile:
            logger.info("Starting recursive iteration through netCDF4 groups.")
            self.iterate_recursive_over_groups(ncfile, result)

        return result

    def _extract_variable_data(
        self,
        variable: nc.Variable,
        var_name: str,
        time_indices: Optional[List[int]] = None,
        spatial_indices: Optional[Dict[str, Union[slice, List[int]]]] = None,
        group_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Extract data and metadata from a NetCDF4 variable.

        Args:
            variable (nc.Variable): The NetCDF4 variable.
            var_name (str): Name of the variable.
            time_indices (Optional[List[int]]): Time indices to read.
            spatial_indices (Optional[Dict[str, Union[slice, List[int]]]]):
                Spatial indices.
            group_path (Optional[str]): Path of the group containing the variable.

        Returns:
            Dict[str, Any]: Variable data and metadata.

        """
        var_data = {
            "variable_name": var_name,
            "group_path": group_path,
            "full_path": f"{group_path}/{var_name}" if group_path else var_name,
        }

        try:
            # Get variable attributes
            var_data["attributes"] = {
                attr: variable.getncattr(attr) for attr in variable.ncattrs()
            }

            # Get variable dimensions
            var_data["dimensions"] = variable.dimensions
            var_data["shape"] = variable.shape
            var_data["dtype"] = str(variable.dtype)

            # Read the actual data with proper indexing
            data_slice = self._build_data_slice(variable, time_indices, spatial_indices)
            var_data["data"] = variable[data_slice]
            var_data["sliced_shape"] = var_data["data"].shape

            # Convert masked arrays to regular arrays if needed
            if hasattr(var_data["data"], "mask"):
                var_data["data"] = np.ma.filled(var_data["data"], fill_value=np.nan)

            # Calculate data statistics
            if isinstance(var_data["data"], np.ndarray):
                var_data["data_stats"] = {
                    "min": float(np.nanmin(var_data["data"])),
                    "max": float(np.nanmax(var_data["data"])),
                    "mean": float(np.nanmean(var_data["data"])),
                    "std": float(np.nanstd(var_data["data"])),
                    "non_nan_count": int(np.sum(~np.isnan(var_data["data"]))),
                }

        except Exception as e:
            logger.error(f"Error reading data for variable '{var_name}': {e}")
            var_data["error"] = str(e)

        return var_data

    def _build_data_slice(
        self,
        variable: nc.Variable,
        time_indices: Optional[List[int]] = None,
        spatial_indices: Optional[Dict[str, Union[slice, List[int]]]] = None,
    ) -> tuple:
        """Build a data slice tuple for reading NetCDF4 variable data.

        Args:
            variable (nc.Variable): The NetCDF4 variable to read.
            time_indices (Optional[List[int]]): Time indices to read.
            spatial_indices (Optional[Dict[str, Union[slice, List[int]]]]):
                Spatial indices.

        Returns:
            tuple: Tuple of slice objects for reading the data.

        """
        dimensions = variable.dimensions
        data_slice = []

        for i, dim_name in enumerate(dimensions):
            if dim_name.lower() in ["time", "t"] and time_indices is not None:
                data_slice.append(time_indices)
            elif spatial_indices and dim_name in spatial_indices:
                data_slice.append(spatial_indices[dim_name])
            else:
                data_slice.append(slice(None))  # Read all data for this dimension

        return tuple(data_slice)

    def get_available_variables_from_netcdf4_with_groups(
        self, netcdf4_file_path: str
    ) -> Dict[str, Any]:
        """Get information about all available variables in a NetCDF4 file with groups.

        Args:
            netcdf4_file_path (str): Path to the NetCDF4 file.

        Returns:
            Dict[str, Any]:
                Dictionary containing information about all variables and groups.

        """
        try:
            if not Path(netcdf4_file_path).exists():
                raise FileNotFoundError(f"NetCDF4 file not found: {netcdf4_file_path}")

            result = {
                "file_path": netcdf4_file_path,
                "global_attributes": {},
                "structure": {},
            }

            with nc.Dataset(netcdf4_file_path, "r") as dataset:
                # Global attributes
                result["global_attributes"] = {
                    attr: dataset.getncattr(attr) for attr in dataset.ncattrs()
                }

                # Complete file structure
                result["structure"] = self._read_group_structure(dataset)

                # Flatten all variables for easy access
                result["all_variables"] = self._flatten_variables(result["structure"])

            logger.info(
                f"Found {len(result['all_variables'])} variables in NetCDF4 file"
            )
            return result

        except Exception as e:
            logger.error(f"Error reading NetCDF4 file info: {e}")
            raise

    def _flatten_variables(
        self, structure: Dict[str, Any], path_prefix: str = ""
    ) -> Dict[str, Dict[str, Any]]:
        """Flatten the hierarchical variable structure for easy lookup.

        Args:
            structure (Dict[str, Any]): The hierarchical structure.
            path_prefix (str): Current path prefix.

        Returns:
            Dict[str, Dict[str, Any]]: Flattened variables with full paths as keys.

        """
        flattened = {}

        # Add variables from current level
        for var_name, var_info in structure.get("variables", {}).items():
            full_path = f"{path_prefix}/{var_name}" if path_prefix else var_name
            var_info_copy = var_info.copy()
            var_info_copy["full_path"] = full_path
            var_info_copy["group_path"] = path_prefix if path_prefix else "root"
            flattened[full_path] = var_info_copy

        # Recursively add variables from subgroups
        for subgroup_name, subgroup_info in structure.get("subgroups", {}).items():
            subgroup_path = (
                f"{path_prefix}/{subgroup_name}" if path_prefix else subgroup_name
            )
            flattened.update(self._flatten_variables(subgroup_info, subgroup_path))

        return flattened

    def read_variable_from_group(
        self,
        netcdf4_file_path: str,
        variable_name: str,
        group_path: str,
        **slice_kwargs: Dict[str, Union[slice, List[int]]],
    ) -> Dict[str, Any]:
        """Read a variable from a specific group with flexible slicing.

        Args:
            netcdf4_file_path (str): Path to the NetCDF4 file.
            variable_name (str): Name of the variable to read.
            group_path (str): Path to the group containing the variable.
            **slice_kwargs: Keyword arguments for slicing each dimension.

        Returns:
            Dict[str, Any]: Dictionary containing the variable data and metadata.

        Example:
            >>> handler = AssasNetCDF4VariableHandler()
            >>> data = handler.read_variable_from_group(
            ...     netcdf4_file_path='/path/to/dataset.h5',
            ...     variable_name='temperature',
            ...     group_path='physics',
            ...     time=slice(0, 10),
            ...     x=slice(50, 150)
            ... )

        """
        try:
            if not Path(netcdf4_file_path).exists():
                raise FileNotFoundError(f"NetCDF4 file not found: {netcdf4_file_path}")

            logger.info(
                f"Reading variable '{variable_name}' from group '{group_path}': "
                f"{netcdf4_file_path}."
            )

            with nc.Dataset(netcdf4_file_path, "r") as dataset:
                # Access the specific group
                if group_path == "root" or group_path == "":
                    group = dataset
                else:
                    if group_path not in dataset.groups:
                        raise ValueError(
                            f"Group '{group_path}' not found in NetCDF4 file"
                        )
                    group = dataset[group_path]

                # Check if variable exists in the group
                if variable_name not in group.variables:
                    raise ValueError(
                        f"Variable '{variable_name}' not found in group '{group_path}'"
                    )

                variable = group.variables[variable_name]

                # Build slice tuple based on dimension names and slice_kwargs
                data_slice = []
                for dim_name in variable.dimensions:
                    if dim_name in slice_kwargs:
                        data_slice.append(slice_kwargs[dim_name])
                    else:
                        data_slice.append(slice(None))

                # Read the data
                data = variable[tuple(data_slice)]

                # Handle masked arrays
                if hasattr(data, "mask"):
                    data = np.ma.filled(data, fill_value=np.nan)

                result = {
                    "variable_name": variable_name,
                    "group_path": group_path,
                    "full_path": f"{group_path}/{variable_name}"
                    if group_path not in ["root", ""]
                    else variable_name,
                    "file_path": netcdf4_file_path,
                    "dimensions": variable.dimensions,
                    "original_shape": variable.shape,
                    "sliced_shape": data.shape,
                    "slice_applied": slice_kwargs,
                    "data": data,
                    "attributes": {
                        attr: variable.getncattr(attr) for attr in variable.ncattrs()
                    },
                }

                # Add data statistics
                if isinstance(data, np.ndarray):
                    result["stats"] = {
                        "min": float(np.nanmin(data)),
                        "max": float(np.nanmax(data)),
                        "mean": float(np.nanmean(data)),
                        "std": float(np.nanstd(data)),
                        "non_nan_count": int(np.sum(~np.isnan(data))),
                    }

                return result

        except Exception as e:
            logger.error(f"Error reading variable from group: {e}")
            raise

    def read_variable_subset(
        self,
        netcdf4_file_path: str,
        variable_name: str,
        **slice_kwargs: Dict[str, slice],
    ) -> Dict[str, Any]:
        """Read a subset of data for a single variable with flexible slicing.

        Args:
            netcdf4_file_path (str): Path to the NetCDF4 file.
            variable_name (str): Name of the variable to read.
            **slice_kwargs: Keyword arguments for slicing each dimension.
                           Example: time=slice(0, 10), x=slice(0, 100), y=[10, 20, 30]

        Returns:
            Dict[str, Any]: Dictionary containing the variable data and metadata.

        Example:
            >>> handler = AssasNetCDF4VariableHandler()
            >>> data = handler.read_variable_subset(
            ...     netcdf4_file_path='/path/to/dataset.h5',
            ...     variable_name='temperature',
            ...     time=slice(0, 10),
            ...     x=slice(50, 150),
            ...     y=slice(25, 75)
            ... )

        """
        try:
            if not Path(netcdf4_file_path).exists():
                raise FileNotFoundError(f"NetCDF4 file not found: {netcdf4_file_path}")

            logger.info(f"Reading variable '{variable_name}' from {netcdf4_file_path}.")

            # First, try to find the variable in any group
            variables_found = self.read_variables_from_netcdf4(
                netcdf4_file_path=netcdf4_file_path, variable_names=[variable_name]
            )

            if variable_name not in variables_found["variables"]:
                raise ValueError(
                    f"Variable '{variable_name}' not found in NetCDF4 file"
                )

            var_info = variables_found["variables"][variable_name]
            if "error" in var_info:
                raise ValueError(var_info["error"])

            # Now read with specific slicing
            with nc.Dataset(netcdf4_file_path, "r") as dataset:
                # Find the variable location
                variable = None
                group_path = var_info.get("group_path")

                if group_path and group_path != "root":
                    group = dataset[group_path]
                    variable = group.variables[variable_name]
                else:
                    variable = dataset.variables[variable_name]

                dimensions = variable.dimensions

                # Build slice tuple based on dimension names and slice_kwargs
                data_slice = []
                for dim_name in dimensions:
                    if dim_name in slice_kwargs:
                        data_slice.append(slice_kwargs[dim_name])
                    else:
                        data_slice.append(slice(None))

                # Read the data
                data = variable[tuple(data_slice)]

                # Handle masked arrays
                if hasattr(data, "mask"):
                    data = np.ma.filled(data, fill_value=np.nan)

                result = {
                    "variable_name": variable_name,
                    "file_path": netcdf4_file_path,
                    "group_path": group_path,
                    "dimensions": dimensions,
                    "original_shape": variable.shape,
                    "sliced_shape": data.shape,
                    "slice_applied": slice_kwargs,
                    "data": data,
                    "attributes": {
                        attr: variable.getncattr(attr) for attr in variable.ncattrs()
                    },
                }

                # Add data statistics
                if isinstance(data, np.ndarray):
                    result["stats"] = {
                        "min": float(np.nanmin(data)),
                        "max": float(np.nanmax(data)),
                        "mean": float(np.nanmean(data)),
                        "std": float(np.nanstd(data)),
                        "non_nan_count": int(np.sum(~np.isnan(data))),
                    }

                return result

        except Exception as e:
            logger.error(f"Error reading variable subset: {e}")
            raise

    def get_variable_info(
        self, netcdf4_file_path: str, variable_name: str
    ) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific variable.

        Args:
            netcdf4_file_path (str): Path to the NetCDF4 file.
            variable_name (str): Name of the variable to get info for.

        Returns:
            Optional[Dict[str, Any]]:
            Variable information dictionary or None if not found.

        """
        try:
            if not Path(netcdf4_file_path).exists():
                raise FileNotFoundError(f"NetCDF4 file not found: {netcdf4_file_path}")

            logger.info(
                f"Getting variable info for '{variable_name}' from: {netcdf4_file_path}"
            )

            with nc.Dataset(netcdf4_file_path, "r") as dataset:
                # First, find the variable in the file structure
                variable_location = self._find_variable_in_groups(
                    dataset, variable_name
                )

                if not variable_location:
                    logger.warning(
                        f"Variable '{variable_name}' not found in NetCDF4 file"
                    )
                    return None

                variable, group_path = variable_location

                # Extract variable information
                var_info = {
                    "name": variable_name,
                    "full_path": f"{group_path}/{variable_name}"
                    if group_path
                    else variable_name,
                    "group": group_path if group_path else "root",
                    "dimensions": variable.dimensions,
                    "shape": variable.shape,
                    "dtype": str(variable.dtype),
                    "size": variable.size,
                    "ndim": variable.ndim,
                }

                # Get all variable attributes
                attributes = {}
                for attr_name in variable.ncattrs():
                    try:
                        attr_value = variable.getncattr(attr_name)
                        attributes[attr_name] = (
                            str(attr_value) if attr_value is not None else ""
                        )
                    except Exception as e:
                        logger.warning(f"Could not read attribute '{attr_name}': {e}")
                        attributes[attr_name] = ""

                # Map common attributes to standard fields
                var_info.update(
                    {
                        "long_name": attributes.get("long_name", ""),
                        "unit": attributes.get("unit", attributes.get("units", "")),
                        "domain": attributes.get("domain", ""),
                        "strategy": attributes.get("strategy", ""),
                        "subgroup": attributes.get("subgroup", ""),
                        "variable_type": attributes.get("variable_type", "data"),
                        "description": attributes.get(
                            "description", attributes.get("long_name", "")
                        ),
                        "attributes": attributes,
                    }
                )

                # Determine subgroup from path if not in attributes
                if not var_info["subgroup"] and group_path:
                    path_parts = group_path.split("/")
                    if len(path_parts) > 1:
                        var_info["subgroup"] = "/".join(path_parts[1:])
                    elif len(path_parts) == 1:
                        var_info["subgroup"] = "-"
                elif not var_info["subgroup"]:
                    var_info["subgroup"] = "-"

                logger.info(
                    f"Successfully retrieved info for variable '{variable_name}'"
                )
                return var_info

        except Exception as e:
            logger.error(f"Error getting variable info for '{variable_name}': {e}")
            raise

    def get_variable_statistics(
        self, netcdf4_file_path: str, variable_name: str
    ) -> Dict[str, Any]:
        """Get statistical information about a specific variable.

        Args:
            netcdf4_file_path (str): Path to the NetCDF4 file.
            variable_name (str): Name of the variable to get statistics for.

        Returns:
            Dict[str, Any]: Statistical information about the variable.

        """
        try:
            if not Path(netcdf4_file_path).exists():
                raise FileNotFoundError(f"NetCDF4 file not found: {netcdf4_file_path}")

            logger.info(
                f"Computing statistics for variable '{variable_name}' "
                f"from: {netcdf4_file_path}"
            )

            with nc.Dataset(netcdf4_file_path, "r") as dataset:
                # Find the variable
                variable_location = self._find_variable_in_groups(
                    dataset, variable_name
                )

                if not variable_location:
                    raise ValueError(
                        f"Variable '{variable_name}' not found in NetCDF4 file"
                    )

                variable, group_path = variable_location

                # Initialize statistics dictionary
                stats = {
                    "variable_name": variable_name,
                    "shape": list(
                        variable.shape
                    ),  # Convert to list for JSON serialization
                    "size": int(variable.size),
                    "dtype": str(variable.dtype),
                    "dimensions": list(variable.dimensions),  # Convert to list
                    "has_missing_values": False,
                    "missing_value_count": 0,
                    "valid_value_count": 0,
                }

                # For very large arrays, compute statistics on a sample
                if variable.size > 1e6:  # If more than 1 million elements
                    logger.info(
                        f"Large variable detected ({variable.size} elements), "
                        f"sampling for statistics"
                    )
                    stats.update(self._compute_sampled_statistics(variable))
                else:
                    # Read all data for smaller variables
                    try:
                        data = variable[:]

                        # Handle masked arrays properly
                        if hasattr(data, "mask") and np.ma.is_masked(data):
                            # Convert masked array to regular array with NaN
                            data = np.ma.filled(data, fill_value=np.nan)

                            # Now work with regular array
                            if np.issubdtype(data.dtype, np.floating):
                                nan_mask = np.isnan(data)
                                valid_data = data[~nan_mask]
                                stats["has_missing_values"] = bool(np.any(nan_mask))
                                stats["missing_value_count"] = int(np.sum(nan_mask))
                                stats["valid_value_count"] = int(np.sum(~nan_mask))
                            else:
                                # For non-floating types, no NaN values
                                valid_data = data
                                stats["valid_value_count"] = int(data.size)
                        else:
                            # Regular array - check for NaN values
                            if np.issubdtype(data.dtype, np.floating):
                                nan_mask = np.isnan(data)
                                valid_data = data[~nan_mask]
                                stats["has_missing_values"] = bool(np.any(nan_mask))
                                stats["missing_value_count"] = int(np.sum(nan_mask))
                                stats["valid_value_count"] = int(np.sum(~nan_mask))
                            else:
                                valid_data = data
                                stats["valid_value_count"] = int(data.size)

                        # Compute statistics on valid data
                        if len(valid_data) > 0:
                            # Use safe statistical functions that handle edge cases
                            stats.update(
                                {
                                    "min": self._safe_float_convert(np.min(valid_data)),
                                    "max": self._safe_float_convert(np.max(valid_data)),
                                    "mean": self._safe_float_convert(
                                        np.mean(valid_data)
                                    ),
                                    "std": self._safe_float_convert(np.std(valid_data)),
                                }
                            )

                            # Use numpy percentile that works with all array types
                            if len(valid_data) > 1:
                                try:
                                    stats["median"] = self._safe_float_convert(
                                        np.percentile(valid_data, 50, method="linear")
                                    )
                                except (TypeError, ValueError):
                                    # Fallback for older numpy versions
                                    # or incompatible data
                                    stats["median"] = self._safe_float_convert(
                                        np.median(valid_data)
                                    )
                            else:
                                stats["median"] = stats["mean"]

                            # Additional statistics for numerical data
                            if (
                                np.issubdtype(valid_data.dtype, np.number)
                                and len(valid_data) > 1
                            ):
                                try:
                                    stats.update(
                                        {
                                            "var": self._safe_float_convert(
                                                np.var(valid_data)
                                            ),
                                            "percentile_25": self._safe_float_convert(
                                                np.percentile(
                                                    valid_data, 25, method="linear"
                                                )
                                            ),
                                            "percentile_75": self._safe_float_convert(
                                                np.percentile(
                                                    valid_data, 75, method="linear"
                                                )
                                            ),
                                        }
                                    )
                                except (TypeError, ValueError):
                                    # Fallback for older numpy or incompatible data
                                    try:
                                        sorted_data = np.sort(valid_data)
                                        n = len(sorted_data)
                                        stats.update(
                                            {
                                                "var": self._safe_float_convert(
                                                    np.var(valid_data)
                                                ),
                                                "percentile_25": (
                                                    self._safe_float_convert(
                                                        sorted_data[int(0.25 * n)]
                                                    )
                                                ),
                                                "percentile_75": (
                                                    self._safe_float_convert(
                                                        sorted_data[int(0.75 * n)]
                                                    )
                                                ),
                                            }
                                        )
                                    except Exception:
                                        stats.update(
                                            {
                                                "var": None,
                                                "percentile_25": None,
                                                "percentile_75": None,
                                            }
                                        )
                            else:
                                stats.update(
                                    {
                                        "var": None,
                                        "percentile_25": None,
                                        "percentile_75": None,
                                    }
                                )
                        else:
                            # No valid data
                            stats.update(
                                {
                                    "min": None,
                                    "max": None,
                                    "mean": None,
                                    "std": None,
                                    "median": None,
                                    "var": None,
                                    "percentile_25": None,
                                    "percentile_75": None,
                                }
                            )

                    except Exception as e:
                        logger.warning(
                            f"Could not read data for statistics computation: {e}"
                        )
                        stats["error"] = str(e)

                # Add dimension information
                stats["dimension_info"] = {}
                for i, dim_name in enumerate(variable.dimensions):
                    if dim_name in dataset.dimensions:
                        stats["dimension_info"][dim_name] = {
                            "size": int(len(dataset.dimensions[dim_name])),
                            "index": int(i),
                            "unlimited": bool(
                                dataset.dimensions[dim_name].isunlimited()
                            ),
                        }

                logger.info(
                    f"Successfully computed statistics for variable '{variable_name}'"
                )
                return stats

        except Exception as e:
            logger.error(
                f"Error computing statistics for variable '{variable_name}': {e}"
            )
            raise

    def _safe_float_convert(self, value: NumericType) -> Optional[float]:
        """Safely convert a value to float, handling NaN and inf values.

        Args:
            value: Value to convert.

        Returns:
            Optional[float]: Float value or None if invalid.

        """
        try:
            if value is None:
                return None

            float_val = float(value)

            # Check for NaN or infinite values
            if np.isnan(float_val) or np.isinf(float_val):
                return None

            return float_val
        except (ValueError, TypeError, OverflowError):
            return None

    def _compute_sampled_statistics(
        self, variable: nc.Variable, sample_size: int = 100000
    ) -> Dict[str, Any]:
        """Compute statistics on a sample of a large variable.

        Args:
            variable (nc.Variable): The NetCDF4 variable.
            sample_size (int): Number of elements to sample.

        Returns:
            Dict[str, Any]: Statistical information computed on the sample.

        """
        try:
            logger.info(f"Computing statistics on sample of {sample_size} elements")

            # Generate random indices for sampling
            total_size = variable.size
            actual_sample_size = min(sample_size, total_size)
            sample_indices = np.random.choice(
                total_size, actual_sample_size, replace=False
            )

            # Convert flat indices to multidimensional indices
            multi_indices = np.unravel_index(sample_indices, variable.shape)

            # Sample the data
            sampled_data = variable[multi_indices]

            stats = {"sampled": True, "sample_size": int(len(sampled_data))}

            # Handle masked arrays properly
            if hasattr(sampled_data, "mask") and np.ma.is_masked(sampled_data):
                # Convert to regular array with NaN
                sampled_data = np.ma.filled(sampled_data, fill_value=np.nan)

            # Handle NaN values
            if np.issubdtype(sampled_data.dtype, np.floating):
                nan_mask = np.isnan(sampled_data)
                valid_data = sampled_data[~nan_mask]
                stats["has_missing_values"] = bool(np.any(nan_mask))
                stats["missing_value_count"] = int(np.sum(nan_mask))
                stats["valid_value_count"] = int(np.sum(~nan_mask))
            else:
                valid_data = sampled_data
                stats["valid_value_count"] = int(sampled_data.size)

            # Compute statistics
            if len(valid_data) > 0:
                stats.update(
                    {
                        "min": self._safe_float_convert(np.min(valid_data)),
                        "max": self._safe_float_convert(np.max(valid_data)),
                        "mean": self._safe_float_convert(np.mean(valid_data)),
                        "std": self._safe_float_convert(np.std(valid_data)),
                    }
                )

                # Safe percentile computation
                if len(valid_data) > 1:
                    try:
                        stats["median"] = self._safe_float_convert(
                            np.percentile(valid_data, 50, method="linear")
                        )
                    except (TypeError, ValueError):
                        stats["median"] = self._safe_float_convert(
                            np.median(valid_data)
                        )

                    if np.issubdtype(valid_data.dtype, np.number):
                        try:
                            stats.update(
                                {
                                    "var": self._safe_float_convert(np.var(valid_data)),
                                    "percentile_25": self._safe_float_convert(
                                        np.percentile(valid_data, 25, method="linear")
                                    ),
                                    "percentile_75": self._safe_float_convert(
                                        np.percentile(valid_data, 75, method="linear")
                                    ),
                                }
                            )
                        except (TypeError, ValueError):
                            stats.update(
                                {
                                    "var": None,
                                    "percentile_25": None,
                                    "percentile_75": None,
                                }
                            )
                    else:
                        stats.update(
                            {
                                "var": None,
                                "percentile_25": None,
                                "percentile_75": None,
                            }
                        )
                else:
                    stats.update(
                        {
                            "median": stats.get("mean"),
                            "var": None,
                            "percentile_25": None,
                            "percentile_75": None,
                        }
                    )
            else:
                stats.update(
                    {
                        "min": None,
                        "max": None,
                        "mean": None,
                        "std": None,
                        "median": None,
                        "var": None,
                        "percentile_25": None,
                        "percentile_75": None,
                    }
                )

            return stats

        except Exception as e:
            logger.error(f"Error computing sampled statistics: {e}")
            return {"error": str(e), "sampled": True}

    def _extract_variable_data(
        self,
        variable: nc.Variable,
        var_name: str,
        time_indices: Optional[List[int]] = None,
        spatial_indices: Optional[Dict[str, Union[slice, List[int]]]] = None,
        group_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Extract data and metadata from a NetCDF4 variable.

        Args:
            variable (nc.Variable): The NetCDF4 variable.
            var_name (str): Name of the variable.
            time_indices (Optional[List[int]]): Time indices to read.
            spatial_indices (Optional[Dict[str, Union[slice, List[int]]]]):
                Spatial indices.
            group_path (Optional[str]): Path of the group containing the variable.

        Returns:
            Dict[str, Any]: Variable data and metadata.

        """
        var_data = {
            "variable_name": var_name,
            "group_path": group_path,
            "full_path": f"{group_path}/{var_name}" if group_path else var_name,
        }

        try:
            # Get variable attributes
            var_data["attributes"] = {
                attr: str(variable.getncattr(attr)) for attr in variable.ncattrs()
            }

            # Get variable dimensions
            var_data["dimensions"] = list(variable.dimensions)
            var_data["shape"] = list(variable.shape)
            var_data["dtype"] = str(variable.dtype)

            # Read the actual data with proper indexing
            data_slice = self._build_data_slice(variable, time_indices, spatial_indices)
            raw_data = variable[data_slice]

            # Handle masked arrays properly
            if hasattr(raw_data, "mask") and np.ma.is_masked(raw_data):
                var_data["data"] = np.ma.filled(raw_data, fill_value=np.nan)
            else:
                var_data["data"] = raw_data

            var_data["sliced_shape"] = list(var_data["data"].shape)

            # Calculate data statistics safely
            if isinstance(var_data["data"], np.ndarray):
                try:
                    # Use safe conversion for statistics
                    data_for_stats = var_data["data"]
                    if np.issubdtype(data_for_stats.dtype, np.floating):
                        valid_mask = ~np.isnan(data_for_stats)
                        valid_data = data_for_stats[valid_mask]
                    else:
                        valid_data = data_for_stats

                    if len(valid_data) > 0:
                        var_data["data_stats"] = {
                            "min": self._safe_float_convert(np.min(valid_data)),
                            "max": self._safe_float_convert(np.max(valid_data)),
                            "mean": self._safe_float_convert(np.mean(valid_data)),
                            "std": self._safe_float_convert(np.std(valid_data)),
                            "non_nan_count": int(len(valid_data)),
                        }
                    else:
                        var_data["data_stats"] = {
                            "min": None,
                            "max": None,
                            "mean": None,
                            "std": None,
                            "non_nan_count": 0,
                        }
                except Exception as stats_error:
                    logger.warning(f"Error computing data statistics: {stats_error}")
                    var_data["data_stats"] = {"error": str(stats_error)}

        except Exception as e:
            logger.error(f"Error reading data for variable '{var_name}': {e}")
            var_data["error"] = str(e)

        return var_data

        # Search in all groups recursively

    def search_groups(
        self,
        group: nc.Group,
        path: str = "",
        actual_var_name: str = None,
    ) -> Optional[Tuple[nc.Variable, str]]:
        """Recursively search for a variable in all subgroups.

        Args:
            group (nc.Group): The NetCDF4 group to search within.
            path (str, optional): The current group path.
            actual_var_name (str, optional): The variable name to search for.

        Returns:
            Optional[Tuple[nc.Variable, str]]:
                The found variable and its group path, or None if not found.

        """
        for subgroup_name, subgroup in group.groups.items():
            subgroup_path = f"{path}/{subgroup_name}" if path else subgroup_name

            # Check variables in this subgroup
            if actual_var_name is not None and actual_var_name in subgroup.variables:
                return (subgroup.variables[actual_var_name], subgroup_path)

            # Recursively search deeper
            result = self.search_groups(
                group=subgroup, path=subgroup_path, actual_var_name=actual_var_name
            )
            if result:
                return result
        return None

    def _find_variable_in_groups(
        self,
        dataset: nc.Dataset,
        variable_name: str,
    ) -> Optional[Tuple[nc.Variable, Optional[str]]]:
        """Find a variable in the dataset, searching through all groups.

        Args:
            dataset (nc.Dataset): The NetCDF4 dataset.
            variable_name (str): Name of the variable to find.

        Returns:
            Optional[Tuple[nc.Variable, Optional[str]]]:
                Tuple of (variable, group_path) or None if not found.

        """
        # Check if variable name includes a group path
        if "/" in variable_name:
            group_path, actual_var_name = variable_name.rsplit("/", 1)
            try:
                group = dataset[group_path]
                if actual_var_name in group.variables:
                    return (group.variables[actual_var_name], group_path)
            except (KeyError, IndexError):
                pass
        else:
            actual_var_name = variable_name

        # Search in root first
        if actual_var_name in dataset.variables:
            return (dataset.variables[actual_var_name], None)

        return self.search_groups(group=dataset, actual_var_name=actual_var_name)

    def collect_variables(
        self, group: nc.Group, variables: List[Dict[str, Any]], path: str = ""
    ) -> None:
        """Collect variables from a NetCDF4 group and its subgroups.

        Args:
            group (nc.Group): The NetCDF4 group to collect variables from.
            variables (List[Dict[str, Any]]):
                The list to append variable information to.
            path (str, optional): The current group path.

        """
        # Process variables in current group
        for var_name, variable in group.variables.items():
            full_path = f"{path}/{var_name}" if path else var_name

            var_info = {
                "name": var_name,
                "full_path": full_path,
                "group": path if path else "root",
                "shape": variable.shape,
                "dimensions": variable.dimensions,
                "dtype": str(variable.dtype),
                "size": variable.size,
            }

            # Add common attributes
            attrs = {attr: variable.getncattr(attr) for attr in variable.ncattrs()}
            var_info.update(
                {
                    "long_name": attrs.get("long_name", ""),
                    "unit": attrs.get("unit", attrs.get("units", "")),
                    "domain": attrs.get("domain", ""),
                    "variable_type": attrs.get("variable_type", "data"),
                }
            )

            variables.append(var_info)

        # Recursively process subgroups
        for subgroup_name, subgroup in group.groups.items():
            subgroup_path = f"{path}/{subgroup_name}" if path else subgroup_name
            self.collect_variables(subgroup, variables, subgroup_path)

    def list_all_variables(
        self,
        netcdf4_file_path: str,
    ) -> List[Dict[str, Any]]:
        """List all variables in the NetCDF4 file with basic information.

        Returns:
            List[Dict[str, Any]]: List of variable information dictionaries.

        """
        try:
            if not Path(netcdf4_file_path).exists():
                raise FileNotFoundError(f"NetCDF4 file not found: {netcdf4_file_path}")

            variables = []

            with nc.Dataset(netcdf4_file_path, "r") as dataset:
                self.collect_variables(dataset, variables)

            logger.info(f"Listed {len(variables)} variables from NetCDF4 file")
            return variables

        except Exception as e:
            logger.error(f"Error listing variables: {e}")
            raise
