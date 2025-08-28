"""AssasNetCDF4VariableHandler class.

This module provides the AssasNetCDF4VariableHandler class for reading and managing
NetCDF4 variables, including support for groups and hierarchical data structures.
"""

import netCDF4 as nc
import numpy as np
import logging
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

    def explore_groups(self, group: nc.Group, path: str = "") -> Dict[str, Any]:
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
            group_info["subgroups"][subgroup_name] = self.explore_groups(
                subgroup, subgroup_path
            )

        return group_info

    def get_group_structure(self, netcdf4_file: str) -> Dict[str, Any]:
        """Get the group structure from a NetCDF4 file.

        Args:
            netcdf4_file (str): Path to the NetCDF4 file.

        Returns:
            Dict[str, Any]: Dictionary containing group structure information.

        """
        with nc.Dataset(netcdf4_file, "r") as dataset:
            return self.explore_groups(dataset)

    def _search_groups(
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
            result = self._search_groups(
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

        return self._search_groups(group=dataset, actual_var_name=actual_var_name)

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

    def read_metadata_for_variables(
        self,
        netcdf4_file: str,
        group: str = None,
    ) -> List[dict]:
        """Read metadata from all variables in the NetCDF4 file.

        Returns:
            List[dict]: A list of dictionaries containing variable metadata.

        """
        result = []
        logger.info(f"Reading metadata from netCDF4 file {netcdf4_file}.")

        with nc.Dataset(netcdf4_file, "r") as ncfile:
            logger.info("Starting recursive iteration through netCDF4 groups.")
            if group is not None:
                self.iterate_recursive_over_groups(ncfile.groups[group], result)
            else:
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
                    "shape": list(variable.shape),
                    "size": int(variable.size),
                    "dtype": str(variable.dtype),
                    "dimensions": list(variable.dimensions),
                    "has_missing_values": False,
                    "missing_value_count": 0,
                    "valid_value_count": 0,
                }

                # For very large arrays, compute statistics on a sample
                if variable.size > 1e6:
                    logger.info(
                        f"Large variable detected ({variable.size} elements), "
                        f"sampling for statistics"
                    )
                    stats.update(self._compute_sampled_statistics(variable))
                else:
                    # Read all data for smaller variables
                    try:
                        data = variable[:]

                        # Convert ALL masked arrays to regular numpy arrays immediately
                        valid_data = self._extract_valid_data(data)

                        if valid_data is not None and len(valid_data) > 0:
                            stats["has_missing_values"] = len(valid_data) < data.size
                            stats["missing_value_count"] = int(
                                data.size - len(valid_data)
                            )
                            stats["valid_value_count"] = int(len(valid_data))

                            # Compute statistics using only regular numpy arrays
                            stats.update(self._compute_basic_statistics(valid_data))

                        else:
                            # No valid data
                            stats.update(self._get_empty_stats())

                    except Exception as e:
                        logger.warning(
                            f"Could not read data for statistics computation: {e}"
                        )
                        stats["error"] = str(e)

                # Add dimension information
                stats["dimension_info"] = self._get_dimension_info(variable, dataset)

                logger.info(
                    f"Successfully computed statistics for variable '{variable_name}'"
                )
                return stats

        except Exception as e:
            logger.error(
                f"Error computing statistics for variable '{variable_name}': {e}"
            )
            raise

    def _extract_valid_data(
        self, data: Union[np.ndarray, np.ma.MaskedArray]
    ) -> Optional[np.ndarray]:
        """Extract valid data from potentially masked arrays.

        Args:
            data: Input data (regular array or masked array)

        Returns:
            np.ndarray or None: Valid data as regular numpy array

        """
        try:
            # Handle masked arrays
            if np.ma.is_masked(data):
                # Extract only valid (non-masked) values
                valid_data = np.ma.compressed(data)
                # Ensure it's a regular numpy array, not masked
                if hasattr(valid_data, "mask"):
                    valid_data = np.asarray(valid_data)
            else:
                # Handle regular arrays with potential NaN values
                if np.issubdtype(data.dtype, np.floating):
                    # For floating point data, remove NaN values
                    flat_data = data.flatten()
                    valid_mask = ~np.isnan(flat_data)
                    valid_data = flat_data[valid_mask]
                else:
                    # For non-floating data, use all values
                    valid_data = data.flatten()

            # Final safety check - ensure it's a regular numpy array
            if hasattr(valid_data, "mask"):
                valid_data = np.asarray(valid_data)

            return valid_data if len(valid_data) > 0 else None

        except Exception as e:
            logger.warning(f"Error extracting valid data: {e}")
            return None

    def _compute_basic_statistics(self, valid_data: np.ndarray) -> Dict[str, Any]:
        """Compute basic statistics on clean, valid data.

        Args:
            valid_data: Clean numpy array with valid data only

        Returns:
            Dict with computed statistics

        """
        try:
            # Ensure we have a regular numpy array
            if hasattr(valid_data, "mask"):
                valid_data = np.asarray(valid_data)

            stats = {
                "min": self._safe_float_convert(np.min(valid_data)),
                "max": self._safe_float_convert(np.max(valid_data)),
                "mean": self._safe_float_convert(np.mean(valid_data)),
                "std": self._safe_float_convert(np.std(valid_data)),
            }

            # Compute percentiles using manual sorting to avoid numpy warnings
            if len(valid_data) > 1:
                # Sort data manually to avoid quantile/percentile warnings
                sorted_data = np.sort(valid_data)

                # Manual percentile computation
                stats["median"] = self._safe_float_convert(
                    self._compute_percentile(sorted_data, 50)
                )

                if np.issubdtype(valid_data.dtype, np.number):
                    stats.update(
                        {
                            "var": self._safe_float_convert(np.var(valid_data)),
                            "percentile_25": self._safe_float_convert(
                                self._compute_percentile(sorted_data, 25)
                            ),
                            "percentile_75": self._safe_float_convert(
                                self._compute_percentile(sorted_data, 75)
                            ),
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

            return stats

        except Exception as e:
            logger.error(f"Error computing basic statistics: {e}")
            return self._get_empty_stats()

    def _compute_percentile(self, sorted_data: np.ndarray, percentile: float) -> float:
        """Manually compute percentile from sorted data to avoid numpy warnings.

        Args:
            sorted_data: Pre-sorted numpy array
            percentile: Percentile to compute (0-100)

        Returns:
            float: Computed percentile value

        """
        try:
            if len(sorted_data) == 0:
                return np.nan

            if len(sorted_data) == 1:
                return float(sorted_data[0])

            # Linear interpolation method
            n = len(sorted_data)
            index = (percentile / 100.0) * (n - 1)

            if index == int(index):
                # Exact index
                return float(sorted_data[int(index)])
            else:
                # Interpolate between two values
                lower_index = int(np.floor(index))
                upper_index = int(np.ceil(index))

                if upper_index >= n:
                    upper_index = n - 1
                if lower_index < 0:
                    lower_index = 0

                weight = index - lower_index
                lower_val = float(sorted_data[lower_index])
                upper_val = float(sorted_data[upper_index])

                return lower_val + weight * (upper_val - lower_val)

        except Exception:
            return np.nan

    def _get_empty_stats(self) -> Dict[str, Any]:
        """Return empty statistics when no valid data is available."""
        return {
            "min": None,
            "max": None,
            "mean": None,
            "std": None,
            "median": None,
            "var": None,
            "percentile_25": None,
            "percentile_75": None,
        }

    def _get_dimension_info(
        self, variable: nc.Variable, dataset: nc.Dataset
    ) -> Dict[str, Any]:
        """Get dimension information for a variable."""
        dimension_info = {}
        for i, dim_name in enumerate(variable.dimensions):
            if dim_name in dataset.dimensions:
                dimension_info[dim_name] = {
                    "size": int(len(dataset.dimensions[dim_name])),
                    "index": int(i),
                    "unlimited": bool(dataset.dimensions[dim_name].isunlimited()),
                }
        return dimension_info

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

            stats = {"sampled": True, "sample_size": int(actual_sample_size)}

            # Extract valid data using the same method as regular statistics
            valid_data = self._extract_valid_data(sampled_data)

            if valid_data is not None and len(valid_data) > 0:
                stats["has_missing_values"] = len(valid_data) < sampled_data.size
                stats["missing_value_count"] = int(sampled_data.size - len(valid_data))
                stats["valid_value_count"] = int(len(valid_data))

                # Compute statistics using the same method
                stats.update(self._compute_basic_statistics(valid_data))
            else:
                stats.update(self._get_empty_stats())

            return stats

        except Exception as e:
            logger.error(f"Error computing sampled statistics: {e}")
            return {"error": str(e), "sampled": True}

    def search_time_variable(self, group: nc.Group) -> Optional[nc.Variable]:
        """Search for the time_points variable in a NetCDF4 group."""
        if "time_points" in group.variables:
            return group.variables["time_points"]

        for subgroup in group.groups.values():
            result = self.search_time_variable(subgroup)
            if result is not None:
                return result

        return None

    def get_time_points(self, netcdf4_file_path: str) -> np.ndarray:
        """Get all time points from the NetCDF4 file.

        Args:
            netcdf4_file_path (str): Path to the NetCDF4 file.

        Returns:
            np.ndarray: Array of time points.

        """
        try:
            if not Path(netcdf4_file_path).exists():
                raise FileNotFoundError(f"NetCDF4 file not found: {netcdf4_file_path}")

            with nc.Dataset(netcdf4_file_path, "r") as dataset:
                time_variable = None

                if "time_points" in dataset.variables:
                    time_variable = dataset.variables["time_points"]
                else:
                    time_variable = self.search_time_variable(dataset)

                if time_variable is None:
                    raise ValueError("time_points variable not found in NetCDF4 file")

                time_data = time_variable[:]

                if hasattr(time_data, "mask") and np.ma.is_masked(time_data):
                    time_data = np.ma.filled(time_data, fill_value=np.nan)

                return time_data

        except Exception as e:
            logger.error(f"Error reading time points: {e}")
            raise

    def get_time_points_by_indices(
        self, netcdf4_file_path: str, indices: List[int]
    ) -> np.ndarray:
        """Get time points by specific indices.

        Args:
            netcdf4_file_path (str): Path to the NetCDF4 file.
            indices (List[int]): List of time indices to retrieve.

        Returns:
            np.ndarray: Array of selected time points.

        """
        try:
            all_time_points = self.get_time_points(netcdf4_file_path)

            # Validate indices
            max_index = len(all_time_points) - 1
            valid_indices = [i for i in indices if 0 <= i <= max_index]

            if len(valid_indices) != len(indices):
                logger.warning(f"Some indices out of range. Valid range: 0-{max_index}")

            return all_time_points[valid_indices]

        except Exception as e:
            logger.error(f"Error reading time points by indices: {e}")
            raise

    def get_time_points_by_range(
        self,
        netcdf4_file_path: str,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> np.ndarray:
        """Get time points by range.

        Args:
            netcdf4_file_path (str): Path to the NetCDF4 file.
            start (Optional[int]): Start index (inclusive).
            end (Optional[int]): End index (exclusive).

        Returns:
            np.ndarray: Array of time points in the specified range.

        """
        try:
            all_time_points = self.get_time_points(netcdf4_file_path)

            return all_time_points[start:end]

        except Exception as e:
            logger.error(f"Error reading time points by range: {e}")
            raise

    def get_variable_data(
        self, netcdf4_file_path: str, variable_name: str
    ) -> np.ndarray:
        """Get all data for a specific variable.

        Args:
            netcdf4_file_path (str): Path to the NetCDF4 file.
            variable_name (str): Name of the variable.

        Returns:
            np.ndarray: Variable data array.

        """
        try:
            if not Path(netcdf4_file_path).exists():
                raise FileNotFoundError(f"NetCDF4 file not found: {netcdf4_file_path}")

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

                # Read all data
                data = variable[:]

                # Handle masked arrays
                if hasattr(data, "mask") and np.ma.is_masked(data):
                    data = np.ma.filled(data, fill_value=np.nan)

                return data

        except Exception as e:
            logger.error(f"Error reading variable data for '{variable_name}': {e}")
            raise

    def get_variable_data_by_indices(
        self, netcdf4_file_path: str, variable_name: str, time_indices: List[int]
    ) -> np.ndarray:
        """Get variable data for specific time indices.

        Args:
            netcdf4_file_path (str): Path to the NetCDF4 file.
            variable_name (str): Name of the variable.
            time_indices (List[int]): List of time indices to retrieve.

        Returns:
            np.ndarray: Variable data for specified time indices.

        """
        try:
            if not Path(netcdf4_file_path).exists():
                raise FileNotFoundError(f"NetCDF4 file not found: {netcdf4_file_path}")

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

                # Identify time dimension
                time_dim_index = None
                for i, dim_name in enumerate(variable.dimensions):
                    if dim_name.lower() in ["time", "t", "time_points"]:
                        time_dim_index = i
                        break

                if time_dim_index is None:
                    # No time dimension, return full data
                    logger.warning(
                        f"No time dimension found for variable '{variable_name}'"
                    )
                    return self.get_variable_data(netcdf4_file_path, variable_name)

                # Build slice tuple
                slices = [slice(None)] * variable.ndim
                slices[time_dim_index] = time_indices

                # Read data with slicing
                data = variable[tuple(slices)]

                # Handle masked arrays
                if hasattr(data, "mask") and np.ma.is_masked(data):
                    data = np.ma.filled(data, fill_value=np.nan)

                return data

        except Exception as e:
            logger.error(
                f"Error reading variable data by indices for '{variable_name}': {e}"
            )
            raise

    def get_variable_data_by_range(
        self,
        netcdf4_file_path: str,
        variable_name: str,
        time_start: Optional[int] = None,
        time_end: Optional[int] = None,
    ) -> np.ndarray:
        """Get variable data for a time range.

        Args:
            netcdf4_file_path (str): Path to the NetCDF4 file.
            variable_name (str): Name of the variable.
            time_start (Optional[int]): Start time index (inclusive).
            time_end (Optional[int]): End time index (exclusive).

        Returns:
            np.ndarray: Variable data for the specified time range.

        """
        try:
            if not Path(netcdf4_file_path).exists():
                raise FileNotFoundError(f"NetCDF4 file not found: {netcdf4_file_path}")

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

                # Identify time dimension
                time_dim_index = None
                for i, dim_name in enumerate(variable.dimensions):
                    if dim_name.lower() in ["time", "t", "time_points"]:
                        time_dim_index = i
                        break

                if time_dim_index is None:
                    # No time dimension, return full data
                    logger.warning(
                        f"No time dimension found for variable '{variable_name}'"
                    )
                    return self.get_variable_data(netcdf4_file_path, variable_name)

                # Build slice tuple
                slices = [slice(None)] * variable.ndim
                slices[time_dim_index] = slice(time_start, time_end)

                # Read data with slicing
                data = variable[tuple(slices)]

                # Handle masked arrays
                if hasattr(data, "mask") and np.ma.is_masked(data):
                    data = np.ma.filled(data, fill_value=np.nan)

                return data

        except Exception as e:
            logger.error(
                f"Error reading variable data by range for '{variable_name}': {e}"
            )
            raise
