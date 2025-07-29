"""Configuration for ASSAS NetCDF4 metadata variables and group structure.

This module defines the complete NetCDF4 file structure including data groups
and metadata organization for the ASSAS NetCDF4 format.
"""

import json
import pkg_resources

with pkg_resources.resource_stream(
    __name__, "astec_config/assas_netcdf4_domain_group_config.json"
) as json_file:
    DOMAIN_GROUP_CONFIG = json.load(json_file)
with pkg_resources.resource_stream(
    __name__, "astec_config/assas_netcdf4_meta_data_var_names.json"
) as json_file:
    META_DATA_VAR_NAMES = json.load(json_file)


# Utility function to get metadata variables for a group
def get_metadata_vars_for_group(group_name: str, subgroup_name: str = None) -> list:
    """Get metadata variable names for a specific group/subgroup."""
    if group_name not in DOMAIN_GROUP_CONFIG:
        return []

    group_config = DOMAIN_GROUP_CONFIG[group_name]

    if subgroup_name and "subgroups" in group_config:
        if subgroup_name in group_config["subgroups"]:
            subgroup_config = group_config["subgroups"][subgroup_name]
            return subgroup_config.get("metadata_vars", [])

    # Return all metadata vars for the group
    metadata_vars = []
    if "subgroups" in group_config:
        for sg_name, sg_config in group_config["subgroups"].items():
            if "metadata_vars" in sg_config:
                metadata_vars.extend(sg_config["metadata_vars"])

    return metadata_vars


# Function to get target group for metadata variable
def get_target_group_for_metadata(meta_var_name: str) -> str:
    """Get the target group path for a metadata variable."""
    if meta_var_name in META_DATA_VAR_NAMES:
        return META_DATA_VAR_NAMES[meta_var_name].get("target_group", "global_metadata")
    return "global_metadata"


# Function to get all subgroups with metadata
def get_all_metadata_subgroups() -> dict:
    """Get all subgroups that contain metadata variables."""
    metadata_subgroups = {}

    for group_name, group_config in DOMAIN_GROUP_CONFIG.items():
        if "subgroups" in group_config:
            for subgroup_name, subgroup_config in group_config["subgroups"].items():
                if (
                    "metadata_vars" in subgroup_config
                    and subgroup_config["metadata_vars"]
                ):
                    full_path = f"{group_name}/{subgroup_name}"
                    metadata_subgroups[full_path] = {
                        "description": subgroup_config["description"],
                        "metadata_vars": subgroup_config["metadata_vars"],
                        "domains": subgroup_config.get("domains", []),
                    }

    return metadata_subgroups


# Function to validate metadata configuration
def validate_metadata_config() -> list:
    """Validate that all metadata variables have corresponding target groups."""
    errors = []

    for meta_var_name, meta_config in META_DATA_VAR_NAMES.items():
        target_group = meta_config.get("target_group")
        if not target_group:
            errors.append(f"Missing target_group for {meta_var_name}")
            continue

        # Parse target group path
        if "/" in target_group:
            group_name, subgroup_name = target_group.split("/", 1)
        else:
            group_name, subgroup_name = target_group, None

        # Check if group exists
        if group_name not in DOMAIN_GROUP_CONFIG:
            errors.append(f"Target group '{group_name}' not found for {meta_var_name}")
            continue

        # Check if subgroup exists
        if subgroup_name:
            group_config = DOMAIN_GROUP_CONFIG[group_name]
            if (
                "subgroups" not in group_config
                or subgroup_name not in group_config["subgroups"]
            ):
                errors.append(
                    f"Target subgroup '{subgroup_name}' not found "
                    f"in group '{group_name}' for {meta_var_name}"
                )

    return errors
