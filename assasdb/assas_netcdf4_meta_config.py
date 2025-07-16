"""Configuration for ASSAS NetCDF4 metadata variables and group structure.

This module defines the complete NetCDF4 file structure including data groups
and metadata organization for the ASSAS NetCDF4 format.
"""

# Enhanced domain group configuration with metadata subgroups
DOMAIN_GROUP_CONFIG = {
    "other": {
        "description": "Other variables and metadata",
        "odessa_name": "OTHER",
        "domains": [
            "global",
            "private",
            "cavity",
            "lower_plenum",
        ],
        "subgroups": {
            "global": {
                "description": "Global variables",
                "domains": ["global"],
                # "metadata_vars": ["private_meta"],
            },
            "private": {
                "description": "Private variables",
                "domains": ["private"],
                # "metadata_vars": ["private_meta"],
            },
            "cavity": {
                "description": "Cavity variables",
                "domains": ["cavity"],
                # "metadata_vars": ["private_meta"],
            },
            "lower_plenum": {
                "description": "Lower plenum variables",
                "domains": ["lower_plenum"],
                # "metadata_vars": ["private_meta"],
            },
        },
    },
    "primary": {
        "description": "Primary circuit variables and metadata",
        "odessa_name": "PRIMARY",
        "domains": [
            "primary_wall",
            "primary_volume",
            "primary_junction",
            "primary_pipe",
        ],
        "subgroups": {
            "wall": {
                "description": "Primary circuit thermal variables",
                "domains": ["primary_wall"],
                "metadata_vars": ["primary_wall_meta"],
            },
            "volume": {
                "description": "Primary circuit geometry variables",
                "domains": ["primary_volume"],
                "metadata_vars": ["primary_volume_meta"],
            },
            "junction": {
                "description": "Primary circuit geometry variables",
                "domains": ["primary_junction"],
                "metadata_vars": ["primary_junction_meta"],
            },
            "pipe": {
                "description": "Primary circuit geometry variables",
                "domains": ["primary_pipe"],
                "metadata_vars": ["primary_pipe_meta"],
            },
            "metadata": {
                "description": "Primary circuit metadata",
                "metadata_vars": [
                    "primary_wall_meta",
                    "primary_volume_meta",
                    "primary_junction_meta",
                    "primary_pipe_meta",
                ],
            },
        },
    },
    "secondary": {
        "description": "Secondary circuit variables and metadata",
        "odessa_name": "SECONDAR",
        "domains": [
            "secondar_wall",
            "secondar_volume",
            "secondar_junction",
        ],
        "subgroups": {
            "wall": {
                "description": "Secondary circuit thermal variables",
                "domains": ["secondar_wall"],
                "metadata_vars": ["secondary_wall_meta"],
            },
            "volume": {
                "description": "Secondary circuit volume variables",
                "domains": ["secondar_volume"],
                "metadata_vars": ["secondary_volume_meta"],
            },
            "junction": {
                "description": "Secondary circuit junction variables",
                "domains": ["secondar_junction"],
                "metadata_vars": ["secondary_junction_meta"],
            },
            "metadata": {
                "description": "Secondary circuit metadata",
                "metadata_vars": [
                    "secondary_volume_meta",
                    "secondary_junction_meta",
                    "secondary_wall_meta",
                ],
            },
        },
    },
    "vessel": {
        "description": "Reactor vessel variables and metadata",
        "odessa_name": "VESSEL",
        "domains": [
            "vessel_general",
            "vessel_mesh",
            "vesel_face",
        ],
        "subgroups": {
            "general": {
                "description": "Vessel general variables",
                "domains": ["vessel_general"],
                "metadata_vars": ["vessel_general_meta"],
            },
            "mesh": {
                "description": "Vessel mesh variables",
                "domains": ["vessel_mesh"],
                "metadata_vars": ["vessel_mesh_meta"],
            },
            "face": {
                "description": "Vessel face variables",
                "domains": ["vessel_face"],
                "metadata_vars": ["vessel_face_meta"],
            },
            "metadata": {
                "description": "Vessel metadata",
                "metadata_vars": [
                    "vessel_mesh_meta",
                    "vessel_general_meta",
                    "vessel_face_meta",
                ],
            },
        },
    },
    "connection": {
        "description": "Connection variables and metadata",
        "odessa_name": "CONNECTI",
        "domains": [
            "connecti",
            "connecti_fp",
        ],
        "subgroups": {
            "general": {
                "description": "Connection variables",
                "domains": ["connecti"],
                "metadata_vars": ["connection_meta"],
            },
            "fission": {
                "description": "Connection fission products variables",
                "domains": ["connecti_fp"],
                # "metadata_vars": ["connection_fp_meta"],
            },
            "metadata": {
                "description": "Connection metadata",
                "metadata_vars": [
                    "connection_meta",
                ],
            },
        },
    },
    "containment": {
        "description": "Containment variables and metadata",
        "odessa_name": "CONTAINM",
        "domains": [
            "containment_conn",
            "containment_wall",
            "containment_dome",
            "containment_pool",
            "containment_zone",
            "containment_environ",
            "containment_general",
        ],
        "subgroups": {
            "conn": {
                "description": "Containment conn variables",
                "domains": ["containment_conn"],
                "metadata_vars": ["containment_conn_meta"],
            },
            "wall": {
                "description": "Containment wall variables",
                "domains": ["containment_wall"],
                "metadata_vars": ["containment_wall_meta"],
            },
            "dome": {
                "description": "Containment dome variables",
                "domains": ["containment_dome"],
                # "metadata_vars": ["containment_dome_meta"],
            },
            "pool": {
                "description": "Containment pool variables",
                "domains": ["containment_pool"],
                # "metadata_vars": ["containment_pool_meta"],
            },
            "zone": {
                "description": "Containment zone variables",
                "domains": ["containment_zone"],
                # "metadata_vars": ["containment_pool_meta"],
            },
            "envrion": {
                "description": "Containment environ variables",
                "domains": ["containment_environ"],
                # "metadata_vars": ["containment_pool_meta"],
            },
            "general": {
                "description": "Containment general variables",
                "domains": ["containment_general"],
                # "metadata_vars": ["containment_pool_meta"],
            },
            "metadata": {
                "description": "Connection metadata",
                "metadata_vars": [
                    "containment_conn_meta",
                    "containment_wall_meta",
                ],
            },
        },
    },
}

# Complete metadata variable configuration (updated to match subgroups)
META_DATA_VAR_NAMES = {
    # Primary Circuit Metadata
    "primary_volume_meta": {
        "domain": "PRIMARY",
        "element": "VOLUME",
        "attribute": "NAME",
        "target_group": "primary/volume",
        "description": "Primary circuit volume names and identifiers",
    },
    "primary_junction_meta": {
        "domain": "PRIMARY",
        "element": "JUNCTION",
        "attribute": ["NAME", "NV_DOWN", "NV_UP"],
        "target_group": "primary/junction",
        "description": "Primary circuit junction connections",
    },
    "primary_pump_meta": {
        "domain": "PRIMARY",
        "element": "PUMP",
        "attribute": ["NAME"],
        "target_group": "primary/pipe",
        "description": "Primary circuit pump names",
    },
    "primary_wall_meta": {
        "domain": "PRIMARY",
        "element": "WALL",
        "attribute": ["NAME"],
        "target_group": "primary/wall",
        "description": "Primary circuit wall structure names",
    },
    "primary_pipe_meta": {
        "domain": "PRIMARY",
        "element": "PIPE",
        "attribute": ["NAME"],
        "target_group": "primary/pipe",
        "description": "Primary circuit pipe specifications",
    },
    # Secondary Circuit Metadata
    "secondary_volume_meta": {
        "domain": "SECONDAR",
        "element": "VOLUME",
        "attribute": "NAME",
        "target_group": "secondary/volume",
        "description": "Secondary circuit volume names and identifiers",
    },
    "secondary_junction_meta": {
        "domain": "SECONDAR",
        "element": "JUNCTION",
        "attribute": ["NAME", "NV_DOWN", "NV_UP"],
        "target_group": "secondary/geometry",
        "description": "Secondary circuit junction connections",
    },
    "secondary_pump_meta": {
        "domain": "SECONDAR",
        "element": "PUMP",
        "attribute": ["NAME"],
        "target_group": "secondary/pump",
        "description": "Secondary circuit pump names",
    },
    "secondary_wall_meta": {
        "domain": "SECONDAR",
        "element": "WALL",
        "attribute": ["NAME"],
        "target_group": "secondary/wall",
        "description": "Secondary circuit wall structure names",
    },
    "vessel_mesh_meta": {
        "domain": "VESSEL",
        "element": "MESH",
        "attribute": ["NAME"],
        "target_group": "vessel/mesh",
        "description": "Vessel mesh element names",
    },
    "vessel_face_meta": {
        "domain": "VESSEL",
        "element": "FACE",
        "attribute": ["NAME"],
        "target_group": "vessel/face",
        "description": "Vessel face element names",
    },
    "connection_meta": {
        "domain": None,
        "element": "CONNECTI",
        "attribute": ["NAME", "FROM", "TO", "TYPE"],
        "target_group": "connection/metadata",
        "description": "Connection topology and types",
    },
    "connection_valves_meta": {
        "domain": "CONNECTI",
        "element": "VALVE",
        "attribute": ["NAME"],
    },
}


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
