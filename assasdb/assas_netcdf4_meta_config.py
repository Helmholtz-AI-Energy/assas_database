"""Configuration for ASSAS NetCDF4 metadata variables.

This module defines the metadata variable names and their associated
domains, elements, and attributes for the ASSAS NetCDF4 format.
This configuration is used to map the metadata variables to their
corresponding elements and attributes in the ASSAS database.
"""

META_DATA_VAR_NAMES = {
    "primary_volume_meta": {
        "domain": "PRIMARY",
        "element": "VOLUME",
        "attribute": "NAME",
    },
    "secondary_volume_meta": {
        "domain": "SECONDAR",
        "element": "VOLUME",
        "attribute": "NAME",
    },
    "primary_junction_meta": {
        "domain": "PRIMARY",
        "element": "JUNCTION",
        "attribute": ["NAME", "NV_DOWN", "NV_UP"],
    },
    "secondary_junction_meta": {
        "domain": "SECONDAR",
        "element": "JUNCTION",
        "attribute": ["NAME", "NV_DOWN", "NV_UP"],
    },
    "connection_valves_meta": {
        "domain": "CONNECTI",
        "element": "VALVE",
        "attribute": ["NAME"],
    },
    "connection_meta": {
        "domain": None,
        "element": "CONNECTI",
        "attribute": ["NAME", "FROM", "TO", "TYPE"],
    },
    "primary_pump_meta": {
        "domain": "PRIMARY",
        "element": "PUMP",
        "attribute": ["NAME"],
    },
    "secondary_pump_meta": {
        "domain": "SECONDAR",
        "element": "PUMP",
        "attribute": ["NAME"],
    },
    "primary_wall_meta": {
        "domain": "PRIMARY",
        "element": "WALL",
        "attribute": ["NAME"],
    },
    "secondary_wall_meta": {
        "domain": "SECONDAR",
        "element": "WALL",
        "attribute": ["NAME"],
    },
    "vessel_mesh_meta": {"domain": "VESSEL", "element": "MESH", "attribute": ["NAME"]},
}
