"""Component for managing units in NetCDF4 variables with ASTEC-specific mappings.

This module provides functionality to normalize, validate, and convert units,
as well as to map ASTEC units to CF-compliant units.
It uses the `cf_units` and `pint` libraries for unit handling.
"""

import cf_units
import pint
import numpy as np
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class AssasUnitManager:
    """Manage units for NetCDF4 variables with ASTEC-specific mappings."""

    def __init__(self) -> None:
        """Initialize the unit manager with ASTEC and CF-compliant units."""
        self.cf_units = cf_units
        self.pint_registry = pint.UnitRegistry()

        # Common ASTEC unit mappings to CF-compliant units
        self.astec_unit_mapping = {
            "[K]": "kelvin",
            "[C]": "celsius",
            "[Pa]": "pascal",
            "[kg/m3]": "kg m-3",
            "[kg/m^3]": "kg m-3",
            "[m/s]": "m s-1",
            "[J/kg]": "J kg-1",
            "[W/m2]": "W m-2",
            "[W/m^2]": "W m-2",
            "[kg/s]": "kg s-1",
            "[m]": "meter",
            "[s]": "second",
            "[Bq]": "becquerel",
            "[Bq/h]": "Bq h-1",
            "[%]": "percent",
            "[-]": "dimensionless",
            "N/A": "dimensionless",
            "": "dimensionless",
            "none": "dimensionless",
        }

        # CF standard names mapping
        self.cf_standard_names = {
            "temperature": "temperature",
            "pressure": "air_pressure",
            "density": "density",
            "velocity": "velocity",
            "mass": "mass",
            "time": "time",
        }

    def normalize_unit_string(self, unit_str: str) -> str:
        """Normalize unit string to CF-compliant format."""
        if not unit_str or unit_str.strip() == "":
            return "dimensionless"

        # Clean and normalize
        clean_unit = unit_str.strip()

        # Check ASTEC mappings first
        if clean_unit in self.astec_unit_mapping:
            normalized = self.astec_unit_mapping[clean_unit]
            logger.debug(f"Mapped ASTEC unit '{clean_unit}' to '{normalized}'")
            return normalized

        # Try to parse with cf-units
        try:
            cf_unit = cf_units.Unit(clean_unit)
            return str(cf_unit)
        except Exception as e:
            logger.debug(f"CF-units parsing failed for '{clean_unit}': {e}")
            pass

        # Try with pint
        try:
            pint_unit = self.pint_registry.parse_expression(clean_unit)
            return str(pint_unit.units)
        except Exception as e:
            logger.debug(f"Pint parsing failed for '{clean_unit}': {e}")
            pass

        # Return original if all else fails
        logger.warning(f"Could not normalize unit: {unit_str}")
        return clean_unit

    def validate_unit(self, unit_str: str) -> Tuple[bool, str, Optional[str]]:
        """Validate unit string and return normalized version."""
        normalized = self.normalize_unit_string(unit_str)

        try:
            cf_unit = cf_units.Unit(normalized)
            return True, str(cf_unit), None
        except Exception as e:
            try:
                pint_unit = self.pint_registry.parse_expression(normalized)
                return True, str(pint_unit.units), "pint_only"
            except Exception as pe:
                return False, normalized, f"CF error: {e}, Pint error: {pe}"

    def get_cf_standard_name(self, var_name: str, units: str) -> Optional[str]:
        """Get CF standard name for common variables."""
        var_lower = var_name.lower()

        for key, standard_name in self.cf_standard_names.items():
            if key in var_lower:
                return standard_name

        return None

    def convert_units(
        self, value: np.ndarray, from_unit: str, to_unit: str
    ) -> np.ndarray:
        """Convert values between units."""
        try:
            # Try CF-units first
            cf_from = cf_units.Unit(from_unit)
            cf_to = cf_units.Unit(to_unit)
            return cf_from.convert(value, cf_to)
        except Exception as e:
            logger.debug(
                f"CF-units conversion failed from {from_unit} to {to_unit}: {e}."
            )
            try:
                # Try Pint
                quantity = value * self.pint_registry.parse_expression(from_unit)
                converted = quantity.to(to_unit)
                return converted.magnitude
            except Exception as e:
                logger.error(
                    f"Unit conversion failed from {from_unit} to {to_unit}: {e}."
                )
                return value
