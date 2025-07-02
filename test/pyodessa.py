"""PyOdessa: Dummy PyOdessa Interface.

This module provides a dummy interface for PyOdessa, which is used for testing purposes.
It includes a dummy R1 class, a function to get saving times, and a function to
restore an OdessaBase object.
"""


class R1(list):
    """Dummy R1 class, behaves like a list."""

    pass


def get_saving_times(input_path):
    """Return a dummy list of time points."""
    return [0, 1, 2, 3]


def restore(input_path, time_point):
    """Return a dummy OdessaBase object."""
    return OdessaBase()


class OdessaBase:
    """Dummy OdessaBase object with minimal interface for testing."""

    def __init__(self):
        """Initialize an empty data structure."""
        print("Initializing OdessaBase with empty data structure")
        self._data = {}

    def len(self, name=None):
        """Return a dummy length for any structure."""
        # Return a dummy length for any structure
        return 2

    def __getitem__(self, key):
        """Return dummy data for testing; adjust as needed."""
        if isinstance(key, int):
            # If key is an integer, return a dummy list
            return [1.0]
        return [1.0, 2.0]  # or whatever makes sense for your test

    def get(self, name):
        """Return dummy data based on the name."""
        # Return dummy data based on the name
        # For demonstration, return a list, float, or another OdessaBase as needed
        # if (
        #    "MESH" in name
        #    or "FACE" in name
        #    or "PIPE" in name
        #    or "WALL" in name
        #    or "VOLUME" in name
        #    or "JUNCTION" in name
        #    or "CONNECTI" in name
        #    or "SOURCE" in name
        #    or "PUMP" in name
        #    or "VALVE" in name
        # ):
        #    return [1.0, 2.0]
        # if "THER" in name or "GEOM" in name or "QMAV" in name:
        #    return np.array([1.0, 2.0])
        # if "TIME" in name:
        #    return R1([1.0, 2.0])
        # if "GENERAL" in name:
        #    return np.array([1.0, 2.0])
        # if "ZONE" in name:
        #    return OdessaBase()
        # if (
        #    "CONTAINM" in name
        #    or "COMP" in name
        # or "RADIATION" in name
        # or "HEATFLUX" in name
        # or "SURFACE" in name
        # ):
        return OdessaBase()
