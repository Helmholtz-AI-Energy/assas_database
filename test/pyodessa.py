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
        self._data = {}

    def len(self, name):
        # Return a dummy length for any structure
        return 2

    def get(self, name):
        # Return dummy data based on the name
        # For demonstration, return a list, float, or another OdessaBase as needed
        if (
            "MESH" in name
            or "FACE" in name
            or "PIPE" in name
            or "WALL" in name
            or "VOLUME" in name
            or "JUNCTION" in name
            or "CONNECTI" in name
            or "SOURCE" in name
            or "PUMP" in name
            or "VALVE" in name
        ):
            return [1.0, 2.0]
        if "THER" in name or "GEOM" in name or "QMAV" in name:
            return [3.0]
        if "GENERAL" in name:
            return 42.0
        return 1.0
