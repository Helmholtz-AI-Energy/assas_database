from collections import namedtuple


class Duration(namedtuple("Duration", "weeks, days, hours, minutes, seconds")):
    """
    Represents a duration in weeks, days, hours, minutes, and seconds.
    This class provides a human-readable string representation of the duration.
    """

    def __str__(self) -> str:
        """
        Returns a human-readable string representation of the duration.
        The string will only include units that have a non-zero value.
        For example, "1 week, 2 days, 3 hours, 4 minutes, 5 seconds".
        """
        return ", ".join(self._get_formatted_units())

    def _get_formatted_units(self):  # TODO: fix type hint
        """
        Generates a list of formatted strings for each non-zero unit of the duration.
        Each unit is represented as "<value> <unit_name>", where the unit name is
        singular if the value is 1, and plural otherwise.

        Yields:
            str: A formatted string for each non-zero unit of the duration.
        """
        for unit_name, value in self._asdict().items():
            if value > 0:
                if value == 1:
                    unit_name = unit_name.rstrip("s")
                yield "{} {}".format(value, unit_name)


def get_duration(seconds: int) -> Duration:
    """
    Converts a number of seconds into a Duration object representing weeks, days,
    hours, minutes, and seconds.

    Args:
        seconds (int): The total number of seconds to convert.

    Returns:
        Duration: An instance of the Duration class representing the converted time.
    """

    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    weeks, days = divmod(days, 7)

    return Duration(weeks, days, hours, minutes, seconds)
