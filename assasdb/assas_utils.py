"""Utility functions for ASSAS database.

This module provides utility functions and classes for handling durations
and converting seconds into a more human-readable format.
"""

import sys
import os
import logging

from collections import namedtuple
from typing import Iterator
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from dotenv import load_dotenv


def find_env_file() -> Path | None:
    """Find .env deterministically without importing assasdb."""
    explicit = os.getenv("ASSAS_ENV_FILE")
    if explicit:
        p = Path(explicit).expanduser().resolve()
        return p if p.exists() else None

    # Search upwards from this file
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        candidate = parent / ".env"
        if candidate.exists():
            return candidate.resolve()

    # Fallback: cwd
    candidate = (Path.cwd() / ".env").resolve()
    return candidate if candidate.exists() else None


def redact_mongo_uri(uri: str) -> str:
    """Redact credentials in mongodb://user:pass@host URIs before logging."""
    if not uri:
        return ""
    try:
        parts = urlsplit(uri)
        netloc = parts.netloc
        if "@" in netloc and ":" in netloc.split("@", 1)[0]:
            creds, host = netloc.split("@", 1)
            user = creds.split(":", 1)[0]
            netloc = f"{user}:***@{host}"
            return urlunsplit(
                (parts.scheme, netloc, parts.path, parts.query, parts.fragment)
            )
        return uri
    except Exception:
        return "<redacted>"


def require_env(
    logger: logging.Logger, keys: list[str], env_path: Path | None
) -> dict[str, str]:
    """Fetch required env vars, exit non-zero if any are missing/empty."""
    values: dict[str, str] = {}
    missing: list[str] = []
    for k in keys:
        v = (os.getenv(k) or "").strip()
        if not v:
            missing.append(k)
        else:
            values[k] = v

    if missing:
        logger.error("Missing required environment variables: %s", ", ".join(missing))
        logger.error(
            "Tip: ensure your .env is loaded (loaded: %s).",
            str(env_path) if env_path else "no",
        )
        sys.exit(2)

    return values


def load_assas_env() -> Path | None:
    """Load .env from a deterministic location and return the resolved path.

    Return:
        Path | None: The path to the loaded .env file, or None if not found.

    """
    # 1) Explicit override (best for cron)
    explicit = os.getenv("ASSAS_ENV_FILE")
    if explicit:
        p = Path(explicit).expanduser().resolve()
        if p.exists():
            load_dotenv(p, override=False)
            return p

    # 2) Search upwards from this file for ".env" (repo layout safe)
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        candidate = parent / ".env"
        if candidate.exists():
            load_dotenv(candidate, override=False)
            return candidate.resolve()

    # 3) Fallback: current working directory
    cwd_candidate = (Path.cwd() / ".env").resolve()
    if cwd_candidate.exists():
        load_dotenv(cwd_candidate, override=False)
        return cwd_candidate

    return None


class Duration(namedtuple("Duration", "weeks, days, hours, minutes, seconds")):
    """Represent a duration in weeks, days, hours, minutes, and seconds.

    This class provides a human-readable string representation of the duration.
    """

    def __str__(self) -> str:
        """Return a human-readable string representation of the duration.

        The string will only include units that have a non-zero value.
        For example, "1 week, 2 days, 3 hours, 4 minutes, 5 seconds".

        Returns:
            str: A formatted string representing the duration, with non-zero units.

        """
        return ", ".join(self._get_formatted_units())

    def _get_formatted_units(self) -> Iterator[str]:
        """Generate a list of formatted strings for each non-zero unit of the duration.

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
    """Convert a number of seconds into a Duration object.

    This function takes a total number of seconds and converts it into a more
    human-readable format, breaking it down into weeks, days, hours, minutes,
    and seconds. The resulting Duration object can be used to easily access
    each component of the time duration.

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
