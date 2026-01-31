"""ASSAS Logging Module with Rolling File Handler."""

import sys
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler


class AssasLoggingConfig:
    """Configure logging for ASSAS application."""

    DEFAULT_LOG_DIR = Path("logs")
    DEFAULT_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
    DEFAULT_BACKUP_COUNT = 5
    DEFAULT_FORMAT = "%(asctime)s %(process)d %(name)s %(levelname)s: %(message)s"
    DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(
        self,
        log_dir: Path = None,
        log_level: int = logging.INFO,
        max_bytes: int = DEFAULT_MAX_BYTES,
        backup_count: int = DEFAULT_BACKUP_COUNT,
        use_timed_rotation: bool = False,
        when: str = "midnight",  # For TimedRotatingFileHandler
    ) -> None:
        """Initialize logging configuration.

        Args:
            log_dir: Directory to store log files
            log_level: Logging level (e.g., logging.INFO)
            max_bytes: Max size of log file before rotation (for RotatingFileHandler)
            backup_count: Number of backup files to keep
            use_timed_rotation: Use time-based rotation instead of size-based
            when: When to rotate (for TimedRotatingFileHandler, e.g., 'midnight', 'H')

        """
        self.log_dir = log_dir or self.DEFAULT_LOG_DIR
        self.log_level = log_level
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.use_timed_rotation = use_timed_rotation
        self.when = when

        # Create logs directory
        self.log_dir.mkdir(exist_ok=True, parents=True)

    def create_formatter(self) -> logging.Formatter:
        """Create log formatter."""
        return logging.Formatter(
            fmt=self.DEFAULT_FORMAT,
            datefmt=self.DEFAULT_DATE_FORMAT,
        )

    def create_console_handler(self) -> logging.StreamHandler:
        """Create console handler."""
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(self.log_level)
        handler.setFormatter(self.create_formatter())
        return handler

    def create_rotating_file_handler(self, log_filename: str) -> RotatingFileHandler:
        """Create rotating file handler based on size."""
        log_file = self.log_dir / log_filename
        handler = RotatingFileHandler(
            filename=log_file,
            maxBytes=self.max_bytes,
            backupCount=self.backup_count,
        )
        handler.setLevel(self.log_level)
        handler.setFormatter(self.create_formatter())
        return handler

    def create_timed_rotating_file_handler(
        self, log_filename: str
    ) -> TimedRotatingFileHandler:
        """Create timed rotating file handler (e.g., daily rotation)."""
        log_file = self.log_dir / log_filename
        handler = TimedRotatingFileHandler(
            filename=log_file,
            when=self.when,
            interval=1,
            backupCount=self.backup_count,
        )
        handler.setLevel(self.log_level)
        handler.setFormatter(self.create_formatter())
        return handler

    def setup_root_logger(self) -> logging.Logger:
        """Configure root logger with console and file handlers."""
        root_logger = logging.getLogger()
        root_logger.setLevel(self.log_level)

        # Remove existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Add console handler
        root_logger.addHandler(self.create_console_handler())

        # Add file handler
        log_filename = "assas.log"
        if self.use_timed_rotation:
            root_logger.addHandler(
                self.create_timed_rotating_file_handler(log_filename)
            )
        else:
            root_logger.addHandler(self.create_rotating_file_handler(log_filename))

        return root_logger

    def setup_logger(self, name: str, filename: str = None) -> logging.Logger:
        """Configure specific logger with its own file handler.

        Args:
            name: Logger name (e.g., 'assas_app', 'assas_database')
            filename: Optional custom log filename

        Returns:
            Configured logger instance

        """
        logger = logging.getLogger(name)
        logger.setLevel(self.log_level)

        # Remove existing handlers to avoid duplicates
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        # Don't propagate to root logger if we're adding file handler
        logger.propagate = False

        # Add console handler
        logger.addHandler(self.create_console_handler())

        # Add file handler
        log_filename = filename or f"{name}.log"
        if self.use_timed_rotation:
            logger.addHandler(self.create_timed_rotating_file_handler(log_filename))
        else:
            logger.addHandler(self.create_rotating_file_handler(log_filename))
        return logger


def get_logger(name: str) -> logging.Logger:
    """Get logger by name (assumes logging is already configured)."""
    return logging.getLogger(name)
