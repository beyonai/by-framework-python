"""Logging utilities for by-framework."""

import json
import logging
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler


class JSONFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.

    Outputs log records as JSON for easier parsing by log aggregation systems.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add extra fields if present
        if hasattr(record, "worker_id"):
            log_data["worker_id"] = record.worker_id
        if hasattr(record, "message_id"):
            log_data["message_id"] = record.message_id
        if hasattr(record, "session_id"):
            log_data["session_id"] = record.session_id
        if hasattr(record, "execution_id"):
            log_data["execution_id"] = record.execution_id

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


def setup_logging(
    name: str = "by-framework",
    level: int = logging.INFO,
    use_json: bool = False,
    log_file: str | None = "by-framework.log",
) -> logging.Logger:
    """
    Set up unified logging configuration

    Args:
        name: Logger name
        level: Log level
        use_json: Whether to use JSON formatted output
        log_file: Log file path, None means no file output

    Returns:
        Configured logger object
    """
    # pylint: disable=redefined-outer-name
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    # Prevent duplicate handlers
    if logger.handlers:
        return logger

    # Select formatter
    if use_json:
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - "
            "%(filename)s:%(lineno)d - %(message)s"
        )

    # Console output handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File output handler
    if log_file:
        file_handler = RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get logger with the specified name.

    Args:
        name: Logger name, typically using __name__

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


# Expose default logger
logger = setup_logging()
