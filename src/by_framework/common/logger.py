"""Logging utilities for by-framework."""

import json
import logging
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler


class ContextFilter(logging.Filter):
    """
    Filter to enrich log records with current AgentContext variables.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            from by_framework.worker.context import (
                current_agent_context_var,
                current_worker_id_var,
            )

            ctx = current_agent_context_var.get()
            if ctx is not None:
                for attr, ctx_field in [
                    ("trace_id", "trace_id"),
                    ("session_id", "session_id"),
                    ("message_id", "message_id"),
                    ("execution_id", "execution_id"),
                    ("agent_type", "current_agent_id"),
                ]:
                    if not hasattr(record, attr):
                        val = getattr(ctx, ctx_field, None)
                        setattr(record, attr, val or "")
            else:
                for attr in (
                    "trace_id",
                    "session_id",
                    "message_id",
                    "execution_id",
                    "agent_type",
                ):
                    if not hasattr(record, attr):
                        setattr(record, attr, "")

            # Inject worker_id from dedicated context var set by the runner.
            if not hasattr(record, "worker_id") or not record.worker_id:
                record.worker_id = current_worker_id_var.get()
        except Exception:  # pylint: disable=broad-exception-caught
            pass

        # Inject OTel span_id when tracing is active (lazy import, no hard dep).
        if not getattr(record, "span_id", ""):
            try:
                from opentelemetry import trace as _otel_trace

                span_ctx = _otel_trace.get_current_span().get_span_context()
                record.span_id = f"{span_ctx.span_id:016x}" if span_ctx.is_valid else ""
            except Exception:  # pylint: disable=broad-exception-caught
                record.span_id = ""

        return True


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

        # Add extra fields if present and non-empty
        for key in (
            "worker_id",
            "message_id",
            "session_id",
            "trace_id",
            "execution_id",
            "agent_type",
            "task_group_id",
            "span_id",
        ):
            val = getattr(record, key, None)
            if val:
                log_data[key] = val

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


def setup_logging(
    name: str = "by-framework",
    level: int = logging.INFO,
    use_json: bool = False,
    log_file: str | None = None,
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
    console_handler.addFilter(ContextFilter())
    logger.addHandler(console_handler)

    # File output handler
    if log_file:
        file_handler = RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        file_handler.addFilter(ContextFilter())
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
    base_logger = setup_logging()
    if name == base_logger.name:
        return base_logger

    child_logger = logging.getLogger(name)
    child_logger.setLevel(base_logger.level)
    child_logger.propagate = False

    if not child_logger.handlers:
        for handler in base_logger.handlers:
            child_logger.addHandler(handler)

    return child_logger


# Expose default logger
logger = setup_logging()
