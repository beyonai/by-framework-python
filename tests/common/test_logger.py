import json
import logging

# logger is now pre-configured or exported
from by_framework.common.logger import (
    JSONFormatter,
    get_logger,
    observability_log_extra,
    setup_logging,
)


class TestLogger:
    """Test logging functionality."""

    def test_logger_initialization(self):
        """Test that the logger can be initialized correctly."""
        lg = setup_logging()
        assert isinstance(lg, logging.Logger)
        assert lg.name == "by-framework"
        assert lg.level == logging.INFO

    def test_logger_with_custom_name(self):
        """Test creating a logger with a custom name."""
        lg = setup_logging(name="custom-logger")
        assert lg.name == "custom-logger"

    def test_logger_with_custom_level(self):
        """Test creating a logger with a custom log level."""
        lg = setup_logging(level=logging.DEBUG)
        assert lg.level == logging.DEBUG

    def test_logger_handlers(self):
        """Default setup_logging has only a console handler (log_file=None)."""
        lg = setup_logging()
        assert len(lg.handlers) == 1

        has_console_handler = any("StreamHandler" in str(type(h)) for h in lg.handlers)
        assert has_console_handler, "Console handler not configured"

    def test_logger_handlers_with_file(self, tmp_path):
        """Passing log_file adds a rotating file handler alongside the console one."""
        log_path = str(tmp_path / "test.log")
        lg = setup_logging(name="by-framework-file-test", log_file=log_path)
        assert len(lg.handlers) == 2

        has_console = any("StreamHandler" in str(type(h)) for h in lg.handlers)
        has_file = any("RotatingFileHandler" in str(type(h)) for h in lg.handlers)
        assert has_console, "Console handler not configured"
        assert has_file, "File handler not configured"

    def test_logger_formatter(self):
        """Test that log format is correctly configured."""
        lg = setup_logging()

        for handler in lg.handlers:
            assert isinstance(handler.formatter, logging.Formatter)
            # Check if format includes necessary elements
            assert "%(asctime)s" in handler.formatter._fmt
            assert "%(name)s" in handler.formatter._fmt
            assert "%(levelname)s" in handler.formatter._fmt
            assert "%(filename)s:%(lineno)d" in handler.formatter._fmt
            assert "%(message)s" in handler.formatter._fmt

    def test_logger_log_methods(self, capsys):
        """Test that logger's log methods work."""
        lg = setup_logging(name="test-logger", level=logging.DEBUG)

        # Test logs at different levels
        lg.debug("Debug message")
        lg.info("Info message")
        lg.warning("Warning message")
        lg.error("Error message")
        lg.critical("Critical message")

        captured = capsys.readouterr()
        assert "Debug message" in captured.err or "Debug message" in captured.out
        assert "Info message" in captured.err or "Info message" in captured.out
        assert "Warning message" in captured.err or "Warning message" in captured.out
        assert "Error message" in captured.err or "Error message" in captured.out
        assert "Critical message" in captured.err or "Critical message" in captured.out

    def test_logger_no_duplicate_handlers(self):
        """Test setup_logging called multiple times does not add duplicate handlers."""
        lg = setup_logging()
        initial_handler_count = len(lg.handlers)

        # Call setup_logging again
        lg = setup_logging()
        assert len(lg.handlers) == initial_handler_count

    def test_get_logger_uses_unified_formatter(self):
        """Package loggers should not fall back to bare Python logging output."""
        lg = get_logger("by_framework_trace_langfuse.langfuse")

        assert lg.handlers
        assert lg.propagate is False
        formatter = lg.handlers[0].formatter
        assert isinstance(formatter, logging.Formatter)
        assert "%(asctime)s" in formatter._fmt
        assert "%(name)s" in formatter._fmt
        assert "%(levelname)s" in formatter._fmt
        assert "%(filename)s:%(lineno)d" in formatter._fmt
        assert "%(message)s" in formatter._fmt

    def test_observability_log_extra_formats_structured_correlation_fields(self):
        """Explicit log extras should use stable observability field names."""
        extra = observability_log_extra(
            trace_id="trace-1",
            session_id="sess-1",
            execution_id="exec-1",
            message_id="msg-1",
            worker_id="worker-1",
            agent_type="planner",
            ignored="value",
            empty="",
        )
        record = logging.LogRecord(
            name="test-logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=21,
            msg="structured event",
            args=(),
            exc_info=None,
        )
        for key, value in extra["extra"].items():
            setattr(record, key, value)

        payload = json.loads(JSONFormatter().format(record))

        assert payload["trace_id"] == "trace-1"
        assert payload["session_id"] == "sess-1"
        assert payload["execution_id"] == "exec-1"
        assert payload["message_id"] == "msg-1"
        assert payload["worker_id"] == "worker-1"
        assert payload["agent_type"] == "planner"
        assert "ignored" not in payload
        assert "empty" not in payload
