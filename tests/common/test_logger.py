import logging

# logger is now pre-configured or exported
from by_framework import setup_logging


class TestLogger:
    """测试日志功能"""

    def test_logger_initialization(self):
        """测试日志记录器是否能正确初始化"""
        lg = setup_logging()
        assert isinstance(lg, logging.Logger)
        assert lg.name == "gateway-sdk"
        assert lg.level == logging.INFO

    def test_logger_with_custom_name(self):
        """测试使用自定义名称创建日志记录器"""
        lg = setup_logging(name="custom-logger")
        assert lg.name == "custom-logger"

    def test_logger_with_custom_level(self):
        """测试使用自定义日志级别创建日志记录器"""
        lg = setup_logging(level=logging.DEBUG)
        assert lg.level == logging.DEBUG

    def test_logger_handlers(self):
        """测试日志记录器是否配置了正确的处理器"""
        lg = setup_logging()
        assert len(lg.handlers) == 2

        # 检查是否有控制台处理器和文件处理器
        has_console_handler = False
        has_file_handler = False

        for handler in lg.handlers:
            handler_type = str(type(handler))
            if "StreamHandler" in handler_type:
                has_console_handler = True
            elif "RotatingFileHandler" in handler_type:
                has_file_handler = True

        assert has_console_handler, "控制台处理器未配置"
        assert has_file_handler, "文件处理器未配置"

    def test_logger_formatter(self):
        """测试日志格式是否正确配置"""
        lg = setup_logging()

        for handler in lg.handlers:
            assert isinstance(handler.formatter, logging.Formatter)
            # 检查格式是否包含必要的元素
            assert "%(asctime)s" in handler.formatter._fmt
            assert "%(name)s" in handler.formatter._fmt
            assert "%(levelname)s" in handler.formatter._fmt
            assert "%(filename)s:%(lineno)d" in handler.formatter._fmt
            assert "%(message)s" in handler.formatter._fmt

    def test_logger_log_methods(self, capsys):
        """测试日志记录器的日志方法是否工作"""
        lg = setup_logging(name="test-logger", level=logging.DEBUG)

        # 测试不同级别的日志
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
        """测试多次调用 setup_logging 不会添加重复的处理器"""
        lg = setup_logging()
        initial_handler_count = len(lg.handlers)

        # 再次调用 setup_logging
        lg = setup_logging()
        assert len(lg.handlers) == initial_handler_count
