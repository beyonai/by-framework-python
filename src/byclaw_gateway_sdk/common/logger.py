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
    name: str = "gateway-sdk",
    level: int = logging.INFO,
    use_json: bool = False,
    log_file: str | None = "gateway-sdk.log",
) -> logging.Logger:
    """
    设置统一的日志配置

    Args:
        name: 日志名称
        level: 日志级别
        use_json: 是否使用 JSON 格式化输出
        log_file: 日志文件路径，None 则不写文件

    Returns:
        配置好的 logger 对象
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    # 防止重复添加处理器
    if logger.handlers:
        return logger

    # 选择格式化器
    if use_json:
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
        )

    # 控制台输出处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 文件输出处理器
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
    获取指定名称的 logger。

    Args:
        name: logger 名称，通常使用 __name__

    Returns:
        Logger 实例
    """
    return logging.getLogger(name)


# 暴露默认 logger
logger = setup_logging()
