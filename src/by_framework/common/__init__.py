"""Common utilities and shared components for by-framework."""

from by_framework.errors import (
    CommandValidationError,
    ExecutionDataError,
    ExecutionNotFoundError,
    FrameworkError,
    MessageDataNotFoundError,
    MessageParseError,
    RedisConnectionError,
    SessionMismatchError,
    StreamGroupExistsError,
    TerminalStateError,
    UnsupportedCommandError,
    WorkerLockError,
    WorkerNotFoundError,
    WorkerRegistryNotSetError,
)

from .config import (
    FrameworkConfig,
    LoggingConfig,
    RedisConfig,
    WorkerConfig,
    get_config,
    init_config,
)
from .constants import RedisKeys
from .emitter import (DataLayoutBuilder, DefaultSseLayoutBuilder, GatewayDataEmitter)
from .logger import get_logger, logger, setup_logging
from .redis_client import (
    Redis,
    close_redis,
    get_redis,
    init_redis,
    init_redis_from_url,
)

__all__ = [
    "RedisKeys",
    "logger",
    "get_logger",
    "setup_logging",
    "get_redis",
    "init_redis",
    "init_redis_from_url",
    "close_redis",
    "Redis",
    "DataLayoutBuilder",
    "DefaultSseLayoutBuilder",
    "GatewayDataEmitter",
    # Config
    "FrameworkConfig",
    "RedisConfig",
    "WorkerConfig",
    "LoggingConfig",
    "get_config",
    "init_config",
    # Exceptions
    "FrameworkError",
    "RedisConnectionError",
    "StreamGroupExistsError",
    "ExecutionNotFoundError",
    "ExecutionDataError",
    "SessionMismatchError",
    "TerminalStateError",
    "UnsupportedCommandError",
    "MessageParseError",
    "MessageDataNotFoundError",
    "WorkerNotFoundError",
    "WorkerLockError",
    "WorkerRegistryNotSetError",
    "CommandValidationError",
]
