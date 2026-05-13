"""By-Framework unified error handling.

All framework-level exceptions are defined in this package.
"""

from by_framework.errors.base import FrameworkError
from by_framework.errors.common import (RedisConnectionError, StreamGroupExistsError)
from by_framework.errors.execution import (
    ExecutionDataError,
    ExecutionNotFoundError,
    SessionMismatchError,
    TerminalStateError,
)
from by_framework.errors.http import (
    DiscoveryHttpClientError,
    HttpClientError,
    HttpRequestError,
)
from by_framework.errors.protocol import (
    CommandValidationError,
    MessageDataNotFoundError,
    MessageParseError,
    UnsupportedCommandError,
)
from by_framework.errors.registry import (
    WorkerLockError,
    WorkerNotFoundError,
    WorkerRegistryNotSetError,
)

__all__ = [
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
    "CommandValidationError",
    "WorkerNotFoundError",
    "WorkerLockError",
    "WorkerRegistryNotSetError",
    "HttpClientError",
    "HttpRequestError",
    "DiscoveryHttpClientError",
]
