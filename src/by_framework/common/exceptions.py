"""By-Framework exception definitions.

This module is now a compatibility alias for by_framework.errors,
recommend using by_framework.errors directly.
"""

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
    "WorkerNotFoundError",
    "WorkerLockError",
    "WorkerRegistryNotSetError",
    "CommandValidationError",
]
