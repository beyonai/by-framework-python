"""Common error types for by-framework.

This module provides error classes for Redis connection issues
and stream consumer group conflicts.
"""

from by_framework.errors.base import FrameworkError


class RedisConnectionError(FrameworkError):
    """Redis connection exception."""

    def __init__(
        self,
        message: str = "Failed to connect to Redis",
        cause: Exception | None = None,
    ):
        super().__init__(message, cause)


class StreamGroupExistsError(FrameworkError):
    """Redis Stream consumer group already exists."""

    def __init__(self, group_name: str, stream_name: str):
        super().__init__(
            f"Consumer group '{group_name}' already exists in stream '{stream_name}'"
        )
        self.group_name = group_name
        self.stream_name = stream_name
