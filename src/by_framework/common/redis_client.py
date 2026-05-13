"""Redis client module.

Provides singleton Redis client initialization and management.
"""

from typing import Optional

from redis.asyncio import Redis

from .exceptions import RedisConnectionError, StreamGroupExistsError

_redis_client: Optional[Redis] = None


def _handle_redis_error(e: Exception, operation: str = "Redis operation") -> Exception:  # pylint: disable=unused-argument
    """Convert Redis errors to SDK-specific exceptions."""
    error_msg = str(e)
    if "BUSYGROUP" in error_msg:
        # This will be further refined by the caller with stream/group info
        return StreamGroupExistsError(group_name="unknown", stream_name="unknown")
    return e


def init_redis(
    host: str = "localhost",
    port: int = 6379,
    db: int = 0,
    password: Optional[str] = None,
    username: Optional[str] = None,
    decode_responses: bool = True,
    max_connections: Optional[int] = None,
    **kwargs,
) -> Redis:
    """Initialize and return a global singleton Redis client.

    If already initialized and you don't want to recreate, it will be
    reused directly (can re-init after close_redis()).
    """
    global _redis_client
    if _redis_client is None:
        if max_connections is not None:
            kwargs["max_connections"] = max_connections

        try:
            _redis_client = Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                username=username,
                decode_responses=decode_responses,
                **kwargs,
            )
        except Exception as e:
            raise RedisConnectionError(f"Failed to initialize Redis client: {e}") from e
    return _redis_client


def get_redis() -> Redis:
    """Get the initialized global Redis client.

    If not initialized, will automatically initialize with default config.
    """
    global _redis_client  # pylint: disable=global-variable-not-assigned
    if _redis_client is None:
        return init_redis()
    return _redis_client


async def close_redis():
    """Close the global Redis client connection to release resources."""
    global _redis_client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None
