"""Redis client module.

Provides singleton Redis client initialization and management.
"""

from typing import Optional

from redis.asyncio import Redis
from redis.asyncio.cluster import ClusterNode, RedisCluster

from .config import RedisConfig
from .constants import get_key_schema_version
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
    config: Optional[RedisConfig] = None,
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

    A RedisConfig can be passed via `config` instead of individual kwargs;
    when provided, its fields take precedence over the individual params.
    """
    global _redis_client
    if _redis_client is None:
        if config is not None:
            host = config.host
            port = config.port
            db = config.db
            password = config.password or None
            username = config.username
            decode_responses = config.decode_responses
            # config.max_connections of None means "not specified" - don't
            # let it silently discard an explicitly-passed max_connections
            # kwarg (e.g. worker/app.py's max_concurrency-derived value).
            if config.max_connections is not None:
                max_connections = config.max_connections

            if config.mode == "cluster" and get_key_schema_version() != "v2":
                raise RedisConnectionError(
                    "REDIS_MODE=cluster requires REDIS_KEY_SCHEMA_VERSION=v2 "
                    "(v1 key format has no hash tags and will hit CROSSSLOT "
                    "errors under Cluster). Set REDIS_KEY_SCHEMA_VERSION=v2 "
                    "and complete the key migration first."
                )

        if max_connections is not None:
            kwargs["max_connections"] = max_connections

        try:
            if config is not None and config.mode == "cluster":
                _redis_client = RedisCluster(
                    startup_nodes=[
                        ClusterNode(node_host, node_port)
                        for node_host, node_port in (config.cluster_nodes or [])
                    ],
                    password=password,
                    username=username,
                    decode_responses=decode_responses,
                    **kwargs,
                )
            else:
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


def init_redis_from_url(url: str) -> Redis:
    """Initialize the global Redis singleton from a redis:// or rediss:// URL."""
    global _redis_client  # pylint: disable=global-statement
    if _redis_client is None:
        try:
            _redis_client = Redis.from_url(url, decode_responses=True)
        except Exception as e:
            raise RedisConnectionError(
                f"Failed to initialize Redis from URL: {e}"
            ) from e
    return _redis_client


def get_redis() -> Redis:
    """Get the initialized global Redis client.

    If not initialized, automatically initialize from environment config.
    """
    global _redis_client  # pylint: disable=global-variable-not-assigned
    if _redis_client is None:
        return init_redis(config=RedisConfig.from_env())
    return _redis_client


async def close_redis():
    """Close the global Redis client connection to release resources."""
    global _redis_client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None
