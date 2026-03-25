from typing import Optional

from redis.asyncio import Redis

from .exceptions import RedisConnectionError, StreamGroupExistsError

_redis_client: Optional[Redis] = None


def _handle_redis_error(e: Exception, operation: str = "Redis operation") -> Exception:
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
    """
    初始化并返回一个全局单例 Redis 客户端。
    如果已经初始化，且不想重新创建，则会直接重用（可以通过 close_redis() 后重新 init）。
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
    """
    获取已初始化的全局 Redis 客户端。
    如果未初始化，会使用默认配置 (localhost:6379, db=0) 自动初始化。
    """
    global _redis_client
    if _redis_client is None:
        return init_redis()
    return _redis_client


async def close_redis():
    """
    关闭全局 Redis 客户端连接释放资源。
    """
    global _redis_client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None
