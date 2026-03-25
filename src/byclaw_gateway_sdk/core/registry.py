"""
Worker registry module.

Provides worker registration, discovery, and execution tracking
through Redis-backed storage.
"""

import json
import logging
import random
import time
import uuid
from typing import Any, List, Optional

from byclaw_gateway_sdk.common.constants import (
    EXEC_FIELD_PREFIX,
    MSG_MAP_PREFIX,
    RedisKeys,
)
from byclaw_gateway_sdk.common.exceptions import ExecutionDataError
from byclaw_gateway_sdk.common.redis_client import Redis, get_redis

logger = logging.getLogger("byclaw_gateway_sdk.registry")


class WorkerRegistry:
    """Worker 注册中心，负责 worker 的注册、发现和执行追踪。

    通过 Redis 有序集合和 Hash 结构存储 worker 信息和执行状态。
    """

    def __init__(self, redis_client: Optional[Redis] = None):
        self.redis = redis_client or get_redis()
        self._lock_tokens: dict[str, str] = {}

    async def register_worker(self, worker_id: str, capabilities: List[str]):
        now = int(time.time() * 1000)
        await self.redis.zadd(RedisKeys.ACTIVE_WORKERS, {worker_id: now})
        for capability in capabilities:
            await self.redis.sadd(RedisKeys.worker_capabilities(worker_id), capability)
            await self.redis.sadd(RedisKeys.capability_workers(capability), worker_id)

    async def unregister_worker(self, worker_id: str):
        """Unregister a worker and remove it from all capability sets. Mirrors TS unregisterWorker."""
        capabilities_raw = await self.redis.smembers(
            RedisKeys.worker_capabilities(worker_id)
        )
        await self.redis.zrem(RedisKeys.ACTIVE_WORKERS, worker_id)
        await self.redis.delete(RedisKeys.worker_capabilities(worker_id))
        for cap_raw in capabilities_raw:
            capability = cap_raw.decode() if isinstance(cap_raw, bytes) else cap_raw
            await self.redis.srem(RedisKeys.capability_workers(capability), worker_id)

    async def get_target_worker(self, agent_id: str) -> Optional[str]:
        workers = await self.redis.smembers(RedisKeys.capability_workers(agent_id))
        if not workers:
            return None
        return random.choice(list(workers))

    async def get_all_workers(self) -> dict[str, Any]:
        """获取所有已注册的 Worker 信息。

        Returns:
            包含所有 worker ID 及其能力和最后活跃时间的字典
        """
        redis_inst = self.redis
        worker_ids = await redis_inst.zrange(
            RedisKeys.ACTIVE_WORKERS, 0, -1, withscores=True
        )

        result = {}
        for worker_id_raw, score in worker_ids:
            worker_id = (
                worker_id_raw.decode()
                if isinstance(worker_id_raw, bytes)
                else worker_id_raw
            )
            caps_raw = await redis_inst.smembers(
                RedisKeys.worker_capabilities(worker_id)
            )
            capabilities = [c.decode() if isinstance(c, bytes) else c for c in caps_raw]
            result[worker_id] = {
                "capabilities": capabilities,
                "last_seen": int(score) if score else 0,
            }
        return result

    async def claim_worker_id(self, worker_id: str, ttl_seconds: int = 60) -> str:
        """尝试获取 Worker ID 的独占锁。

        Args:
            worker_id: 要获取锁的 Worker ID
            ttl_seconds: 锁的 TTL 秒数

        Returns:
            锁令牌

        Raises:
            ValueError: 如果 worker_id 已被占用
        """
        token = uuid.uuid4().hex
        ok = await self.redis.set(
            RedisKeys.worker_lock(worker_id),
            token,
            nx=True,
            ex=ttl_seconds,
        )
        if not ok:
            raise ValueError(f"worker_id already in use: {worker_id}")
        self._lock_tokens[worker_id] = token
        return token

    async def refresh_worker_id_lock(
        self, worker_id: str, ttl_seconds: int = 60
    ) -> bool:
        """刷新 Worker ID 锁的 TTL。

        Args:
            worker_id: Worker ID
            ttl_seconds: 新的 TTL 秒数

        Returns:
            如果刷新成功返回 True，否则返回 False
        """
        token = self._lock_tokens.get(worker_id)
        if not token:
            return False

        current = await self.redis.get(RedisKeys.worker_lock(worker_id))
        if isinstance(current, bytes):
            current = current.decode("utf-8")
        if current != token:
            return False

        result = await self.redis.expire(RedisKeys.worker_lock(worker_id), ttl_seconds)
        return bool(result)

    async def release_worker_id(
        self, worker_id: str, token: Optional[str] = None
    ) -> bool:
        """释放 Worker ID 的独占锁。

        Args:
            worker_id: Worker ID
            token: 可选的锁令牌

        Returns:
            如果释放成功返回 True，否则返回 False
        """
        expected = token or self._lock_tokens.get(worker_id)
        if not expected:
            return False

        key = RedisKeys.worker_lock(worker_id)
        current = await self.redis.get(key)
        if isinstance(current, bytes):
            current = current.decode("utf-8")
        if current != expected:
            return False

        await self.redis.delete(key)
        self._lock_tokens.pop(worker_id, None)
        return True

    async def save_execution(self, execution: dict[str, Any]):
        """保存执行数据到 Redis。

        Args:
            execution: 执行信息字典，包含 execution_id, message_id, session_id 等
        """
        execution_id = execution["execution_id"]
        message_id = execution["message_id"]
        session_id = execution["session_id"]

        reg_key = RedisKeys.session_registry(session_id)
        encoded_data = json.dumps(execution, ensure_ascii=False)

        # 使用 Pipeline 保证原子性并设置 TTL
        pipe = self.redis.pipeline()
        pipe.hset(reg_key, f"{EXEC_FIELD_PREFIX}{execution_id}", encoded_data)
        pipe.hset(reg_key, f"{MSG_MAP_PREFIX}{message_id}", execution_id)
        pipe.expire(reg_key, RedisKeys.DEFAULT_SESSION_TTL)
        await pipe.execute()

    async def get_execution(
        self, execution_id: str, session_id: str = ""
    ) -> Optional[dict[str, Any]]:
        """
        获取执行详情。
        注意：在新架构下，调用者应当提供 session_id 以优化查询性能。
        如果未提供 session_id，则需要从全局搜索（如果不推荐这样做的话）。
        """
        if not session_id:
            logger.warning(
                "get_execution called without session_id, this is inefficient in the new registry architecture."
            )
            # 兼容逻辑：如果确实没有 session_id，可能需要扫全表或报错
            return None

        reg_key = RedisKeys.session_registry(session_id)
        data = await self.redis.hget(reg_key, f"{EXEC_FIELD_PREFIX}{execution_id}")
        if not data:
            return None

        if isinstance(data, bytes):
            data = data.decode("utf-8")

        try:
            return json.loads(data)
        except json.JSONDecodeError as err:
            raise ExecutionDataError(execution_id, cause=err) from err

    async def get_execution_by_message_id(
        self, message_id: str, session_id: str = ""
    ) -> Optional[dict[str, Any]]:
        """
        根据 message_id 获取执行详情。
        """
        if not session_id:
            # 在某些流程中（如取消），可能只有 message_id。
            # 为了支持这种情况，我们维持 GatewayClient 端的 session_id 传递。
            return None

        reg_key = RedisKeys.session_registry(session_id)
        execution_id = await self.redis.hget(reg_key, f"{MSG_MAP_PREFIX}{message_id}")
        if isinstance(execution_id, bytes):
            execution_id = execution_id.decode("utf-8")

        if not execution_id:
            return None
        return await self.get_execution(execution_id, session_id)

    async def mark_execution_cancelling(
        self, execution_id: str, session_id: str, reason: str
    ):
        """标记执行状态为 CANCELLING。

        Args:
            execution_id: 执行ID
            session_id: 会话ID
            reason: 取消原因
        """
        current = await self.get_execution(execution_id, session_id)
        if current is None:
            return

        current["status"] = "CANCELLING"
        current["cancel_requested"] = True
        current["cancel_reason"] = reason
        current["updated_at"] = int(time.time() * 1000)

        reg_key = RedisKeys.session_registry(session_id)
        pipe = self.redis.pipeline()
        pipe.hset(
            reg_key,
            f"{EXEC_FIELD_PREFIX}{execution_id}",
            json.dumps(current, ensure_ascii=False),
        )
        pipe.expire(reg_key, RedisKeys.DEFAULT_SESSION_TTL)
        await pipe.execute()

    async def mark_execution_finished(
        self, execution_id: str, session_id: str, status: str
    ):
        """标记执行为已完成状态。

        Args:
            execution_id: 执行ID
            session_id: 会话ID
            status: 最终状态
        """
        current = await self.get_execution(execution_id, session_id)
        if current is None:
            return

        current["status"] = status
        current["finished_at"] = int(time.time() * 1000)
        current["updated_at"] = current["finished_at"]

        reg_key = RedisKeys.session_registry(session_id)
        pipe = self.redis.pipeline()
        pipe.hset(
            reg_key,
            f"{EXEC_FIELD_PREFIX}{execution_id}",
            json.dumps(current, ensure_ascii=False),
        )
        pipe.expire(reg_key, RedisKeys.DEFAULT_SESSION_TTL)
        await pipe.execute()

    def _encode_execution(self, execution: dict[str, Any]) -> dict[str, str]:
        # 已废弃，因为我们改用 JSON 存储
        return {}

    def _decode_execution(self, execution: dict[str, Any]) -> dict[str, Any]:
        # 已废弃，因为我们改用 JSON 存储
        return {}
