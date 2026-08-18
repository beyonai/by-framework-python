"""Task Group planning, persistence, and join semantics."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Optional

from by_framework.common.constants import (
    TASK_GROUP_FIELD_ABORTED,
    TASK_GROUP_FIELD_COMPLETED,
    TASK_GROUP_FIELD_JOIN_CLAIM,
    TASK_GROUP_FIELD_JOIN_CLAIM_EXPIRES_AT,
    TASK_GROUP_FIELD_JOINED,
    TASK_GROUP_FIELD_PROTOCOL_VERSION,
    TASK_GROUP_FIELD_SOURCE_AGENT,
    TASK_GROUP_FIELD_TASK_ORDER,
    TASK_GROUP_FIELD_TOTAL,
    TASK_GROUP_JOIN_CLAIM_TTL_MS,
    TASK_GROUP_PROTOCOL_V2,
    TASK_GROUP_TTL_SECONDS,
    RedisKeys,
)
from by_framework.common.logger import logger
from by_framework.common.redis_client import Redis

_RECORD_REPLY_SCRIPT = """
local total_raw = redis.call('HGET', KEYS[1], ARGV[1])
if not total_raw then
    return {0, 0, 0}
end
local total = tonumber(total_raw)
local completed = tonumber(redis.call('HGET', KEYS[1], ARGV[2]) or '0')
if redis.call('HEXISTS', KEYS[1], ARGV[3]) == 1 then
    return {1, completed, total}
end

local added = redis.call('HSETNX', KEYS[2], ARGV[4], ARGV[5])
if added == 1 then
    redis.call('EXPIRE', KEYS[2], tonumber(ARGV[6]))
    completed = redis.call('HINCRBY', KEYS[1], ARGV[2], 1)
end
if completed < total then
    return {2, completed, total}
end
if redis.call('HGET', KEYS[1], ARGV[7]) == '1' then
    return {5, completed, total}
end

local now_ms = tonumber(ARGV[10])
local current_claim = redis.call('HGET', KEYS[1], ARGV[8])
local claim_expires_at = tonumber(
    redis.call('HGET', KEYS[1], ARGV[9]) or '0'
)
if current_claim and current_claim ~= ARGV[11] and claim_expires_at > now_ms then
    return {4, completed, total}
end

redis.call('HSET', KEYS[1], ARGV[8], ARGV[11])
redis.call('HSET', KEYS[1], ARGV[9], now_ms + tonumber(ARGV[12]))
return {3, completed, total}
"""

_MARK_JOINED_SCRIPT = """
if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then
    return 0
end
redis.call('HSET', KEYS[1], ARGV[3], '1')
redis.call('HDEL', KEYS[1], ARGV[1], ARGV[4])
return 1
"""

# Internal WorkerRunner control statuses. They never leave the runner as
# execution states: one keeps the stream entry pending, the other is ACK-only.
TASK_GROUP_JOIN_PENDING_STATUS = "__task_group_join_pending__"
TASK_GROUP_JOINED_STATUS = "__task_group_joined__"


class TaskGroupJoinState(StrEnum):
    """Outcome of recording one Task Group reply."""

    NOT_FOUND = "not_found"
    ABORTED = "aborted"
    WAITING = "waiting"
    READY = "ready"
    CLAIMED = "claimed"
    JOINED = "joined"


@dataclass(frozen=True)
class TaskGroupJoinDecision:
    """Atomic Group Join decision returned by :meth:`record_reply`."""

    state: TaskGroupJoinState
    completed: int
    total: int
    claim_token: str = ""


class TaskGroupStore:
    """Own the invariants shared by Task Group dispatch and Group Join."""

    def __init__(self, redis_client: Redis, *, worker_id: str = ""):
        self.redis = redis_client
        self.worker_id = worker_id

    async def create(
        self,
        task_group_id: str,
        *,
        message_ids: Sequence[str],
        source_agent_type: str,
    ) -> None:
        """Persist the complete group contract before any task is dispatched."""
        if not message_ids:
            raise ValueError("Task Group requires at least one message_id")
        if len(message_ids) != len(set(message_ids)):
            raise ValueError("Task Group message_ids must be unique within a batch")
        group_key = RedisKeys.task_group(task_group_id)
        await self.redis.hset(
            group_key,
            mapping={
                TASK_GROUP_FIELD_TOTAL: str(len(message_ids)),
                TASK_GROUP_FIELD_COMPLETED: "0",
                TASK_GROUP_FIELD_SOURCE_AGENT: source_agent_type,
                TASK_GROUP_FIELD_PROTOCOL_VERSION: TASK_GROUP_PROTOCOL_V2,
                TASK_GROUP_FIELD_TASK_ORDER: json.dumps(list(message_ids)),
            },
        )
        await self.redis.expire(group_key, TASK_GROUP_TTL_SECONDS)

    async def abort(self, task_group_id: str) -> None:
        """Mark a partially-dispatched group so late replies are discarded."""
        await self.redis.hset(
            RedisKeys.task_group(task_group_id),
            TASK_GROUP_FIELD_ABORTED,
            "1",
        )

    @staticmethod
    def build_result(
        *,
        status: str,
        reply_data: Any,
        content: Any,
        target_agent_type: str,
        metadata: dict,
        extra_payload: dict,
    ) -> dict[str, Any]:
        """Return the shared single/batch result shape persisted by Group Join."""
        result = {
            "status": status,
            "reply_data": reply_data,
            "content": content,
            "target_agent_type": target_agent_type,
            "metadata": metadata,
            "extra_payload": extra_payload,
        }
        if status == "FAILED":
            failure = reply_data if isinstance(reply_data, dict) else {}
            result["error"] = failure.get("error")
            result["error_code"] = failure.get("error_code")
        return result

    async def record_reply(
        self,
        task_group_id: str,
        *,
        task_message_id: str,
        result: dict,
        now_ms: Optional[int] = None,
        claim_token: Optional[str] = None,
    ) -> TaskGroupJoinDecision:
        """Store and count a reply exactly once, then claim a completed join."""
        token = claim_token or uuid.uuid4().hex
        raw = await self.redis.eval(
            _RECORD_REPLY_SCRIPT,
            2,
            RedisKeys.task_group(task_group_id),
            RedisKeys.task_group_results(task_group_id),
            TASK_GROUP_FIELD_TOTAL,
            TASK_GROUP_FIELD_COMPLETED,
            TASK_GROUP_FIELD_ABORTED,
            task_message_id,
            json.dumps(result),
            str(TASK_GROUP_TTL_SECONDS),
            TASK_GROUP_FIELD_JOINED,
            TASK_GROUP_FIELD_JOIN_CLAIM,
            TASK_GROUP_FIELD_JOIN_CLAIM_EXPIRES_AT,
            str(now_ms if now_ms is not None else int(time.time() * 1000)),
            token,
            str(TASK_GROUP_JOIN_CLAIM_TTL_MS),
        )
        code, completed, total = (int(value) for value in raw)
        states = {
            0: TaskGroupJoinState.NOT_FOUND,
            1: TaskGroupJoinState.ABORTED,
            2: TaskGroupJoinState.WAITING,
            3: TaskGroupJoinState.READY,
            4: TaskGroupJoinState.CLAIMED,
            5: TaskGroupJoinState.JOINED,
        }
        return TaskGroupJoinDecision(
            state=states[code],
            completed=completed,
            total=total,
            claim_token=token if code == 3 else "",
        )

    async def mark_joined(self, task_group_id: str, claim_token: str) -> bool:
        """Commit a successful caller resume owned by ``claim_token``."""
        result = await self.redis.eval(
            _MARK_JOINED_SCRIPT,
            1,
            RedisKeys.task_group(task_group_id),
            TASK_GROUP_FIELD_JOIN_CLAIM,
            claim_token,
            TASK_GROUP_FIELD_JOINED,
            TASK_GROUP_FIELD_JOIN_CLAIM_EXPIRES_AT,
        )
        return bool(result)

    async def is_joined(self, task_group_id: str) -> bool:
        """Return whether caller resumption for this group is committed."""
        joined = await self.redis.hget(
            RedisKeys.task_group(task_group_id), TASK_GROUP_FIELD_JOINED
        )
        return joined == "1" or joined == b"1"

    async def aggregate(
        self,
        task_group_id: str,
        *,
        total: Optional[int] = None,
        log_incomplete: bool = True,
    ) -> list[dict[str, Any]]:
        """Return results in dispatch order from the shared persistence shape."""
        group_key = RedisKeys.task_group(task_group_id)
        results_key = RedisKeys.task_group_results(task_group_id)
        raw_results = await self.redis.hgetall(results_key)
        raw_order = await self.redis.hget(group_key, TASK_GROUP_FIELD_TASK_ORDER)
        try:
            order = json.loads(raw_order) if raw_order else []
        except (TypeError, ValueError):
            logger.error(
                "[%s] TaskGroup %s has an unreadable %s field (%r); falling "
                "back to Redis hash order",
                self.worker_id,
                task_group_id,
                TASK_GROUP_FIELD_TASK_ORDER,
                raw_order,
            )
            order = []

        order_set = set(order)
        ordered_ids = [message_id for message_id in order if message_id in raw_results]
        ordered_ids.extend(
            message_id for message_id in raw_results if message_id not in order_set
        )
        aggregated = [
            {
                "message_id": message_id,
                **json.loads(raw_results[message_id]),
            }
            for message_id in ordered_ids
        ]

        if total is None:
            raw_total = await self.redis.hget(group_key, TASK_GROUP_FIELD_TOTAL)
            total = int(raw_total) if raw_total is not None else None
        if log_incomplete and total is not None and len(aggregated) != total:
            missing = [
                message_id for message_id in order if message_id not in raw_results
            ]
            logger.error(
                "[%s] TaskGroup %s aggregated %d result(s) but expected %d; "
                "missing sub-task message_ids=%s. Resuming the caller with an "
                "incomplete result set.",
                self.worker_id,
                task_group_id,
                len(aggregated),
                total,
                missing or "unknown",
            )
        return aggregated

    async def collect(
        self,
        task_group_id: str,
        *,
        timeout: float,
    ) -> list[dict[str, Any]]:
        """Poll the same ordered result view used by automatic Group Join."""
        if not task_group_id:
            return []

        raw_total = await self.redis.hget(
            RedisKeys.task_group(task_group_id), TASK_GROUP_FIELD_TOTAL
        )
        total = int(raw_total) if raw_total is not None else None
        start_time = asyncio.get_running_loop().time()
        results: list[dict[str, Any]] = []
        while total is None or len(results) < total:
            if asyncio.get_running_loop().time() - start_time >= timeout:
                break
            results = await self.aggregate(
                task_group_id,
                total=total,
                log_incomplete=False,
            )
            if total is not None and len(results) >= total:
                break
            await asyncio.sleep(0.1)
        return results

    @staticmethod
    def resolve_message_ids(
        explicit_ids: Sequence[Optional[str]],
        *,
        shared_message_id: Optional[str],
        generate_message_id: Callable[[], str],
    ) -> list[str]:
        """Resolve collision-free sub-task IDs before creating any group state.

        The legacy batch-level ``message_id`` remains accepted. For a batch it
        becomes a stable prefix; for a single task it keeps its historical
        exact value. An explicit per-task ID always wins.
        """
        is_batch = len(explicit_ids) > 1
        resolved = [
            explicit_id
            or (
                f"{shared_message_id}:{index}"
                if shared_message_id and is_batch
                else shared_message_id
            )
            or generate_message_id()
            for index, explicit_id in enumerate(explicit_ids)
        ]
        if len(resolved) != len(set(resolved)):
            raise ValueError("Task Group message_ids must be unique within a batch")
        return resolved
