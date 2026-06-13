"""
WorkerManager — admin-side API for controlling worker lifecycle and agent-type access.

Responsibilities:
  - Suspend / resume / evict individual workers (lifecycle control).
  - Deny / allow workers from consuming a specific agent_type (admission control).

Lifecycle commands are delivered via two channels:
  1. Push: XADD to byai_gateway:ctrl:worker:{worker_id} (immediate delivery).
  2. Pull: HSET to byai_gateway:registry:worker:admin:{worker_id} (durable fallback,
     read by the worker's heartbeat loop every heartbeat interval).
"""

from __future__ import annotations

import uuid
from typing import Any, Optional

from by_framework.common.constants import RedisKeys
from by_framework.common.redis_client import Redis, get_redis
from by_framework.core.protocol.commands import (
    EvictWorkerCommand,
    ResumeWorkerCommand,
    SuspendWorkerCommand,
)
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.core.registry import WorkerRegistry


def _admin_header() -> MessageHeader:
    return MessageHeader(
        session_id="admin",
        trace_id=uuid.uuid4().hex,
        message_id=uuid.uuid4().hex,
    )


class WorkerManager:
    """Admin API for worker lifecycle and agent-type admission control."""

    def __init__(
        self,
        redis_client: Optional[Redis] = None,
        registry: Optional[WorkerRegistry] = None,
    ):
        self.redis = redis_client or get_redis()
        self.registry = registry or WorkerRegistry(self.redis)

    # ------------------------------------------------------------------
    # Lifecycle control
    # ------------------------------------------------------------------

    async def suspend_worker(self, worker_id: str, reason: str = "") -> None:
        """Pause a running worker from consuming new tasks.

        The worker finishes in-flight tasks but stops accepting new ones.
        It resumes only when resume_worker() is called or it restarts.
        """
        await self.registry.set_worker_admin_state(worker_id, "suspended", reason)
        command = SuspendWorkerCommand(header=_admin_header(), reason=reason)
        await self.redis.xadd(
            RedisKeys.worker_ctrl_stream(worker_id),
            command.to_redis_payload(),
        )

    async def resume_worker(self, worker_id: str) -> None:
        """Resume a previously suspended worker."""
        await self.registry.set_worker_admin_state(worker_id, "active", "")
        command = ResumeWorkerCommand(header=_admin_header())
        await self.redis.xadd(
            RedisKeys.worker_ctrl_stream(worker_id),
            command.to_redis_payload(),
        )

    async def evict_worker(
        self,
        worker_id: str,
        *,
        force: bool = False,
        reason: str = "",
    ) -> None:
        """Shut down a worker.

        Args:
            worker_id: Target worker ID.
            force: When True, cancels in-flight tasks immediately instead of
                waiting for them to finish.
            reason: Human-readable eviction reason.
        """
        await self.registry.set_worker_admin_state(worker_id, "evicted", reason)
        command = EvictWorkerCommand(header=_admin_header(), reason=reason, force=force)
        await self.redis.xadd(
            RedisKeys.worker_ctrl_stream(worker_id),
            command.to_redis_payload(),
        )

    # ------------------------------------------------------------------
    # Agent-type admission control
    # ------------------------------------------------------------------

    async def deny_worker_for_type(self, agent_type: str, worker_id: str) -> None:
        """Prevent worker_id from consuming the agent_type stream.

        Takes effect on the worker's next heartbeat cycle (or immediately if
        the worker checks the denylist before each XREADGROUP call).
        """
        await self.registry.deny_worker_for_type(agent_type, worker_id)

    async def allow_worker_for_type(self, agent_type: str, worker_id: str) -> None:
        """Remove worker_id from the denylist for agent_type."""
        await self.registry.allow_worker_for_type(agent_type, worker_id)

    async def get_type_denylist(self, agent_type: str) -> list[str]:
        """Return all worker_ids currently denied for agent_type."""
        return await self.registry.get_agent_type_denylist(agent_type)

    # ------------------------------------------------------------------
    # Status queries
    # ------------------------------------------------------------------

    async def get_worker_admin_state(self, worker_id: str) -> dict[str, Any]:
        """Return the admin-controlled state for a worker.

        Returns an empty dict when no admin state has been set (default active).
        """
        return await self.registry.get_worker_admin_state(worker_id)

    async def clear_worker_admin_state(self, worker_id: str) -> None:
        """Remove all admin state for a worker, restoring default-active behaviour."""
        await self.registry.clear_worker_admin_state(worker_id)
