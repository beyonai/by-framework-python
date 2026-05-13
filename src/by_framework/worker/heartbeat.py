"""Worker heartbeat component for maintaining worker presence in cluster."""

import asyncio
from typing import List, Optional

from by_framework.common.constants import RedisKeys
from by_framework.common.logger import logger
from by_framework.common.redis_client import Redis, get_redis
from by_framework.core.registry import WorkerRegistry


class WorkerHeartbeat:
    """
    Standalone heartbeat component to maintain worker presence in the cluster.
    This can be used without inheriting from GatewayWorker.
    """

    def __init__(
        self,
        worker_id: str,
        agent_types: List[str],
        redis_client: Optional[Redis] = None,
        registry: Optional[WorkerRegistry] = None,
        interval: int = RedisKeys.WORKER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        lease_ttl_seconds: int = RedisKeys.WORKER_DEFAULT_LEASE_TTL_SECONDS,
    ):
        self.worker_id = worker_id
        self.agent_types = agent_types
        self.redis = redis_client or get_redis()
        self.registry = registry or WorkerRegistry(self.redis)
        self.interval = interval
        self.lease_ttl_seconds = lease_ttl_seconds
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        """Start the heartbeat background task."""
        if self._task:
            return

        # Register static membership once, then mark the worker online.
        await self.registry.register_worker_membership(self.worker_id, self.agent_types)
        await self.registry.heartbeat_worker(self.worker_id, self.lease_ttl_seconds)

        async def _loop():
            while True:
                try:
                    await self.registry.heartbeat_worker(
                        self.worker_id, self.lease_ttl_seconds
                    )
                    logger.debug("[%s] Standalone heartbeat sent", self.worker_id)
                except Exception as e:  # pylint: disable=broad-exception-caught
                    logger.error(
                        "[%s] Standalone heartbeat failed: %s", self.worker_id, e
                    )
                await asyncio.sleep(self.interval)

        self._task = asyncio.create_task(_loop())
        logger.info("[%s] Standalone heartbeat started", self.worker_id)

    async def stop(self):
        """Stop the heartbeat background task."""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
            logger.info("[%s] Standalone heartbeat stopped", self.worker_id)
