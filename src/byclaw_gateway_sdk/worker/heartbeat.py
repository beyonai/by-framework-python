import asyncio
from typing import List, Optional

from byclaw_gateway_sdk.common.logger import logger
from byclaw_gateway_sdk.common.redis_client import Redis, get_redis
from byclaw_gateway_sdk.core.registry import WorkerRegistry


class WorkerHeartbeat:
    """
    Standalone heartbeat component to maintain worker presence in the cluster.
    This can be used without inheriting from GatewayWorker.
    """

    def __init__(
        self,
        worker_id: str,
        capabilities: List[str],
        redis_client: Optional[Redis] = None,
        registry: Optional[WorkerRegistry] = None,
        interval: int = 30,
    ):
        self.worker_id = worker_id
        self.capabilities = capabilities
        self.redis = redis_client or get_redis()
        self.registry = registry or WorkerRegistry(self.redis)
        self.interval = interval
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        """Start the heartbeat background task."""
        if self._task:
            return

        # Initial registration
        await self.registry.register_worker(self.worker_id, self.capabilities)

        async def _loop():
            while True:
                try:
                    await self.registry.register_worker(
                        self.worker_id, self.capabilities
                    )
                    logger.debug("[%s] Standalone heartbeat sent", self.worker_id)
                except Exception as e:
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
