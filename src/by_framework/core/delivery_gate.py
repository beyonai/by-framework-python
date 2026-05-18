"""Pending-delivery gate for the availability control plane."""

from __future__ import annotations

import json
from typing import Any

from by_framework.common.constants import RedisKeys
from by_framework.common.redis_client import Redis
from by_framework.core.availability import PendingDelivery


class DeliveryGate:
    """Reference component that releases pending deliveries after wakeup."""

    def __init__(self, redis: Redis):
        self.redis = redis

    async def dispatch_ready(
        self,
        execution_id: str,
        *,
        last_id: str = "0-0",
        count: int = 100,
    ) -> int:
        """Dispatch pending deliveries matching a ready wakeup request."""
        messages = await self.redis.xread(
            streams={RedisKeys.control_plane_delivery_pending_stream(): last_id},
            count=count,
            block=0,
        )
        dispatched = 0
        pending_deliveries: list[PendingDelivery] = []
        for _, entries in messages or []:
            for _, fields in entries:
                pending = self._decode_pending(fields)
                if pending is None or pending.execution_id != execution_id:
                    continue
                pending_deliveries.append(pending)

        for pending in sorted(
            pending_deliveries, key=lambda delivery: delivery.priority, reverse=True
        ):
            await self.redis.xadd(
                pending.delivery_stream,
                {"data": json.dumps(pending.command_payload)},
            )
            dispatched += 1
        return dispatched

    @staticmethod
    def _decode_pending(fields: dict[Any, Any]) -> PendingDelivery | None:
        raw = fields.get(b"data") if isinstance(fields, dict) else None
        if raw is None and isinstance(fields, dict):
            raw = fields.get("data")
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return PendingDelivery.from_dict(json.loads(raw))
