"""Reference wakeup controller for the availability control plane."""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any, Protocol

from by_framework.common.constants import RedisKeys
from by_framework.common.redis_client import Redis
from by_framework.core.availability import (
    WakeupDecision,
    WakeupDecisionStatus,
    WakeupRequest,
)


class WakeupProvider(Protocol):
    """Manager-owned adapter that starts or signals workers."""

    async def wakeup(self, request: WakeupRequest) -> WakeupDecision | dict[str, Any]:
        """Handle a wakeup request and return a controller decision."""
        ...  # pylint: disable=unnecessary-ellipsis


class WakeupController:
    """Small reference controller for processing wakeup management events."""

    def __init__(
        self,
        redis: Redis,
        provider: WakeupProvider,
        dedupe_ttl_seconds: int = 30,
        max_attempts: int = 1,
    ):
        self.redis = redis
        self.provider = provider
        self.dedupe_ttl_seconds = dedupe_ttl_seconds
        self.max_attempts = max(1, max_attempts)

    async def run_once(self, last_id: str = "0-0", block_ms: int = 0) -> str:
        """Process at most one wakeup request and return the next stream id."""
        messages = await self.redis.xread(
            streams={RedisKeys.control_plane_wakeup_stream(): last_id},
            count=1,
            block=block_ms,
        )
        if not messages:
            return last_id

        for _, entries in messages:
            for entry_id, fields in entries:
                request = self._decode_request(fields)
                next_id = (
                    entry_id.decode("utf-8")
                    if isinstance(entry_id, bytes)
                    else str(entry_id)
                )
                if request is None:
                    return next_id
                decision = await self._handle_request(request)
                await self.redis.xadd(
                    RedisKeys.control_plane_wakeup_result_stream(request.execution_id),
                    {"data": json.dumps(asdict(decision))},
                )
                if decision.status == WakeupDecisionStatus.FAILED:
                    await self.redis.xadd(
                        RedisKeys.control_plane_deadletter_stream(),
                        {"data": json.dumps(asdict(decision))},
                    )
                return next_id

        return last_id

    async def _handle_request(self, request: WakeupRequest) -> WakeupDecision:
        if not await self._claim_wakeup(request):
            return WakeupDecision(
                execution_id=request.execution_id,
                target_agent_type=request.target_agent_type,
                status=WakeupDecisionStatus.QUEUED,
                reason="wakeup already in progress",
            )
        return await self._call_provider(request)

    async def _call_provider(self, request: WakeupRequest) -> WakeupDecision:
        last_error = ""
        for _ in range(self.max_attempts):
            try:
                raw_decision = await self.provider.wakeup(request)
            except Exception as error:  # pylint: disable=broad-exception-caught
                last_error = str(error)
                continue

            return self._normalize_decision(raw_decision, request)

        return WakeupDecision(
            execution_id=request.execution_id,
            target_agent_type=request.target_agent_type,
            status=WakeupDecisionStatus.FAILED,
            reason=last_error,
        )

    @staticmethod
    def _normalize_decision(
        raw_decision: WakeupDecision | dict[str, Any],
        request: WakeupRequest,
    ) -> WakeupDecision:
        if isinstance(raw_decision, WakeupDecision):
            return WakeupDecision(
                execution_id=raw_decision.execution_id or request.execution_id,
                target_agent_type=raw_decision.target_agent_type
                or request.target_agent_type,
                status=raw_decision.status,
                selected_agent_type=raw_decision.selected_agent_type,
                worker_ids=raw_decision.worker_ids,
                region=raw_decision.region,
                retry_after_ms=raw_decision.retry_after_ms,
                reason=raw_decision.reason,
            )

        data = dict(raw_decision)
        data.setdefault("execution_id", request.execution_id)
        data.setdefault("target_agent_type", request.target_agent_type)
        return WakeupDecision.from_dict(data)

    async def _claim_wakeup(self, request: WakeupRequest) -> bool:
        if not hasattr(self.redis, "set"):
            return True
        dedupe_key = RedisKeys.control_plane_wakeup_dedupe(
            request.target_agent_type,
            request.user_code or "_",
            request.region or "_",
        )
        claimed = await self.redis.set(
            dedupe_key,
            request.execution_id,
            ex=self.dedupe_ttl_seconds,
            nx=True,
        )
        return bool(claimed)

    @staticmethod
    def _decode_request(fields: dict[Any, Any]) -> WakeupRequest | None:
        raw = fields.get(b"data") if isinstance(fields, dict) else None
        if raw is None and isinstance(fields, dict):
            raw = fields.get("data")
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return WakeupRequest.from_dict(json.loads(raw))
