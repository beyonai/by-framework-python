"""Agent availability control-plane routing.

This module centralizes online-worker checks and wakeup handshakes so client
calls and inter-agent calls share the same offline routing behavior.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Optional

from by_framework.common.constants import RedisKeys
from by_framework.common.redis_client import Redis
from by_framework.core.registry import WorkerRegistry, check_agent_type_online


class RoutePolicy:
    """Policy values for routing and availability checks."""

    FAIL_FAST = "FAIL_FAST"
    SEND_ANYWAY = "SEND_ANYWAY"
    WAKE_AND_WAIT = "WAKE_AND_WAIT"
    WAKE_AND_QUEUE = "WAKE_AND_QUEUE"
    QUEUE_ONLY = "QUEUE_ONLY"


class WakeupDecisionStatus:
    """Wakeup controller decision status values."""

    READY = "READY"
    STARTING = "STARTING"
    QUEUED = "QUEUED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"
    FALLBACK = "FALLBACK"


class AvailabilityStatus:
    """Availability router result values."""

    DELIVER_NOW = "DELIVER_NOW"
    WAIT_AND_DELIVER = "WAIT_AND_DELIVER"
    QUEUE_PENDING = "QUEUE_PENDING"
    REJECT = "REJECT"
    FALLBACK_TO_OTHER_AGENT_TYPE = "FALLBACK_TO_OTHER_AGENT_TYPE"


@dataclass(frozen=True)
class DeliveryIntent:
    """Framework-internal representation of a control-message delivery."""

    execution_id: str
    message_id: str
    session_id: str
    trace_id: str
    source: str
    target_agent_type: str
    user_code: str = ""
    region: str = ""
    priority: int = 0
    policy: str = RoutePolicy.FAIL_FAST
    timeout_ms: int = 30000
    command_payload: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class WakeupRequest:
    """Redis management event emitted when a target agent type is unavailable."""

    execution_id: str
    target_agent_type: str
    session_id: str
    trace_id: str
    message_id: str
    source: str
    policy: str
    timeout_ms: int
    user_code: str = ""
    region: str = ""
    priority: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
    command_payload: dict[str, Any] = field(default_factory=dict)

    def to_redis_payload(self) -> dict[str, str]:
        return {"data": json.dumps(asdict(self))}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WakeupRequest":
        return cls(
            execution_id=str(data.get("execution_id", "")),
            target_agent_type=str(data.get("target_agent_type", "")),
            session_id=str(data.get("session_id", "")),
            trace_id=str(data.get("trace_id", "")),
            message_id=str(data.get("message_id", "")),
            source=str(data.get("source", "")),
            policy=str(data.get("policy", RoutePolicy.FAIL_FAST)),
            timeout_ms=int(data.get("timeout_ms", 30000)),
            user_code=str(data.get("user_code") or data.get("tenant_id", "")),
            region=str(data.get("region", "")),
            priority=int(data.get("priority", 0)),
            metadata=dict(data.get("metadata", {})),
            command_payload=dict(data.get("command_payload", {})),
        )


@dataclass(frozen=True)
class PendingDelivery:
    """Control command held until a wakeup decision allows dispatch."""

    execution_id: str
    message_id: str
    session_id: str
    trace_id: str
    target_agent_type: str
    delivery_stream: str
    command_payload: dict[str, Any]
    user_code: str = ""
    region: str = ""
    priority: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_redis_payload(self) -> dict[str, str]:
        return {"data": json.dumps(asdict(self))}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PendingDelivery":
        return cls(
            execution_id=str(data.get("execution_id", "")),
            message_id=str(data.get("message_id", "")),
            session_id=str(data.get("session_id", "")),
            trace_id=str(data.get("trace_id", "")),
            target_agent_type=str(data.get("target_agent_type", "")),
            delivery_stream=str(data.get("delivery_stream", "")),
            command_payload=dict(data.get("command_payload", {})),
            user_code=str(data.get("user_code") or data.get("tenant_id", "")),
            region=str(data.get("region", "")),
            priority=int(data.get("priority", 0)),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass(frozen=True)
class WakeupDecision:
    """Decision returned by the manager-owned wakeup controller."""

    execution_id: str = ""
    target_agent_type: str = ""
    status: str = WakeupDecisionStatus.FAILED
    selected_agent_type: str = ""
    worker_ids: list[str] = field(default_factory=list)
    region: str = ""
    retry_after_ms: Optional[int] = None
    reason: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WakeupDecision":
        return cls(
            execution_id=str(data.get("execution_id", "")),
            target_agent_type=str(data.get("target_agent_type", "")),
            status=str(data.get("status", WakeupDecisionStatus.FAILED)),
            selected_agent_type=str(data.get("selected_agent_type", "")),
            worker_ids=list(data.get("worker_ids", [])),
            region=str(data.get("region", "")),
            retry_after_ms=data.get("retry_after_ms"),
            reason=str(data.get("reason", "")),
        )


@dataclass(frozen=True)
class AvailabilityResult:
    """Result returned by AvailabilityRouter before control-message delivery."""

    status: str
    stream_name: str = ""
    target_worker_id: str = ""
    error: str = ""
    error_code: str = ""
    selected_agent_type: str = ""
    execution_id: str = ""


class AvailabilityRouter:
    """Shared availability and wakeup router for client and agent calls."""

    def __init__(
        self,
        redis: Redis,
        registry: Optional[WorkerRegistry] = None,
    ):
        self.redis = redis
        self.registry = registry

    async def prepare_delivery(
        self,
        intent: DeliveryIntent,
    ) -> AvailabilityResult:
        """Resolve whether a command can be delivered now or needs wakeup."""
        policy_rejection = await self._check_control_plane_policy(intent)
        if policy_rejection is not None:
            return policy_rejection

        if intent.policy == RoutePolicy.SEND_ANYWAY:
            return AvailabilityResult(
                status=AvailabilityStatus.DELIVER_NOW,
                stream_name=RedisKeys.ctrl_stream(intent.target_agent_type),
                execution_id=intent.execution_id,
            )

        has_online_worker = await self._has_online_agent_type(intent.target_agent_type)
        if has_online_worker:
            return AvailabilityResult(
                status=AvailabilityStatus.DELIVER_NOW,
                stream_name=RedisKeys.ctrl_stream(intent.target_agent_type),
                execution_id=intent.execution_id,
            )

        fallback = await self._resolve_configured_fallback(intent)
        if fallback is not None:
            return fallback

        if intent.policy == RoutePolicy.FAIL_FAST:
            return self._unavailable(intent)

        if intent.policy == RoutePolicy.WAKE_AND_WAIT:
            return await self._wake_and_wait(intent)

        if intent.policy == RoutePolicy.WAKE_AND_QUEUE:
            return await self._wake_and_queue(intent)

        if intent.policy == RoutePolicy.QUEUE_ONLY:
            return await self._queue_pending(intent)

        return AvailabilityResult(
            status=AvailabilityStatus.REJECT,
            error=f"Unsupported offline route policy '{intent.policy}'",
            error_code="AGENT_TYPE_UNAVAILABLE",
            execution_id=intent.execution_id,
        )

    async def _check_control_plane_policy(
        self, intent: DeliveryIntent
    ) -> Optional[AvailabilityResult]:
        circuit = await self._read_json_key(
            RedisKeys.control_plane_agent_circuit(intent.target_agent_type)
        )
        if circuit and str(circuit.get("state", "")).upper() == "OPEN":
            return AvailabilityResult(
                status=AvailabilityStatus.REJECT,
                error=str(circuit.get("reason") or "agent circuit is open"),
                error_code="AGENT_CIRCUIT_OPEN",
                execution_id=intent.execution_id,
            )

        if intent.user_code:
            quota = await self._read_json_key(
                RedisKeys.control_plane_user_quota(intent.user_code)
            )
            if quota and quota.get("available") is False:
                return AvailabilityResult(
                    status=AvailabilityStatus.REJECT,
                    error=str(quota.get("reason") or "tenant quota exceeded"),
                    error_code="TENANT_QUOTA_EXCEEDED",
                    execution_id=intent.execution_id,
                )

        return None

    async def _resolve_configured_fallback(
        self, intent: DeliveryIntent
    ) -> Optional[AvailabilityResult]:
        fallback = await self._read_json_key(
            RedisKeys.control_plane_agent_fallback(intent.target_agent_type)
        )
        if not fallback:
            return None

        selected = str(
            fallback.get("selected_agent_type")
            or fallback.get("agent_type")
            or fallback.get("target_agent_type")
            or ""
        )
        if not selected:
            return None

        if await self._has_online_agent_type(selected):
            return AvailabilityResult(
                status=AvailabilityStatus.FALLBACK_TO_OTHER_AGENT_TYPE,
                stream_name=RedisKeys.ctrl_stream(selected),
                selected_agent_type=selected,
                execution_id=intent.execution_id,
            )

        return None

    async def _wake_and_queue(self, intent: DeliveryIntent) -> AvailabilityResult:
        request = WakeupRequest(
            execution_id=intent.execution_id,
            target_agent_type=intent.target_agent_type,
            session_id=intent.session_id,
            trace_id=intent.trace_id,
            message_id=intent.message_id,
            source=intent.source,
            policy=intent.policy,
            timeout_ms=intent.timeout_ms,
            user_code=intent.user_code,
            region=intent.region,
            priority=intent.priority,
            metadata=intent.metadata,
            command_payload=intent.command_payload,
        )
        await self.redis.xadd(
            RedisKeys.control_plane_wakeup_stream(), request.to_redis_payload()
        )
        return await self._queue_pending(intent)

    async def _queue_pending(self, intent: DeliveryIntent) -> AvailabilityResult:
        pending = PendingDelivery(
            execution_id=intent.execution_id,
            message_id=intent.message_id,
            session_id=intent.session_id,
            trace_id=intent.trace_id,
            target_agent_type=intent.target_agent_type,
            delivery_stream=RedisKeys.ctrl_stream(intent.target_agent_type),
            command_payload=intent.command_payload,
            user_code=intent.user_code,
            region=intent.region,
            priority=intent.priority,
            metadata=intent.metadata,
        )
        await self.redis.xadd(
            RedisKeys.control_plane_delivery_pending_stream(),
            pending.to_redis_payload(),
        )
        return AvailabilityResult(
            status=AvailabilityStatus.QUEUE_PENDING,
            stream_name=RedisKeys.control_plane_delivery_pending_stream(),
            execution_id=intent.execution_id,
        )

    async def _wake_and_wait(self, intent: DeliveryIntent) -> AvailabilityResult:
        request = WakeupRequest(
            execution_id=intent.execution_id,
            target_agent_type=intent.target_agent_type,
            session_id=intent.session_id,
            trace_id=intent.trace_id,
            message_id=intent.message_id,
            source=intent.source,
            policy=intent.policy,
            timeout_ms=intent.timeout_ms,
            user_code=intent.user_code,
            region=intent.region,
            priority=intent.priority,
            metadata=intent.metadata,
            command_payload=intent.command_payload,
        )
        await self.redis.xadd(
            RedisKeys.control_plane_wakeup_stream(), request.to_redis_payload()
        )

        decision = await self._wait_for_wakeup_decision(intent)
        if decision is None:
            return AvailabilityResult(
                status=AvailabilityStatus.REJECT,
                error=(
                    f"Timed out waiting for worker wakeup for agent_type "
                    f"'{intent.target_agent_type}'"
                ),
                error_code="AGENT_TYPE_UNAVAILABLE",
                execution_id=intent.execution_id,
            )

        if decision.status == WakeupDecisionStatus.READY:
            has_online_worker = await self._has_online_agent_type(
                intent.target_agent_type
            )
            if has_online_worker:
                return AvailabilityResult(
                    status=AvailabilityStatus.WAIT_AND_DELIVER,
                    stream_name=RedisKeys.ctrl_stream(intent.target_agent_type),
                    execution_id=intent.execution_id,
                )
            return self._unavailable(intent)

        if decision.status == WakeupDecisionStatus.FALLBACK:
            selected = decision.selected_agent_type
            return AvailabilityResult(
                status=AvailabilityStatus.FALLBACK_TO_OTHER_AGENT_TYPE,
                stream_name=RedisKeys.ctrl_stream(selected) if selected else "",
                selected_agent_type=selected,
                execution_id=intent.execution_id,
            )

        return AvailabilityResult(
            status=AvailabilityStatus.REJECT,
            error=decision.reason
            or f"Wakeup rejected for agent_type '{intent.target_agent_type}'",
            error_code="AGENT_TYPE_UNAVAILABLE",
            execution_id=intent.execution_id,
        )

    async def _wait_for_wakeup_decision(
        self, intent: DeliveryIntent
    ) -> Optional[WakeupDecision]:
        deadline = time.monotonic() + max(intent.timeout_ms, 0) / 1000
        result_stream = RedisKeys.control_plane_wakeup_result_stream(
            intent.execution_id
        )
        last_id = "0-0"

        while True:
            remaining_ms = int((deadline - time.monotonic()) * 1000)
            if remaining_ms <= 0:
                return None

            messages = await self.redis.xread(
                streams={result_stream: last_id},
                count=1,
                block=remaining_ms,
            )
            if not messages:
                return None

            for _, entries in messages:
                for entry_id, fields in entries:
                    last_id = (
                        entry_id.decode("utf-8")
                        if isinstance(entry_id, bytes)
                        else str(entry_id)
                    )
                    decision = self._decode_decision(fields)
                    if decision is None:
                        continue
                    if (
                        decision.execution_id
                        and decision.execution_id != intent.execution_id
                    ):
                        continue
                    if decision.status in (
                        WakeupDecisionStatus.STARTING,
                        WakeupDecisionStatus.QUEUED,
                    ):
                        break
                    return decision

    async def _has_online_agent_type(self, agent_type: str) -> bool:
        if self.registry is not None:
            has_online_agent_type, _ = await self.registry.has_online_agent_type(
                agent_type
            )
            return bool(has_online_agent_type)

        has_online_agent_type, _ = await check_agent_type_online(
            self.redis, agent_type, check_active=True
        )
        return bool(has_online_agent_type)

    @staticmethod
    def _decode_decision(fields: dict[Any, Any]) -> Optional[WakeupDecision]:
        raw = fields.get(b"data") if isinstance(fields, dict) else None
        if raw is None and isinstance(fields, dict):
            raw = fields.get("data")
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return WakeupDecision.from_dict(json.loads(raw))

    async def _read_json_key(self, key: str) -> Optional[dict[str, Any]]:
        if not hasattr(self.redis, "get"):
            return None
        raw = await self.redis.get(key)
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        if not isinstance(raw, str):
            return None
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return decoded if isinstance(decoded, dict) else None

    @staticmethod
    def _unavailable(intent: DeliveryIntent) -> AvailabilityResult:
        return AvailabilityResult(
            status=AvailabilityStatus.REJECT,
            error=f"No online worker found for agent_type '{intent.target_agent_type}'",
            error_code="AGENT_TYPE_UNAVAILABLE",
            execution_id=intent.execution_id,
        )
