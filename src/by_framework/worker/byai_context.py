"""Typed Byai facade for AgentContext."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from by_framework.core.availability import RoutePolicy
from by_framework.core.protocol.byai_types import ByaiContent

from .context import AgentContext


@dataclass(frozen=True)
class ByaiAgentTask:
    """Typed task descriptor for Byai group dispatch."""

    target_agent_type: str
    content: ByaiContent
    extra_payload: Optional[dict[str, Any]] = None
    metadata: Optional[dict[str, Any]] = None


class ByaiAgentContext(AgentContext):
    """AgentContext facade with stronger Byai typing."""

    async def call_agent(
        self,
        target_agent_type: str,
        content: ByaiContent,
        extra_payload: Optional[dict[str, Any]] = None,
        wait_for_reply: bool = True,
        metadata: Optional[dict[str, Any]] = None,
        message_id: Optional[str] = None,
        parent_message_id: Optional[str] = None,
        route_policy: str = RoutePolicy.FAIL_FAST,
        availability_timeout_ms: int = 30000,
        region: Optional[str] = None,
        priority: int = 0,
    ) -> dict:
        return await super().call_agent(
            target_agent_type=target_agent_type,
            content=content,
            extra_payload=extra_payload,
            wait_for_reply=wait_for_reply,
            metadata=metadata,
            message_id=message_id,
            parent_message_id=parent_message_id,
            route_policy=route_policy,
            availability_timeout_ms=availability_timeout_ms,
            region=region,
            priority=priority,
        )

    async def dispatch_group(
        self,
        tasks: list[ByaiAgentTask],
        wait_for_reply: bool = True,
        message_id: Optional[str] = None,
        parent_message_id: Optional[str] = None,
    ) -> dict:
        return await super().dispatch_group(
            tasks=[
                {
                    "target_agent_type": task.target_agent_type,
                    "content": task.content,
                    "extra_payload": task.extra_payload or {},
                    "metadata": task.metadata or {},
                }
                for task in tasks
            ],
            wait_for_reply=wait_for_reply,
            message_id=message_id,
            parent_message_id=parent_message_id,
        )
