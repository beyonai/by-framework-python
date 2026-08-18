"""Typed Byai facade for AgentContext."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from by_framework.core.availability import RoutePolicy
from by_framework.core.protocol.byai_types import ByaiContent

from .context import AgentContext


@dataclass(frozen=True)
class ByaiAgentTask:
    """Typed task descriptor for Byai group dispatch.

    Mirrors the per-task keys AgentContext.call_agents accepts, so a task in a
    batch can be routed exactly like the equivalent single call_agent call.
    """

    target_agent_type: str
    content: ByaiContent
    extra_payload: Optional[dict[str, Any]] = None
    metadata: Optional[dict[str, Any]] = None
    message_id: Optional[str] = None
    route_policy: str = RoutePolicy.FAIL_FAST
    availability_timeout_ms: int = 30000
    region: Optional[str] = None
    priority: int = 0


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

    async def call_agents(
        self,
        tasks: list[ByaiAgentTask],
        wait_for_reply: bool = True,
        message_id: Optional[str] = None,
        parent_message_id: Optional[str] = None,
    ) -> dict:
        """Typed batch dispatch — the plural of this facade's call_agent.

        This override, not dispatch_group's, is what has to exist: call_agents
        is the primary batch entry point, so without it a caller holding a
        ByaiAgentContext would reach AgentContext.call_agents and have its
        ByaiAgentTask dataclasses subscripted as dicts.
        """
        return await super().call_agents(
            tasks=[
                {
                    "target_agent_type": task.target_agent_type,
                    "content": task.content,
                    "extra_payload": task.extra_payload or {},
                    "metadata": task.metadata or {},
                    "message_id": task.message_id,
                    "route_policy": task.route_policy,
                    "availability_timeout_ms": task.availability_timeout_ms,
                    "region": task.region,
                    "priority": task.priority,
                }
                for task in tasks
            ],
            wait_for_reply=wait_for_reply,
            message_id=message_id,
            parent_message_id=parent_message_id,
        )

    async def dispatch_group(
        self,
        tasks: list[ByaiAgentTask],
        wait_for_reply: bool = True,
        message_id: Optional[str] = None,
        parent_message_id: Optional[str] = None,
    ) -> dict:
        """Alias for call_agents, kept permanently for source compatibility."""
        return await self.call_agents(
            tasks,
            wait_for_reply=wait_for_reply,
            message_id=message_id,
            parent_message_id=parent_message_id,
        )
