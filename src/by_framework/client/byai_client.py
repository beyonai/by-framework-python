"""Byai-specific GatewayClient with BaiYingMessage support."""

from typing import List, Optional, Union

from by_framework.common.redis_client import Redis
from by_framework.core.availability import RoutePolicy
from by_framework.core.protocol.byai_codec import serialize_byai_content
from by_framework.core.protocol.message import BaiYingMessage
from by_framework.core.registry import WorkerRegistry

from .client import GatewayClient, GatewayInterceptor


class ByaiGatewayClient(GatewayClient):
    """A specialized GatewayClient for the Byai domain.

    It automatically includes the ByaiMessageInterceptor to handle
    BaiYingMessage objects.
    """

    def __init__(
        self,
        registry: Optional[WorkerRegistry] = None,
        redis_client: Optional[Redis] = None,
        interceptors: Optional[List[GatewayInterceptor]] = None,
    ):
        super().__init__(
            registry=registry,
            redis_client=redis_client,
            interceptors=interceptors,
        )

    async def send_message(
        self,
        target_agent_type: str,
        session_id: str,
        content: Union[str, BaiYingMessage, List[BaiYingMessage]],
        user_code: str = "",
        user_name: str = "",
        action_type: str = "ASK_AGENT",
        parent_message_id: str = "",
        message_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        extra_payload: Optional[dict] = None,
        metadata: Optional[dict] = None,
        target_worker_id: Optional[str] = None,
        route_policy: str = RoutePolicy.FAIL_FAST,
        availability_timeout_ms: int = 30000,
        region: Optional[str] = None,
        priority: int = 0,
    ):
        serialized_content = serialize_byai_content(content)
        return await super().send_message(
            target_agent_type=target_agent_type,
            session_id=session_id,
            content=serialized_content,
            user_code=user_code,
            user_name=user_name,
            action_type=action_type,
            parent_message_id=parent_message_id,
            message_id=message_id,
            trace_id=trace_id,
            extra_payload=extra_payload,
            metadata=metadata,
            target_worker_id=target_worker_id,
            route_policy=route_policy,
            availability_timeout_ms=availability_timeout_ms,
            region=region,
            priority=priority,
        )
