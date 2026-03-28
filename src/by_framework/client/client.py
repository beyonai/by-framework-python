"""
Gateway client module.

Provides the GatewayClient class for sending messages and cancel requests
to Gateway workers via Redis streams.
"""

import time
import uuid
from typing import TYPE_CHECKING, Any, Dict, Generic, List, Optional, TypeVar

from by_framework.common.constants import (
    CANCEL_MESSAGE_ID_PREFIX,
    MESSAGE_ID_PREFIX,
    RedisKeys,
)
from by_framework.common.exceptions import WorkerRegistryNotSetError
from by_framework.common.redis_client import Redis, get_redis
from by_framework.core.protocol.action_type import ActionType
from by_framework.core.protocol.commands import (
    AskAgentCommand,
    CancelMode,
    CancelTaskCommand,
    ResumeCommand,
)
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.core.protocol.responses import (
    CancelTaskResponse,
    ExecutionStatus,
    SendMessageResponse,
)
from by_framework.core.registry import WorkerRegistry

from .interceptors import GatewayInterceptor

if TYPE_CHECKING:
    pass

T = TypeVar("T")


class GatewayClient(Generic[T]):
    """Gateway 客户端，用于向 Gateway workers 发送消息和取消请求。

    通过 Redis streams 与 workers 通信，支持拦截器模式处理消息内容。

    Args:
        registry: WorkerRegistry 实例，用于 worker 发现
        redis_client: Redis 客户端实例
        interceptors: 消息拦截器列表
    """

    def __init__(
        self,
        registry: Optional[WorkerRegistry] = None,
        redis_client: Optional[Redis] = None,
        interceptors: Optional[List[GatewayInterceptor]] = None,
    ):
        self.registry = registry
        self.redis = (
            redis_client or (registry.redis if registry else None) or get_redis()
        )
        self.interceptors = interceptors or []

    def add_interceptor(self, interceptor: GatewayInterceptor):
        self.interceptors.append(interceptor)

    async def cancel_task(
        self,
        message_id: str,
        session_id: str,
        reason: str = "",
        target_agent_type: str = "",
        requested_by: str = "client",
        cancel_mode: str = CancelMode.GRACEFUL,
    ) -> CancelTaskResponse:
        if self.registry is None:
            raise ValueError("GatewayClient requires a WorkerRegistry to cancel tasks")

        execution = await self.registry.get_execution_by_message_id(
            message_id, session_id=session_id
        )
        if not execution:
            return CancelTaskResponse(
                success=False,
                message_id=message_id,
                execution_id="",
                worker_id="",
                status=ExecutionStatus.NOT_FOUND,
                timestamp=int(time.time() * 1000),
                error=f"execution not found for message_id={message_id}",
            )

        if execution.get("session_id") != session_id:
            return CancelTaskResponse(
                success=False,
                message_id=message_id,
                execution_id=execution.get("execution_id", ""),
                worker_id=execution.get("worker_id", ""),
                status=ExecutionStatus.NOT_FOUND,
                timestamp=int(time.time() * 1000),
                error=f"session mismatch for message_id={message_id}",
            )

        execution_status = execution.get("status", "")
        if execution_status in {"COMPLETED", "FAILED", "CANCELLED"}:
            return CancelTaskResponse(
                success=False,
                message_id=message_id,
                execution_id=execution.get("execution_id", ""),
                worker_id=execution.get("worker_id", ""),
                status=ExecutionStatus.ALREADY_FINISHED,
                timestamp=int(time.time() * 1000),
                error=f"execution already in terminal state: {execution_status}",
            )

        execution_id = execution["execution_id"]
        worker_id = execution["worker_id"]
        await self.registry.mark_execution_cancelling(execution_id, session_id, reason)

        cancel_command = CancelTaskCommand(
            header=MessageHeader(
                message_id=f"{CANCEL_MESSAGE_ID_PREFIX}{uuid.uuid4().hex[:8]}",
                session_id=session_id,
                trace_id=uuid.uuid4().hex,
                target_agent_type=target_agent_type
                or execution.get("target_agent_type", ""),
                parent_message_id=message_id,
            ),
            target_message_id=message_id,
            target_execution_id=execution_id,
            target_worker_id=worker_id,
            reason=reason,
            requested_by=requested_by,
            cancel_mode=cancel_mode,
        )

        await self.redis.xadd(
            RedisKeys.worker_ctrl_stream(worker_id),
            cancel_command.to_redis_payload(),
        )

        return CancelTaskResponse(
            success=True,
            message_id=message_id,
            execution_id=execution_id,
            worker_id=worker_id,
            status=ExecutionStatus.CANCEL_REQUESTED,
            timestamp=int(time.time() * 1000),
        )

    async def send_message(
        self,
        target_agent_type: str,
        session_id: str,
        content: T,  # Use Generic T for type hinting, handled by interceptors
        tenant_id: str = "",
        action_type: str = "ASK_AGENT",
        parent_message_id: str = "",
        message_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        target_worker_id: Optional[str] = None,
    ) -> SendMessageResponse:
        """
        Send a message to the gateway.

        Routing logic:
        - If target_worker_id is provided, the message is sent directly to that
          worker's control stream (bypassing capability-based routing).
        - Otherwise, the message is sent to the capability-based control stream
          and routed to any available worker that declares the target_agent_type.
        """
        # 1. Prepare parameters for interceptors
        params = {
            "target_agent_type": target_agent_type,
            "session_id": session_id,
            "tenant_id": tenant_id,
            "content": content,
            "action_type": action_type,
            "parent_message_id": parent_message_id,
            "payload": payload or {},
            "metadata": metadata or {},
        }

        # 2. Run interceptors
        for interceptor in self.interceptors:
            params = interceptor.before_send(params)

        # 3. Resolve worker_id (skip registry lookup if targeting a specific worker)
        if target_worker_id:
            worker_id = target_worker_id
        elif self.registry is None:
            raise WorkerRegistryNotSetError("send messages")
        else:
            worker_id = await self.registry.get_target_worker(params["target_agent_type"])
            if not worker_id:
                return SendMessageResponse(
                    success=False,
                    status=ExecutionStatus.FAILED,
                    message_id="",
                    trace_id="",
                    target_worker_id="",
                    timestamp=int(time.time() * 1000),
                )

        if not message_id:
            message_id = f"{MESSAGE_ID_PREFIX}{uuid.uuid4().hex[:8]}"
        if not trace_id:
            trace_id = uuid.uuid4().hex

        header = MessageHeader(
            message_id=message_id,
            session_id=params["session_id"],
            trace_id=trace_id,
            target_agent_type=params["target_agent_type"],
            parent_message_id=params["parent_message_id"],
            tenant_id=params["tenant_id"],
            metadata=params["metadata"],
        )
        if params["action_type"] == ActionType.RESUME.value:
            resume_payload = params["payload"]
            extra_payload = dict(resume_payload)
            status = extra_payload.pop("status", "")
            reply_data = extra_payload.pop("reply_data", None)
            command = ResumeCommand(
                header=header,
                content=params["content"],
                status=status,
                reply_data=reply_data,
                extra_payload=extra_payload,
            )
        else:
            command = AskAgentCommand(
                header=header,
                content=params["content"],
                wait_for_reply=bool(params["payload"].get("wait_for_reply", False)),
                extra_payload={
                    k: v for k, v in params["payload"].items() if k != "wait_for_reply"
                },
            )

        # 4. Route to the appropriate stream
        if target_worker_id:
            stream_name = RedisKeys.worker_ctrl_stream(worker_id)
        else:
            stream_name = RedisKeys.ctrl_stream(params["target_agent_type"])
        await self.redis.xadd(stream_name, command.to_redis_payload())

        return SendMessageResponse(
            success=True,
            message_id=message_id,
            trace_id=trace_id,
            target_worker_id=worker_id,
            timestamp=int(time.time() * 1000),
            status=ExecutionStatus.QUEUED,
        )
