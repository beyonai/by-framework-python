"""
Gateway client module.

Provides the GatewayClient class for sending messages and cancel requests
to Gateway workers via Redis streams.
"""

import json
import time
import uuid
from dataclasses import dataclass
from dataclasses import fields as dataclass_fields
from typing import (TYPE_CHECKING, Any, AsyncIterator, Dict, List, Optional, Protocol)

from by_framework.common.constants import (
    CANCEL_MESSAGE_ID_PREFIX,
    EXECUTION_ID_PREFIX,
    MESSAGE_ID_PREFIX,
    RedisKeys,
)
from by_framework.common.logger import logger
from by_framework.common.redis_client import Redis, get_redis
from by_framework.core.availability import (
    AvailabilityRouter,
    AvailabilityStatus,
    DeliveryIntent,
    RoutePolicy,
)
from by_framework.core.protocol.action_type import ActionType
from by_framework.core.protocol.commands import (
    AskAgentCommand,
    CancelMode,
    CancelTaskCommand,
    ReloadPluginsCommand,
    ResumeCommand,
)
from by_framework.core.protocol.data_message import DataMessage
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.core.protocol.responses import (
    CancelSessionResponse,
    CancelTaskResponse,
    ExecutionStatus,
    SendMessageResponse,
)
from by_framework.core.registry import WorkerRegistry
from by_framework.errors import WorkerRegistryNotSetError
from by_framework.trace.span_recorder import (
    ObservabilityConfig,
    SpanRecorder,
    TraceSpan,
    build_observability_config,
    sanitize_io_value,
    str_to_uint64,
)
from by_framework.trace.trace_schema import TraceRecord
from by_framework.trace.trace_writer import TraceWriteClient

if TYPE_CHECKING:
    pass


class GatewayInterceptor(Protocol):
    """Protocol for client-side request interceptors."""

    def before_send(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Executed before the command is built and routed."""
        ...  # pylint: disable=unnecessary-ellipsis


@dataclass(frozen=True)
class RouteResolution:
    stream_name: str
    target_worker_id: str = ""


@dataclass(frozen=True)
class DataStreamEntry:
    """A decoded entry from a session data stream."""

    stream_id: str
    message: DataMessage


class GatewayClient:
    """Gateway client for sending messages and cancel requests to Gateway workers.

    Communicates with workers via Redis streams, supporting interceptor pattern
    for message content processing.

    Args:
        registry: WorkerRegistry instance for worker discovery
        redis_client: Redis client instance
        interceptors: Message interceptor list
    """

    def __init__(
        self,
        registry: Optional[WorkerRegistry] = None,
        redis_client: Optional[Redis] = None,
        interceptors: Optional[List[GatewayInterceptor]] = None,
        span_recorder: Optional[SpanRecorder] = None,
        obs_config: Optional[ObservabilityConfig] = None,
    ):
        self.registry = registry
        self.redis = (
            redis_client or (registry.redis if registry else None) or get_redis()
        )
        self.interceptors = interceptors or []
        self._obs_config = obs_config or build_observability_config()
        self.span_recorder = span_recorder or SpanRecorder(
            self.redis, config=self._obs_config
        )
        self._trace_writer = TraceWriteClient(
            self.redis, ttl_seconds=self._obs_config.ttl_seconds
        )
        self._langfuse_dispatch_fn = self._resolve_langfuse_dispatch_fn()

    @staticmethod
    def _resolve_langfuse_dispatch_fn() -> Any:
        try:
            from by_framework_trace_langfuse import start_client_dispatch_observation

            return start_client_dispatch_observation
        except ImportError:
            return None

    def add_interceptor(self, interceptor: GatewayInterceptor):
        self.interceptors.append(interceptor)

    @staticmethod
    def _decode_redis_value(value: Any) -> Any:
        """Decode Redis bytes values while preserving already-decoded clients."""
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return value

    @classmethod
    def _decode_data_stream_entry(
        cls, stream_id: Any, fields: Dict[Any, Any]
    ) -> DataStreamEntry:
        raw = fields.get(b"data")
        if raw is None:
            raw = fields.get("data")
        if raw is None:
            raise ValueError("data stream entry missing 'data' field")

        payload = json.loads(cls._decode_redis_value(raw))
        data_message_fields = {field.name for field in dataclass_fields(DataMessage)}
        return DataStreamEntry(
            stream_id=cls._decode_redis_value(stream_id),
            message=DataMessage(
                **{
                    key: value
                    for key, value in payload.items()
                    if key in data_message_fields
                }
            ),
        )

    async def read_data_messages(
        self,
        session_id: str,
        last_id: str = "0-0",
        block_ms: int = 0,
        count: int = 100,
    ) -> List[DataStreamEntry]:
        """Read decoded messages from the session data stream.

        Pass the last returned ``stream_id`` as ``last_id`` to continue from
        the next entry. ``block_ms`` is passed to Redis XREAD; ``0`` means
        block indefinitely on standard Redis clients.
        """
        stream_name = RedisKeys.session_data_stream(session_id)
        messages = await self.redis.xread(
            streams={stream_name: last_id},
            count=count,
            block=block_ms,
        )

        results: List[DataStreamEntry] = []
        for _, msg_list in messages or []:
            for stream_id, fields in msg_list:
                results.append(self._decode_data_stream_entry(stream_id, fields))
        return results

    async def get_data_message_checkpoint(
        self,
        session_id: str,
        consumer_name: str,
    ) -> str:
        """Return the last committed data stream ID for a named consumer."""
        checkpoint = await self.redis.get(
            RedisKeys.session_data_checkpoint(session_id, consumer_name)
        )
        if checkpoint is None:
            return "0-0"
        return self._decode_redis_value(checkpoint)

    async def commit_data_message(
        self,
        session_id: str,
        stream_id: str,
        consumer_name: str,
    ) -> None:
        """Commit a data stream ID as processed for a named consumer."""
        await self.redis.set(
            RedisKeys.session_data_checkpoint(session_id, consumer_name),
            stream_id,
            ex=RedisKeys.DEFAULT_SESSION_TTL,
        )

    async def read_data_messages_from_checkpoint(
        self,
        session_id: str,
        consumer_name: str,
        block_ms: int = 0,
        count: int = 100,
        auto_commit: bool = False,
    ) -> List[DataStreamEntry]:
        """Read messages starting after a named consumer's committed checkpoint."""
        last_id = await self.get_data_message_checkpoint(session_id, consumer_name)
        entries = await self.read_data_messages(
            session_id=session_id,
            last_id=last_id,
            block_ms=block_ms,
            count=count,
        )
        if auto_commit and entries:
            await self.commit_data_message(
                session_id=session_id,
                stream_id=entries[-1].stream_id,
                consumer_name=consumer_name,
            )
        return entries

    async def iter_data_messages(
        self,
        session_id: str,
        last_id: str = "$",
        block_ms: int = 5000,
        count: int = 100,
    ) -> AsyncIterator[DataStreamEntry]:
        """Continuously consume decoded messages from the session data stream.

        The iterator does not stop on its own. Callers should break when their
        business-level terminal event is observed.
        """
        current_id = last_id
        while True:
            entries = await self.read_data_messages(
                session_id=session_id,
                last_id=current_id,
                block_ms=block_ms,
                count=count,
            )
            for entry in entries:
                current_id = entry.stream_id
                yield entry

    async def consume_data_messages(
        self,
        session_id: str,
        consumer_name: str,
        block_ms: int = 5000,
        count: int = 100,
    ) -> AsyncIterator[DataStreamEntry]:
        """Continuously consume data stream messages with checkpoint commits.

        Each entry is committed after the caller's loop body completes and asks
        for the next item. If processing fails or the iterator is closed before
        the next item, the current entry is not committed and will be retried
        from the checkpoint on the next consumer run. The iterator does not stop
        on its own; callers should break on their terminal event.
        """
        current_id = await self.get_data_message_checkpoint(
            session_id=session_id,
            consumer_name=consumer_name,
        )
        while True:
            entries = await self.read_data_messages(
                session_id=session_id,
                last_id=current_id,
                block_ms=block_ms,
                count=count,
            )
            for entry in entries:
                yield entry
                await self.commit_data_message(
                    session_id=session_id,
                    stream_id=entry.stream_id,
                    consumer_name=consumer_name,
                )
                current_id = entry.stream_id

    async def reload_plugins_for_agent_type(
        self,
        agent_type: str,
        reason: str = "",
        reload_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Fan out a reload command to all online workers of an agent type."""
        if self.registry is None:
            raise WorkerRegistryNotSetError("reload plugins for agent type")

        has_online_agent_type, worker_ids = await self.registry.has_online_agent_type(
            agent_type
        )
        if not reload_id:
            reload_id = f"reload-{uuid.uuid4().hex[:8]}"

        if not has_online_agent_type or not worker_ids:
            return {
                "reload_id": reload_id,
                "agent_type": agent_type,
                "worker_ids": [],
                "dispatched_count": 0,
            }

        for worker_id in worker_ids:
            command = ReloadPluginsCommand(
                header=MessageHeader(
                    message_id=f"{MESSAGE_ID_PREFIX}{uuid.uuid4().hex[:8]}",
                    session_id=f"reload:{agent_type}",
                    trace_id=uuid.uuid4().hex,
                    target_agent_type=agent_type,
                    metadata=metadata or {},
                ),
                reload_id=reload_id,
                reason=reason,
            )
            await self.redis.xadd(
                RedisKeys.worker_ctrl_stream(worker_id),
                command.to_redis_payload(),
            )

        return {
            "reload_id": reload_id,
            "agent_type": agent_type,
            "worker_ids": list(worker_ids),
            "dispatched_count": len(worker_ids),
        }

    async def collect_reload_acks(
        self,
        reload_id: str,
        last_id: str = "0-0",
        block_ms: int = 0,
        count: int = 100,
    ) -> List[Dict[str, Any]]:
        """Read reload ACK payloads from the ACK stream."""
        messages = await self.redis.xread(
            streams={RedisKeys.plugin_reload_ack_stream(reload_id): last_id},
            count=count,
            block=block_ms,
        )
        results: List[Dict[str, Any]] = []
        for _, msg_list in messages or []:
            for _, fields in msg_list:
                raw = fields.get(b"data") if isinstance(fields, dict) else None
                if raw is None and isinstance(fields, dict):
                    raw = fields.get("data")
                if raw is None:
                    continue
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                results.append(dict(json.loads(raw)))
        return results

    async def get_worker_status(self, worker_id: str) -> Dict[str, Any]:
        """Return worker liveness metadata and execution state summary."""
        if self.registry is None:
            raise WorkerRegistryNotSetError("get worker status")
        return await self.registry.get_worker_execution_summary(worker_id)

    async def _resolve_agent_type_route(
        self, target_agent_type: str, route_policy: str
    ) -> RouteResolution:
        """Resolve agent-type-mode routing.

        Agent type sends always publish to the agent-type stream. When the
        online check is enabled, we only verify that at least one online worker exists.
        """
        if self.registry is None:
            raise WorkerRegistryNotSetError("send messages")

        if route_policy == RoutePolicy.SEND_ANYWAY:
            return RouteResolution(stream_name=RedisKeys.ctrl_stream(target_agent_type))

        has_online_agent_type, _workers = await self.registry.has_online_agent_type(  # pylint: disable=C0103,unused-variable
            target_agent_type
        )
        if has_online_agent_type:
            return RouteResolution(stream_name=RedisKeys.ctrl_stream(target_agent_type))

        raise ValueError(f"No online worker found for agent_type '{target_agent_type}'")

    async def _resolve_direct_worker_route(
        self,
        target_worker_id: str,
        check_online: bool,
    ) -> RouteResolution:
        """Resolve direct-worker routing for debug or worker-specific control."""
        if self.registry is not None and check_online:
            is_online = await self.registry.is_worker_online(target_worker_id)
            if not is_online:
                raise LookupError(
                    f"Target worker '{target_worker_id}' is not online "
                    "or not registered"
                )
        return RouteResolution(
            stream_name=RedisKeys.worker_ctrl_stream(target_worker_id),
            target_worker_id=target_worker_id,
        )

    def _build_gateway_command(
        self,
        *,
        action_type: str,
        header: MessageHeader,
        content: Any,
        extra_payload: Dict[str, Any],
    ) -> AskAgentCommand | ResumeCommand:
        """Build a gateway command from parameters."""
        if action_type == ActionType.RESUME.value:
            resume_extra_payload = dict(extra_payload)
            status = resume_extra_payload.pop("status", "")
            reply_data = resume_extra_payload.pop("reply_data", None)
            return ResumeCommand(
                header=header,
                content=content,
                status=status,
                reply_data=reply_data,
                extra_payload=resume_extra_payload,
            )

        return AskAgentCommand(
            header=header,
            content=content,
            wait_for_reply=bool(extra_payload.get("wait_for_reply", False)),
            extra_payload={
                k: v for k, v in extra_payload.items() if k != "wait_for_reply"
            },
        )

    async def cancel_task(
        self,
        message_id: str,
        session_id: str,
        reason: str = "",
        target_agent_type: str = "",  # pylint: disable=unused-argument
        requested_by: str = "client",
        cancel_mode: str = CancelMode.GRACEFUL,
    ) -> CancelTaskResponse:
        """Cascade cancel the specified task and all its subtasks.

        Rebuilds the task tree via parent_message_id chain in session registry,
        traverses BFS from target message_id, and cancels all non-terminal subtasks.
        """
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

        # --- Cascade cancel: build task tree and BFS traverse ---
        all_executions = await self.registry.get_all_session_executions(session_id)

        # Build parent_message_id -> children mapping
        children_map: dict[str, list[dict]] = {}
        for ex in all_executions:
            parent = ex.get("parent_message_id", "")
            if parent:
                children_map.setdefault(parent, []).append(ex)

        # BFS: from target message_id, collect all nodes that need to be cancelled
        terminal_states = {"COMPLETED", "FAILED", "CANCELLED"}
        queue = [execution]
        to_cancel: list[dict] = []
        terminal_ancestors: list[dict] = []

        while queue:
            current = queue.pop(0)
            cur_status = current.get("status", "")
            # Even if current node is completed, still need to traverse its subtasks
            # (they may still be running)
            if cur_status not in terminal_states:
                to_cancel.append(current)
            else:
                # Terminal nodes also need cancel_requested flag to prevent
                # sub-Agent callback waking them up
                terminal_ancestors.append(current)
            # Always add child tasks to the queue
            cur_msg_id = current.get("message_id", "")
            if cur_msg_id in children_map:
                queue.extend(children_map[cur_msg_id])

        # Mark cancel_requested on terminal ancestors (without changing their state)
        for ancestor in terminal_ancestors:
            await self.registry.mark_cancel_requested(
                ancestor.get("execution_id", ""), session_id, reason
            )

        if not to_cancel:
            return CancelTaskResponse(
                success=False,
                message_id=message_id,
                execution_id=execution.get("execution_id", ""),
                worker_id=execution.get("worker_id", ""),
                status=ExecutionStatus.ALREADY_FINISHED,
                timestamp=int(time.time() * 1000),
                error=f"execution already in terminal state: {execution_status}",
            )

        # Mark and send control command for each node to cancel
        for node in to_cancel:
            node_execution_id = node.get("execution_id", "")
            node_worker_id = node.get("worker_id", "")
            node_message_id = node.get("message_id", "")

            await self.registry.mark_execution_cancelling(
                node_execution_id, session_id, reason
            )

            if node_worker_id:
                node_trace_id = (
                    node.get("trace_id")
                    or execution.get("trace_id")
                    or uuid.uuid4().hex
                )
                cancel_command = CancelTaskCommand(
                    header=MessageHeader(
                        message_id=f"{CANCEL_MESSAGE_ID_PREFIX}{uuid.uuid4().hex[:8]}",
                        session_id=session_id,
                        trace_id=node_trace_id,
                        target_agent_type=node.get("target_agent_type", ""),
                        parent_message_id=node_message_id,
                    ),
                    target_message_id=node_message_id,
                    target_execution_id=node_execution_id,
                    target_worker_id=node_worker_id,
                    reason=reason,
                    requested_by=requested_by,
                    cancel_mode=cancel_mode,
                )
                await self.redis.xadd(
                    RedisKeys.worker_ctrl_stream(node_worker_id),
                    cancel_command.to_redis_payload(),
                )

        return CancelTaskResponse(
            success=True,
            message_id=message_id,
            execution_id=execution.get("execution_id", ""),
            worker_id=execution.get("worker_id", ""),
            status=ExecutionStatus.CANCEL_REQUESTED,
            timestamp=int(time.time() * 1000),
            cancelled_count=len(to_cancel),
        )

    async def cancel_session(
        self,
        session_id: str,
        reason: str = "",
        requested_by: str = "client",
        cancel_mode: str = CancelMode.GRACEFUL,
    ) -> CancelSessionResponse:
        """Cancel every active execution registered under a session.

        Unlike cancel_task, this does not require a starting message_id:
        it cancels the whole session's active executions in one call.
        """
        if self.registry is None:
            raise ValueError("GatewayClient requires a WorkerRegistry to cancel tasks")

        all_executions = await self.registry.get_all_session_executions(session_id)

        if not all_executions:
            return CancelSessionResponse(
                success=False,
                session_id=session_id,
                status=ExecutionStatus.NOT_FOUND,
                timestamp=int(time.time() * 1000),
                cancelled_count=0,
                already_finished_count=0,
            )

        terminal_states = {"COMPLETED", "FAILED", "CANCELLED"}
        to_cancel = [
            node
            for node in all_executions
            if node.get("status", "") not in terminal_states
        ]
        terminal_nodes = [
            node for node in all_executions if node.get("status", "") in terminal_states
        ]

        # Flag terminal executions so a still-running descendant can't wake
        # them back up via callback, without changing their status.
        for node in terminal_nodes:
            await self.registry.mark_cancel_requested(
                node.get("execution_id", ""), session_id, reason
            )

        if not to_cancel:
            return CancelSessionResponse(
                success=False,
                session_id=session_id,
                status=ExecutionStatus.ALREADY_FINISHED,
                timestamp=int(time.time() * 1000),
                cancelled_count=0,
                already_finished_count=len(terminal_nodes),
            )

        for node in to_cancel:
            node_execution_id = node.get("execution_id", "")
            node_worker_id = node.get("worker_id", "")
            node_message_id = node.get("message_id", "")

            await self.registry.mark_execution_cancelling(
                node_execution_id, session_id, reason
            )

            if node_worker_id:
                cancel_command = CancelTaskCommand(
                    header=MessageHeader(
                        message_id=f"{CANCEL_MESSAGE_ID_PREFIX}{uuid.uuid4().hex[:8]}",
                        session_id=session_id,
                        trace_id=node.get("trace_id") or uuid.uuid4().hex,
                        target_agent_type=node.get("target_agent_type", ""),
                        parent_message_id=node_message_id,
                    ),
                    target_message_id=node_message_id,
                    target_execution_id=node_execution_id,
                    target_worker_id=node_worker_id,
                    reason=reason,
                    requested_by=requested_by,
                    cancel_mode=cancel_mode,
                )
                await self.redis.xadd(
                    RedisKeys.worker_ctrl_stream(node_worker_id),
                    cancel_command.to_redis_payload(),
                )

        return CancelSessionResponse(
            success=True,
            session_id=session_id,
            status=ExecutionStatus.CANCEL_REQUESTED,
            timestamp=int(time.time() * 1000),
            cancelled_count=len(to_cancel),
            already_finished_count=len(terminal_nodes),
        )

    async def send_message(
        self,
        target_agent_type: str,
        session_id: str,
        content: Any,
        user_code: str = "",
        user_name: str = "",
        action_type: str = "ASK_AGENT",
        parent_message_id: str = "",
        message_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        extra_payload: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        target_worker_id: Optional[str] = None,
        route_policy: str = RoutePolicy.FAIL_FAST,
        availability_timeout_ms: int = 30000,
        region: Optional[str] = None,
        priority: int = 0,
    ) -> SendMessageResponse:
        """
        Send a message to the gateway.

        Routing logic:
        - If target_worker_id is provided, the message is sent directly to that
          worker's control stream (bypassing agent-type-based routing).
        - Otherwise, the message is sent to the agent-type-based control stream
          and routed to any available worker that declares the target_agent_type.

        Args:
            route_policy: Controls online checks and unavailable-agent behavior.
        """
        # 1. Prepare parameters for interceptors
        params = {
            "target_agent_type": target_agent_type,
            "session_id": session_id,
            "user_code": user_code,
            "user_name": user_name,
            "content": content,
            "action_type": action_type,
            "parent_message_id": parent_message_id,
            "extra_payload": extra_payload or {},
            "metadata": metadata or {},
        }

        # 2. Run interceptors
        for interceptor in self.interceptors:
            params = interceptor.before_send(params)

        if not message_id:
            message_id = f"{MESSAGE_ID_PREFIX}{uuid.uuid4().hex[:8]}"
        if not trace_id:
            trace_id = uuid.uuid4().hex
        trace_start_ts = int(time.time() * 1000)
        is_root_dispatch = not params.get("parent_message_id", "")

        # Write trace root start so the trace is visible even before the worker
        # picks up the task.  Only written for root dispatches (no parent).
        if is_root_dispatch:
            await self._write_trace_root_start(
                trace_id=trace_id,
                message_id=message_id,
                session_id=str(params.get("session_id", "")),
                target_agent_type=str(params.get("target_agent_type", "")),
                content=params.get("content"),
                start_ts=trace_start_ts,
            )

        metadata = dict(params.get("metadata", {}) or {})
        trace_parent_span_id = metadata.pop("trace_parent_span_id", "")
        langfuse_parent_observation_id = metadata.pop(
            "langfuse_parent_observation_id", ""
        )
        if not trace_parent_span_id:
            trace_parent_span_id = (
                f"{str_to_uint64(f'{message_id}:client.dispatch'):016x}"
            )

        langfuse_client_dispatch = None
        if not langfuse_parent_observation_id:
            langfuse_client_dispatch = self._start_langfuse_client_dispatch_observation(
                trace_id=trace_id,
                message_id=message_id,
                target_agent_type=params["target_agent_type"],
                session_id=params["session_id"],
                user_code=params["user_code"],
                user_name=params["user_name"],
                content=params["content"],
                metadata=metadata,
            )
            observation_id = getattr(langfuse_client_dispatch, "id", "")
            langfuse_parent_observation_id = observation_id or trace_parent_span_id

        header = MessageHeader(
            message_id=message_id,
            session_id=params["session_id"],
            trace_id=trace_id,
            target_agent_type=params["target_agent_type"],
            parent_message_id=params["parent_message_id"],
            user_code=params["user_code"],
            user_name=params["user_name"],
            metadata=metadata,
            trace_parent_span_id=trace_parent_span_id,
            langfuse_parent_observation_id=langfuse_parent_observation_id,
        )
        command = self._build_gateway_command(
            action_type=params["action_type"],
            header=header,
            content=params["content"],
            extra_payload=params["extra_payload"],
        )

        # A RESUME reuses the message_id of the original AskAgentCommand so the
        # worker can look the suspended execution back up. Reuse its
        # execution_id too, and skip re-initializing the registry record for
        # it below -- initialize_execution() would otherwise overwrite the
        # message_id -> execution_id mapping and detach this resume from the
        # execution it's meant to continue.
        resumed_execution = None
        if (
            params["action_type"] == ActionType.RESUME.value
            and self.registry
            and hasattr(self.registry, "get_execution_by_message_id")
        ):
            resumed_execution = await self.registry.get_execution_by_message_id(
                message_id, session_id=params["session_id"]
            )

        execution_id = (
            resumed_execution["execution_id"]
            if resumed_execution
            else f"{EXECUTION_ID_PREFIX}{uuid.uuid4().hex[:8]}"
        )

        # 3. Resolve route and optionally probe agent type/liveness
        should_dispatch_control = True
        try:
            if target_worker_id:
                route = await self._resolve_direct_worker_route(
                    target_worker_id,
                    route_policy != RoutePolicy.SEND_ANYWAY,
                )
            else:
                avail_start_ms = int(time.time() * 1000)
                availability = await AvailabilityRouter(
                    self.redis, self.registry
                ).prepare_delivery(
                    DeliveryIntent(
                        execution_id=execution_id,
                        message_id=message_id,
                        session_id=params["session_id"],
                        trace_id=trace_id,
                        source="client",
                        target_agent_type=params["target_agent_type"],
                        user_code=params["user_code"],
                        region=region or "",
                        priority=priority,
                        policy=route_policy,
                        timeout_ms=availability_timeout_ms,
                        command_payload=command.to_dict(),
                        metadata=params["metadata"],
                    )
                )
                try:
                    from by_framework.metrics import record_availability_metrics

                    record_availability_metrics(
                        agent_type=params["target_agent_type"],
                        policy=route_policy,
                        status=availability.status,
                        routing_ms=float(int(time.time() * 1000) - avail_start_ms),
                    )
                except Exception:  # pylint: disable=broad-exception-caught
                    pass
                if availability.status not in (
                    AvailabilityStatus.DELIVER_NOW,
                    AvailabilityStatus.WAIT_AND_DELIVER,
                    AvailabilityStatus.FALLBACK_TO_OTHER_AGENT_TYPE,
                    AvailabilityStatus.QUEUE_PENDING,
                ):
                    if self.registry and hasattr(
                        self.registry, "record_failed_route_decision"
                    ):
                        await self.registry.record_failed_route_decision(
                            execution_id=execution_id,
                            message_id=message_id,
                            session_id=params["session_id"],
                            trace_id=trace_id,
                            target_agent_type=params["target_agent_type"],
                            parent_message_id=params["parent_message_id"] or "",
                            source_agent_type="client",
                            route_policy=route_policy,
                            route_status=availability.status,
                            stream_name=availability.stream_name or "",
                            selected_agent_type=availability.selected_agent_type or "",
                            availability_error_code=availability.error_code or "",
                            availability_error=availability.error or "",
                        )
                    response = SendMessageResponse(
                        success=False,
                        status=ExecutionStatus.FAILED,
                        message_id="",
                        trace_id="",
                        target_worker_id="",
                        timestamp=int(time.time() * 1000),
                        error=availability.error,
                        error_code=availability.error_code
                        or ExecutionStatus.ERR_AGENT_TYPE_UNAVAILABLE,
                    )
                    self._end_langfuse_client_dispatch_observation(
                        langfuse_client_dispatch,
                        output={"success": False, "error": availability.error},
                        error=availability.error,
                    )
                    if is_root_dispatch:
                        await self._write_trace_root_end(
                            trace_id=trace_id,
                            status="FAILED",
                            end_ts=int(time.time() * 1000),
                            error=availability.error or "",
                        )
                    return response
                if availability.status == AvailabilityStatus.QUEUE_PENDING:
                    should_dispatch_control = False
                route = RouteResolution(
                    stream_name=availability.stream_name,
                    target_worker_id=availability.target_worker_id,
                )
                if availability.selected_agent_type:
                    params["target_agent_type"] = availability.selected_agent_type
                    command.header.target_agent_type = availability.selected_agent_type
        except LookupError as err:
            self._end_langfuse_client_dispatch_observation(
                langfuse_client_dispatch,
                output={"success": False, "error": str(err)},
                error=str(err),
            )
            if is_root_dispatch:
                await self._write_trace_root_end(
                    trace_id=trace_id,
                    status="FAILED",
                    end_ts=int(time.time() * 1000),
                    error=str(err),
                )
            return SendMessageResponse(
                success=False,
                status=ExecutionStatus.FAILED,
                message_id="",
                trace_id="",
                target_worker_id=target_worker_id or "",
                timestamp=int(time.time() * 1000),
                error=str(err),
                error_code=ExecutionStatus.ERR_WORKER_NOT_ONLINE,
            )
        except ValueError as err:
            self._end_langfuse_client_dispatch_observation(
                langfuse_client_dispatch,
                output={"success": False, "error": str(err)},
                error=str(err),
            )
            if is_root_dispatch:
                await self._write_trace_root_end(
                    trace_id=trace_id,
                    status="FAILED",
                    end_ts=int(time.time() * 1000),
                    error=str(err),
                )
            return SendMessageResponse(
                success=False,
                status=ExecutionStatus.FAILED,
                message_id="",
                trace_id="",
                target_worker_id="",
                timestamp=int(time.time() * 1000),
                error=str(err),
                error_code=ExecutionStatus.ERR_AGENT_TYPE_UNAVAILABLE,
            )

        # Initialize execution tracking. Skipped for a RESUME that reattached
        # to an existing execution -- that execution is already tracked, and
        # re-initializing it here would overwrite its message_id mapping.
        if (
            not resumed_execution
            and self.registry
            and hasattr(self.registry, "initialize_execution")
        ):
            try:
                await self.registry.initialize_execution(
                    {
                        "execution_id": execution_id,
                        "message_id": message_id,
                        "session_id": params["session_id"],
                        "trace_id": trace_id,
                        "parent_message_id": params["parent_message_id"] or "",
                        "source_agent_type": "client",
                        "target_agent_type": params["target_agent_type"],
                        "stream_name": route.stream_name,
                        "status": "QUEUED",
                        "route_policy": route_policy,
                        "route_status": availability.status
                        if not target_worker_id
                        else "DIRECT_WORKER",
                        "selected_agent_type": availability.selected_agent_type
                        if not target_worker_id
                        else "",
                        "availability_error_code": availability.error_code
                        if not target_worker_id
                        else "",
                        "availability_error": availability.error
                        if not target_worker_id
                        else "",
                    }
                )
            except Exception:  # pylint: disable=broad-exception-caught
                pass  # Fallback if registry fails

        # 4. Route to the appropriate stream
        dispatch_started_at = int(time.time() * 1000)
        if should_dispatch_control:
            await self.redis.xadd(route.stream_name, command.to_redis_payload())
        await self._record_client_dispatch_span(
            trace_id=trace_id,
            message_id=message_id,
            session_id=params["session_id"],
            parent_message_id=params["parent_message_id"] or "",
            target_agent_type=params["target_agent_type"],
            target_worker_id=route.target_worker_id,
            route_policy=route_policy,
            route_status=availability.status
            if not target_worker_id
            else "DIRECT_WORKER",
            start_ts=dispatch_started_at,
            end_ts=int(time.time() * 1000),
        )

        response = SendMessageResponse(
            success=True,
            message_id=message_id,
            trace_id=trace_id,
            target_worker_id=route.target_worker_id,
            timestamp=int(time.time() * 1000),
            status=ExecutionStatus.QUEUED,
        )
        self._end_langfuse_client_dispatch_observation(
            langfuse_client_dispatch,
            output={
                "success": True,
                "message_id": message_id,
                "trace_id": trace_id,
                "target_worker_id": route.target_worker_id,
                "status": response.status,
            },
        )
        return response

    def _start_langfuse_client_dispatch_observation(
        self,
        *,
        trace_id: str,
        message_id: str,
        target_agent_type: str,
        session_id: str,
        user_code: str,
        user_name: str,
        content: Any,
        metadata: Dict[str, Any],
    ) -> Any:
        if self._langfuse_dispatch_fn is None:
            return None
        try:
            return self._langfuse_dispatch_fn(
                trace_id=trace_id,
                message_id=message_id,
                target_agent_type=target_agent_type,
                session_id=session_id,
                user_code=user_code,
                user_name=user_name,
                content=content,
                metadata=metadata,
            )
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.warning(
                "Langfuse client.dispatch observation skipped: %s",
                err,
            )
            return None

    @staticmethod
    def _end_langfuse_client_dispatch_observation(
        observation: Any,
        *,
        output: Any,
        error: str = "",
    ) -> None:
        if observation is None:
            return
        try:
            if error and hasattr(observation, "update"):
                observation.update(level="ERROR", status_message=error)
            observation.end(output=output)
        except TypeError:
            try:
                observation.update(output=output)
                observation.end()
            except Exception:  # pylint: disable=broad-exception-caught
                pass
        except Exception:  # pylint: disable=broad-exception-caught
            pass

    async def _record_client_dispatch_span(
        self,
        *,
        trace_id: str,
        message_id: str,
        session_id: str,
        parent_message_id: str,
        target_agent_type: str,
        target_worker_id: str,
        route_policy: str,
        route_status: str,
        start_ts: int,
        end_ts: int,
    ) -> None:
        try:
            logger.info(
                "Recording client dispatch span: message_id=%s, trace_id=%s",
                message_id,
                trace_id,
            )
            await self.span_recorder.record_span(
                TraceSpan(
                    trace_id=trace_id,
                    span_id=f"{message_id}:client.dispatch",
                    parent_span_id="",
                    operation="client.dispatch",
                    component="client",
                    start_ts=start_ts,
                    end_ts=end_ts,
                    status="COMPLETED",
                    session_id=session_id,
                    message_id=message_id,
                    parent_message_id=parent_message_id,
                    worker_id=target_worker_id,
                    source_agent_type="client",
                    target_agent_type=target_agent_type,
                    route_policy=route_policy,
                    route_status=route_status,
                )
            )
            logger.info(
                "Client dispatch span recorded successfully for message_id=%s",
                message_id,
            )
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.warning(
                "Failed to record client dispatch span: %s", err, exc_info=True
            )

    async def _write_trace_root_start(
        self,
        *,
        trace_id: str,
        message_id: str,
        session_id: str,
        target_agent_type: str,
        content: Any,
        start_ts: int,
    ) -> None:
        """Write trace root start — best-effort, never propagates errors."""
        try:
            await self._trace_writer.record_trace(
                TraceRecord(
                    trace_id=trace_id,
                    name=target_agent_type,
                    session_id=session_id,
                    root_message_id=message_id,
                    root_agent_type=target_agent_type,
                    input=sanitize_io_value(content, self._obs_config),
                    status="QUEUED",
                    start_ts=start_ts,
                )
            )
        except Exception:  # pylint: disable=broad-exception-caught
            pass

    async def _write_trace_root_end(
        self,
        *,
        trace_id: str,
        status: str,
        end_ts: int,
        error: str = "",
    ) -> None:
        """Write trace root end on routing failure — best-effort."""
        try:
            meta: dict = {"error": error} if error else {}
            await self._trace_writer.record_trace(
                TraceRecord(
                    trace_id=trace_id,
                    status=status,
                    end_ts=end_ts,
                    metadata=meta,
                )
            )
        except Exception:  # pylint: disable=broad-exception-caught
            pass
