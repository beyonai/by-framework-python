"""
Agent context module.

Provides the AgentContext class which serves as the runtime context for agent
task execution, providing access to session state, event emission,
and inter-agent communication.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from contextlib import asynccontextmanager
from contextvars import ContextVar
from importlib import import_module
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

from typing_extensions import deprecated

from by_framework.common.constants import (
    EXECUTION_ID_PREFIX,
    MESSAGE_ID_PREFIX,
    TASK_GROUP_FIELD_COMPLETED,
    TASK_GROUP_FIELD_SOURCE_AGENT,
    TASK_GROUP_FIELD_TOTAL,
    TASK_GROUP_ID_PREFIX,
    TASK_GROUP_TTL_SECONDS,
    RedisKeys,
)
from by_framework.common.emitter import DataLayoutBuilder, GatewayDataEmitter
from by_framework.common.logger import logger
from by_framework.common.redis_client import Redis, get_redis
from by_framework.core.availability import (
    AvailabilityRouter,
    AvailabilityStatus,
    DeliveryIntent,
    RoutePolicy,
)
from by_framework.core.extensions import AgentConfig
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.protocol.commands import AskAgentCommand, ResumeCommand
from by_framework.core.protocol.content_codec import ContentCodec, WireContent
from by_framework.core.protocol.content_type import SseReasonMessageType
from by_framework.core.protocol.event_type import EventType
from by_framework.core.protocol.events import (
    ArtifactEvent,
    AskUserEvent,
    StateChangeEvent,
    StreamChunkEvent,
)
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.core.runtime import AgentRuntimeState
from by_framework.core.runtime.file_permissions import FilePermissionPolicy
from by_framework.core.runtime.filestore.base import FileStorage
from by_framework.trace.span_recorder import (SpanRecorder, TraceSpan, str_to_uint64)

if TYPE_CHECKING:
    from by_framework.core.extensions import PluginRegistry


# Context variable for tracking current (message_id, parent_message_id)
_current_ids_var: ContextVar[tuple[str, str]] = ContextVar(
    "_current_ids_var", default=("", "")
)

# Context variable for tracking current AgentContext
current_agent_context_var: ContextVar[Optional[AgentContext]] = ContextVar(
    "current_agent_context_var", default=None
)

# Context variable for the current worker id; set by WorkerRunner before processing.
current_worker_id_var: ContextVar[str] = ContextVar("current_worker_id_var", default="")
_LANGFUSE_CURRENT_OBSERVATION_GETTER: Callable[[], Any] | None | bool = None


class AgentContext:
    """Agent runtime context.

    Provides access to session state, event emission, and inter-agent
    communication during task execution.

    Args:
        session_id: Session ID
        trace_id: Trace ID
        redis_client: Redis client instance
        data_stream_name: Data stream name
        current_agent_id: Current agent ID
        current_message_id: Current message ID
        current_command: Current command
        cancel_event: Cancel event
        cancel_reason: Cancel reason
        plugin_registry: Plugin registry
    """

    def __init__(
        self,
        session_id: str,
        trace_id: str,
        redis_client: Optional[Redis] = None,
        data_stream_name: Optional[str] = None,
        current_agent_id: str = "",
        message_id: str = "",
        parent_message_id: str = "",
        current_command: Optional[Any] = None,
        cancel_event: Optional[asyncio.Event] = None,
        cancel_reason: str = "",
        plugin_registry: Optional[PluginRegistry] = None,
        user_code: Optional[str] = None,
        user_name: Optional[str] = None,
        workspace_dir: Optional[str] = None,
        agent_configs: Optional[list[AgentConfig]] = None,
        agent_configs_version: int = 0,
        storage: Optional[FileStorage] = None,
        permission_policy: Optional[FilePermissionPolicy] = None,
        content_codec: Optional[ContentCodec] = None,
        layout_builder: Optional[DataLayoutBuilder] = None,
        is_sub_agent: bool = False,
        execution_id: str = "",
        worker_id: str = "",
        span_recorder: Optional[SpanRecorder] = None,
    ):
        self.redis = redis_client or get_redis()
        self.session_id = session_id
        self.trace_id = trace_id
        self.data_stream_name = data_stream_name
        self.current_agent_id = current_agent_id
        self.execution_id = execution_id
        self.worker_id = worker_id
        self.span_recorder = span_recorder or SpanRecorder(self.redis)
        self._chunk_count: int = 0

        # Record initial IDs
        self._initial_message_id = message_id
        self._initial_parent_message_id = parent_message_id

        # Initialize IDs for current execution context
        # Note: Here we set the initial ID for the current coroutine
        _current_ids_var.set((message_id, parent_message_id))

        self.current_command = current_command
        self.cancel_event = cancel_event
        self.cancel_reason = cancel_reason
        self.emitter = GatewayDataEmitter(
            self.redis, data_stream_name, layout_builder=layout_builder
        )
        self._response_buffer = []  # Used to collect streaming response content
        self._is_history_saved = False  # Prevent duplicate saves
        self._is_stream_finished = False  # Flag: has APP_STREAM_RESPONSE been sent
        self.content_codec = content_codec
        # Flag: stream permission transferred to sub-calls not waiting
        self._permission_transferred = False
        # Flag: execution suspended due to calling agents or waiting
        self._is_suspended = False
        self._trace_parent_observation_id = ""
        self._token_usage: dict[str, Any] = {}
        self.plugin_registry = plugin_registry
        self.agent_configs_version = agent_configs_version
        self.is_sub_agent = is_sub_agent

        # New: AgentRuntimeState for unified state management
        self._agent_runtime_state = AgentRuntimeState(
            session_id=session_id,
            user_code=user_code,
            user_name=user_name,
            storage=storage,
            workspace_dir=workspace_dir,
            agent_configs=agent_configs,
            permission_policy=permission_policy,
            agent_id=current_agent_id,
        )

    @asynccontextmanager
    async def use_context(self):
        """Asynchronous context manager to bind this AgentContext.

        Binds this context to the current coroutine context.
        """
        token = current_agent_context_var.set(self)
        try:
            yield
        finally:
            current_agent_context_var.reset(token)

    @property
    def message_id(self) -> str:
        """Get the current context's message ID."""
        return _current_ids_var.get()[0]

    @message_id.setter
    def message_id(self, value: str) -> None:
        """Manually set the current context's message ID."""
        _, p = _current_ids_var.get()
        _current_ids_var.set((value, p))

    @property
    def parent_message_id(self) -> str:
        """Get the current context's parent message ID."""
        return _current_ids_var.get()[1]

    @parent_message_id.setter
    def parent_message_id(self, value: str) -> None:
        """Manually set the current context's parent message ID."""
        m, _ = _current_ids_var.get()
        _current_ids_var.set((m, value))

    @property
    def initial_message_id(self) -> str:
        """Get the original task's message ID."""
        return self._initial_message_id

    @property
    def initial_parent_message_id(self) -> str:
        """Get the original task's parent message ID."""
        return self._initial_parent_message_id

    @asynccontextmanager
    async def sub_step(
        self,
        title: str,
        content_type: str = SseReasonMessageType.think_text.value,
        event_type: str = EventType.REASONING_LOG_DELTA.value,
    ):
        """Start a hierarchical internal sub-step.

        This context manager automatically:
        1. Generate a new sub_id.
        2. Send a status message identifying the start of this step.
        3. Automatically switch the current context's message_id within the block.

        Args:
            title: Step name
        """
        sub_id = self.generate_message_id()
        parent_id = self.message_id

        # 1. Emit hierarchical title (usually displayed as reasoning log)
        await self.emit_chunk(
            event=title,
            message_id=sub_id,
            parent_message_id=parent_id,
            content_type=content_type,
            event_type=event_type,
        )

        # 2. Push new ID context
        token = _current_ids_var.set((sub_id, parent_id))
        try:
            yield sub_id, parent_id
        finally:
            # 3. Restore old ID context
            _current_ids_var.reset(token)

    def get_trace_stack(self) -> List[tuple[str, str]]:
        """Get the current ID trace stack."""
        # ContextVar doesn't support stack traversal; return current pair.
        return [_current_ids_var.get()]

    @property
    def agent_runtime_state(self) -> AgentRuntimeState:
        """Get the unified agent runtime state container.

        Provides access to:
        - session_manager: Session management and file management
        - config_manager: Agent configuration management

        Returns:
            AgentRuntimeState instance
        """
        return self._agent_runtime_state

    @property
    def agent_configs(self) -> list[AgentConfig]:
        """Get the list of agent configurations.

        Deprecated: Use agent_runtime_state.config_manager.list_configs() instead.
        """
        return self._agent_runtime_state.config_manager.list_configs()

    @deprecated("use agent_runtime_state.config_manager instead")
    def set_agent_configs(self, new_configs: list[AgentConfig]) -> None:
        self._agent_runtime_state.config_manager.set_configs(new_configs)

    @deprecated("use agent_runtime_state.config_manager instead")
    def get_agent_config(self, agent_id: str) -> AgentConfig | None:
        return self._agent_runtime_state.config_manager.get_config(agent_id)

    @deprecated("use agent_runtime_state.config_manager instead")
    def list_agent_configs(self) -> list[AgentConfig]:
        return self._agent_runtime_state.config_manager.list_configs()

    def is_cancel_requested(self) -> bool:
        return bool(self.cancel_event and self.cancel_event.is_set())

    async def check_cancelled(self) -> None:
        if self.is_cancel_requested():
            raise asyncio.CancelledError(self.cancel_reason or "task cancelled")

    async def get_active_workers(self) -> Dict[str, Any]:
        """
        Get all active workers and their capability information in the cluster
        """
        from by_framework.core.registry import WorkerRegistry

        registry = WorkerRegistry(self.redis)
        return await registry.get_all_workers()

    def generate_message_id(self) -> str:
        """Generate a new message ID.

        Uses the predefined MESSAGE_ID_PREFIX and UUID fragment.
        """
        return f"{MESSAGE_ID_PREFIX}{uuid.uuid4().hex[:8]}"

    async def emit_chunk(
        self,
        event: Union[StreamChunkEvent, str],
        event_type: Optional[str] = None,
        content_type: Optional[str] = None,
        message_id: Optional[str] = None,
        parent_message_id: Optional[str] = None,
    ) -> None:
        """Emit a streaming chunk event.

        Sub-agent content becomes reasoning logs; otherwise defaults to ANSWER_DELTA.
        """
        # Forced policy: sub-agents emit reasoning logs, else use ANSWER_DELTA
        if self.is_sub_agent:
            event_type = EventType.REASONING_LOG_DELTA.value
        elif event_type is None:
            event_type = EventType.ANSWER_DELTA.value

        # 1. Collect content
        content = ""
        if isinstance(event, StreamChunkEvent):
            content = event.content or ""
        elif isinstance(event, str):
            content = event

        if content:
            self._response_buffer.append(content)

        # Check if this is a stream end marker
        if event_type == EventType.APP_STREAM_RESPONSE.value:
            # Permission: if invoked by another agent, must return to caller
            is_agent_return = isinstance(self.current_command, ResumeCommand)
            has_source_agent = (
                self.current_command
                and bool(self.current_command.header.source_agent_type)
                and not is_agent_return
            )
            if has_source_agent:
                logger.warning(
                    "[%s] Agent %s attempted APP_STREAM_RESPONSE, "
                    "but permission held by caller. Event dropped.",
                    self.trace_id,
                    self.current_agent_id,
                )
                await self.flush_to_history()
                return

            self._is_stream_finished = True

        # 2. Send raw chunk
        self._chunk_count += 1
        emitted_message_id = message_id if message_id else self.message_id
        emitted_parent_message_id = (
            parent_message_id if parent_message_id else self.parent_message_id
        )
        await self.emitter.emit_chunk(
            self.session_id,
            self.trace_id,
            event,
            self.current_agent_id,
            message_id=emitted_message_id,
            parent_message_id=emitted_parent_message_id,
            event_type=event_type,
            content_type=content_type,
        )

        # 3. If it's a stream end marker, trigger persistence to history
        if event_type == EventType.APP_STREAM_RESPONSE.value:
            await self.flush_to_history()

    async def flush_to_history(self) -> None:
        """Persist the current buffer content as an assistant reply to history"""
        if self._is_history_saved or not self._response_buffer:
            return

        full_content = "".join(self._response_buffer)
        await self.agent_runtime_state.session_manager.history.save_message(
            role="assistant",
            content=full_content,
            metadata={
                "trace_id": self.trace_id,
                "agent_id": self.current_agent_id,
                "parent_message_id": self.parent_message_id,
            },
        )
        self._is_history_saved = True

    async def emit_state(
        self,
        event: Union[StateChangeEvent, str],
        event_type: Optional[str] = None,
        content_type: Optional[str] = None,
        message_id: Optional[str] = None,
        parent_message_id: Optional[str] = None,
    ) -> None:
        await self.emitter.emit_state(
            self.session_id,
            self.trace_id,
            event,
            self.current_agent_id,
            message_id=message_id if message_id else self.message_id,
            parent_message_id=parent_message_id
            if parent_message_id
            else self.parent_message_id,
            event_type=event_type,
            content_type=content_type,
        )

    async def emit_artifact(
        self,
        event: Union[ArtifactEvent, str],
        event_type: Optional[str] = None,
        content_type: Optional[str] = None,
        message_id: Optional[str] = None,
        parent_message_id: Optional[str] = None,
    ) -> None:
        await self.emitter.emit_artifact(
            self.session_id,
            self.trace_id,
            event,
            self.current_agent_id,
            message_id=message_id if message_id else self.message_id,
            parent_message_id=parent_message_id
            if parent_message_id
            else self.parent_message_id,
            event_type=event_type,
            content_type=content_type,
        )

    async def ask_user(
        self,
        event: Union[AskUserEvent, str],
        message_id: Optional[str] = None,
        parent_message_id: Optional[str] = None,
    ) -> dict:
        """
        Suspend execution and ask the user for a prompt.
        Accepts an AskUserEvent or a raw string prompt.
        """
        await self.emitter.ask_user(
            self.session_id,
            self.trace_id,
            event,
            self.current_agent_id,
            message_id=message_id if message_id else self.message_id,
            parent_message_id=parent_message_id
            if parent_message_id
            else self.parent_message_id,
        )
        self._is_suspended = True
        return {"status": AgentState.WAITING_USER.value}

    async def update_execution_state(self, status: str) -> None:
        """Update the underlying execution state of the current task.

        Does not mix state data into the data stream returned to the frontend;
        operates entirely on the control channel.
        """
        from by_framework.core.registry import WorkerRegistry

        registry = WorkerRegistry(self.redis)
        if hasattr(registry, "update_execution_status_by_message"):
            await registry.update_execution_status_by_message(
                self.message_id, self.session_id, status
            )

    def set_trace_parent_observation_id(self, observation_id: str) -> None:
        """Set the stable observation id used by trace plugins for parenting."""
        self._trace_parent_observation_id = str(observation_id or "")

    def get_trace_parent_observation_id(self) -> str:
        """Return the stable observation id used by worker return tracing."""
        return self._trace_parent_observation_id

    def record_token_usage(
        self,
        *,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        model: str = "",
    ) -> None:
        """Accumulate token usage from a single LLM call.

        Intended for agent implementations and framework adapters.  Each call
        adds to the running totals for the current execution so that the final
        worker.execute span and the Langfuse observation reflect the aggregate.
        """
        self._token_usage["prompt_tokens"] = self._token_usage.get(
            "prompt_tokens", 0
        ) + max(0, prompt_tokens)
        self._token_usage["completion_tokens"] = self._token_usage.get(
            "completion_tokens", 0
        ) + max(0, completion_tokens)
        self._token_usage["total_tokens"] = (
            self._token_usage["prompt_tokens"] + self._token_usage["completion_tokens"]
        )
        if model:
            self._token_usage["model"] = model

    def get_token_usage(self) -> dict[str, Any]:
        """Return accumulated token usage for this execution."""
        return dict(self._token_usage)

    async def call_agent(
        self,
        target_agent_type: str,
        content: object,
        extra_payload: Optional[Dict[str, Any]] = None,
        wait_for_reply: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
        message_id: Optional[str] = None,
        parent_message_id: Optional[str] = None,
        route_policy: str = RoutePolicy.FAIL_FAST,
        availability_timeout_ms: int = 30000,
        region: Optional[str] = None,
        priority: int = 0,
    ) -> dict:
        """Push a control-flow message to another agent.

        If wait_for_reply is True, source_agent_type is injected for routing.

        Args:
            route_policy: Controls online checks and unavailable-agent behavior.
        """
        message_id = message_id or self.generate_message_id()
        parent_message_id = parent_message_id if parent_message_id else self.message_id
        merged_extra_payload = dict(extra_payload or {})
        if wait_for_reply:
            merged_extra_payload["wait_for_reply"] = True
            self._is_suspended = True
        else:
            self._permission_transferred = True

        serialized_content = self._serialize_outbound_content(content)

        metadata = dict(metadata or {})
        call_parent_span_id = f"{message_id}:client.dispatch"
        trace_parent_span_id = self._resolve_call_trace_parent_span_id(
            call_parent_span_id
        )
        metadata.setdefault("trace_parent_span_id", trace_parent_span_id)
        metadata.setdefault("framework_parent_span_id", call_parent_span_id)
        langfuse_parent_observation_id = (
            str(metadata.get("langfuse_parent_observation_id", "") or "")
            or self._resolve_call_langfuse_parent_id()
        )

        command = AskAgentCommand(
            header=MessageHeader(
                message_id=message_id,
                session_id=self.session_id,
                trace_id=self.trace_id,
                source_agent_type=self.current_agent_id if wait_for_reply else "",
                target_agent_type=target_agent_type,
                parent_message_id=parent_message_id,
                user_code=self.agent_runtime_state.session_manager.user_code,
                user_name=self.agent_runtime_state.session_manager.user_name,
                metadata=metadata,
                trace_parent_span_id=trace_parent_span_id,
                langfuse_parent_observation_id=langfuse_parent_observation_id,
            ),
            content=serialized_content,
            wait_for_reply=wait_for_reply,
            extra_payload={
                k: v for k, v in merged_extra_payload.items() if k != "wait_for_reply"
            },
        )
        execution_id = f"{EXECUTION_ID_PREFIX}{uuid.uuid4().hex[:8]}"

        original_langfuse_parent_observation_id = (
            command.header.langfuse_parent_observation_id
        )
        original_metadata_langfuse_parent = command.header.metadata.get(
            "langfuse_parent_observation_id"
        )
        if self.plugin_registry:
            await self.plugin_registry.on_call_agent_start(self, command)

        availability = await AvailabilityRouter(self.redis).prepare_delivery(
            DeliveryIntent(
                execution_id=execution_id,
                message_id=message_id,
                session_id=self.session_id,
                trace_id=self.trace_id,
                source=self.current_agent_id,
                target_agent_type=target_agent_type,
                user_code=self.agent_runtime_state.session_manager.user_code,
                region=region or "",
                priority=priority,
                policy=route_policy,
                timeout_ms=availability_timeout_ms,
                command_payload=command.to_dict(),
                metadata=metadata or {},
            )
        )
        if availability.status not in (
            AvailabilityStatus.DELIVER_NOW,
            AvailabilityStatus.WAIT_AND_DELIVER,
            AvailabilityStatus.FALLBACK_TO_OTHER_AGENT_TYPE,
            AvailabilityStatus.QUEUE_PENDING,
        ):
            from by_framework.core.registry import WorkerRegistry

            registry_client = WorkerRegistry(self.redis)
            await registry_client.record_failed_route_decision(
                execution_id=execution_id,
                message_id=message_id,
                session_id=self.session_id,
                trace_id=self.trace_id,
                parent_message_id=parent_message_id or "",
                source_agent_type=self.current_agent_id if wait_for_reply else "",
                target_agent_type=target_agent_type,
                route_policy=route_policy,
                route_status=availability.status,
                stream_name=availability.stream_name or "",
                selected_agent_type=availability.selected_agent_type or "",
                availability_error_code=availability.error_code or "",
                availability_error=availability.error or "",
            )
            if self.plugin_registry:
                await self.plugin_registry.on_call_agent_error(
                    self, command, RuntimeError(availability.error)
                )
            command.header.langfuse_parent_observation_id = (
                original_langfuse_parent_observation_id
            )
            if original_metadata_langfuse_parent is None:
                command.header.metadata.pop("langfuse_parent_observation_id", None)
            else:
                command.header.metadata["langfuse_parent_observation_id"] = (
                    original_metadata_langfuse_parent
                )
            return {
                "status": AgentState.FAILED.value,
                "message_id": "",
                "parent_message_id": parent_message_id or "",
                "target_agent_type": target_agent_type,
                "error": availability.error,
                "error_code": availability.error_code or "AGENT_TYPE_UNAVAILABLE",
            }
        should_dispatch_control = (
            availability.status != AvailabilityStatus.QUEUE_PENDING
        )
        if availability.selected_agent_type:
            target_agent_type = availability.selected_agent_type
            command.header.target_agent_type = availability.selected_agent_type
        delivery_stream = availability.stream_name or RedisKeys.ctrl_stream(
            target_agent_type
        )

        from by_framework.core.registry import WorkerRegistry

        registry = WorkerRegistry(self.redis)
        if hasattr(registry, "initialize_execution"):
            try:
                await registry.initialize_execution(
                    {
                        "execution_id": execution_id,
                        "message_id": message_id,
                        "session_id": self.session_id,
                        "trace_id": self.trace_id,
                        "parent_message_id": parent_message_id or "",
                        "source_agent_type": self.current_agent_id
                        if wait_for_reply
                        else "",
                        "target_agent_type": target_agent_type,
                        "stream_name": delivery_stream,
                        "status": "QUEUED",
                        "route_policy": route_policy,
                        "route_status": availability.status,
                        "selected_agent_type": availability.selected_agent_type,
                        "availability_error_code": availability.error_code,
                        "availability_error": availability.error,
                    }
                )
            except Exception:  # pylint: disable=broad-exception-caught
                pass  # Fallback if registry fails

        try:
            dispatch_started_at = int(time.time() * 1000)
            if should_dispatch_control:
                await self.redis.xadd(delivery_stream, command.to_redis_payload())
            await self._record_agent_dispatch_span(
                message_id=message_id,
                parent_message_id=parent_message_id,
                source_agent_type=self.current_agent_id if wait_for_reply else "",
                target_agent_type=target_agent_type,
                target_worker_id=availability.target_worker_id,
                route_policy=route_policy,
                route_status=availability.status,
                start_ts=dispatch_started_at,
                end_ts=int(time.time() * 1000),
            )
        except Exception as error:
            if self.plugin_registry:
                await self.plugin_registry.on_call_agent_error(self, command, error)
            command.header.langfuse_parent_observation_id = (
                original_langfuse_parent_observation_id
            )
            if original_metadata_langfuse_parent is None:
                command.header.metadata.pop("langfuse_parent_observation_id", None)
            else:
                command.header.metadata["langfuse_parent_observation_id"] = (
                    original_metadata_langfuse_parent
                )
            raise

        result = {
            "status": AgentState.QUEUED.value,
            "message_id": message_id,
            "parent_message_id": parent_message_id,
            "target_agent_type": target_agent_type,
        }
        if self.plugin_registry:
            await self.plugin_registry.on_call_agent_complete(self, command, result)
        return result

    async def _record_agent_dispatch_span(
        self,
        *,
        message_id: str,
        parent_message_id: str,
        source_agent_type: str,
        target_agent_type: str,
        target_worker_id: str,
        route_policy: str,
        route_status: str,
        start_ts: int,
        end_ts: int,
    ) -> None:
        parent_span_id = (
            f"{self.execution_id}:worker.execute"
            if self.execution_id
            else f"{self.message_id}:worker.execute"
        )
        try:
            await self.span_recorder.record_span(
                TraceSpan(
                    trace_id=self.trace_id,
                    span_id=f"{message_id}:client.dispatch",
                    parent_span_id=parent_span_id,
                    operation="client.dispatch",
                    component="agent_context",
                    start_ts=start_ts,
                    end_ts=end_ts,
                    status="COMPLETED",
                    session_id=self.session_id,
                    message_id=message_id,
                    parent_message_id=parent_message_id,
                    worker_id=target_worker_id,
                    source_agent_type=source_agent_type,
                    target_agent_type=target_agent_type,
                    route_policy=route_policy,
                    route_status=route_status,
                )
            )
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.debug("Failed to record agent dispatch span: %s", err)

    async def dispatch_group(
        self,
        tasks: list[dict[str, Any]],
        wait_for_reply: bool = True,
        message_id: Optional[str] = None,
        parent_message_id: Optional[str] = None,
    ) -> dict:
        """
        Dispatch multiple tasks concurrently as a group.
        The caller agent will be resumed ONLY when ALL tasks in the group are completed.

        Args:
            tasks: A list of dicts, each containing:
                   {
                       "target_agent_type": str,
                       "content": str,
                       "extra_payload": Optional[Dict[str, Any]],
                       "metadata": Optional[Dict[str, Any]]
                   }
            wait_for_reply: bool. If True, sets up Redis counters to wait for all.
        """
        if not tasks:
            return {"status": "EMPTY", "task_group_id": ""}

        task_group_id = f"{TASK_GROUP_ID_PREFIX}{uuid.uuid4().hex[:8]}"
        total_tasks = len(tasks)
        group_dispatch_start_ts = int(time.time() * 1000)

        if wait_for_reply:
            group_key = RedisKeys.task_group(task_group_id)
            await self.redis.hset(  # type: ignore
                group_key,
                mapping={
                    TASK_GROUP_FIELD_TOTAL: str(total_tasks),
                    TASK_GROUP_FIELD_COMPLETED: "0",
                    TASK_GROUP_FIELD_SOURCE_AGENT: self.current_agent_id,
                },
            )
            # Ensure the key expires to prevent leak
            await self.redis.expire(group_key, TASK_GROUP_TTL_SECONDS)
            self._is_suspended = True
        else:
            self._permission_transferred = True

        dispatched = []
        for task in tasks:
            target_agent_type = task["target_agent_type"]
            content = task.get("content", "")
            extra_payload = task.get("extra_payload", {})
            metadata = dict(task.get("metadata", {}) or {})
            serialized_content = self._serialize_outbound_content(content)

            current_message_id = message_id or self.generate_message_id()
            parent_message_id = (
                parent_message_id if parent_message_id else self.message_id
            )
            call_parent_span_id = f"{current_message_id}:client.dispatch"
            trace_parent_span_id = self._resolve_call_trace_parent_span_id(
                call_parent_span_id
            )
            metadata.setdefault("trace_parent_span_id", trace_parent_span_id)
            metadata.setdefault("framework_parent_span_id", call_parent_span_id)
            langfuse_parent_observation_id = (
                str(metadata.get("langfuse_parent_observation_id", "") or "")
                or self._resolve_call_langfuse_parent_id()
            )
            merged_extra_payload = dict(extra_payload)
            if wait_for_reply:
                merged_extra_payload["wait_for_reply"] = True

            command = AskAgentCommand(
                header=MessageHeader(
                    message_id=current_message_id,
                    session_id=self.session_id,
                    trace_id=self.trace_id,
                    source_agent_type=self.current_agent_id if wait_for_reply else "",
                    target_agent_type=target_agent_type,
                    parent_message_id=parent_message_id,
                    task_group_id=task_group_id,
                    user_code=self.agent_runtime_state.session_manager.user_code,
                    user_name=self.agent_runtime_state.session_manager.user_name,
                    metadata=metadata,
                    trace_parent_span_id=trace_parent_span_id,
                    langfuse_parent_observation_id=langfuse_parent_observation_id,
                ),
                content=serialized_content,
                wait_for_reply=wait_for_reply,
                extra_payload={
                    k: v
                    for k, v in merged_extra_payload.items()
                    if k != "wait_for_reply"
                },
            )

            if self.plugin_registry:
                await self.plugin_registry.on_call_agent_start(self, command)

            execution_id = f"{EXECUTION_ID_PREFIX}{uuid.uuid4().hex[:8]}"
            from by_framework.core.registry import WorkerRegistry

            registry = WorkerRegistry(self.redis)
            if hasattr(registry, "initialize_execution"):
                try:
                    await registry.initialize_execution(
                        {
                            "execution_id": execution_id,
                            "message_id": current_message_id,
                            "session_id": self.session_id,
                            "trace_id": self.trace_id,
                            "parent_message_id": parent_message_id or "",
                            "source_agent_type": self.current_agent_id
                            if wait_for_reply
                            else "",
                            "target_agent_type": target_agent_type,
                            "stream_name": RedisKeys.ctrl_stream(target_agent_type),
                            "status": "QUEUED",
                        }
                    )
                except Exception:  # pylint: disable=broad-exception-caught
                    pass  # Fallback if registry fails

            dispatch_started_at = int(time.time() * 1000)
            try:
                await self.redis.xadd(
                    RedisKeys.ctrl_stream(target_agent_type), command.to_redis_payload()
                )
                await self._record_agent_dispatch_span(
                    message_id=current_message_id,
                    parent_message_id=parent_message_id,
                    source_agent_type=self.current_agent_id if wait_for_reply else "",
                    target_agent_type=target_agent_type,
                    target_worker_id="",
                    route_policy=RoutePolicy.SEND_ANYWAY,
                    route_status="GROUP_DISPATCH",
                    start_ts=dispatch_started_at,
                    end_ts=int(time.time() * 1000),
                )
            except Exception as error:
                if self.plugin_registry:
                    await self.plugin_registry.on_call_agent_error(self, command, error)
                raise

            if self.plugin_registry:
                await self.plugin_registry.on_call_agent_complete(
                    self,
                    command,
                    {
                        "status": AgentState.QUEUED.value,
                        "message_id": current_message_id,
                        "parent_message_id": parent_message_id,
                        "target_agent_type": target_agent_type,
                    },
                )

            dispatched.append(
                {
                    "message_id": current_message_id,
                    "target_agent_type": target_agent_type,
                }
            )

        # Record aggregate span for the entire group dispatch.
        group_parent_span_id = (
            f"{self.execution_id}:worker.execute"
            if self.execution_id
            else f"{self.message_id}:worker.execute"
        )
        try:
            await self.span_recorder.record_span(
                TraceSpan(
                    trace_id=self.trace_id,
                    span_id=f"{task_group_id}:agent.dispatch_group",
                    parent_span_id=group_parent_span_id,
                    operation="agent.dispatch_group",
                    component="agent_context",
                    start_ts=group_dispatch_start_ts,
                    end_ts=int(time.time() * 1000),
                    status="COMPLETED",
                    session_id=self.session_id,
                    execution_id=self.execution_id,
                    message_id=self.message_id,
                    target_agent_type=self.current_agent_id,
                    metadata={
                        "task_group_id": task_group_id,
                        "task_count": total_tasks,
                        "wait_for_reply": wait_for_reply,
                    },
                )
            )
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.debug("Failed to record dispatch_group span: %s", err)

        return {
            "status": "GROUP_QUEUED",
            "task_group_id": task_group_id,
            "dispatched_tasks": dispatched,
        }

    def _serialize_outbound_content(self, content: object) -> WireContent:
        if self.content_codec is not None:
            return self.content_codec.serialize(content)

        if self._is_wire_content(content):
            return content

        raise TypeError(
            "AgentContext requires a content codec to serialize non-wire content"
        )

    def _resolve_call_trace_parent_span_id(self, call_parent_span_id: str) -> str:
        """Return the OTel/Phoenix parent id for an outbound agent call."""
        phoenix_span = getattr(self, "_phoenix_span", None)
        if phoenix_span:
            try:
                span_context = phoenix_span.get_span_context()
                if span_context and span_context.span_id:
                    return f"{span_context.span_id:016x}"
            except Exception:  # pylint: disable=broad-exception-caught
                pass

        current_otel_span_id = self._current_otel_span_id_hex()
        if current_otel_span_id:
            return current_otel_span_id

        return f"{str_to_uint64(call_parent_span_id):016x}"

    def _resolve_call_langfuse_parent_id(self) -> str:
        """Return the Langfuse parent for an outbound agent call."""
        current_langfuse_observation_id = self._current_langfuse_observation_id()
        if current_langfuse_observation_id:
            return current_langfuse_observation_id

        current_otel_span_id = self._current_otel_span_id_hex()
        if current_otel_span_id:
            return current_otel_span_id

        current_obs = getattr(
            self,
            "_langfuse_call_parent_observation",
            None,
        ) or getattr(self, "_langfuse_observation", None)
        current_obs_id = getattr(current_obs, "id", None) if current_obs else None
        if current_obs_id:
            return str(current_obs_id)

        if self.current_command:
            return getattr(
                self.current_command.header, "langfuse_parent_observation_id", ""
            ) or self.current_command.header.metadata.get(
                "langfuse_parent_observation_id", ""
            )
        return ""

    @staticmethod
    def _current_langfuse_observation_id() -> str:
        global _LANGFUSE_CURRENT_OBSERVATION_GETTER  # pylint: disable=global-statement
        try:
            if not (
                os.environ.get("LANGFUSE_SECRET_KEY")
                and os.environ.get("LANGFUSE_PUBLIC_KEY")
                and os.environ.get("LANGFUSE_BASE_URL")
            ):
                return ""

            if _LANGFUSE_CURRENT_OBSERVATION_GETTER is False:
                return ""
            if _LANGFUSE_CURRENT_OBSERVATION_GETTER is None:
                get_client = getattr(import_module("langfuse"), "get_client", None)
                if get_client is None:
                    _LANGFUSE_CURRENT_OBSERVATION_GETTER = False
                    return ""
                client = get_client()
                getter = getattr(client, "get_current_observation_id", None)
                _LANGFUSE_CURRENT_OBSERVATION_GETTER = getter or False

            if _LANGFUSE_CURRENT_OBSERVATION_GETTER is False:
                return ""
            observation_id = _LANGFUSE_CURRENT_OBSERVATION_GETTER()
            return str(observation_id or "")
        except Exception:  # pylint: disable=broad-exception-caught
            return ""

    @staticmethod
    def _current_otel_span_id_hex() -> str:
        try:
            from opentelemetry import trace as otel_trace

            span_context = otel_trace.get_current_span().get_span_context()
            if span_context and span_context.is_valid and span_context.span_id:
                return f"{span_context.span_id:016x}"
        except Exception:  # pylint: disable=broad-exception-caught
            return ""
        return ""

    @staticmethod
    def _is_wire_content(content: object) -> bool:
        if isinstance(content, str):
            return True
        if not isinstance(content, list):
            return False
        return all(isinstance(item, dict) for item in content)

    async def collect_group_results(
        self,
        task_group_id: str,
        timeout: float = 30.0,
    ) -> list[dict[str, Any]]:
        """Collect results of all subtasks in the task group.

        Called when last subtask completes. Returns collected results
        if timeout is reached before all complete.

        Args:
            task_group_id: task_group_id returned by dispatch_group
            timeout: Maximum timeout in seconds to wait

        Returns:
            List of subtask results, each includes:
            {
                "message_id": str,
                "status": str,
                "reply_data": Any,
                "content": Optional[str]
            }
        """
        if not task_group_id:
            return []

        results_key = RedisKeys.task_group_results(task_group_id)
        group_key = RedisKeys.task_group(task_group_id)
        field = TASK_GROUP_FIELD_TOTAL
        total_str = await self.redis.hget(group_key, field)  # type: ignore
        if total_str is None:
            # No group found, try to get whatever results exist
            total = float("inf")
        else:
            total = int(total_str)

        start_time = asyncio.get_running_loop().time()
        results: list[dict[str, Any]] = []

        while len(results) < total:
            elapsed = asyncio.get_running_loop().time() - start_time
            if elapsed >= timeout:
                break

            raw_results = await self.redis.hgetall(results_key)  # type: ignore
            if raw_results:
                results = [
                    {
                        "message_id": msg_id,
                        **json.loads(data),
                    }
                    for msg_id, data in raw_results.items()
                ]
                if len(results) >= total:
                    break

            # Wait a bit before polling again
            await asyncio.sleep(0.1)

        return results
