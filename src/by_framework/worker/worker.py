"""
Gateway worker abstract base class.

Provides the abstract GatewayWorker class that handles message processing,
lifecycle management, and plugin integration.
"""

# pylint: disable=wrong-import-position

import asyncio
import json
import traceback
import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, List, Optional

if TYPE_CHECKING:
    from by_framework.core.registry import WorkerRegistry
    from by_framework.worker._execution_tracking import RunningExecution

from by_framework.common.config import WorkerConfig
from by_framework.common.constants import (
    MESSAGE_ID_PREFIX,
    TASK_GROUP_FIELD_COMPLETED,
    TASK_GROUP_FIELD_TOTAL,
    TASK_GROUP_TTL_SECONDS,
    RedisKeys,
)
from by_framework.common.emitter import DataLayoutBuilder
from by_framework.common.logger import logger
from by_framework.common.redis_client import Redis, get_redis
from by_framework.core.extensions import AgentConfigsSnapshot, PluginRegistry
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.protocol.commands import (
    CancelTaskCommand,
    GatewayCommand,
    ResumeCommand,
)
from by_framework.core.protocol.content_codec import ContentCodec
from by_framework.core.protocol.event_type import EventType
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.core.protocol.results import (
    AgentTaskResult,
    JsonValue,
    ProcessCommandResult,
    normalize_process_result,
)
from by_framework.core.runtime.file_permissions import FilePermissionPolicy
from by_framework.core.runtime.filestore.base import FileStorage
from by_framework.worker.context import AgentContext, current_agent_context_var
from by_framework.worker.heartbeat import WorkerHeartbeat

from .sandbox.hook_sandbox import active_workspace


class GatewayWorker(ABC):
    """Gateway Worker abstract base class.

    Business parties define specific business processing logic by inheriting from this
    class and implementing the process_command method. Worker is responsible for
    receiving commands from Redis streams, handling lifecycle events, and integrating
    with the plugin system.

    Args:
        worker_id: Worker unique identifier
        redis_client: Redis client instance
        registry: WorkerRegistry instance
        workspace_manager: WorkspaceManager instance
        sandbox: Sandbox instance
        plugin_registry: PluginRegistry instance
    """

    def __init__(
        self,
        worker_id: str,
        redis_client: Optional[Redis] = None,
        registry: Optional["WorkerRegistry"] = None,
        workspace_manager=None,
        sandbox=None,
        plugin_registry: Optional[PluginRegistry] = None,
        storage: Optional[FileStorage] = None,
        permission_policy: Optional[FilePermissionPolicy] = None,
        layout_builder: Optional[DataLayoutBuilder] = None,
        **kwargs,  # pylint: disable=unused-argument
    ):
        self.worker_id = worker_id
        self.redis = redis_client or get_redis()
        self.registry: Optional["WorkerRegistry"] = registry
        self.workspace_manager = workspace_manager
        self.sandbox = sandbox
        self.logger = logger
        self.plugin_registry = plugin_registry or PluginRegistry()
        self.storage = storage
        self.permission_policy = permission_policy
        self.layout_builder = layout_builder
        self._heartbeat: Optional[WorkerHeartbeat] = None

    @property
    def heartbeat_interval(self) -> int:
        """Return the heartbeat interval from config."""
        return WorkerConfig.heartbeat_interval_seconds

    @property
    def heartbeat_lease_ttl_seconds(self) -> int:
        """Return the worker online lease TTL from config."""
        return WorkerConfig.heartbeat_lease_ttl_seconds

    @abstractmethod
    def get_agent_types(self) -> List[str]:
        """Return a list of agent types this worker can handle."""
        pass

    async def on_cancel_task(self, command: CancelTaskCommand) -> None:
        """Called when a task cancellation is requested.

        Override this to perform custom cleanup (e.g. closing resources,
        stopping loops). Note that the task itself will also be cancelled
        via asyncio.Task.cancel() by the runner.
        """
        pass

    async def process_command(
        self, command: GatewayCommand, context: AgentContext
    ) -> ProcessCommandResult:
        """Preferred worker entrypoint for typed command handling."""
        raise NotImplementedError("Override process_command(...)")

    def prepare_command_for_processing(self, command: GatewayCommand) -> GatewayCommand:
        """Allow subclasses to adapt command content for business handling."""
        return command

    def get_content_codec(self) -> ContentCodec | None:
        """Return the codec used for domain content serialization, if any."""
        return None

    def get_data_layout_builder(self) -> DataLayoutBuilder | None:
        """Return the builder used for emitted event data payloads."""
        return self.layout_builder

    def get_context_class(self) -> type[AgentContext]:
        """Return the context class used for business command execution."""
        return AgentContext

    async def _resolve_agent_configs_snapshot(
        self,
        execution: Optional["RunningExecution"],
        session_id: str,
    ) -> AgentConfigsSnapshot:
        """Resolve the config snapshot bound to the current execution."""
        if execution and execution.is_resumed:
            snapshot_key = ""
            expected_version = 0
            if execution.existing_data:
                snapshot_key = execution.existing_data.get(
                    "agent_configs_snapshot_key", ""
                )
                expected_version = execution.existing_data.get(
                    "agent_configs_version", 0
                )
            if not snapshot_key:
                logger.error(
                    "[%s] Agent config snapshot restore failed: execution_id=%s "
                    "session_id=%s message_id=%s reason=missing_snapshot_key "
                    "expected_version=%s",
                    self.worker_id,
                    execution.execution_id,
                    session_id,
                    execution.message_id,
                    expected_version,
                )
                raise RuntimeError(
                    "Missing persisted agent config snapshot key for resumed execution"
                )
            registry = self.registry
            if registry is None:
                logger.error(
                    "[%s] Agent config snapshot restore failed: execution_id=%s "
                    "session_id=%s message_id=%s snapshot_key=%s "
                    "reason=missing_registry expected_version=%s",
                    self.worker_id,
                    execution.execution_id,
                    session_id,
                    execution.message_id,
                    snapshot_key,
                    expected_version,
                )
                raise RuntimeError(
                    "Worker registry is required to load persisted agent config "
                    "snapshots for resumed execution"
                )
            try:
                snapshot = await registry.load_agent_configs_snapshot(snapshot_key)
            except Exception as err:
                logger.exception(
                    "[%s] Agent config snapshot restore failed: execution_id=%s "
                    "session_id=%s message_id=%s snapshot_key=%s "
                    "expected_version=%s",
                    self.worker_id,
                    execution.execution_id,
                    session_id,
                    execution.message_id,
                    snapshot_key,
                    expected_version,
                )
                raise RuntimeError(
                    "Failed to restore persisted agent config snapshot: "
                    f"{snapshot_key}"
                ) from err
            if snapshot is None:
                logger.error(
                    "[%s] Agent config snapshot restore failed: execution_id=%s "
                    "session_id=%s message_id=%s snapshot_key=%s "
                    "reason=snapshot_not_found expected_version=%s",
                    self.worker_id,
                    execution.execution_id,
                    session_id,
                    execution.message_id,
                    snapshot_key,
                    expected_version,
                )
                raise RuntimeError(
                    f"Persisted agent config snapshot not found: {snapshot_key}"
                )
            return snapshot

        snapshot = self.plugin_registry.get_agent_configs_snapshot()
        registry = self.registry
        if execution and registry is not None:
            try:
                snapshot_key = await registry.persist_agent_configs_snapshot(
                    execution.execution_id,
                    snapshot,
                )
                await registry.update_execution_fields(
                    execution.execution_id,
                    session_id,
                    agent_configs_version=snapshot.version,
                    agent_configs_snapshot_key=snapshot_key,
                )
            except Exception as err:
                logger.exception(
                    "[%s] Agent config snapshot persist failed: execution_id=%s "
                    "session_id=%s message_id=%s version=%s",
                    self.worker_id,
                    execution.execution_id,
                    session_id,
                    execution.message_id,
                    snapshot.version,
                )
                raise RuntimeError(
                    "Failed to persist agent config snapshot for execution: "
                    f"{execution.execution_id}"
                ) from err
        return snapshot

    async def start_heartbeat(self):
        """Start periodic heartbeat registration"""
        # Call plugin startup hook
        await self.plugin_registry.on_worker_startup(self)

        # Create and start WorkerHeartbeat
        self._heartbeat = WorkerHeartbeat(
            worker_id=self.worker_id,
            agent_types=self.get_agent_types(),
            redis_client=self.redis,
            registry=self.registry,
            interval=self.heartbeat_interval,
            lease_ttl_seconds=self.heartbeat_lease_ttl_seconds,
        )
        await self._heartbeat.start()

    async def stop_heartbeat(self):
        """Stop periodic heartbeat registration"""
        if self._heartbeat is not None:
            await self._heartbeat.stop()
            self._heartbeat = None
            logger.info("[%s] Heartbeat stopped", self.worker_id)

    async def _enqueue_agent_return(
        self,
        command: GatewayCommand,
        status: str,
        reply_data: JsonValue,
        content: str | list[dict[str, Any]] = "",
        metadata: Optional[dict[str, JsonValue]] = None,
        extra_payload: Optional[dict[str, JsonValue]] = None,
    ):
        """Enqueue agent return response to source agent."""
        header = command.header
        source_agent_type = header.source_agent_type
        if not source_agent_type:
            return

        target_agent_type = header.target_agent_type
        trace_id = header.trace_id
        user_code = header.user_code
        user_name = header.user_name
        merged_metadata = {
            **dict(header.metadata),
            **dict(metadata or {}),
        }

        callback_command = ResumeCommand(
            header=MessageHeader(
                message_id=header.parent_message_id
                or f"{MESSAGE_ID_PREFIX}{uuid.uuid4().hex[:8]}",
                session_id=header.session_id,
                trace_id=trace_id if trace_id else uuid.uuid4().hex,
                source_agent_type=target_agent_type
                if target_agent_type
                else self.worker_id,
                target_agent_type=source_agent_type,
                parent_message_id=header.message_id,
                task_group_id=header.task_group_id or "",
                user_code=user_code if user_code else "",
                user_name=user_name if user_name else "",
                metadata=merged_metadata,
                trace_parent_span_id=header.trace_parent_span_id
                or header.metadata.get("trace_parent_span_id", ""),
                langfuse_parent_observation_id=header.langfuse_parent_observation_id
                or header.metadata.get("langfuse_parent_observation_id", ""),
            ),
            status=status,
            content=content,
            reply_data=reply_data,
            extra_payload=dict(extra_payload or {}),
        )
        await self.redis.xadd(
            RedisKeys.ctrl_stream(callback_command.header.target_agent_type),
            callback_command.to_redis_payload(),
        )

    async def _persist_agent_return_state(self, paths: dict, command: GatewayCommand):
        await asyncio.to_thread(self._persist_agent_return_state_sync, paths, command)

    def _persist_agent_return_state_sync(self, paths: dict, command: GatewayCommand):
        """Synchronously persist agent return state to disk."""
        if not paths or "public" not in paths:
            return

        header = command.header
        state_dir = Path(paths["public"]) / "session" / "agent_returns"

        if header.task_group_id:
            group_dir = state_dir / header.task_group_id
            group_dir.mkdir(parents=True, exist_ok=True)
            state_file = group_dir / f"{header.message_id}.json"
        else:
            state_dir.mkdir(parents=True, exist_ok=True)
            file_key = header.parent_message_id or header.message_id
            state_file = state_dir / f"{file_key}.json"

        state_file.write_text(
            json.dumps(
                {
                    "message_id": header.message_id,
                    "parent_message_id": header.parent_message_id,
                    "source_agent_type": header.source_agent_type,
                    "target_agent_type": header.target_agent_type,
                    "action_type": command.to_dict()["action_type"],
                    "status": command.status
                    if isinstance(command, ResumeCommand)
                    else "",
                    "content": command.content
                    if isinstance(command, ResumeCommand)
                    else None,
                    "reply_data": command.reply_data
                    if isinstance(command, ResumeCommand)
                    else None,
                    "trace_id": header.trace_id,
                    "session_id": header.session_id,
                    "user_code": header.user_code,
                    "user_name": header.user_name,
                    "metadata": dict(header.metadata),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    async def _handle_message(
        self,
        command: GatewayCommand,
        cancel_event: Optional[asyncio.Event] = None,
        cancel_reason: str = "",
        execution: Optional["RunningExecution"] = None,
    ) -> AgentTaskResult:
        """Handle incoming gateway command message."""
        trace_id = uuid.uuid4().hex
        raw_command = command
        command = self.prepare_command_for_processing(command)
        header = raw_command.header

        # Whether it's a return from calling another Agent or a return from waiting for
        # user input, RESUME is uniformly used to indicate the resumption of a suspended
        # task. Essentially, both are “resuming execution of the current workflow from a
        # suspended/waiting state”, so they are uniformly handled in lifecycle and state
        # recovery logic (like reloading workspace, persisting state, etc.).
        is_agent_return = isinstance(raw_command, ResumeCommand)
        source_agent_type = header.source_agent_type
        has_source_agent = bool(source_agent_type) and not is_agent_return

        # Get workspace dir from workspace_manager if available
        # Note: We don't use hasattr check because it doesn't work well with mocks
        workspace_dir = None

        # Determine context parent message id
        message_id = header.message_id
        parent_message_id = header.parent_message_id
        if execution and execution.is_resumed:
            parent_message_id = execution.parent_message_id
            logger.info(
                "[%s] Task Resumed: Successfully restored parent_message_id=%s "
                "from execution snapshot.",
                self.worker_id,
                parent_message_id,
            )
        else:
            logger.info(
                "[%s] New Task: message_id=%s, parent_message_id=%s",
                self.worker_id,
                message_id,
                parent_message_id,
            )

        agent_config_snapshot = await self._resolve_agent_configs_snapshot(
            execution,
            header.session_id,
        )

        context = self.get_context_class()(
            session_id=header.session_id,
            trace_id=header.trace_id if header.trace_id else trace_id,
            redis_client=self.redis,
            current_agent_id=header.target_agent_type
            if header.target_agent_type
            else "",
            message_id=message_id,
            parent_message_id=parent_message_id,
            current_command=command,
            cancel_event=cancel_event,
            cancel_reason=cancel_reason,
            plugin_registry=self.plugin_registry,
            user_code=header.user_code,
            user_name=header.user_name,
            workspace_dir=workspace_dir,
            agent_configs=list(agent_config_snapshot.configs),
            agent_configs_version=agent_config_snapshot.version,
            storage=self.storage,
            permission_policy=self.permission_policy,
            content_codec=self.get_content_codec(),
            layout_builder=self.get_data_layout_builder(),
            is_sub_agent=has_source_agent,
            execution_id=execution.execution_id if execution else "",
        )
        if execution:
            execution.context = context
        process_result: Any = None

        logger.info(
            "[%s] Received message: %s (Trace: %s)",
            self.worker_id,
            header.message_id,
            context.trace_id,
        )
        logger.info(
            "[%s] Target Agent Type: %s", self.worker_id, header.target_agent_type
        )
        logger.info("[%s] Session ID: %s", self.worker_id, header.session_id)

        ctx_token = current_agent_context_var.set(context)
        token = None
        try:
            # Call plugin hooks at task start
            await self.plugin_registry.on_task_start(context)

            # 0. Automatically save user message to history
            if not is_agent_return and hasattr(raw_command, "content"):
                await context.agent_runtime_state.session_manager.history.save_message(
                    role="user",
                    content=raw_command.content,
                    metadata={
                        "message_id": header.message_id,
                        "trace_id": header.trace_id,
                    },
                )

            # 1. Setup workspace
            logger.info(
                "[%s] Setting up workspace for session: %s",
                self.worker_id,
                header.session_id,
            )
            paths = await self.workspace_manager.setup_workspace(
                header.session_id,
                header.message_id,
                user_code=header.user_code or "default",
                agent_id=header.target_agent_type or self.worker_id,
            )
            logger.debug("[%s] Workspace paths: %s", self.worker_id, paths)

            # 2. Setup Sandbox
            if self.sandbox:
                logger.info("[%s] Installing sandbox", self.worker_id)
                self.sandbox.install()

            token = active_workspace.set(paths["private"])

            # 3. Process
            logger.info("[%s] Starting task processing", self.worker_id)
            if cancel_event and cancel_event.is_set():
                raise asyncio.CancelledError(
                    f"Task cancelled before processing (reason: {cancel_reason})"
                )

            if is_agent_return:
                await self._persist_agent_return_state(paths, raw_command)

                # Check for scatter-gather join
                if header.task_group_id:
                    group_key = RedisKeys.task_group(header.task_group_id)
                    results_key = RedisKeys.task_group_results(header.task_group_id)
                    total_str = await self.redis.hget(  # type: ignore
                        group_key, TASK_GROUP_FIELD_TOTAL
                    )
                    if total_str is not None:
                        # Store result in Redis Hash for distributed access
                        if isinstance(raw_command, ResumeCommand):
                            result_data = {
                                "status": raw_command.status,
                                "reply_data": raw_command.reply_data,
                                "content": raw_command.content,
                                "metadata": raw_command.header.metadata,
                                "extra_payload": raw_command.extra_payload,
                            }
                            await self.redis.hset(  # type: ignore
                                results_key,
                                header.message_id,
                                json.dumps(result_data),
                            )
                            await self.redis.expire(results_key, TASK_GROUP_TTL_SECONDS)

                        completed = await self.redis.hincrby(  # type: ignore
                            group_key, TASK_GROUP_FIELD_COMPLETED, 1
                        )
                        if completed < int(total_str):
                            logger.info(
                                "[%s] TaskGroup %s completed %d/%s, waiting...",
                                self.worker_id,
                                header.task_group_id,
                                completed,
                                total_str,
                            )
                            return AgentTaskResult(
                                status=f"{AgentState.QUEUED.value}: waiting_for_group"
                            )
                        logger.info(
                            "[%s] TaskGroup %s ALL COMPLETED (%s)!",
                            self.worker_id,
                            header.task_group_id,
                            total_str,
                        )

                # await context.emit_state(
                #     StateChangeEvent(state=AgentState.RESUMED.value)
                # )
            process_result = await self.process_command(command, context)
            task_result = normalize_process_result(process_result)

            # Determine the execution status to return
            # Prefer extracting status from business return results
            # (e.g., QUEUED, WAITING_USER, etc.)
            final_status = task_result.status

            if has_source_agent:
                await self._enqueue_agent_return(
                    raw_command,
                    status=task_result.status,
                    content=task_result.content,
                    reply_data=task_result.reply_data,
                    metadata=task_result.metadata,
                    extra_payload=task_result.extra_payload,
                )
            logger.info(
                "[%s] Task completed successfully with status: %s",
                self.worker_id,
                final_status,
            )
            # Call plugin hook on task completion
            await self.plugin_registry.on_task_complete(context, process_result)

            from by_framework.core.protocol.agent_state import is_terminal_state

            should_emit_stream_end = (
                not has_source_agent
                and is_terminal_state(final_status)
                and not getattr(context, "_permission_transferred", False)
                and not getattr(context, "_is_suspended", False)
            )

            final_message = None
            if isinstance(task_result.content, str) and task_result.content:
                final_message = task_result.content
            elif isinstance(task_result.reply_data, str) and task_result.reply_data:
                final_message = task_result.reply_data
            elif task_result.reply_data is not None:
                final_message = json.dumps(task_result.reply_data, ensure_ascii=False)

            if final_message is not None:
                await context.emit_chunk(
                    final_message, event_type=EventType.FINAL_ANSWER.value
                )

            # Apply APP_STREAM_RESPONSE sending logic at the framework level
            if should_emit_stream_end:
                # If having permission and business hasn't closed the stream itself,
                # automatically send stream end event here
                if not getattr(context, "_is_stream_finished", False):
                    await context.emit_chunk(
                        "", event_type=EventType.APP_STREAM_RESPONSE.value
                    )
                    context._is_stream_finished = True
            else:
                # Fallback: if no permission to send stream end (or suspended),
                # force flush to history on completion
                await context.flush_to_history()

            return task_result

        except asyncio.CancelledError as e:
            reason = str(e)
            if not reason:
                reason = execution.cancel_reason if execution else cancel_reason
            logger.info("[%s] Task cancellation requested: %s", self.worker_id, reason)

            if has_source_agent:
                # Cascade cancellation scenario: check if parent Agent is also marked
                # for cancellation, skip callback if so
                # Note: parent Agent may be in COMPLETED state but marked
                # with cancel_requested
                should_callback = True
                parent_msg_id = header.parent_message_id
                if parent_msg_id and hasattr(self, "registry") and self.registry:
                    try:
                        parent_exec = await self.registry.get_execution_by_message_id(
                            parent_msg_id, session_id=header.session_id
                        )
                        if parent_exec and parent_exec.get("cancel_requested"):
                            should_callback = False
                            logger.info(
                                "[%s] Skipping cancel callback to parent "
                                "(parent cancel_requested): %s",
                                self.worker_id,
                                parent_msg_id,
                            )
                    except Exception:  # pylint: disable=broad-exception-caught
                        pass  # Conservatively send callback when query fails
                if should_callback:
                    await self._enqueue_agent_return(
                        command,
                        status=AgentState.CANCELLED.value,
                        reply_data={"reason": reason},
                    )

            should_emit_stream_end = not has_source_agent and not getattr(
                context, "_permission_transferred", False
            )
            if should_emit_stream_end and not getattr(
                context, "_is_stream_finished", False
            ):
                await context.emit_chunk(
                    "", event_type=EventType.APP_STREAM_RESPONSE.value
                )
            else:
                await context.flush_to_history()

            return AgentTaskResult(status=AgentState.CANCELLED.value)

        except Exception as e:  # pylint: disable=broad-exception-caught
            error_msg = f"[{self.worker_id}] Task failed: {str(e)}"
            logger.error(error_msg)
            if has_source_agent:
                await self._enqueue_agent_return(
                    command,
                    status=AgentState.FAILED.value,
                    reply_data={"error": str(e)},
                )
            logger.error(traceback.format_exc())
            # Call plugin hook on task error
            await self.plugin_registry.on_task_error(context, e)

            should_emit_stream_end = not has_source_agent and not getattr(
                context, "_permission_transferred", False
            )
            if should_emit_stream_end and not getattr(
                context, "_is_stream_finished", False
            ):
                await context.emit_chunk(
                    "", event_type=EventType.APP_STREAM_RESPONSE.value
                )
            else:
                await context.flush_to_history()

            return AgentTaskResult(
                status=AgentState.FAILED.value,
                reply_data={"error": str(e)},
                metadata={
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "failed_stage": "process_command",
                },
            )
        finally:
            current_agent_context_var.reset(ctx_token)
            # 4. Cleanup
            if token is not None:
                active_workspace.reset(token)
            if self.sandbox:
                logger.info("[%s] Uninstalling sandbox", self.worker_id)
                self.sandbox.uninstall()
            logger.info("[%s] Cleaning up task: %s", self.worker_id, header.message_id)
            await self.workspace_manager.cleanup_task(
                header.session_id,
                header.message_id,
                user_code=header.user_code or "default",
                agent_id=header.target_agent_type or self.worker_id,
            )
