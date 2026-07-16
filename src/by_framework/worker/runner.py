"""
WorkerRunner - Main orchestration class for Gateway Worker.

Manages message consumption, execution tracking, and worker lifecycle.
"""

import asyncio
import hashlib
import time
import uuid
from typing import TYPE_CHECKING, Any, Optional

from by_framework.common.config import WorkerConfig
from by_framework.common.constants import (
    CONTROL_LOOP_SLEEP_SECONDS,
    EXECUTION_ID_PREFIX,
    STREAM_READ_LAST_ID,
    WAIT_FOR_TASKS_TIMEOUT_SECONDS,
    RedisKeys,
)
from by_framework.common.logger import logger
from by_framework.common.redis_client import Redis, get_redis
from by_framework.core.protocol.agent_state import TERMINAL_STATES, AgentState
from by_framework.core.protocol.commands import (
    CancelTaskCommand,
    EvictWorkerCommand,
    ReloadPluginsCommand,
    ResumeWorkerCommand,
    SuspendWorkerCommand,
)
from by_framework.core.protocol.results import AgentTaskResult
from by_framework.core.registry import ExecutionCompletionFields
from by_framework.trace.span_recorder import (
    SpanRecorder,
    TraceSpan,
    build_observability_config,
    live_execution_otel_span,
    sanitize_io_value,
)
from by_framework.trace.trace_schema import TraceRecord
from by_framework.trace.trace_writer import TraceWriteClient
from by_framework.util.generate_message_id import generate_message_id
from by_framework.worker.context import current_worker_id_var
from by_framework.worker.worker import GatewayWorker

from ._control_handling import (
    handle_cancel_task,
    handle_evict_worker,
    handle_reload_plugins,
    handle_resume_worker,
    handle_suspend_worker,
    parse_control_command,
)
from ._execution_tracking import ExecutionTracker, RunningExecution
from ._message_processing import decode_message_id, parse_message_data

if TYPE_CHECKING:
    pass


class WorkerRunner:
    """Orchestrates worker message consumption and execution management."""

    def __init__(
        self,
        redis_client: Optional[Redis] = None,
        worker: Optional[GatewayWorker] = None,
        group_name: str = RedisKeys.CG_AGENT_ENGINES,
        max_concurrency: int = 50,
        fetch_count: int = 10,
        span_recorder: Optional[SpanRecorder] = None,
    ):
        if (
            worker is None
            and redis_client is not None
            and hasattr(redis_client, "worker_id")
        ):
            worker, redis_client = redis_client, None
        self.redis = redis_client or get_redis()
        self.worker = worker
        self.group_name = group_name or self._auto_group_name()
        self.consumer_name = worker.worker_id
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.fetch_count = fetch_count
        # OTel for worker.execute is emitted via a live wrapping span (see
        # _process_message_from_dict); this recorder only feeds the Redis
        # dashboard, so disable its own OTel exporter to avoid double export.
        self._obs_config = build_observability_config()
        self.span_recorder = span_recorder or SpanRecorder(
            self.redis, enable_otel=False, config=self._obs_config
        )
        self._trace_writer = TraceWriteClient(
            self.redis, ttl_seconds=self._obs_config.ttl_seconds
        )
        self._otel_enabled = self._obs_config.otel_enabled
        self._lock_token = None
        self._heartbeat_task = None
        self._control_task = None
        self._consumer_task: Optional[asyncio.Task] = None
        self._metrics_collector_task: Optional[asyncio.Task] = None
        self._running_tasks: set[asyncio.Task] = set()
        self._tracker = ExecutionTracker()
        # Admin-controlled lifecycle: "active" | "suspended" | "evicted"
        self._admin_lifecycle: str = "active"
        self._evict_force: bool = False
        self._evict_event: asyncio.Event = asyncio.Event()
        # In-memory cache of agent_types denied for this worker.
        # Refreshed by the heartbeat thread every heartbeat interval.
        # Avoids per-iteration Redis SISMEMBER calls in the consume loop.
        self._denied_agent_types: frozenset[str] = frozenset()
        # Round-robin cursor for fetch_messages' phase-two blocking read, so
        # no agent_type is permanently starved of the blocking slot.
        self._primary_cursor: int = 0
        self._consumer_last_tick_monotonic = 0.0
        stream_block_seconds = float(WorkerConfig.stream_block_ms) / 1000.0
        self._consumer_health_timeout_seconds = max(
            float(self.worker.heartbeat_lease_ttl_seconds) * 2.0,
            stream_block_seconds * 3.0,
        )

    @property
    def _terminal_execution_states(self) -> frozenset[str]:
        return TERMINAL_STATES

    def _auto_group_name(self) -> str:
        agent_types = sorted(self.worker.get_agent_types())
        payload = ",".join(agent_types)
        digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:10]
        return f"{RedisKeys.CG_AGENT_ENGINES}:{digest}"

    @staticmethod
    def _client_dispatch_parent_span_id(header: Any) -> str:
        """Return the propagated OTel/Phoenix parent span id for a command."""
        parent_span_id = str(getattr(header, "trace_parent_span_id", "") or "")
        if not parent_span_id:
            metadata = getattr(header, "metadata", {}) or {}
            parent_span_id = str(metadata.get("trace_parent_span_id", "") or "")
        return parent_span_id or f"{header.message_id}:client.dispatch"

    @staticmethod
    def _framework_parent_span_id(header: Any) -> str:
        """Return the framework span id used to build the Redis trace tree."""
        metadata = getattr(header, "metadata", {}) or {}
        parent_span_id = str(metadata.get("framework_parent_span_id", "") or "")
        if parent_span_id:
            return parent_span_id
        parent_span_id = str(getattr(header, "trace_parent_span_id", "") or "")
        if parent_span_id:
            return parent_span_id
        parent_span_id = str(metadata.get("trace_parent_span_id", "") or "")
        return parent_span_id or f"{header.message_id}:client.dispatch"

    async def setup_streams(self):
        """Set up Redis streams and consumer groups for all agent types."""
        from redis.exceptions import ResponseError

        for agent_type in self.worker.get_agent_types():
            stream_name = RedisKeys.ctrl_stream(agent_type)
            try:
                await self.redis.xgroup_create(
                    stream_name, self.group_name, id="0", mkstream=True
                )
            except ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise

    async def setup_control_streams(self):
        """Set up control stream for this worker."""
        from redis.exceptions import ResponseError

        stream_name = RedisKeys.worker_ctrl_stream(self.worker.worker_id)
        try:
            await self.redis.xgroup_create(
                stream_name, self.group_name, id="0", mkstream=True
            )
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def _active_agent_type_streams(self) -> dict[str, str]:
        """Return ctrl streams for agent_types not in the in-memory denylist cache.

        Uses self._denied_agent_types which is refreshed by the heartbeat thread
        every heartbeat interval.  No Redis I/O on the hot consume path.
        """
        return {
            RedisKeys.ctrl_stream(agent_type): STREAM_READ_LAST_ID
            for agent_type in self.worker.get_agent_types()
            if agent_type not in self._denied_agent_types
        }

    def _update_denied_agent_types(self, denied: frozenset[str]) -> None:
        """Called by the heartbeat thread to refresh the in-memory denylist cache.

        The assignment of a frozenset reference is atomic in CPython (GIL),
        so cross-thread updates are safe without an explicit lock.
        """
        self._denied_agent_types = denied

    async def fetch_messages(
        self, count: int = 10, block: int = None
    ) -> list[tuple[str, str, dict]]:
        """
        Fetch messages from streams without processing them.

        Two-phase read to stay Cluster-safe: different agent_type ctrl
        streams are different entities/slots, so they can never be combined
        into one XREADGROUP call.

        Phase one: concurrent non-blocking XREADGROUP, one call per active
        stream. If any stream returned messages, return immediately.
        Phase two: only if every stream came back empty, one blocking
        XREADGROUP against a single "primary" stream, chosen by round-robin
        across the declared agent_types so no agent_type is starved of the
        blocking slot.

        Returns:
            List of (stream_name, message_id, data_dict) tuples.
        """
        if block is None:
            block = WorkerConfig.stream_block_ms
        streams = self._active_agent_type_streams()
        if not streams:
            await asyncio.sleep(float(block) / 1000.0)
            return []

        stream_names = list(streams.keys())

        async def read_nonblocking(stream_name: str):
            return await self.redis.xreadgroup(
                groupname=self.group_name,
                consumername=self.consumer_name,
                streams={stream_name: STREAM_READ_LAST_ID},
                count=count,
            )

        first_pass = await asyncio.gather(
            *[read_nonblocking(stream_name) for stream_name in stream_names]
        )
        results = await self._parse_xreadgroup_results(first_pass)
        if results:
            return results

        primary = stream_names[self._primary_cursor % len(stream_names)]
        self._primary_cursor += 1
        blocked = await self.redis.xreadgroup(
            groupname=self.group_name,
            consumername=self.consumer_name,
            streams={primary: STREAM_READ_LAST_ID},
            count=count,
            block=block,
        )
        return await self._parse_xreadgroup_results([blocked])

    @staticmethod
    async def _parse_xreadgroup_results(batches: list) -> list[tuple[str, str, dict]]:
        """Flatten and parse one or more XREADGROUP responses."""
        results = []
        for messages in batches:
            if not messages:
                continue
            for stream_bytes, msg_list in messages:
                stream_name = decode_message_id(stream_bytes)
                for msg_id_bytes, msg_data in msg_list:
                    msg_id = decode_message_id(msg_id_bytes)
                    try:
                        data_dict = await parse_message_data(msg_data)
                        results.append((stream_name, msg_id, data_dict))
                    except Exception:  # pylint: disable=broad-exception-caught
                        logger.error("Failed to parse message: %s", msg_id)
                        continue
        return results

    async def ack_message(self, stream_name: str, message_id: str):
        """Acknowledge a message."""
        await self.redis.xack(stream_name, self.group_name, message_id)

    async def _run_control_once(self, block: int = None) -> bool:
        """Process one batch of control messages."""
        if block is None:
            block = WorkerConfig.stream_block_ms
        stream_name = RedisKeys.worker_ctrl_stream(self.worker.worker_id)
        messages = await self.redis.xreadgroup(
            groupname=self.group_name,
            consumername=self.consumer_name,
            streams={stream_name: STREAM_READ_LAST_ID},
            count=self.fetch_count,
            block=block,
        )

        if not messages:
            return False

        for stream_bytes, msg_list in messages:
            current_stream_name = decode_message_id(stream_bytes)
            for msg_id_bytes, msg_data in msg_list:
                msg_id = decode_message_id(msg_id_bytes)
                try:
                    data_dict = await parse_message_data(msg_data)
                    command = await parse_control_command(data_dict)
                    if isinstance(command, CancelTaskCommand):
                        await self._handle_control_message(
                            current_stream_name, msg_id, command
                        )
                    elif isinstance(command, ReloadPluginsCommand):
                        await handle_reload_plugins(command, self.worker)
                    elif isinstance(command, SuspendWorkerCommand):
                        await handle_suspend_worker(command, self._set_admin_lifecycle)
                        logger.info(
                            "[%s] Worker suspended by admin: %s",
                            self.worker.worker_id,
                            command.reason,
                        )
                    elif isinstance(command, ResumeWorkerCommand):
                        await handle_resume_worker(command, self._set_admin_lifecycle)
                        logger.info(
                            "[%s] Worker resumed by admin",
                            self.worker.worker_id,
                        )
                    elif isinstance(command, EvictWorkerCommand):
                        await handle_evict_worker(
                            command,
                            self._set_admin_lifecycle,
                            request_shutdown=lambda force: setattr(
                                self, "_evict_force", force
                            ),
                        )
                        logger.info(
                            "[%s] Worker eviction requested by admin (force=%s): %s",
                            self.worker.worker_id,
                            command.force,
                            command.reason,
                        )
                    else:
                        # AskAgentCommand or ResumeCommand directly routed here
                        await self._process_message_from_dict(
                            current_stream_name, msg_id, data_dict
                        )
                except Exception as e:  # pylint: disable=broad-exception-caught
                    logger.error("Invalid control message %s: %s", msg_id, e)
                finally:
                    await self.redis.xack(current_stream_name, self.group_name, msg_id)

        return True

    async def _control_loop(self):
        """Main control message processing loop."""
        while True:
            await self._run_control_once()
            await asyncio.sleep(CONTROL_LOOP_SLEEP_SECONDS)

    async def _handle_control_message(
        self, stream_name: str, msg_id: str, command: CancelTaskCommand
    ):
        """Handle a parsed CancelTaskCommand."""
        try:
            await handle_cancel_task(
                command=command,
                active_executions=self._tracker._active_executions,
                message_to_execution=self._tracker._message_to_execution,
                redis_client=self.redis,
                group_name=self.group_name,
                worker=self.worker,
                span_recorder=self.span_recorder,
            )
        finally:
            await self.redis.xack(stream_name, self.group_name, msg_id)

    async def _run_once(self) -> bool:
        """Fetch and start processing messages."""
        await self.semaphore.acquire()

        try:
            messages = await self.fetch_messages(
                count=self.fetch_count, block=WorkerConfig.stream_block_ms
            )

            if not messages:
                self.semaphore.release()
                return False

            for i, (stream_name, msg_id, data_dict) in enumerate(messages):
                if i > 0:
                    await self.semaphore.acquire()

                task = asyncio.create_task(
                    self._process_and_release(stream_name, msg_id, data_dict)
                )
                self._running_tasks.add(task)
                task.add_done_callback(self._running_tasks.discard)

            return True
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Error in _run_once: %s", e)
            self.semaphore.release()
            return False

    async def wait_for_tasks(self, timeout: float = None):
        """Wait for all running tasks to complete."""
        if timeout is None:
            timeout = WAIT_FOR_TASKS_TIMEOUT_SECONDS
        if not self._running_tasks:
            return
        await asyncio.wait(list(self._running_tasks), timeout=timeout)
        if not self._running_tasks:
            return
        await asyncio.wait(list(self._running_tasks), timeout=timeout)

    async def _process_and_release(
        self, stream_name: str, msg_id: str, data_dict: dict
    ):
        """Process a message and release the semaphore."""
        try:
            await self._process_message_from_dict(stream_name, msg_id, data_dict)
        finally:
            self.semaphore.release()

    def _mark_consumer_tick(self) -> None:
        """Record that the consumer loop is alive in this process."""
        self._consumer_last_tick_monotonic = time.monotonic()

    def _is_consumer_healthy(self) -> bool:
        """Return whether the stream consumer loop is recently alive."""
        if not self._consumer_task or self._consumer_task.done():
            return False
        if self._consumer_last_tick_monotonic <= 0:
            return False
        elapsed = time.monotonic() - self._consumer_last_tick_monotonic
        return elapsed <= self._consumer_health_timeout_seconds

    def _set_admin_lifecycle(self, lifecycle: str) -> None:
        """Called by admin command handlers to update lifecycle state."""
        self._admin_lifecycle = lifecycle
        if lifecycle == "evicted":
            self._evict_event.set()

    async def _consume_loop(
        self,
        ready_event: Optional[asyncio.Event] = None,
        start_event: Optional[asyncio.Event] = None,
    ) -> None:
        """Main agent-type stream consumption loop."""
        while True:
            self._mark_consumer_tick()
            if ready_event is not None and not ready_event.is_set():
                ready_event.set()
            if start_event is not None and not start_event.is_set():
                await start_event.wait()

            if self._admin_lifecycle == "suspended":
                await asyncio.sleep(1.0)
                continue
            if self._admin_lifecycle == "evicted":
                logger.info(
                    "[%s] Eviction requested; stopping consumer loop",
                    self.worker.worker_id,
                )
                return

            await self._run_once()
            await asyncio.sleep(CONTROL_LOOP_SLEEP_SECONDS)

    async def _process_message_from_dict(
        self, stream_name: str, msg_id: str, data_dict: dict
    ):
        """Process a message from parsed dict."""
        from by_framework.common.exceptions import UnsupportedCommandError
        from by_framework.core.protocol.commands import (
            AskAgentCommand,
            ResumeCommand,
            command_from_dict,
        )

        # Set worker_id context var so ContextFilter can inject it into log records.
        current_worker_id_var.set(self.worker.worker_id)

        # Error-recovery state: populated as processing advances so the except block
        # can record a failed span even when the exception occurred mid-way.
        err_execution_id = ""
        err_header: Any = None
        err_start_ts = 0
        err_session_id = ""
        chunk_count = 0
        token_usage: dict = {}

        try:
            logger.info("[%s] Processing message: %s", self.worker.worker_id, msg_id)
            command = command_from_dict(data_dict)

            if not isinstance(command, (AskAgentCommand, ResumeCommand)):
                raise UnsupportedCommandError(type(command).__name__)

            header = command.header
            err_header = header
            err_session_id = header.session_id
            # Null check for message_id in header
            if not header.message_id:
                header.message_id = generate_message_id()

            registry = getattr(self.worker, "registry", None)
            existing_execution = None

            if registry and hasattr(registry, "get_execution_by_message_id"):
                existing_execution = await registry.get_execution_by_message_id(
                    header.message_id, session_id=header.session_id
                )

            if isinstance(command, ResumeCommand) and existing_execution is None:
                logger.warning(
                    "[%s] ResumeCommand did not resolve to an existing execution "
                    "(message_id=%s, session_id=%s); starting a new, disconnected "
                    "execution instead of continuing the suspended one.",
                    self.worker.worker_id,
                    header.message_id,
                    header.session_id,
                )

            # Skip terminal state replays
            if (
                existing_execution
                and existing_execution.get("status") in self._terminal_execution_states
                and not isinstance(command, ResumeCommand)
            ):
                await self.redis.xack(stream_name, self.group_name, msg_id)
                logger.info(
                    "[%s] Skipping terminal execution replay: %s -> %s",
                    self.worker.worker_id,
                    header.message_id,
                    existing_execution.get("status"),
                )
                return

            current_task = asyncio.current_task()
            execution_id = (existing_execution or {}).get(
                "execution_id"
            ) or f"{EXECUTION_ID_PREFIX}{uuid.uuid4().hex[:8]}"
            err_execution_id = execution_id
            cancel_reason = (existing_execution or {}).get("cancel_reason", "")
            cancel_requested = bool(
                (existing_execution or {}).get("cancel_requested", False)
            )
            cancel_event = asyncio.Event()
            if cancel_requested:
                cancel_event.set()

            is_resumed_execution = isinstance(command, ResumeCommand) or bool(
                existing_execution
                and existing_execution.get("status") != AgentState.QUEUED.value
            )

            # Track execution
            if current_task is not None:
                execution = RunningExecution(
                    execution_id=execution_id,
                    message_id=header.message_id,
                    parent_message_id=existing_execution["parent_message_id"]
                    if existing_execution and "parent_message_id" in existing_execution
                    else (header.parent_message_id or ""),
                    session_id=header.session_id,
                    worker_id=self.worker.worker_id,
                    task=current_task,
                    cancel_event=cancel_event,
                    cancel_reason=cancel_reason,
                    is_resumed=is_resumed_execution,
                    existing_data=existing_execution,
                )
                self._tracker.add_execution(execution)

            # Save execution to registry
            if registry:
                if existing_execution:
                    if hasattr(registry, "update_execution_status"):
                        await registry.update_execution_status(
                            execution_id,
                            header.session_id,
                            "RUNNING",
                            worker_id=self.worker.worker_id,
                        )
                elif hasattr(registry, "save_execution"):
                    await registry.save_execution(
                        {
                            "execution_id": execution_id,
                            "message_id": header.message_id,
                            "session_id": header.session_id,
                            "trace_id": header.trace_id,
                            "parent_message_id": header.parent_message_id or "",
                            "worker_id": self.worker.worker_id,
                            "target_agent_type": header.target_agent_type,
                            "stream_name": stream_name,
                            "redis_message_id": msg_id,
                            "status": "RUNNING",
                            "cancel_requested": cancel_requested,
                            "cancel_reason": cancel_reason,
                            "created_at": 0,
                            "started_at": 0,
                            "finished_at": 0,
                        }
                    )

            # Process command inside a live OTel span so any spans produced
            # within the agent (e.g. LangGraph/Langfuse LLM calls) nest under
            # worker.execute via normal OTel context propagation.
            execution_started_at = int(time.time() * 1000)
            try:
                from by_framework.metrics import record_execution_started_metrics

                record_execution_started_metrics(agent_type=header.target_agent_type)
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.debug("Failed to record execution start metric: %s", e)
            err_start_ts = execution_started_at
            client_dispatch_parent_span_id = self._client_dispatch_parent_span_id(
                header
            )
            framework_parent_span_id = self._framework_parent_span_id(header)
            async with live_execution_otel_span(
                trace_id=header.trace_id,
                span_id=f"{execution_id}:worker.execute",
                parent_span_id=client_dispatch_parent_span_id,
                operation="worker.execute",
                attributes={
                    "component": "worker",
                    "session_id": header.session_id,
                    "execution_id": execution_id,
                    "message_id": header.message_id,
                    "worker_id": self.worker.worker_id,
                    "target_agent_type": header.target_agent_type,
                },
                start_ts=execution_started_at,
                otel_enabled=self._otel_enabled,
            ) as execute_span:
                task_result = await self.worker._handle_message(
                    command,
                    cancel_event=cancel_event,
                    cancel_reason=cancel_reason,
                    execution=self._tracker.get_execution(execution_id),
                )
                execution_finished_at = int(time.time() * 1000)
                final_status = task_result.status
                completion_fields = self._build_completion_observability_fields(
                    task_result
                )
                execute_span.set_status(
                    final_status,
                    error_message=str(completion_fields.get("error_message", "")),
                )
                # Extract chunk count and token usage from
                # the AgentContext if available.
                running_exec = self._tracker.get_execution(execution_id)
                exec_context = getattr(running_exec, "context", None)
                chunk_count = int(getattr(exec_context, "_chunk_count", 0))
                token_usage = dict(getattr(exec_context, "_token_usage", {}) or {})

            # Mark finished
            if registry and hasattr(registry, "mark_execution_finished"):
                await registry.mark_execution_finished(
                    execution_id,
                    header.session_id,
                    final_status,
                    completion_fields,
                )

            # Record Prometheus metrics
            try:
                created_at = int((existing_execution or {}).get("created_at", 0) or 0)
                queue_wait_ms_val = None
                if created_at > 0 and execution_started_at >= created_at:
                    queue_wait_ms_val = execution_started_at - created_at

                from by_framework.metrics import record_execution_metrics

                record_execution_metrics(
                    status=final_status,
                    agent_type=header.target_agent_type,
                    worker_id=self.worker.worker_id,
                    execution_ms=max(
                        0.0, float(execution_finished_at - execution_started_at)
                    ),
                    queue_wait_ms_val=float(queue_wait_ms_val)
                    if queue_wait_ms_val is not None
                    else None,
                )
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.debug("Failed to record execution metrics: %s", e)

            await self._record_worker_execute_span(
                trace_id=header.trace_id,
                execution_id=execution_id,
                message_id=header.message_id,
                parent_message_id=header.parent_message_id or "",
                session_id=header.session_id,
                worker_id=self.worker.worker_id,
                target_agent_type=header.target_agent_type,
                status=final_status,
                completion_fields=completion_fields,
                route_policy=str((existing_execution or {}).get("route_policy", "")),
                route_status=str((existing_execution or {}).get("route_status", "")),
                start_ts=execution_started_at,
                end_ts=execution_finished_at,
                chunk_count=chunk_count,
                parent_span_id=framework_parent_span_id,
                tokens=token_usage,
            )

            # Finalize trace root when the root execution completes so the
            # trace record has a stable end_ts / status / output.
            if not (header.parent_message_id or ""):
                await self._write_trace_root_end(
                    trace_id=header.trace_id,
                    status=final_status,
                    end_ts=execution_finished_at,
                    output=task_result.reply_data,
                    error=str(completion_fields.get("error_message", "")),
                )

            await self.redis.xack(stream_name, self.group_name, msg_id)
            logger.info("[%s] Message processed: %s", self.worker.worker_id, msg_id)

        except UnsupportedCommandError:
            logger.error("Unsupported command in message %s", msg_id)
            await self.redis.xack(stream_name, self.group_name, msg_id)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("Error processing message %s: %s", msg_id, e, exc_info=True)
            # Record a failed span so the execution is visible in traces.
            if err_execution_id and err_start_ts and err_header:
                try:
                    await self._record_worker_execute_span(
                        trace_id=err_header.trace_id,
                        execution_id=err_execution_id,
                        message_id=err_header.message_id,
                        parent_message_id=err_header.parent_message_id or "",
                        session_id=err_session_id,
                        worker_id=self.worker.worker_id,
                        target_agent_type=err_header.target_agent_type,
                        status="FAILED",
                        completion_fields={
                            "error_message": str(e),
                            "error_type": type(e).__name__,
                        },
                        route_policy="",
                        route_status="",
                        start_ts=err_start_ts,
                        end_ts=int(time.time() * 1000),
                        parent_span_id=self._framework_parent_span_id(err_header),
                    )
                except Exception:  # pylint: disable=broad-exception-caught
                    pass
            # Finalize trace root on unexpected failure.
            if err_header and not (err_header.parent_message_id or ""):
                await self._write_trace_root_end(
                    trace_id=err_header.trace_id,
                    status="FAILED",
                    end_ts=int(time.time() * 1000),
                    error=str(e),
                )
            # Mark execution as FAILED so the next Redis re-delivery is immediately
            # acked by the terminal-replay guard (line ~316), preventing infinite retry.
            if err_execution_id and err_session_id:
                err_registry = getattr(self.worker, "registry", None)
                if err_registry and hasattr(err_registry, "mark_execution_finished"):
                    try:
                        await err_registry.mark_execution_finished(
                            err_execution_id,
                            err_session_id,
                            "FAILED",
                            {"error_message": str(e), "error_type": type(e).__name__},
                        )
                    except Exception:  # pylint: disable=broad-exception-caught
                        pass
        finally:
            # Clean up tracking
            message_id = data_dict.get("header", {}).get("message_id", "")
            self._tracker.remove_by_message(message_id)

    @staticmethod
    def _build_completion_observability_fields(
        task_result: AgentTaskResult,
    ) -> ExecutionCompletionFields:
        """Extract structured terminal metadata for execution observability."""
        fields: ExecutionCompletionFields = {}
        metadata = task_result.metadata or {}
        for key in ("error_type", "error_message", "error_code", "failed_stage"):
            value = metadata.get(key)
            if value:
                fields[key] = value  # type: ignore[literal-required]
        if "retryable" in metadata:
            fields["retryable"] = bool(metadata["retryable"])

        if "error_message" not in fields and isinstance(task_result.reply_data, dict):
            error = task_result.reply_data.get("error")
            if error:
                fields["error_message"] = str(error)
        return fields

    async def _record_worker_execute_span(
        self,
        *,
        trace_id: str,
        execution_id: str,
        message_id: str,
        parent_message_id: str,
        session_id: str,
        worker_id: str,
        target_agent_type: str,
        status: str,
        completion_fields: ExecutionCompletionFields,
        route_policy: str,
        route_status: str,
        start_ts: int,
        end_ts: int,
        parent_span_id: str = "",
        chunk_count: int = 0,
        tokens: Optional[dict] = None,
    ) -> None:
        try:
            await self.span_recorder.record_span(
                TraceSpan(
                    trace_id=trace_id,
                    span_id=f"{execution_id}:worker.execute",
                    parent_span_id=parent_span_id or f"{message_id}:client.dispatch",
                    operation="worker.execute",
                    component="worker",
                    start_ts=start_ts,
                    end_ts=end_ts,
                    status=status,
                    session_id=session_id,
                    execution_id=execution_id,
                    message_id=message_id,
                    parent_message_id=parent_message_id,
                    worker_id=worker_id,
                    target_agent_type=target_agent_type,
                    error_type=str(completion_fields.get("error_type", "")),
                    error_message=str(completion_fields.get("error_message", "")),
                    error_code=str(completion_fields.get("error_code", "")),
                    failed_stage=str(completion_fields.get("failed_stage", "")),
                    retryable=bool(completion_fields.get("retryable", False)),
                    route_policy=route_policy,
                    route_status=route_status,
                    chunk_count=chunk_count,
                    tokens=tokens or {},
                )
            )
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.debug("Failed to record worker execute span: %s", err)

    async def _write_trace_root_end(
        self,
        *,
        trace_id: str,
        status: str,
        end_ts: int,
        output: Any = None,
        error: str = "",
    ) -> None:
        """Finalize trace root record — best-effort, never propagates errors."""
        try:
            sanitized_output = (
                sanitize_io_value(output, self._obs_config)
                if output is not None
                else None
            )
            meta: dict = {"error": error} if error else {}
            await self._trace_writer.record_trace(
                TraceRecord(
                    trace_id=trace_id,
                    status=status,
                    end_ts=end_ts,
                    output=sanitized_output,
                    metadata=meta,
                )
            )
        except Exception:  # pylint: disable=broad-exception-caught
            pass

    async def _claim_worker_id_with_retry(
        self,
        max_wait_seconds: Optional[float] = None,
        retry_interval_seconds: Optional[float] = None,
    ) -> str:
        """Claim the worker ID, retrying if another instance holds it.

        Retries are useful for crash-recovery: if Worker 1 just died its lease
        will expire within one TTL period.  Worker 2 keeps trying until the
        lease is gone or max_wait_seconds is exhausted.
        """
        if max_wait_seconds is None:
            max_wait_seconds = WorkerConfig.worker_id_claim_max_wait_seconds
        if retry_interval_seconds is None:
            retry_interval_seconds = WorkerConfig.worker_id_claim_retry_interval_seconds

        deadline = time.monotonic() + max_wait_seconds
        attempt = 0
        while True:
            try:
                return await self.worker.registry.claim_worker_id(
                    self.worker.worker_id,
                    ttl_seconds=self.worker.heartbeat_lease_ttl_seconds,
                )
            except ValueError:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise
                wait = min(
                    retry_interval_seconds * (2 ** min(attempt, 3)), remaining, 10.0
                )
                logger.info(
                    "[%s] Worker ID already claimed; will retry in %.1fs "
                    "(%.0fs remaining)",
                    self.worker.worker_id,
                    wait,
                    remaining,
                )
                await asyncio.sleep(wait)
                attempt += 1

    async def start(self):
        """Start the worker runner main loop."""
        self._running_tasks = set()
        try:
            if hasattr(self.worker.registry, "claim_worker_id"):
                self._lock_token = await self._claim_worker_id_with_retry()

            await self.setup_streams()
            await self.setup_control_streams()
            self._control_task = asyncio.create_task(self._control_loop())
            reader_ready = asyncio.Event()
            reader_start = asyncio.Event()
            self._consumer_task = asyncio.create_task(
                self._consume_loop(reader_ready, reader_start)
            )
            await reader_ready.wait()

            await self.worker.start_heartbeat(
                health_check=self._is_consumer_healthy,
                lifecycle_callback=self._set_admin_lifecycle,
                denylist_refresh=self._update_denied_agent_types,
            )
            reader_start.set()
            heartbeat_task = self.worker.heartbeat_task
            try:
                from by_framework.metrics.collector import MetricsCollector

                self._metrics_collector_task = asyncio.create_task(
                    MetricsCollector(
                        self.redis,
                        worker_id=self.worker.worker_id,
                    ).run()
                )
            except Exception as metrics_collector_err:  # pylint: disable=broad-exception-caught
                logger.debug("MetricsCollector not started: %s", metrics_collector_err)
            logger.info(
                "[%s] Runner started with max_concurrency=%d, waiting for tasks...",
                self.worker.worker_id,
                self.semaphore._value + (1 if hasattr(self.semaphore, "_value") else 0),
            )

            while True:
                if heartbeat_task and heartbeat_task.done():
                    exc = heartbeat_task.exception()
                    if exc:
                        raise exc
                if self._consumer_task.done():
                    exc = self._consumer_task.exception()
                    if exc:
                        raise exc
                    if self._admin_lifecycle == "evicted":
                        # Consumer loop exited cleanly due to eviction command.
                        # Drain in-flight tasks before shutting down.
                        logger.info(
                            "[%s] Eviction: waiting for %d in-flight task(s)",
                            self.worker.worker_id,
                            len(self._running_tasks),
                        )
                        if not self._evict_force:
                            await self.wait_for_tasks(
                                timeout=WAIT_FOR_TASKS_TIMEOUT_SECONDS
                            )
                        return
                    raise RuntimeError(
                        f"Worker '{self.worker.worker_id}' consumer loop stopped"
                    )
                await asyncio.sleep(CONTROL_LOOP_SLEEP_SECONDS)
        finally:
            await self._shutdown()

    async def _shutdown(self):
        """Graceful shutdown sequence."""
        if self._metrics_collector_task:
            self._metrics_collector_task.cancel()
            await asyncio.gather(self._metrics_collector_task, return_exceptions=True)
            self._metrics_collector_task = None
        if self._consumer_task:
            self._consumer_task.cancel()
            await asyncio.gather(self._consumer_task, return_exceptions=True)
            self._consumer_task = None
        if self._running_tasks:
            await asyncio.gather(*self._running_tasks, return_exceptions=True)
        if self._control_task:
            self._control_task.cancel()
            await asyncio.gather(self._control_task, return_exceptions=True)
            self._control_task = None

        if hasattr(self.worker, "stop_heartbeat"):
            await self.worker.stop_heartbeat()

        released_worker_id = False
        if self._lock_token and hasattr(self.worker.registry, "release_worker_id"):
            released_worker_id = await self.worker.registry.release_worker_id(
                self.worker.worker_id,
                self._lock_token,
            )
        elif not hasattr(self.worker.registry, "claim_worker_id") and hasattr(
            self.worker.registry, "mark_worker_inactive"
        ):
            await self.worker.registry.mark_worker_inactive(self.worker.worker_id)
            released_worker_id = True

        if released_worker_id and hasattr(
            self.worker.registry, "unregister_worker_membership"
        ):
            await self.worker.registry.unregister_worker_membership(
                self.worker.worker_id
            )

        plugin_registry = getattr(self.worker, "plugin_registry", None)
        if plugin_registry is None:
            plugin_registry = getattr(self.worker, "plugins", None)

        if plugin_registry:
            if getattr(plugin_registry, "log_hook_stats_on_shutdown", True):
                plugin_registry.log_hook_stats()
            await plugin_registry.on_worker_shutdown(self.worker)
