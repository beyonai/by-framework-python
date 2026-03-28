"""
WorkerRunner - Main orchestration class for Gateway Worker.

Manages message consumption, execution tracking, and worker lifecycle.
"""

import asyncio
import hashlib
import uuid
from typing import TYPE_CHECKING, Optional

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
from by_framework.core.protocol.agent_state import TERMINAL_STATES
from by_framework.core.protocol.commands import CancelTaskCommand
from by_framework.worker.worker import GatewayWorker

from ._control_handling import handle_cancel_task, parse_control_command
from ._execution_tracking import ExecutionTracker, RunningExecution
from ._message_processing import (decode_message_id, parse_message_data)

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
        self._lock_token = None
        self._heartbeat_task = None
        self._control_task = None
        self._running_tasks: set[asyncio.Task] = set()
        self._tracker = ExecutionTracker()

    @property
    def _terminal_execution_states(self) -> frozenset[str]:
        return TERMINAL_STATES

    def _auto_group_name(self) -> str:
        caps = sorted(self.worker.get_capabilities())
        payload = ",".join(caps)
        digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:10]
        return f"{RedisKeys.CG_AGENT_ENGINES}:{digest}"

    async def setup_streams(self):
        """Set up Redis streams and consumer groups for all capabilities."""
        from redis.exceptions import ResponseError

        for cap in self.worker.get_capabilities():
            stream_name = RedisKeys.ctrl_stream(cap)
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

    async def fetch_messages(
        self, count: int = 10, block: int = None
    ) -> list[tuple[str, str, dict]]:
        """
        Fetch messages from streams without processing them.

        Returns:
            List of (stream_name, message_id, data_dict) tuples.
        """
        if block is None:
            block = WorkerConfig.stream_block_ms
        streams = {
            RedisKeys.ctrl_stream(cap): STREAM_READ_LAST_ID
            for cap in self.worker.get_capabilities()
        }

        messages = await self.redis.xreadgroup(
            groupname=self.group_name,
            consumername=self.consumer_name,
            streams=streams,
            count=count,
            block=block,
        )

        results = []
        if messages:
            for stream_bytes, msg_list in messages:
                stream_name = decode_message_id(stream_bytes)
                for msg_id_bytes, msg_data in msg_list:
                    msg_id = decode_message_id(msg_id_bytes)
                    try:
                        data_dict = await parse_message_data(msg_data)
                        results.append((stream_name, msg_id, data_dict))
                    except Exception:
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
                    else:
                        # AskAgentCommand or ResumeCommand directly routed to this worker
                        await self._process_message_from_dict(
                            current_stream_name, msg_id, data_dict
                        )
                except Exception as e:
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
        except Exception as e:
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

        try:
            logger.info("[%s] Processing message: %s", self.worker.worker_id, msg_id)
            command = command_from_dict(data_dict)

            if not isinstance(command, (AskAgentCommand, ResumeCommand)):
                raise UnsupportedCommandError(type(command).__name__)

            header = command.header
            registry = getattr(self.worker, "registry", None)
            existing_execution = None

            if registry and hasattr(registry, "get_execution_by_message_id"):
                existing_execution = await registry.get_execution_by_message_id(
                    header.message_id, session_id=header.session_id
                )

            # Skip terminal state replays
            if (
                existing_execution
                and existing_execution.get("status") in self._terminal_execution_states
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
            cancel_reason = (existing_execution or {}).get("cancel_reason", "")
            cancel_requested = bool(
                (existing_execution or {}).get("cancel_requested", False)
            )
            cancel_event = asyncio.Event()
            if cancel_requested:
                cancel_event.set()

            # Track execution
            if current_task is not None:
                execution = RunningExecution(
                    execution_id=execution_id,
                    message_id=header.message_id,
                    session_id=header.session_id,
                    worker_id=self.worker.worker_id,
                    task=current_task,
                    cancel_event=cancel_event,
                    cancel_reason=cancel_reason,
                )
                self._tracker.add_execution(execution)

            # Save execution to registry
            if (
                registry
                and hasattr(registry, "save_execution")
                and not existing_execution
            ):
                await registry.save_execution(
                    {
                        "execution_id": execution_id,
                        "message_id": header.message_id,
                        "session_id": header.session_id,
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

            # Process command
            final_status = await self.worker._handle_message(
                command,
                cancel_event=cancel_event,
                cancel_reason=cancel_reason,
                execution=self._tracker.get_execution(execution_id),
            )

            # Mark finished
            if registry and hasattr(registry, "mark_execution_finished"):
                await registry.mark_execution_finished(
                    execution_id, header.session_id, final_status
                )

            await self.redis.xack(stream_name, self.group_name, msg_id)
            logger.info("[%s] Message processed: %s", self.worker.worker_id, msg_id)

        except UnsupportedCommandError:
            logger.error("Unsupported command in message %s", msg_id)
            await self.redis.xack(stream_name, self.group_name, msg_id)
        except Exception as e:
            logger.error("Error processing message %s: %s", msg_id, e)
        finally:
            # Clean up tracking
            message_id = data_dict.get("header", {}).get("message_id", "")
            self._tracker.remove_by_message(message_id)

    async def start(self):
        """Start the worker runner main loop."""
        self._running_tasks = set()
        try:
            if hasattr(self.worker.registry, "claim_worker_id"):
                self._lock_token = await self.worker.registry.claim_worker_id(
                    self.worker.worker_id
                )

            await self.setup_streams()
            await self.setup_control_streams()
            await self.worker.start_heartbeat()
            self._control_task = asyncio.create_task(self._control_loop())
            logger.info(
                "[%s] Runner started with max_concurrency=%d, waiting for tasks...",
                self.worker.worker_id,
                self.semaphore._value + (1 if hasattr(self.semaphore, "_value") else 0),
            )

            while True:
                await self._run_once()
                if hasattr(self.worker.registry, "refresh_worker_id_lock"):
                    await self.worker.registry.refresh_worker_id_lock(
                        self.worker.worker_id
                    )
                await asyncio.sleep(CONTROL_LOOP_SLEEP_SECONDS)
        finally:
            await self._shutdown()

    async def _shutdown(self):
        """Graceful shutdown sequence."""
        if self._running_tasks:
            await asyncio.gather(*self._running_tasks, return_exceptions=True)
        if self._control_task:
            self._control_task.cancel()
            await asyncio.gather(self._control_task, return_exceptions=True)
            self._control_task = None

        if hasattr(self.worker, "stop_heartbeat"):
            self.worker.stop_heartbeat()

        if hasattr(self.worker, "plugins") and self.worker.plugins:
            if getattr(self.worker.plugins, "log_hook_stats_on_shutdown", True):
                self.worker.plugins.log_hook_stats()
            await self.worker.plugins.on_worker_shutdown(self.worker)

        if self._lock_token and hasattr(self.worker.registry, "release_worker_id"):
            await self.worker.registry.release_worker_id(
                self.worker.worker_id,
                self._lock_token,
            )
