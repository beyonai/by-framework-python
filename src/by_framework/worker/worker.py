"""
Gateway worker abstract base class.

Provides the abstract GatewayWorker class that handles message processing,
lifecycle management, and plugin integration.
"""

import asyncio
import json
import traceback
import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List, Optional

from by_framework.common.config import WorkerConfig
from by_framework.common.constants import (
    MESSAGE_ID_PREFIX,
    TASK_GROUP_FIELD_COMPLETED,
    TASK_GROUP_FIELD_TOTAL,
    TASK_GROUP_TTL_SECONDS,
    RedisKeys,
)
from by_framework.common.logger import logger
from by_framework.common.redis_client import Redis, get_redis
from by_framework.core.extensions import PluginRegistry
from by_framework.core.runtime.history import HistoryManager
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.protocol.commands import (
    CancelTaskCommand,
    GatewayCommand,
    ResumeCommand,
)
from by_framework.core.protocol.events import StateChangeEvent
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.core.runtime.filestore.base import FileStorage
from by_framework.worker.context import AgentContext

from .sandbox.hook_sandbox import active_workspace


class GatewayWorker(ABC):
    """Gateway Worker 抽象基类。

    业务方通过继承此类并实现 process_command 方法来定义具体的业务处理逻辑。
    Worker 负责接收来自 Redis streams 的命令、处理生命周期事件、并与插件系统集成。

    Args:
        worker_id: Worker 唯一标识符
        redis_client: Redis 客户端实例
        registry: WorkerRegistry 实例
        workspace_manager: WorkspaceManager 实例
        sandbox: 沙箱实例
        plugin_registry: PluginRegistry 实例
    """

    def __init__(
        self,
        worker_id: str,
        redis_client: Optional[Redis] = None,
        registry=None,
        workspace_manager=None,
        sandbox=None,
        plugin_registry: Optional[PluginRegistry] = None,
        storage: Optional[FileStorage] = None,
        **kwargs,
    ):
        self.worker_id = worker_id
        self.redis = redis_client or get_redis()
        self.registry = registry
        self.workspace_manager = workspace_manager
        self.sandbox = sandbox
        self.logger = logger
        self.plugin_registry = plugin_registry or PluginRegistry()
        self.storage = storage

    @abstractmethod
    def get_capabilities(self) -> List[str]:
        """Return a list of agent IDs this worker can handle."""
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
    ) -> Any:
        """Preferred worker entrypoint for typed command handling."""
        raise NotImplementedError("Override process_command(...)")

    async def start_heartbeat(self):
        """Start periodic heartbeat registration"""
        # 调用插件的 startup 钩子
        await self.plugin_registry.on_worker_startup(self)

        async def _heartbeat_loop():
            while True:
                try:
                    await self.registry.register_worker(
                        self.worker_id, self.get_capabilities()
                    )
                    logger.info("[%s] Heartbeat sent", self.worker_id)
                except Exception as e:
                    logger.error("[%s] Heartbeat failed: %s", self.worker_id, e)
                await asyncio.sleep(WorkerConfig.heartbeat_interval)

        # Initial registration
        await self.registry.register_worker(self.worker_id, self.get_capabilities())
        # Start background loop
        self._heartbeat_task = asyncio.create_task(_heartbeat_loop())

    def stop_heartbeat(self):
        """Stop periodic heartbeat registration"""
        if hasattr(self, "_heartbeat_task") and self._heartbeat_task:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None
            logger.info("[%s] Heartbeat stopped", self.worker_id)

    async def _enqueue_agent_return(
        self, command: GatewayCommand, status: str, reply_data: Any
    ):
        header = command.header
        source_agent_id = header.source_agent_id
        if not source_agent_id:
            return

        target_agent_type = header.target_agent_type
        trace_id = header.trace_id
        message_id = header.message_id
        tenant_id = header.tenant_id

        callback_command = ResumeCommand(
            header=MessageHeader(
                message_id=f"{MESSAGE_ID_PREFIX}{uuid.uuid4().hex[:8]}",
                session_id=header.session_id,
                trace_id=trace_id if trace_id else uuid.uuid4().hex,
                source_agent_id=target_agent_type
                if target_agent_type
                else self.worker_id,
                target_agent_type=source_agent_id,
                parent_message_id=message_id if message_id else "",
                task_group_id=header.task_group_id or "",
                tenant_id=tenant_id if tenant_id else "",
            ),
            status=status,
            reply_data=reply_data,
        )
        await self.redis.xadd(
            RedisKeys.ctrl_stream(callback_command.header.target_agent_type),
            callback_command.to_redis_payload(),
        )

    async def _persist_agent_return_state(self, paths: dict, command: GatewayCommand):
        await asyncio.to_thread(self._persist_agent_return_state_sync, paths, command)

    def _persist_agent_return_state_sync(self, paths: dict, command: GatewayCommand):
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
                    "source_agent_id": header.source_agent_id,
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
        execution: Optional[Any] = None,
    ) -> str:
        trace_id = uuid.uuid4().hex
        header = command.header

        # 无论是调用其他 Agent 返回，还是等待用户输入返回，统一使用 RESUME 表示挂起任务的恢复。
        # 本质上都是“从挂起/等待状态恢复执行当前的工作流”，
        # 因此在生命周期和状态恢复逻辑（如重载 workspace, 持久化 state 等）上统一处理。
        is_agent_return = isinstance(command, ResumeCommand)
        source_agent_id = header.source_agent_id
        has_source_agent = bool(source_agent_id) and not is_agent_return

        # Get workspace dir from workspace_manager if available
        # Note: We don't use hasattr check because it doesn't work well with mocks
        workspace_dir = None

        context = AgentContext(
            session_id=header.session_id,
            trace_id=header.trace_id if header.trace_id else trace_id,
            redis_client=self.redis,
            current_agent_id=header.target_agent_type
            if header.target_agent_type
            else "",
            current_message_id=header.message_id,
            current_command=command,
            cancel_event=cancel_event,
            cancel_reason=cancel_reason,
            plugin_registry=self.plugin_registry,
            tenant_id=header.tenant_id,
            workspace_dir=workspace_dir,
            agent_configs=self.plugin_registry.agent_configs,
            storage=self.storage,
        )
        if execution:
            execution.context = context
        process_result: Any = None

        logger.info(
            "[%s] Received message: %s (Trace: %s)",
            self.worker_id,
            header.message_id,
            trace_id,
        )
        logger.info(
            "[%s] Target Agent Type: %s", self.worker_id, header.target_agent_type
        )
        logger.info("[%s] Session ID: %s", self.worker_id, header.session_id)

        token = None
        try:
            # 任务开始时调用插件钩子
            await self.plugin_registry.on_task_start(context)

            # 0. 自动保存用户消息到历史
            if not is_agent_return and hasattr(command, "content"):
                await context.agent_runtime_state.session_manager.history.save_message(
                    role="user",
                    content=command.content,
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
                header.session_id, header.message_id
            )
            logger.debug("[%s] Workspace paths: %s", self.worker_id, paths)

            # 2. Setup Sandbox
            if self.sandbox:
                logger.info("[%s] Installing sandbox", self.worker_id)
                self.sandbox.install()

            token = active_workspace.set(paths["private"])

            # 3. Process
            logger.info("[%s] Starting task processing", self.worker_id)
            if is_agent_return:
                await self._persist_agent_return_state(paths, command)

                # Check for scatter-gather join
                if header.task_group_id:
                    group_key = RedisKeys.task_group(header.task_group_id)
                    results_key = RedisKeys.task_group_results(header.task_group_id)
                    total_str = await self.redis.hget(group_key, TASK_GROUP_FIELD_TOTAL)
                    if total_str is not None:
                        # Store result in Redis Hash for distributed access
                        if isinstance(command, ResumeCommand):
                            result_data = {
                                "status": command.status,
                                "reply_data": command.reply_data,
                                "content": command.content,
                            }
                            await self.redis.hset(
                                results_key,
                                header.message_id,
                                json.dumps(result_data),
                            )
                            await self.redis.expire(
                                results_key, TASK_GROUP_TTL_SECONDS
                            )

                        completed = await self.redis.hincrby(
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
                            return f"{AgentState.QUEUED.value}: waiting_for_group"
                        logger.info(
                            "[%s] TaskGroup %s ALL COMPLETED (%s)!",
                            self.worker_id,
                            header.task_group_id,
                            total_str,
                        )

                await context.emit_state(
                    StateChangeEvent(state=AgentState.RESUMED.value)
                )
            process_result = await self.process_command(command, context)
            if has_source_agent:
                await self._enqueue_agent_return(
                    command,
                    status=AgentState.COMPLETED.value,
                    reply_data=process_result,
                )
                await context.emit_state(
                    StateChangeEvent(
                        state=f"{AgentState.QUEUED.value}: {source_agent_id}"
                    )
                )
            else:
                await context.emit_state(
                    StateChangeEvent(state=AgentState.COMPLETED.value)
                )
            logger.info("[%s] Task completed successfully", self.worker_id)
            # 任务完成时调用插件钩子
            await self.plugin_registry.on_task_complete(context, process_result)

            # 兜底：如果业务没发 appStreamResponse，在这里强制刷入历史
            await context.flush_to_history()

            return AgentState.COMPLETED.value

        except asyncio.CancelledError as e:
            logger.info("[%s] Task cancellation requested: %s", self.worker_id, str(e))
            await context.emit_state(
                StateChangeEvent(state=AgentState.CANCELLING.value)
            )
            if has_source_agent:
                await self._enqueue_agent_return(
                    command,
                    status=AgentState.CANCELLED.value,
                    reply_data={"reason": str(e)},
                )
            await context.emit_state(StateChangeEvent(state=AgentState.CANCELLED.value))
            return AgentState.CANCELLED.value

        except Exception as e:
            error_msg = f"[{self.worker_id}] Task failed: {str(e)}"
            logger.error(error_msg)
            if has_source_agent:
                await self._enqueue_agent_return(
                    command,
                    status=AgentState.FAILED.value,
                    reply_data={"error": str(e)},
                )
            await context.emit_state(
                StateChangeEvent(state=f"{AgentState.FAILED.value}: {str(e)}")
            )
            logger.error("[%s] Stack trace:", self.worker_id)
            logger.error(traceback.format_exc())
            # 任务出错时调用插件钩子
            await self.plugin_registry.on_task_error(context, e)
            return AgentState.FAILED.value
        finally:
            # 4. Cleanup
            if token is not None:
                active_workspace.reset(token)
            if self.sandbox:
                logger.info("[%s] Uninstalling sandbox", self.worker_id)
                self.sandbox.uninstall()
            logger.info("[%s] Cleaning up task: %s", self.worker_id, header.message_id)
            await self.workspace_manager.cleanup_task(
                header.session_id, header.message_id
            )
