"""
Agent context module.

Provides the AgentContext class which serves as the runtime context for agent
task execution, providing access to session state, event emission,
and inter-agent communication.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import uuid
import warnings
from typing_extensions import deprecated
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from by_framework.common.constants import (
    MESSAGE_ID_PREFIX,
    TASK_GROUP_FIELD_COMPLETED,
    TASK_GROUP_FIELD_SOURCE_AGENT,
    TASK_GROUP_FIELD_TOTAL,
    TASK_GROUP_ID_PREFIX,
    TASK_GROUP_TTL_SECONDS,
    RedisKeys,
)
from by_framework.common.emitter import GatewayDataEmitter
from by_framework.common.redis_client import Redis, get_redis
from by_framework.core.extensions import AgentConfig
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.runtime import AgentRuntimeState
from by_framework.core.protocol.commands import AskAgentCommand
from by_framework.core.protocol.event_type import EventType
from by_framework.core.protocol.events import (
    ArtifactEvent,
    AskUserEvent,
    StateChangeEvent,
    StreamChunkEvent,
)
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.core.runtime.filestore.base import FileStorage

if TYPE_CHECKING:
    from by_framework.core.extensions import PluginRegistry


class AgentContext:
    """Agent 运行时上下文。

    在任务执行过程中提供对会话状态、事件发射和智能体间通信的访问。
    通过 AgentContext，worker 可以向用户发送流式响应、调用其他智能体、
    分发任务组等。

    Args:
        session_id: 会话ID
        trace_id: 追踪ID
        redis_client: Redis 客户端实例
        data_stream_name: 数据流名称
        current_agent_id: 当前智能体ID
        current_message_id: 当前消息ID
        current_command: 当前命令
        cancel_event: 取消事件
        cancel_reason: 取消原因
        plugin_registry: 插件注册表
    """

    def __init__(
        self,
        session_id: str,
        trace_id: str,
        redis_client: Optional[Redis] = None,
        data_stream_name: Optional[str] = None,
        current_agent_id: str = "",
        current_message_id: str = "",
        current_command: Optional[Any] = None,
        cancel_event: Optional[asyncio.Event] = None,
        cancel_reason: str = "",
        plugin_registry: Optional[PluginRegistry] = None,
        tenant_id: Optional[str] = None,
        workspace_dir: Optional[str] = None,
        agent_configs: Optional[list[AgentConfig]] = None,
        storage: Optional[FileStorage] = None,
    ):
        self.redis = redis_client or get_redis()
        self.session_id = session_id
        self.trace_id = trace_id
        self.data_stream_name = data_stream_name
        self.current_agent_id = current_agent_id
        self.current_message_id = current_message_id
        self.current_command = current_command
        self.cancel_event = cancel_event
        self.cancel_reason = cancel_reason
        self.emitter = GatewayDataEmitter(self.redis, data_stream_name)
        self._response_buffer = []  # 用于收集流式回复内容
        self._is_history_saved = False  # 防止重复保存
        self.plugin_registry = plugin_registry

        # New: AgentRuntimeState for unified state management
        self._agent_runtime_state = AgentRuntimeState(
            session_id=session_id,
            tenant_id=tenant_id,
            storage=storage,
            workspace_dir=workspace_dir,
            agent_configs=agent_configs,
        )

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
        获取集群中所有活跃的 worker 及其能力信息
        """
        from by_framework.core.registry import WorkerRegistry

        registry = WorkerRegistry(self.redis)
        return await registry.get_all_workers()

    async def _emit_event(
        self,
        event_type: str,
        data: Optional[Dict[str, Any]] = None,
        state_msg: str = "",
        artifact_url: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ):
        await self.emitter.emit_event(
            session_id=self.session_id,
            trace_id=self.trace_id,
            event_type=event_type,
            source_agent_id=self.current_agent_id,
            message_id=self.current_message_id,
            data=data,
            state_msg=state_msg,
            artifact_url=artifact_url,
            metadata=metadata,
        )

    async def emit_chunk(
        self, event: Union[StreamChunkEvent, str], event_type: Optional[str] = None, content_type: Optional[str] = None
    ) -> None:
        # 1. 收集内容
        content = ""
        if isinstance(event, StreamChunkEvent):
            content = event.content or ""
        elif isinstance(event, str):
            content = event

        if content:
            self._response_buffer.append(content)

        # 2. 发送原始分片
        await self.emitter.emit_chunk(
            self.session_id,
            self.trace_id,
            event,
            self.current_agent_id,
            message_id=self.current_message_id,
            event_type=event_type,
            content_type=content_type,
        )

        # 3. 检查是否是流结束标识，如果是则触发入库
        if event_type == EventType.APP_STREAM_RESPONSE.value:
            await self.flush_to_history()

    async def flush_to_history(self) -> None:
        """将当前缓冲区内容作为 assistant 回复存入历史"""
        if self._is_history_saved or not self._response_buffer:
            return

        full_content = "".join(self._response_buffer)
        await self.agent_runtime_state.session_manager.history.save_message(
            role="assistant",  # Role constant for assistant messages
            content=full_content,
            metadata={
                "trace_id": self.trace_id,
                "agent_id": self.current_agent_id,
                "message_id": self.current_message_id,
            },
        )
        self._is_history_saved = True

    async def emit_state(
        self, event: Union[StateChangeEvent, str], event_type: Optional[str] = None, content_type: Optional[str] = None
    ) -> None:
        await self.emitter.emit_state(
            self.session_id,
            self.trace_id,
            event,
            self.current_agent_id,
            message_id=self.current_message_id,
            event_type=event_type,
            content_type=content_type,
        )

    async def emit_artifact(
        self, event: Union[ArtifactEvent, str], event_type: Optional[str] = None, content_type: Optional[str] = None
    ) -> None:
        await self.emitter.emit_artifact(
            self.session_id,
            self.trace_id,
            event,
            self.current_agent_id,
            message_id=self.current_message_id,
            event_type=event_type,
            content_type=content_type,
        )

    async def ask_user(self, event: Union[AskUserEvent, str]) -> dict:
        """
        Suspend execution and ask the user for a prompt.
        Accepts an AskUserEvent or a raw string prompt.
        """
        await self.emitter.ask_user(
            self.session_id,
            self.trace_id,
            event,
            self.current_agent_id,
            message_id=self.current_message_id,
        )
        return {"status": AgentState.WAITING_USER.value}

    async def call_agent(
        self,
        target_agent_type: str,
        content: Union[str, List[Dict[str, Any]]],
        payload: Optional[Dict[str, Any]] = None,
        wait_for_reply: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> dict:
        """
        Push a control-flow message to another agent.
        If wait_for_reply is True, source_agent_id will be injected for callback routing.
        """
        message_id = f"{MESSAGE_ID_PREFIX}{uuid.uuid4().hex[:8]}"
        merged_payload = dict(payload or {})
        if wait_for_reply:
            merged_payload["wait_for_reply"] = True

        command = AskAgentCommand(
            header=MessageHeader(
                message_id=message_id,
                session_id=self.session_id,
                trace_id=self.trace_id,
                source_agent_id=self.current_agent_id if wait_for_reply else "",
                target_agent_type=target_agent_type,
                parent_message_id=self.current_message_id,
                metadata=metadata or {},
            ),
            content=content,
            wait_for_reply=wait_for_reply,
            extra_payload={
                k: v for k, v in merged_payload.items() if k != "wait_for_reply"
            },
        )
        await self.redis.xadd(
            RedisKeys.ctrl_stream(target_agent_type), command.to_redis_payload()
        )

        return {
            "status": AgentState.QUEUED.value,
            "message_id": message_id,
            "target_agent_type": target_agent_type,
        }

    async def dispatch_group(
        self,
        tasks: list[dict[str, Any]],
        wait_for_reply: bool = True,
    ) -> dict:
        """
        Dispatch multiple tasks concurrently as a group.
        The caller agent will be resumed ONLY when ALL tasks in the group are completed.

        Args:
            tasks: A list of dicts, each containing:
                   {
                       "target_agent_type": str,
                       "content": str,
                       "payload": Optional[Dict[str, Any]],
                       "metadata": Optional[Dict[str, Any]]
                   }
            wait_for_reply: bool. If True, sets up Redis counters to wait for all.
        """
        if not tasks:
            return {"status": "EMPTY", "task_group_id": ""}

        task_group_id = f"{TASK_GROUP_ID_PREFIX}{uuid.uuid4().hex[:8]}"
        total_tasks = len(tasks)

        if wait_for_reply:
            group_key = RedisKeys.task_group(task_group_id)
            await self.redis.hset(
                group_key,
                mapping={
                    TASK_GROUP_FIELD_TOTAL: str(total_tasks),
                    TASK_GROUP_FIELD_COMPLETED: "0",
                    TASK_GROUP_FIELD_SOURCE_AGENT: self.current_agent_id,
                },
            )
            # Ensure the key expires to prevent leak
            await self.redis.expire(group_key, TASK_GROUP_TTL_SECONDS)

        dispatched = []
        for task in tasks:
            target_agent_type = task["target_agent_type"]
            content = task.get("content", "")
            payload = task.get("payload", {})
            metadata = task.get("metadata", {})

            message_id = f"{MESSAGE_ID_PREFIX}{uuid.uuid4().hex[:8]}"
            merged_payload = dict(payload)
            if wait_for_reply:
                merged_payload["wait_for_reply"] = True

            command = AskAgentCommand(
                header=MessageHeader(
                    message_id=message_id,
                    session_id=self.session_id,
                    trace_id=self.trace_id,
                    source_agent_id=self.current_agent_id if wait_for_reply else "",
                    target_agent_type=target_agent_type,
                    parent_message_id=self.current_message_id,
                    task_group_id=task_group_id,
                    metadata=metadata,
                ),
                content=content,
                wait_for_reply=wait_for_reply,
                extra_payload={
                    k: v for k, v in merged_payload.items() if k != "wait_for_reply"
                },
            )
            await self.redis.xadd(
                RedisKeys.ctrl_stream(target_agent_type), command.to_redis_payload()
            )

            dispatched.append(
                {"message_id": message_id, "target_agent_type": target_agent_type}
            )

        return {
            "status": "GROUP_QUEUED",
            "task_group_id": task_group_id,
            "dispatched_tasks": dispatched,
        }

    async def collect_group_results(
        self,
        task_group_id: str,
        timeout: float = 30.0,
    ) -> list[dict[str, Any]]:
        """
        收集任务组所有子任务的结果。

        当最后一个子任务完成后调用，返回所有子任务的结果列表。
        如果在超时时间内没有收集到所有结果，返回已收集到的结果。

        Args:
            task_group_id: dispatch_group 返回的 task_group_id
            timeout: 等待所有结果的最大超时时间（秒）

        Returns:
            包含所有子任务结果的列表，每个元素包含:
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

        total_str = await self.redis.hget(group_key, TASK_GROUP_FIELD_TOTAL)
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

            raw_results = await self.redis.hgetall(results_key)
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
