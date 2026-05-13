"""Gateway message processor for handling agent commands and events."""

# pylint: disable=wrong-import-position

import traceback
import uuid
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Optional

if TYPE_CHECKING:
    from redis.asyncio import Redis

from by_framework.common.constants import MESSAGE_ID_PREFIX
from by_framework.common.emitter import DataLayoutBuilder
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.protocol.commands import GatewayCommand, ResumeCommand
from by_framework.core.protocol.events import StateChangeEvent
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.core.protocol.results import (
    JsonValue,
    ProcessCommandResult,
    normalize_process_result,
)
from by_framework.core.runtime.file_permissions import FilePermissionPolicy
from by_framework.worker.context import AgentContext

ContextHandler = Callable[
    [GatewayCommand, AgentContext], Awaitable[ProcessCommandResult]
]


class GatewayProcessor:
    """
    Decoupled message processor that handles the lifecycle of a Gateway message.
    Encapsulates state changes, context creation, and callback routing.
    """

    def __init__(
        self,
        worker_id: str,
        redis_client: Optional["Redis"] = None,
        workspace_manager: Optional[Any] = None,
        sandbox: Optional[Any] = None,
        permission_policy: Optional[FilePermissionPolicy] = None,
        layout_builder: Optional[DataLayoutBuilder] = None,
    ):
        from by_framework.common.logger import logger
        from by_framework.common.redis_client import get_redis

        self.worker_id = worker_id
        self.redis = redis_client or get_redis()
        self.workspace_manager = workspace_manager
        self.sandbox = sandbox
        self.permission_policy = permission_policy
        self.layout_builder = layout_builder
        self.logger = logger

    async def process(self, command: GatewayCommand, handler: ContextHandler) -> Any:
        """
        Process a single message using the provided handler function.
        Handles workspace setup, state emission, and error reporting.
        """

        trace_id = uuid.uuid4().hex
        header = command.header
        is_agent_return = isinstance(command, ResumeCommand)
        source_agent_type = header.source_agent_type
        has_source_agent = bool(source_agent_type) and not is_agent_return

        context = AgentContext(
            session_id=header.session_id,
            user_code=header.user_code,
            user_name=header.user_name,
            trace_id=header.trace_id if header.trace_id else trace_id,
            redis_client=self.redis,
            current_agent_id=header.target_agent_type or "",
            message_id=header.message_id,
            parent_message_id=header.parent_message_id,
            current_command=command,
            permission_policy=self.permission_policy,
            layout_builder=self.layout_builder,
        )

        self.logger.info(
            "[%s] Processing message: %s", self.worker_id, header.message_id
        )

        try:
            # Lifecycle start
            if is_agent_return:
                pass
                # TODO temporarily removed
                # await context.emit_state(
                #     StateChangeEvent(state=AgentState.RESUMED.value)
                # )

            # Optional Workspace Management
            if self.workspace_manager:
                await self.workspace_manager.setup_workspace(
                    header.session_id,
                    header.message_id,
                    user_code=header.user_code or "default",
                    agent_id=header.target_agent_type or self.worker_id,
                )
                if self.sandbox:
                    self.sandbox.install()

                # Note: workspace vars should be set by user or handled here.
                # For simplicity in decoupled mode, we leave complex workspace context
                # to user if they don't use GatewayWorker

            # Execute User Logic
            result = await handler(command, context)
            task_result = normalize_process_result(result)

            # Lifecycle Success
            if has_source_agent:
                await self._enqueue_callback(
                    command,
                    task_result.status,
                    task_result.reply_data,
                    content=task_result.content,
                    metadata=task_result.metadata,
                    extra_payload=task_result.extra_payload,
                )

            import json

            from by_framework.core.protocol.agent_state import is_terminal_state
            from by_framework.core.protocol.event_type import EventType

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

            if not has_source_agent:
                if (
                    is_terminal_state(task_result.status)
                    and not getattr(context, "_is_suspended", False)
                    and not getattr(context, "_permission_transferred", False)
                    and not getattr(context, "_is_stream_finished", False)
                ):
                    await context.emit_chunk(
                        "", event_type=EventType.APP_STREAM_RESPONSE.value
                    )
                    context._is_stream_finished = True

            return result

        except Exception as e:
            self.logger.error("[%s] Processing failed: %s", self.worker_id, str(e))
            self.logger.error(traceback.format_exc())

            if has_source_agent:
                await self._enqueue_callback(
                    command, AgentState.FAILED.value, {"error": str(e)}
                )

            await context.emit_state(
                StateChangeEvent(state=f"{AgentState.FAILED.value}: {str(e)}")
            )
            raise

    async def _enqueue_callback(
        self,
        original_command: GatewayCommand,
        status: str,
        reply_data: JsonValue,
        content: str | list[dict[str, Any]] = "",
        metadata: Optional[dict[str, JsonValue]] = None,
        extra_payload: Optional[dict[str, JsonValue]] = None,
    ):
        """Enqueue callback response to source agent."""
        from by_framework.common.constants import RedisKeys

        header = original_command.header
        merged_metadata = {
            **dict(header.metadata),
            **dict(metadata or {}),
        }
        callback_command = ResumeCommand(
            header=MessageHeader(
                message_id=f"{MESSAGE_ID_PREFIX}{uuid.uuid4().hex[:8]}",
                session_id=header.session_id,
                trace_id=header.trace_id or uuid.uuid4().hex,
                source_agent_type=header.target_agent_type or self.worker_id,
                target_agent_type=header.source_agent_type,
                parent_message_id=header.message_id,
                user_code=header.user_code,
                user_name=header.user_name,
                metadata=merged_metadata,
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
