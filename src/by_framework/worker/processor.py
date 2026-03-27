import traceback
import uuid
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Optional

if TYPE_CHECKING:
    from redis.asyncio import Redis

from by_framework.common.constants import MESSAGE_ID_PREFIX
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.protocol.commands import (GatewayCommand, ResumeCommand)
from by_framework.core.protocol.events import StateChangeEvent
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.worker.context import AgentContext

ContextHandler = Callable[[GatewayCommand, AgentContext], Awaitable[Any]]


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
    ):
        from by_framework.common.logger import logger
        from by_framework.common.redis_client import get_redis

        self.worker_id = worker_id
        self.redis = redis_client or get_redis()
        self.workspace_manager = workspace_manager
        self.sandbox = sandbox
        self.logger = logger

    async def process(self, command: GatewayCommand, handler: ContextHandler) -> Any:
        """
        Process a single message using the provided handler function.
        Handles workspace setup, state emission, and error reporting.
        """

        trace_id = uuid.uuid4().hex
        header = command.header
        is_agent_return = isinstance(command, ResumeCommand)
        source_agent_id = header.source_agent_id
        has_source_agent = bool(source_agent_id) and not is_agent_return

        context = AgentContext(
            session_id=header.session_id,
            trace_id=header.trace_id if header.trace_id else trace_id,
            redis_client=self.redis,
            current_agent_id=header.target_agent_type or "",
            current_message_id=header.message_id,
            current_command=command,
        )

        self.logger.info(
            "[%s] Processing message: %s", self.worker_id, header.message_id
        )

        try:
            # Lifecycle start
            if is_agent_return:
                await context.emit_state(
                    StateChangeEvent(state=AgentState.RESUMED.value)
                )

            # Optional Workspace Management
            if self.workspace_manager:
                await self.workspace_manager.setup_workspace(
                    header.session_id, header.message_id
                )
                if self.sandbox:
                    self.sandbox.install()

                # Note: workspace context variables (active_workspace) should be set by user or handled here
                # For simplicity in decoupled mode, we leave complex workspace context to the user if they don't use GatewayWorker

            # Execute User Logic
            result = await handler(command, context)

            # Lifecycle Success
            if has_source_agent:
                await self._enqueue_callback(
                    command, AgentState.COMPLETED.value, result
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
        self, original_command: GatewayCommand, status: str, reply_data: Any
    ):
        from by_framework.common.constants import RedisKeys

        header = original_command.header
        callback_command = ResumeCommand(
            header=MessageHeader(
                message_id=f"{MESSAGE_ID_PREFIX}{uuid.uuid4().hex[:8]}",
                session_id=header.session_id,
                trace_id=header.trace_id or uuid.uuid4().hex,
                source_agent_id=header.target_agent_type or self.worker_id,
                target_agent_type=header.source_agent_id,
                parent_message_id=header.message_id,
            ),
            status=status,
            reply_data=reply_data,
        )
        await self.redis.xadd(
            RedisKeys.ctrl_stream(callback_command.header.target_agent_type),
            callback_command.to_redis_payload(),
        )
