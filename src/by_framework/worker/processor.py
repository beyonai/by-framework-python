"""Gateway message processor for handling agent commands and events."""

# pylint: disable=wrong-import-position

import traceback
import uuid
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Optional

if TYPE_CHECKING:
    from redis.asyncio import Redis

from by_framework.common.constants import (CLIENT_SOURCE_AGENT_TYPE, MESSAGE_ID_PREFIX)
from by_framework.common.emitter import DataLayoutBuilder
from by_framework.core.protocol.agent_state import (AgentState, is_terminal_state)
from by_framework.core.protocol.commands import GatewayCommand, ResumeCommand
from by_framework.core.protocol.events import StateChangeEvent
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.core.protocol.results import (
    JsonValue,
    ProcessCommandResult,
    normalize_process_result,
)
from by_framework.core.runtime.file_permissions import FilePermissionPolicy
from by_framework.core.wait_gate import consume_wait_entry, emit_orphaned_reply
from by_framework.core.wait_reply import flush_pending_group_replies
from by_framework.worker._resume_metadata import merge_resume_metadata
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

        Returns the handler's result, or None when the message was a reply
        to a wait that is already resolved (see the idempotency gate below);
        such a message is fully handled and should be acknowledged by the
        caller's consume loop like any other.
        """

        trace_id = uuid.uuid4().hex
        header = command.header
        is_agent_return = isinstance(command, ResumeCommand)

        if is_agent_return:
            # Same idempotency gate as WorkerRunner._process_message_from_dict,
            # and for the same reason: this is a second, independent entry
            # point for replies (callers that drive their own consume loop
            # instead of subclassing GatewayWorker). A gate on only one entry
            # point is not a gate — replies arriving via the other one would
            # both wake an already-resolved caller and leave the wait-index
            # entry behind for a sweep to resolve all over again.
            gate = await consume_wait_entry(self.redis, command)
            if not gate.allow:
                self.logger.warning(
                    "[%s] Dropping reply for an already-resolved wait (%s): "
                    "message_id=%s, child_message_id=%s, session_id=%s",
                    self.worker_id,
                    gate.reason,
                    header.message_id,
                    header.parent_message_id,
                    header.session_id,
                )
                await emit_orphaned_reply(
                    self.redis,
                    command,
                    worker_id=self.worker_id,
                    reason=gate.reason,
                )
                return None

        # One lookup feeds both directions: the header this execution replies
        # with, and the header its own handler reads. They are different
        # rules over the same record — see _resolve_reply_header and
        # _restore_inbound_metadata.
        snapshot = (
            await self._load_execution_snapshot(command) if is_agent_return else None
        )
        raw_command = command
        reply_header = self._resolve_reply_header(command, snapshot)
        has_source_agent = reply_header is not None
        command = self._restore_inbound_metadata(command, is_agent_return, snapshot)
        header = command.header

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
            # Same rule as GatewayWorker._handle_message: stand-ins for Task
            # Group members that never reached a worker go out only once the
            # handler has returned normally, and never when it raised.
            await flush_pending_group_replies(self.redis, context, self.worker_id)
            task_result = normalize_process_result(result)

            # Lifecycle Success
            # A suspended execution has no result yet — replying with the
            # value the handler returned so it could unwind would wake the
            # caller early and consume the one reply it waits for. Same rule
            # as GatewayWorker._handle_message, including its exception: a
            # handler that returned a terminal status is finished and will
            # never be resumed to reply later, so it must reply now.
            is_suspended = bool(
                getattr(context, "_is_suspended", False)
            ) and not is_terminal_state(task_result.status)
            if has_source_agent and not is_suspended:
                # raw_command, not the inbound-restored one: the reply is the
                # outbound direction and must not inherit the inbound merge.
                await self._enqueue_callback(
                    raw_command,
                    task_result.status,
                    task_result.reply_data,
                    content=task_result.content,
                    metadata=task_result.metadata,
                    extra_payload=task_result.extra_payload,
                    reply_header=reply_header,
                )

            import json

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
                    raw_command,
                    AgentState.FAILED.value,
                    {"error": str(e)},
                    reply_header=reply_header,
                )

            await context.emit_state(
                StateChangeEvent(state=f"{AgentState.FAILED.value}: {str(e)}")
            )
            raise

    async def _load_execution_snapshot(
        self, command: GatewayCommand
    ) -> Optional[dict[str, Any]]:
        """Read the execution record the original dispatch wrote.

        Fetched once per resume and shared by both restore directions.
        Fail-soft: a registry error degrades to "no record", which each caller
        then handles as its own no-op.
        """
        header = command.header
        try:
            from by_framework.core.registry import WorkerRegistry

            return await WorkerRegistry(self.redis).get_execution_by_message_id(
                header.message_id, session_id=header.session_id
            )
        except Exception as error:  # pylint: disable=broad-exception-caught
            self.logger.warning(
                "[%s] Could not load the execution record of resumed execution "
                "%s: %s",
                self.worker_id,
                header.message_id,
                error,
            )
            return None

    def _resolve_reply_header(
        self,
        command: GatewayCommand,
        snapshot: Optional[dict[str, Any]],
    ) -> Optional[MessageHeader]:
        """Return the header describing the caller this execution owes a reply
        to, or None when nobody is waiting.

        Mirrors ``GatewayWorker._resolve_reply_command``: a resume's header
        describes the sub-agent that just finished, so the caller has to be
        read back from the execution record the original dispatch wrote — and,
        for the same reason as there, ``CLIENT_SOURCE_AGENT_TYPE`` is not a
        caller. It is what a client writes on a root execution's record, and
        nothing consumes its control stream.

        Four fields come from that snapshot, not from the waking message:
        ``source_agent_type``, ``parent_message_id``, ``task_group_id`` and
        ``metadata``. The last one is restored as a full replacement rather
        than a merge, exactly as ``GatewayWorker._resolve_reply_command``
        does it: the waking message (an ``ask_user`` answer, or a sub-call's
        reply) is transient plumbing for that one hop, not something the
        caller ever sent. A snapshot missing the field (an execution recorded
        before it existed) degrades to an empty dict rather than leaking the
        waking message's metadata to the caller.

        This is the OUTBOUND direction only. What the handler itself reads is
        ``_restore_inbound_metadata``, which merges rather than replaces —
        and which must run even when this returns None, since a
        client-dispatched root has no caller but still has its own metadata
        to get back.
        """
        header = command.header
        if not isinstance(command, ResumeCommand):
            return header if header.source_agent_type else None

        caller_agent_type = str((snapshot or {}).get("source_agent_type", "") or "")
        if not caller_agent_type or caller_agent_type == CLIENT_SOURCE_AGENT_TYPE:
            return None
        return replace(
            header,
            source_agent_type=caller_agent_type,
            parent_message_id=str((snapshot or {}).get("parent_message_id", "") or ""),
            task_group_id=str((snapshot or {}).get("task_group_id", "") or ""),
            metadata=dict((snapshot or {}).get("metadata") or {}),
        )

    @staticmethod
    def _restore_inbound_metadata(
        command: GatewayCommand,
        is_agent_return: bool,
        snapshot: Optional[dict[str, Any]],
    ) -> GatewayCommand:
        """Give a resumed handler its own dispatch metadata back.

        Mirrors ``GatewayWorker._restore_inbound_metadata``, including its
        merge-don't-replace rule and its no-mutation rule; see that docstring
        for why the inbound direction differs from the outbound one.
        """
        if not is_agent_return:
            return command
        return replace(
            command,
            header=replace(
                command.header,
                metadata=merge_resume_metadata(
                    (snapshot or {}).get("metadata"),
                    command.header.metadata,
                ),
            ),
        )

    async def _enqueue_callback(
        self,
        original_command: GatewayCommand,
        status: str,
        reply_data: JsonValue,
        content: str | list[dict[str, Any]] = "",
        metadata: Optional[dict[str, JsonValue]] = None,
        extra_payload: Optional[dict[str, JsonValue]] = None,
        reply_header: Optional[MessageHeader] = None,
    ):
        """Enqueue callback response to source agent."""
        from by_framework.common.constants import RedisKeys

        header = reply_header or original_command.header
        merged_metadata = {
            **dict(header.metadata),
            **dict(metadata or {}),
        }
        callback_command = ResumeCommand(
            header=MessageHeader(
                # The caller reattaches its suspended execution by this id, so
                # it must be the caller's own message_id (this dispatch's
                # parent_message_id) — a freshly minted id resolves to no
                # execution and orphans the caller.
                message_id=(
                    header.parent_message_id
                    or f"{MESSAGE_ID_PREFIX}{uuid.uuid4().hex[:8]}"
                ),
                session_id=header.session_id,
                trace_id=header.trace_id or uuid.uuid4().hex,
                source_agent_type=header.target_agent_type or self.worker_id,
                target_agent_type=header.source_agent_type,
                parent_message_id=header.message_id,
                task_group_id=header.task_group_id or "",
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
