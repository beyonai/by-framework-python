import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from by_framework import GatewayProcessor, RedisKeys
from by_framework.core.protocol.commands import (
    AskAgentCommand,
    ResumeCommand,
    command_from_dict,
)
from by_framework.core.protocol.content_type import SseMessageType
from by_framework.core.protocol.message_header import MessageHeader


class CustomLayoutBuilder:

    def build(self, content, role, content_type, source_agent_type, **kwargs):
        return {
            "content": content,
            "content_type": content_type,
            "agent": source_agent_type,
            "message_id": kwargs["order_id"],
        }


@pytest.mark.asyncio
async def test_processor_enqueue_callback_emits_resume_command():
    """Test that _enqueue_callback emits a ResumeCommand to Redis with
    correct status and reply_data."""
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    processor = GatewayProcessor(worker_id="worker-1", redis_client=redis_mock)
    original_command = AskAgentCommand(
        header=MessageHeader(
            message_id="msg-1",
            session_id="sess-1",
            trace_id="trace-1",
            source_agent_type="agent-a",
            target_agent_type="agent-b",
        ),
        content="hello",
    )

    await processor._enqueue_callback(original_command, "SUCCESS", {"answer": 42})

    args, _ = redis_mock.xadd.call_args
    raw = json.loads(args[1]["data"])
    command = command_from_dict(raw)

    assert isinstance(command, ResumeCommand)
    assert command.status == "SUCCESS"
    assert command.reply_data == {"answer": 42}


@pytest.mark.asyncio
async def test_processor_injects_decoded_command_into_context():
    """Test that GatewayProcessor.inject_context makes current_command
    available on context."""
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    processor = GatewayProcessor(worker_id="worker-1", redis_client=redis_mock)
    observed = {}

    async def handler(command, context):
        observed["command"] = command
        observed["context_command"] = getattr(context, "current_command", None)
        return {"ok": True}

    command = ResumeCommand(
        header=MessageHeader(
            message_id="msg-resume-ctx",
            session_id="sess-1",
            trace_id="trace-1",
            target_agent_type="agent-a",
        ),
        status="SUCCESS",
        reply_data={"x": 1},
    )

    await processor.process(command, handler)

    assert isinstance(observed["command"], ResumeCommand)
    assert isinstance(observed["context_command"], ResumeCommand)
    assert observed["command"].reply_data == {"x": 1}


@pytest.mark.asyncio
async def test_processor_passes_layout_builder_to_agent_context():
    redis_mock = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.xadd = MagicMock()
    mock_pipe.expire = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    redis_mock.pipeline = MagicMock(return_value=mock_pipe)
    processor = GatewayProcessor(
        worker_id="worker-1",
        redis_client=redis_mock,
        layout_builder=CustomLayoutBuilder(),
    )

    async def handler(command, context):
        await context.emit_chunk("processor-layout")
        return {"ok": True}

    command = AskAgentCommand(
        header=MessageHeader(
            message_id="msg-layout",
            session_id="sess-1",
            trace_id="trace-1",
            target_agent_type="agent-a",
        ),
        content="hello",
    )

    await processor.process(command, handler)

    call_args_list = mock_pipe.xadd.call_args_list
    # The first call is from the explicit context.emit_chunk("processor-layout")
    # The second call is the automatic FINAL_ANSWER emission.
    args, _ = call_args_list[0]
    raw = json.loads(args[1]["data"])
    assert raw["data"] == {
        "content": "processor-layout",
        "content_type": SseMessageType.text.value,
        "agent": "agent-a",
        "message_id": "msg-layout",
    }


@pytest.mark.asyncio
async def test_processor_callback_message_id_is_the_callers_own_id():
    """The caller reattaches its suspended execution by the reply's
    message_id. A freshly minted id resolves to no execution, so the reply
    starts a disconnected one and the caller waits forever."""
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    processor = GatewayProcessor(worker_id="worker-1", redis_client=redis_mock)
    original_command = AskAgentCommand(
        header=MessageHeader(
            message_id="msg-child",
            session_id="sess-1",
            trace_id="trace-1",
            source_agent_type="agent-a",
            target_agent_type="agent-b",
            parent_message_id="msg-caller",
            task_group_id="tg-1",
        ),
        content="hello",
    )

    await processor._enqueue_callback(original_command, "SUCCESS", {"answer": 42})

    command = command_from_dict(json.loads(redis_mock.xadd.call_args.args[1]["data"]))
    assert command.header.message_id == "msg-caller"
    # The only per-sibling-unique value, which Group Join keys results by.
    assert command.header.parent_message_id == "msg-child"
    assert command.header.task_group_id == "tg-1"


@pytest.mark.asyncio
async def test_processor_does_not_reply_while_suspended():
    """Same rule as GatewayWorker: a suspended execution has no result yet."""
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    processor = GatewayProcessor(worker_id="worker-1", redis_client=redis_mock)

    async def handler(command, context):
        context._is_suspended = True
        return {"status": "QUEUED"}

    await processor.process(
        AskAgentCommand(
            header=MessageHeader(
                message_id="msg-child",
                session_id="sess-1",
                trace_id="trace-1",
                source_agent_type="agent-a",
                target_agent_type="agent-b",
                parent_message_id="msg-caller",
            ),
            content="hello",
        ),
        handler,
    )

    ctrl_writes = [
        call
        for call in redis_mock.xadd.await_args_list
        if call.args[0] == RedisKeys.ctrl_stream("agent-a")
    ]
    assert ctrl_writes == []


def _gate_redis(claimed: int, marked: int):
    """AsyncMock Redis whose wait-index gate answers are pinned."""
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(
            xadd=MagicMock(), expire=MagicMock(), execute=AsyncMock(return_value=[])
        )
    )
    redis_mock.zrem = AsyncMock(return_value=claimed)
    redis_mock.exists = AsyncMock(return_value=marked)
    return redis_mock


def _reply_command():
    return ResumeCommand(
        header=MessageHeader(
            message_id="msg-caller",
            session_id="sess-1",
            trace_id="trace-1",
            source_agent_type="agent-b",
            target_agent_type="agent-a",
            parent_message_id="msg-child",
        ),
        status="COMPLETED",
        reply_data={"value": 1},
    )


@pytest.mark.asyncio
async def test_processor_drops_a_reply_whose_wait_is_already_resolved():
    """GatewayProcessor is a second, independent entry point for replies, so
    it needs the same idempotency gate as WorkerRunner — a gate on only one
    of two doors is not a gate."""
    redis_mock = _gate_redis(claimed=0, marked=1)
    processor = GatewayProcessor(worker_id="worker-1", redis_client=redis_mock)
    handled = []

    async def handler(command, context):
        handled.append(command)
        return {"status": "COMPLETED"}

    result = await processor.process(_reply_command(), handler)

    assert result is None
    assert handled == []


@pytest.mark.asyncio
async def test_processor_keeps_a_reply_that_was_never_registered():
    """RED LINE: no wait-index entry and no marker means "unknown", not
    "duplicate" — replies dispatched before this version must still land."""
    redis_mock = _gate_redis(claimed=0, marked=0)
    processor = GatewayProcessor(worker_id="worker-1", redis_client=redis_mock)
    handled = []

    async def handler(command, context):
        handled.append(command)
        return {"status": "COMPLETED"}

    await processor.process(_reply_command(), handler)

    assert len(handled) == 1


@pytest.mark.asyncio
async def test_processor_resumed_execution_replies_to_the_original_caller():
    """A resume's header names the sub-agent that just finished; the caller
    owed a reply lives in the execution record."""
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    processor = GatewayProcessor(worker_id="worker-1", redis_client=redis_mock)

    async def handler(command, context):
        return {"status": "COMPLETED", "reply_data": {"done": True}}

    with patch(
        "by_framework.core.registry.WorkerRegistry.get_execution_by_message_id",
        new_callable=AsyncMock,
    ) as lookup:
        lookup.return_value = {
            "source_agent_type": "agent-a",
            "parent_message_id": "msg-caller",
            "task_group_id": "tg-1",
        }
        await processor.process(
            ResumeCommand(
                header=MessageHeader(
                    message_id="msg-middle",
                    session_id="sess-1",
                    trace_id="trace-1",
                    source_agent_type="agent-c",
                    target_agent_type="agent-b",
                    parent_message_id="msg-sub",
                ),
                status="COMPLETED",
                reply_data={"from": "c"},
            ),
            handler,
        )

    ctrl_writes = [
        call
        for call in redis_mock.xadd.await_args_list
        if call.args[0] == RedisKeys.ctrl_stream("agent-a")
    ]
    assert len(ctrl_writes) == 1
    reply = command_from_dict(json.loads(ctrl_writes[0].args[1]["data"]))
    assert reply.header.message_id == "msg-caller"
    assert reply.header.parent_message_id == "msg-middle"
    assert reply.header.task_group_id == "tg-1"
    assert reply.reply_data == {"done": True}


async def _resume_reply_to_caller(snapshot, handler_result, waking_metadata):
    """Drive one resumed execution through GatewayProcessor and return the
    reply command it posted to the caller's control stream.

    The waking ResumeCommand names agent-c (the sub-agent that just finished)
    and carries its own metadata for that hop; the caller owed a reply is
    whatever the execution snapshot names.
    """
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    processor = GatewayProcessor(worker_id="worker-1", redis_client=redis_mock)

    async def handler(command, context):
        return handler_result

    with patch(
        "by_framework.core.registry.WorkerRegistry.get_execution_by_message_id",
        new_callable=AsyncMock,
    ) as lookup:
        lookup.return_value = snapshot
        await processor.process(
            ResumeCommand(
                header=MessageHeader(
                    message_id="msg-middle",
                    session_id="sess-1",
                    trace_id="trace-1",
                    source_agent_type="agent-c",
                    target_agent_type="agent-b",
                    parent_message_id="msg-sub",
                    metadata=waking_metadata,
                ),
                status="COMPLETED",
                reply_data={"from": "c"},
            ),
            handler,
        )

    ctrl_writes = [
        call
        for call in redis_mock.xadd.await_args_list
        if call.args[0] == RedisKeys.ctrl_stream("agent-a")
    ]
    assert len(ctrl_writes) == 1
    reply = command_from_dict(json.loads(ctrl_writes[0].args[1]["data"]))
    # Proves the reply went through the resume rebuild rather than straight off
    # the incoming header: the waking command names agent-c as its source and
    # "msg-sub" as its parent, so only the execution snapshot can put this
    # reply on agent-a's stream carrying the caller's own id.
    assert reply.header.message_id == "msg-caller"
    return reply


@pytest.mark.asyncio
async def test_processor_resumed_reply_carries_the_callers_own_metadata():
    """Same rule as GatewayWorker._resolve_reply_command: the caller's
    original dispatch metadata is restored wholesale from the execution
    snapshot. The metadata on the message that woke this execution up is
    plumbing for that one hop and must not reach the caller."""
    reply = await _resume_reply_to_caller(
        snapshot={
            "source_agent_type": "agent-a",
            "parent_message_id": "msg-caller",
            "task_group_id": "tg-1",
            # A's original dispatch metadata, persisted at
            # initialize_execution() time.
            "metadata": {"caller": "original", "request_id": "req-1"},
        },
        handler_result={
            "status": "COMPLETED",
            "reply_data": {"done": True},
            # This link's own metadata: overrides same-named keys from the
            # caller's, leaves the rest alone.
            "metadata": {"caller": "overridden", "tokens": 123},
        },
        waking_metadata={"caller": "should-not-leak", "from_c": "should-not-leak"},
    )

    assert reply.header.metadata["request_id"] == "req-1"
    assert reply.header.metadata["caller"] == "overridden"
    assert reply.header.metadata["tokens"] == 123
    assert "from_c" not in reply.header.metadata


@pytest.mark.asyncio
async def test_processor_resumed_reply_metadata_is_empty_without_a_snapshot_value():
    """An execution recorded before the snapshot carried metadata degrades to
    an empty dict — never to the waking message's metadata."""
    reply = await _resume_reply_to_caller(
        snapshot={
            "source_agent_type": "agent-a",
            "parent_message_id": "msg-caller",
            "task_group_id": "",
        },
        handler_result={"status": "COMPLETED", "reply_data": {"done": True}},
        waking_metadata={"caller": "should-not-leak", "from_c": "should-not-leak"},
    )

    assert reply.header.metadata == {}
