import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from by_framework import GatewayProcessor
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
