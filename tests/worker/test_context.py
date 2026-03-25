import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from byclaw_gateway_sdk import AgentContext
from byclaw_gateway_sdk.core.protocol.commands import (
    AskAgentCommand,
    command_from_dict,
)


@pytest.mark.asyncio
async def test_context_call_agent_with_metadata():
    """Test that AgentContext.call_agent correctly passes metadata to the emitted command."""
    mock_redis = AsyncMock()
    ctx = AgentContext(session_id="s1", trace_id="t1", redis_client=mock_redis)
    await ctx.call_agent(
        target_agent_type="test", content="hello", metadata={"ctx": "val"}
    )
    args, _ = mock_redis.xadd.call_args
    data = json.loads(args[1]["data"])
    command = command_from_dict(data)
    assert command.header.metadata == {"ctx": "val"}


@pytest.mark.asyncio
async def test_context_call_agent_emits_message_decodable_as_command():
    """Test that call_agent emits an AskAgentCommand that can be decoded from Redis payload."""
    mock_redis = AsyncMock()
    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        current_message_id="msg-parent",
    )

    await ctx.call_agent(
        target_agent_type="agent-b",
        content="hello",
        payload={"history": ["m1"]},
        wait_for_reply=True,
    )

    args, _ = mock_redis.xadd.call_args
    raw = json.loads(args[1]["data"])
    command = command_from_dict(raw)

    assert isinstance(command, AskAgentCommand)
    assert command.content == "hello"
    assert command.wait_for_reply is True
    assert command.extra_payload["history"] == ["m1"]


def test_context_reports_no_cancel_by_default():
    """Test that is_cancel_requested returns False when no cancel event is set."""
    ctx = AgentContext(session_id="s1", trace_id="t1")
    assert ctx.is_cancel_requested() is False


@pytest.mark.asyncio
async def test_context_check_cancelled_raises_when_event_set():
    """Test that check_cancelled raises CancelledError when cancel event is set."""
    event = asyncio.Event()
    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        cancel_event=event,
        cancel_reason="user aborted",
    )
    event.set()

    with pytest.raises(asyncio.CancelledError):
        await ctx.check_cancelled()
