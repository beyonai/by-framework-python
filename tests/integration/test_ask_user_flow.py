from unittest.mock import AsyncMock, MagicMock

import pytest

from byclaw_gateway_sdk import AgentContext, AskUserEvent, GatewayWorker
from byclaw_gateway_sdk.core.protocol.commands import (AskAgentCommand, ResumeCommand)
from byclaw_gateway_sdk.core.protocol.message_header import MessageHeader


class DummyWorker(GatewayWorker):

    def get_capabilities(self) -> list[str]:
        return ["dummy"]

    async def process_command(self, command, context: AgentContext) -> dict:
        if isinstance(command, ResumeCommand):
            return {"status": "resumed_from_user"}
        if isinstance(command, AskAgentCommand) and command.content == "ask user":
            return await context.ask_user(AskUserEvent(prompt="Prompt for user"))
        return {"status": "done"}


@pytest.fixture
def mock_redis():
    mock = MagicMock()
    mock.xadd = AsyncMock()
    mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    return mock


@pytest.mark.asyncio
async def test_ask_user_flow(mock_redis):
    """Test that worker emits AskUserEvent when context.ask_user is called."""
    workspace_manager = MagicMock()
    workspace_manager.setup_workspace = AsyncMock(
        return_value={"private": "/tmp", "public": "/tmp"}
    )
    workspace_manager.cleanup_task = AsyncMock()
    worker = DummyWorker(
        worker_id="dummy-1",
        redis_client=mock_redis,
        registry=MagicMock(),
        workspace_manager=workspace_manager,
    )

    # Simulate a brand new message asking to trigger user prompt
    request_msg = AskAgentCommand(
        header=MessageHeader(
            message_id="msg-1",
            session_id="session-1",
            trace_id="trace-1",
            tenant_id="tenant-1",
            target_agent_type="dummy",
        ),
        content="ask user",
    )

    # Process the message
    result = await worker._handle_message(request_msg)
    assert result == "COMPLETED"

    # Verify that an ASK_USER event was emitted
    pipe = mock_redis.pipeline.return_value
    payloads = [call.args[1]["data"] for call in pipe.xadd.call_args_list]
    assert any("reasoningLogDelta" in payload for payload in payloads)
    assert any("Prompt for user" in payload for payload in payloads)


@pytest.mark.asyncio
async def test_ask_user_return_flow(mock_redis):
    """Test that worker handles user reply via ResumeCommand correctly."""
    workspace_manager = MagicMock()
    workspace_manager.setup_workspace = AsyncMock(
        return_value={"private": "/tmp", "public": "/tmp"}
    )
    workspace_manager.cleanup_task = AsyncMock()
    worker = DummyWorker(
        worker_id="dummy-1",
        redis_client=mock_redis,
        registry=MagicMock(),
        workspace_manager=workspace_manager,
    )

    # Simulate an incoming reply from the user
    reply_msg = ResumeCommand(
        header=MessageHeader(
            message_id="msg-2",
            session_id="session-1",
            trace_id="trace-1",
            parent_message_id="trace-1",
            tenant_id="tenant-1",
            target_agent_type="dummy",
        ),
        content="Pink",
    )

    result = await worker._handle_message(reply_msg)
    assert result == "COMPLETED"
