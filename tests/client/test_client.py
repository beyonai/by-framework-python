import json
from unittest.mock import AsyncMock

import pytest

from byclaw_gateway_sdk import ByaiGatewayClient, GatewayClient
from byclaw_gateway_sdk.core.protocol.commands import (
    AskAgentCommand,
    CancelTaskCommand,
    command_from_dict,
)


@pytest.mark.asyncio
async def test_client_send_message_with_metadata():
    """Test that ByaiGatewayClient.send_message correctly passes metadata to the command."""
    mock_redis = AsyncMock()
    mock_registry = AsyncMock()
    mock_registry.get_target_worker.return_value = "worker-1"

    # Test with ByaiGatewayClient
    client = ByaiGatewayClient(redis_client=mock_redis, registry=mock_registry)
    await client.send_message(
        target_agent_type="test",
        session_id="s1",
        tenant_id="t1",
        content="hello",
        metadata={"k": "v"},
    )

    args, _ = mock_redis.xadd.call_args
    data = json.loads(args[1]["data"])
    command = command_from_dict(data)

    assert isinstance(command, AskAgentCommand)
    assert command.header.metadata == {"k": "v"}
    assert command.content == "hello"


@pytest.mark.asyncio
async def test_client_cancel_task_routes_to_worker_control_stream():
    """Test that cancel_task routes a CancelTaskCommand to the worker control stream."""
    registry = AsyncMock()
    registry.get_execution_by_message_id.return_value = {
        "execution_id": "exec-1",
        "message_id": "msg-1",
        "session_id": "sess-1",
        "worker_id": "worker-1",
        "target_agent_type": "langgraph_agent",
        "status": "RUNNING",
    }
    redis = AsyncMock()
    client = GatewayClient(registry=registry, redis_client=redis)

    result = await client.cancel_task(
        message_id="msg-1", session_id="sess-1", reason="user aborted"
    )

    assert result.success is True
    assert result.execution_id == "exec-1"
    assert result.worker_id == "worker-1"
    assert result.status == "CANCEL_REQUESTED"

    args, _ = redis.xadd.call_args
    assert args[0].endswith("ctrl:worker:worker-1")
    raw = json.loads(args[1]["data"])
    command = command_from_dict(raw)
    assert isinstance(command, CancelTaskCommand)
    assert command.target_message_id == "msg-1"


@pytest.mark.asyncio
async def test_client_cancel_task_returns_not_found():
    """Test that cancel_task returns NOT_FOUND when execution does not exist."""
    registry = AsyncMock()
    registry.get_execution_by_message_id.return_value = None
    client = GatewayClient(registry=registry, redis_client=AsyncMock())

    result = await client.cancel_task(message_id="missing", session_id="sess-1")

    assert result.success is False
    assert result.status == "NOT_FOUND"


@pytest.mark.asyncio
async def test_client_cancel_task_returns_already_finished():
    """Test that cancel_task returns ALREADY_FINISHED when execution is already cancelled."""
    registry = AsyncMock()
    registry.get_execution_by_message_id.return_value = {
        "execution_id": "exec-1",
        "message_id": "msg-1",
        "session_id": "sess-1",
        "worker_id": "worker-1",
        "target_agent_type": "langgraph_agent",
        "status": "CANCELLED",
    }
    client = GatewayClient(registry=registry, redis_client=AsyncMock())

    result = await client.cancel_task(message_id="msg-1", session_id="sess-1")

    assert result.success is False
    assert result.status == "ALREADY_FINISHED"
