import json
from dataclasses import asdict
from unittest.mock import AsyncMock

import pytest

from by_framework import ByaiGatewayClient, GatewayClient
from by_framework.common.constants import RedisKeys
from by_framework.core.protocol.commands import (
    AskAgentCommand,
    CancelTaskCommand,
    ReloadPluginsCommand,
    command_from_dict,
)
from by_framework.core.protocol.message import (BaiYingMessage, BaiYingMessageRole)


@pytest.mark.asyncio
async def test_client_send_message_with_target_worker_id():
    """Test that send_message routes to worker control stream when
    target_worker_id is provided."""
    mock_redis = AsyncMock()
    mock_registry = AsyncMock()

    client = GatewayClient(redis_client=mock_redis, registry=mock_registry)
    await client.send_message(
        target_agent_type="langgraph_agent",
        session_id="s1",
        content="hello",
        target_worker_id="worker-42",
    )

    # Should NOT call registry.get_target_worker
    mock_registry.get_target_worker.assert_not_called()

    # Should route to worker-specific stream
    args, _ = mock_redis.xadd.call_args
    assert args[0].endswith("ctrl:worker:worker-42")

    data = json.loads(args[1]["data"])
    command = command_from_dict(data)
    assert isinstance(command, AskAgentCommand)
    assert command.header.target_agent_type == "langgraph_agent"


@pytest.mark.asyncio
async def test_resolve_direct_worker_route():
    """Test that direct worker routing returns an explicit worker stream
    and worker id."""
    mock_registry = AsyncMock()
    mock_registry.is_worker_online.return_value = True
    client = GatewayClient(redis_client=AsyncMock(), registry=mock_registry)

    route = await client._resolve_direct_worker_route(
        "worker-42",
        require_online_worker=True,
    )

    assert asdict(route) == {
        "stream_name": "byai_gateway:ctrl:worker:worker-42",
        "target_worker_id": "worker-42",
    }


@pytest.mark.asyncio
async def test_resolve_agent_type_route():
    """Test that agent-type routing returns an agent-type stream
    without binding a worker."""
    mock_registry = AsyncMock()
    mock_registry.has_online_agent_type.return_value = (True, ["worker-1"])
    client = GatewayClient(redis_client=AsyncMock(), registry=mock_registry)

    route = await client._resolve_agent_type_route(
        "langgraph_agent",
        require_online_worker=True,
    )

    assert asdict(route) == {
        "stream_name": "byai_gateway:ctrl:agent_type:langgraph_agent",
        "target_worker_id": "",
    }


@pytest.mark.asyncio
async def test_client_send_message_with_metadata():
    """Test that ByaiGatewayClient.send_message correctly passes metadata
    to the command."""
    mock_redis = AsyncMock()
    mock_registry = AsyncMock()
    mock_registry.has_online_agent_type.return_value = (True, ["worker-1"])

    # Test with ByaiGatewayClient
    client = ByaiGatewayClient(redis_client=mock_redis, registry=mock_registry)
    await client.send_message(
        target_agent_type="test",
        session_id="s1",
        user_code="t1",
        user_name="user1",
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
async def test_byai_client_send_message_serializes_baiying_message():
    """Test that ByaiGatewayClient serializes BaiYingMessage to protocol wire format."""
    mock_redis = AsyncMock()
    mock_registry = AsyncMock()
    mock_registry.has_online_agent_type.return_value = (True, ["worker-1"])

    client = ByaiGatewayClient(redis_client=mock_redis, registry=mock_registry)
    await client.send_message(
        target_agent_type="test",
        session_id="s1",
        content=BaiYingMessage(role=BaiYingMessageRole.USER, content="hello"),
    )

    args, _ = mock_redis.xadd.call_args
    data = json.loads(args[1]["data"])
    command = command_from_dict(data)

    assert isinstance(command, AskAgentCommand)
    assert command.content == [{"role": "user", "content": "hello"}]


@pytest.mark.asyncio
async def test_client_cancel_task_routes_to_worker_control_stream():
    """Test that cancel_task routes a CancelTaskCommand to the worker control stream."""
    root_exec = {
        "execution_id": "exec-1",
        "message_id": "msg-1",
        "session_id": "sess-1",
        "worker_id": "worker-1",
        "target_agent_type": "langgraph_agent",
        "status": "RUNNING",
        "parent_message_id": "",
    }
    registry = AsyncMock()
    registry.get_execution_by_message_id.return_value = root_exec
    registry.get_all_session_executions.return_value = [root_exec]
    redis = AsyncMock()
    client = GatewayClient(registry=registry, redis_client=redis)

    result = await client.cancel_task(
        message_id="msg-1", session_id="sess-1", reason="user aborted"
    )

    assert result.success is True
    assert result.execution_id == "exec-1"
    assert result.worker_id == "worker-1"
    assert result.status == "CANCEL_REQUESTED"
    assert result.cancelled_count == 1

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
async def test_client_send_message_require_online_worker_no_worker():
    """Test that send_message returns FAILED when require_online_worker=True
    and no worker exists."""
    mock_redis = AsyncMock()
    mock_registry = AsyncMock()
    mock_registry.has_online_agent_type.return_value = (False, [])

    client = GatewayClient(redis_client=mock_redis, registry=mock_registry)
    result = await client.send_message(
        target_agent_type="nonexistent_agent",
        session_id="s1",
        content="hello",
        require_online_worker=True,
    )

    assert result.success is False
    assert result.status == "FAILED"
    assert result.error_code == "AGENT_TYPE_UNAVAILABLE"
    # Should NOT call xadd (message not sent)
    mock_redis.xadd.assert_not_called()
    # Should call has_online_agent_type
    mock_registry.has_online_agent_type.assert_called_once_with("nonexistent_agent")


@pytest.mark.asyncio
async def test_client_send_message_require_online_worker_with_worker():
    """Test that send_message sends normally when require_online_worker=True
    and worker exists."""
    mock_redis = AsyncMock()
    mock_registry = AsyncMock()
    mock_registry.has_online_agent_type.return_value = (True, ["worker-1"])

    client = GatewayClient(redis_client=mock_redis, registry=mock_registry)
    result = await client.send_message(
        target_agent_type="test_agent",
        session_id="s1",
        content="hello",
        require_online_worker=True,
    )

    assert result.success is True
    assert result.status == "QUEUED"
    assert result.target_worker_id == ""
    # Should call xadd (message sent)
    mock_redis.xadd.assert_called_once()
    mock_registry.has_online_agent_type.assert_called_once_with("test_agent")
    mock_registry.get_target_worker.assert_not_called()


@pytest.mark.asyncio
async def test_client_send_message_no_probe():
    """Test that send_message sends to agent-type stream directly when
    require_online_worker=False."""
    mock_redis = AsyncMock()
    mock_registry = AsyncMock()

    client = GatewayClient(redis_client=mock_redis, registry=mock_registry)
    result = await client.send_message(
        target_agent_type="any_agent",
        session_id="s1",
        content="hello",
        require_online_worker=False,
    )

    assert result.success is True
    assert result.status == "QUEUED"
    # Should NOT call has_online_agent_type
    mock_registry.has_online_agent_type.assert_not_called()
    # Should route to agent-type stream (not worker-specific)
    args, _ = mock_redis.xadd.call_args
    assert args[0].endswith("ctrl:agent_type:any_agent")


@pytest.mark.asyncio
async def test_client_send_message_target_worker_id_skips_probe():
    """Test that target_worker_id skips agent-type lookup but checks worker liveness."""
    mock_redis = AsyncMock()
    mock_registry = AsyncMock()
    mock_registry.is_worker_online.return_value = True

    client = GatewayClient(redis_client=mock_redis, registry=mock_registry)
    result = await client.send_message(
        target_agent_type="any_agent",
        session_id="s1",
        content="hello",
        target_worker_id="worker-42",
        require_online_worker=True,
    )

    assert result.success is True
    # Should NOT call has_online_agent_type (skips agent-type probe)
    mock_registry.has_online_agent_type.assert_not_called()
    # Should check if target worker is online
    mock_registry.is_worker_online.assert_called_once_with("worker-42")
    # Should route to worker-specific stream
    args, _ = mock_redis.xadd.call_args
    assert args[0].endswith("ctrl:worker:worker-42")


@pytest.mark.asyncio
async def test_client_send_message_target_worker_id_dead():
    """Test that send_message returns FAILED when target_worker_id is not online."""
    mock_redis = AsyncMock()
    mock_registry = AsyncMock()
    mock_registry.is_worker_online.return_value = False

    client = GatewayClient(redis_client=mock_redis, registry=mock_registry)
    result = await client.send_message(
        target_agent_type="any_agent",
        session_id="s1",
        content="hello",
        target_worker_id="dead_worker",
        require_online_worker=True,
    )

    assert result.success is False
    assert result.status == "FAILED"
    assert result.target_worker_id == "dead_worker"
    assert result.error_code == "WORKER_NOT_ONLINE"
    # Should NOT call xadd (message not sent)
    mock_redis.xadd.assert_not_called()


@pytest.mark.asyncio
async def test_client_cancel_task_returns_already_finished():
    """Test that cancel_task returns ALREADY_FINISHED when execution is
    already cancelled."""
    root_exec = {
        "execution_id": "exec-1",
        "message_id": "msg-1",
        "session_id": "sess-1",
        "worker_id": "worker-1",
        "target_agent_type": "langgraph_agent",
        "status": "CANCELLED",
        "parent_message_id": "",
    }
    registry = AsyncMock()
    registry.get_execution_by_message_id.return_value = root_exec
    registry.get_all_session_executions.return_value = [root_exec]
    client = GatewayClient(registry=registry, redis_client=AsyncMock())

    result = await client.cancel_task(message_id="msg-1", session_id="sess-1")

    assert result.success is False
    assert result.status == "ALREADY_FINISHED"


@pytest.mark.asyncio
async def test_client_cancel_task_marks_registry_when_worker_not_assigned():
    """Test that cancel_task succeeds and marks registry when execution
    exists but has no assigned worker yet."""
    root_exec = {
        "execution_id": "exec-1",
        "message_id": "msg-1",
        "session_id": "sess-1",
        "worker_id": "",
        "target_agent_type": "langgraph_agent",
        "status": "QUEUED",
        "parent_message_id": "",
    }
    registry = AsyncMock()
    registry.get_execution_by_message_id.return_value = root_exec
    registry.get_all_session_executions.return_value = [root_exec]
    redis = AsyncMock()
    client = GatewayClient(registry=registry, redis_client=redis)

    result = await client.cancel_task(message_id="msg-1", session_id="sess-1")

    assert result.success is True
    assert result.status == "CANCEL_REQUESTED"
    # It should mark the execution as cancelling in the registry
    registry.mark_execution_cancelling.assert_called_once_with("exec-1", "sess-1", "")
    # It should NOT send a control message to any worker stream
    redis.xadd.assert_not_called()


@pytest.mark.asyncio
async def test_client_reload_plugins_for_agent_type_fans_out_to_all_online_workers():
    mock_redis = AsyncMock()
    mock_registry = AsyncMock()
    mock_registry.has_online_agent_type.return_value = (
        True,
        ["worker-1", "worker-2"],
    )

    client = GatewayClient(redis_client=mock_redis, registry=mock_registry)
    result = await client.reload_plugins_for_agent_type(
        agent_type="langgraph_agent",
        reason="refresh plugins",
        reload_id="reload-broadcast-1",
    )

    assert result["reload_id"] == "reload-broadcast-1"
    assert result["agent_type"] == "langgraph_agent"
    assert result["worker_ids"] == ["worker-1", "worker-2"]
    assert result["dispatched_count"] == 2
    assert mock_redis.xadd.await_count == 2

    streams = [call.args[0] for call in mock_redis.xadd.await_args_list]
    assert RedisKeys.worker_ctrl_stream("worker-1") in streams
    assert RedisKeys.worker_ctrl_stream("worker-2") in streams

    payloads = [
        json.loads(call.args[1]["data"]) for call in mock_redis.xadd.await_args_list
    ]
    commands = [command_from_dict(payload) for payload in payloads]
    assert all(isinstance(command, ReloadPluginsCommand) for command in commands)
    assert {command.reload_id for command in commands} == {"reload-broadcast-1"}


@pytest.mark.asyncio
async def test_client_reload_plugins_for_agent_type_returns_empty_when_no_workers():
    mock_redis = AsyncMock()
    mock_registry = AsyncMock()
    mock_registry.has_online_agent_type.return_value = (False, [])

    client = GatewayClient(redis_client=mock_redis, registry=mock_registry)
    result = await client.reload_plugins_for_agent_type(
        agent_type="missing_agent",
        reason="refresh plugins",
        reload_id="reload-broadcast-2",
    )

    assert result["worker_ids"] == []
    assert result["dispatched_count"] == 0
    mock_redis.xadd.assert_not_called()


@pytest.mark.asyncio
async def test_client_collect_reload_acks_reads_ack_stream():
    mock_redis = AsyncMock()
    mock_redis.xread.return_value = [
        [
            RedisKeys.plugin_reload_ack_stream("reload-acks-1").encode(),
            [
                (
                    b"1-0",
                    {
                        b"data": json.dumps(
                            {
                                "reload_id": "reload-acks-1",
                                "worker_id": "worker-1",
                                "status": "success",
                                "version_before": 1,
                                "version_after": 2,
                                "error": "",
                            }
                        ).encode()
                    },
                ),
                (
                    b"2-0",
                    {
                        b"data": json.dumps(
                            {
                                "reload_id": "reload-acks-1",
                                "worker_id": "worker-2",
                                "status": "failure",
                                "version_before": 1,
                                "version_after": 1,
                                "error": "boom",
                            }
                        ).encode()
                    },
                ),
            ],
        ]
    ]

    client = GatewayClient(redis_client=mock_redis, registry=AsyncMock())
    results = await client.collect_reload_acks("reload-acks-1", block_ms=10)

    assert len(results) == 2
    assert results[0]["worker_id"] == "worker-1"
    assert results[0]["status"] == "success"
    assert results[1]["worker_id"] == "worker-2"
    assert results[1]["error"] == "boom"


@pytest.mark.asyncio
async def test_client_cascading_cancel_a_b_c():
    """Test that cancel_task cascades cancellation through A -> B -> C chain."""
    exec_a = {
        "execution_id": "exec-a",
        "message_id": "msg-a",
        "session_id": "sess-1",
        "worker_id": "worker-1",
        "target_agent_type": "agent_a",
        "status": "RUNNING",
        "parent_message_id": "",
    }
    exec_b = {
        "execution_id": "exec-b",
        "message_id": "msg-b",
        "session_id": "sess-1",
        "worker_id": "worker-2",
        "target_agent_type": "agent_b",
        "status": "RUNNING",
        "parent_message_id": "msg-a",
    }
    exec_c = {
        "execution_id": "exec-c",
        "message_id": "msg-c",
        "session_id": "sess-1",
        "worker_id": "worker-3",
        "target_agent_type": "agent_c",
        "status": "RUNNING",
        "parent_message_id": "msg-b",
    }
    exec_done = {
        "execution_id": "exec-done",
        "message_id": "msg-done",
        "session_id": "sess-1",
        "worker_id": "worker-4",
        "target_agent_type": "agent_d",
        "status": "COMPLETED",
        "parent_message_id": "msg-a",
    }

    registry = AsyncMock()
    registry.get_execution_by_message_id.return_value = exec_a
    registry.get_all_session_executions.return_value = [
        exec_a,
        exec_b,
        exec_c,
        exec_done,
    ]
    redis = AsyncMock()
    client = GatewayClient(registry=registry, redis_client=redis)

    result = await client.cancel_task(
        message_id="msg-a", session_id="sess-1", reason="cascade test"
    )

    # Should succeed
    assert result.success is True
    assert result.status == "CANCEL_REQUESTED"
    # Should cancel A, B, C but NOT exec_done (already COMPLETED)
    assert result.cancelled_count == 3

    # Should have marked 3 executions as cancelling
    assert registry.mark_execution_cancelling.call_count == 3
    cancel_ids = [
        call.args[0] for call in registry.mark_execution_cancelling.call_args_list
    ]
    assert "exec-a" in cancel_ids
    assert "exec-b" in cancel_ids
    assert "exec-c" in cancel_ids
    assert "exec-done" not in cancel_ids

    # Should have sent CancelTaskCommand to 3 workers
    assert redis.xadd.call_count == 3
    target_streams = [call.args[0] for call in redis.xadd.call_args_list]
    assert any("worker-1" in s for s in target_streams)
    assert any("worker-2" in s for s in target_streams)
    assert any("worker-3" in s for s in target_streams)


@pytest.mark.asyncio
async def test_client_cascading_cancel_root_completed_children_running():
    """Test cascading cancel when root A is COMPLETED (suspended) but B->C
    are still RUNNING.

    Scenario: A calls B, B calls C. A's execution finishes with COMPLETED,
    but B and C are still actively running on other workers.
    Cancelling A should still cascade and cancel B and C.
    """
    exec_a = {
        "execution_id": "exec-a",
        "message_id": "msg-a",
        "session_id": "sess-1",
        "worker_id": "worker-1",
        "target_agent_type": "orchestrator",
        "status": "COMPLETED",
        "parent_message_id": "",
    }
    exec_b = {
        "execution_id": "exec-b",
        "message_id": "msg-b",
        "session_id": "sess-1",
        "worker_id": "worker-2",
        "target_agent_type": "coder",
        "status": "RUNNING",
        "parent_message_id": "msg-a",
    }
    exec_c = {
        "execution_id": "exec-c",
        "message_id": "msg-c",
        "session_id": "sess-1",
        "worker_id": "worker-3",
        "target_agent_type": "reviewer",
        "status": "RUNNING",
        "parent_message_id": "msg-b",
    }

    registry = AsyncMock()
    registry.get_execution_by_message_id.return_value = exec_a
    registry.get_all_session_executions.return_value = [exec_a, exec_b, exec_c]
    redis = AsyncMock()
    client = GatewayClient(registry=registry, redis_client=redis)

    result = await client.cancel_task(
        message_id="msg-a", session_id="sess-1", reason="user abort"
    )

    # Should succeed even though root A is COMPLETED
    assert result.success is True
    assert result.status == "CANCEL_REQUESTED"
    # Should cancel B and C only (A is already COMPLETED)
    assert result.cancelled_count == 2

    # Should have marked B and C as cancelling
    assert registry.mark_execution_cancelling.call_count == 2
    cancel_ids = [
        call.args[0] for call in registry.mark_execution_cancelling.call_args_list
    ]
    assert "exec-b" in cancel_ids
    assert "exec-c" in cancel_ids
    assert "exec-a" not in cancel_ids

    # Should have sent CancelTaskCommand to worker-2 and worker-3
    assert redis.xadd.call_count == 2
    target_streams = [call.args[0] for call in redis.xadd.call_args_list]
    assert any("worker-2" in s for s in target_streams)
    assert any("worker-3" in s for s in target_streams)

    # Should have marked A (COMPLETED) with cancel_requested flag
    registry.mark_cancel_requested.assert_called_once_with(
        "exec-a", "sess-1", "user abort"
    )
