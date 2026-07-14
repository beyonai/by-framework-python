import json
from dataclasses import asdict
from unittest.mock import AsyncMock

import pytest

from by_framework import ByaiGatewayClient, DataStreamEntry, GatewayClient
from by_framework.common.constants import RedisKeys
from by_framework.core.availability import RoutePolicy
from by_framework.core.protocol.commands import (
    AskAgentCommand,
    CancelTaskCommand,
    ReloadPluginsCommand,
    command_from_dict,
)
from by_framework.core.protocol.data_message import DataMessage
from by_framework.core.protocol.message import (BaiYingMessage, BaiYingMessageRole)
from by_framework.errors import WorkerRegistryNotSetError
from by_framework.trace.span_recorder import str_to_uint64


class FakeObservation:
    """Observation double used for client-side Langfuse dispatch tests."""

    id = "obs-client-dispatch"

    def __init__(self):
        self.ended_with = None

    def end(self, **kwargs):
        self.ended_with = kwargs


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
async def test_client_send_message_writes_langfuse_parent_to_header_metadata():
    """Client-side Langfuse dispatch root is propagated to workers via metadata."""
    mock_redis = AsyncMock()
    mock_registry = AsyncMock()
    observation = FakeObservation()

    client = GatewayClient(redis_client=mock_redis, registry=mock_registry)
    client._start_langfuse_client_dispatch_observation = (  # pylint: disable=protected-access
        lambda **kwargs: observation
    )

    await client.send_message(
        target_agent_type="langgraph_agent",
        session_id="s1",
        content="hello",
        target_worker_id="worker-42",
        message_id="msg-client",
        trace_id="trace-client",
        metadata={"request_id": "req-1"},
    )

    args, _ = mock_redis.xadd.call_args
    command = command_from_dict(json.loads(args[1]["data"]))

    assert command.header.metadata == {
        "request_id": "req-1",
    }
    assert command.header.langfuse_parent_observation_id == "obs-client-dispatch"
    expected_span_id = "msg-client:client.dispatch"
    assert command.header.trace_parent_span_id == (
        f"{str_to_uint64(expected_span_id):016x}"
    )
    assert observation.ended_with["output"]["success"] is True
    assert observation.ended_with["output"]["message_id"] == "msg-client"


@pytest.mark.asyncio
async def test_client_send_message_records_client_dispatch_span():
    """Successful client dispatch writes a trace span for end-to-end visibility."""
    mock_redis = AsyncMock()
    mock_registry = AsyncMock()
    span_recorder = AsyncMock()

    client = GatewayClient(
        redis_client=mock_redis,
        registry=mock_registry,
        span_recorder=span_recorder,
    )
    await client.send_message(
        target_agent_type="langgraph_agent",
        session_id="s1",
        content="hello",
        target_worker_id="worker-42",
        message_id="msg-client",
        trace_id="trace-client",
    )

    span_recorder.record_span.assert_awaited_once()
    span = span_recorder.record_span.await_args.args[0]
    assert span.trace_id == "trace-client"
    assert span.span_id == "msg-client:client.dispatch"
    assert span.operation == "client.dispatch"
    assert span.component == "client"
    assert span.session_id == "s1"
    assert span.message_id == "msg-client"
    assert span.target_agent_type == "langgraph_agent"
    assert span.status == "COMPLETED"


@pytest.mark.asyncio
async def test_resolve_direct_worker_route():
    """Test that direct worker routing returns an explicit worker stream
    and worker id."""
    mock_registry = AsyncMock()
    mock_registry.is_worker_online.return_value = True
    client = GatewayClient(redis_client=AsyncMock(), registry=mock_registry)

    route = await client._resolve_direct_worker_route(
        "worker-42",
        check_online=True,
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
        route_policy=RoutePolicy.FAIL_FAST,
    )

    assert asdict(route) == {
        "stream_name": "byai_gateway:ctrl:agent_type:langgraph_agent",
        "target_worker_id": "",
    }


@pytest.mark.asyncio
async def test_client_get_worker_status_delegates_to_registry():
    """Test client worker status lookup delegates to WorkerRegistry."""
    mock_registry = AsyncMock()
    mock_registry.get_worker_execution_summary.return_value = {
        "worker_id": "worker-1",
        "active_count": 1,
    }
    client = GatewayClient(redis_client=AsyncMock(), registry=mock_registry)

    result = await client.get_worker_status("worker-1")

    assert result == {"worker_id": "worker-1", "active_count": 1}
    mock_registry.get_worker_execution_summary.assert_awaited_once_with("worker-1")


@pytest.mark.asyncio
async def test_client_get_worker_status_requires_registry():
    """Test client worker status lookup requires a registry."""
    client = GatewayClient(redis_client=AsyncMock(), registry=None)

    with pytest.raises(WorkerRegistryNotSetError):
        await client.get_worker_status("worker-1")


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
        "trace_id": "trace-original",
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
    assert command.header.trace_id == root_exec["trace_id"]


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
async def test_client_send_message_fail_fast_no_worker():
    """Test that send_message returns FAILED with FAIL_FAST
    and no worker exists."""
    mock_redis = AsyncMock()
    mock_registry = AsyncMock()
    mock_registry.has_online_agent_type.return_value = (False, [])

    client = GatewayClient(redis_client=mock_redis, registry=mock_registry)
    result = await client.send_message(
        target_agent_type="nonexistent_agent",
        session_id="s1",
        content="hello",
    )

    assert result.success is False
    assert result.status == "FAILED"
    assert result.error_code == "AGENT_TYPE_UNAVAILABLE"
    # Should NOT call xadd (message not sent)
    mock_redis.xadd.assert_not_called()
    # Should call has_online_agent_type
    mock_registry.has_online_agent_type.assert_called_once_with("nonexistent_agent")
    # Route failures should be recorded for observability.
    mock_registry.record_failed_route_decision.assert_awaited_once()
    call_kwargs = mock_registry.record_failed_route_decision.await_args.kwargs
    assert call_kwargs["route_policy"] == RoutePolicy.FAIL_FAST
    assert call_kwargs["route_status"] == "REJECT"
    assert call_kwargs["availability_error_code"] == "AGENT_TYPE_UNAVAILABLE"
    assert call_kwargs["source_agent_type"] == "client"


@pytest.mark.asyncio
async def test_client_send_message_fail_fast_with_worker():
    """Test that send_message sends normally with FAIL_FAST
    and worker exists."""
    mock_redis = AsyncMock()
    mock_registry = AsyncMock()
    mock_registry.has_online_agent_type.return_value = (True, ["worker-1"])

    client = GatewayClient(redis_client=mock_redis, registry=mock_registry)
    result = await client.send_message(
        target_agent_type="test_agent",
        session_id="s1",
        content="hello",
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
    route_policy=SEND_ANYWAY."""
    mock_redis = AsyncMock()
    mock_registry = AsyncMock()

    client = GatewayClient(redis_client=mock_redis, registry=mock_registry)
    result = await client.send_message(
        target_agent_type="any_agent",
        session_id="s1",
        content="hello",
        route_policy=RoutePolicy.SEND_ANYWAY,
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
async def test_client_read_data_messages_decodes_session_stream():
    mock_redis = AsyncMock()
    mock_redis.xread.return_value = [
        [
            RedisKeys.session_data_stream("sess-1").encode(),
            [
                (
                    b"1-0",
                    {
                        b"data": json.dumps(
                            {
                                "trace_id": "trace-1",
                                "session_id": "sess-1",
                                "event_type": "answerDelta",
                                "source_agent_type": "agent-a",
                                "message_id": "msg-1",
                                "parent_message_id": "",
                                "timestamp": 123,
                                "data": {"content": "hello"},
                                "state_msg": "",
                                "artifact_url": "",
                                "metadata": {"k": "v"},
                                "future_field": "ignored",
                            }
                        ).encode()
                    },
                )
            ],
        ]
    ]

    client = GatewayClient(redis_client=mock_redis, registry=AsyncMock())
    entries = await client.read_data_messages(
        session_id="sess-1",
        last_id="0-0",
        block_ms=10,
        count=50,
    )

    assert entries == [
        DataStreamEntry(
            stream_id="1-0",
            message=DataMessage(
                trace_id="trace-1",
                session_id="sess-1",
                event_type="answerDelta",
                source_agent_type="agent-a",
                message_id="msg-1",
                parent_message_id="",
                timestamp=123,
                data={"content": "hello"},
                metadata={"k": "v"},
            ),
        )
    ]
    mock_redis.xread.assert_awaited_once_with(
        streams={RedisKeys.session_data_stream("sess-1"): "0-0"},
        count=50,
        block=10,
    )


@pytest.mark.asyncio
async def test_client_iter_data_messages_advances_last_id():
    mock_redis = AsyncMock()
    mock_redis.xread.side_effect = [
        [
            [
                RedisKeys.session_data_stream("sess-1"),
                [
                    (
                        "1-0",
                        {
                            "data": json.dumps(
                                {
                                    "trace_id": "trace-1",
                                    "session_id": "sess-1",
                                    "event_type": "answerDelta",
                                    "data": {"content": "first"},
                                }
                            )
                        },
                    )
                ],
            ]
        ],
        [
            [
                RedisKeys.session_data_stream("sess-1"),
                [
                    (
                        "2-0",
                        {
                            "data": json.dumps(
                                {
                                    "trace_id": "trace-1",
                                    "session_id": "sess-1",
                                    "event_type": "answerDelta",
                                    "data": {"content": "second"},
                                }
                            )
                        },
                    )
                ],
            ]
        ],
    ]

    client = GatewayClient(redis_client=mock_redis, registry=AsyncMock())
    iterator = client.iter_data_messages(
        session_id="sess-1",
        last_id="0-0",
        block_ms=1,
        count=1,
    )

    first = await iterator.__anext__()
    second = await iterator.__anext__()
    await iterator.aclose()

    assert first.stream_id == "1-0"
    assert first.message.data == {"content": "first"}
    assert second.stream_id == "2-0"
    assert second.message.data == {"content": "second"}
    assert mock_redis.xread.await_args_list[0].kwargs["streams"] == {
        RedisKeys.session_data_stream("sess-1"): "0-0"
    }
    assert mock_redis.xread.await_args_list[1].kwargs["streams"] == {
        RedisKeys.session_data_stream("sess-1"): "1-0"
    }


@pytest.mark.asyncio
async def test_client_data_message_checkpoint_defaults_to_start():
    mock_redis = AsyncMock()
    mock_redis.get.return_value = None

    client = GatewayClient(redis_client=mock_redis, registry=AsyncMock())
    checkpoint = await client.get_data_message_checkpoint(
        session_id="sess-1",
        consumer_name="frontend",
    )

    assert checkpoint == "0-0"
    assert (
        RedisKeys.session_data_checkpoint("sess-1", "frontend")
        == "byai_gateway:session:sess-1:consumer:frontend:checkpoint"
    )
    mock_redis.get.assert_awaited_once_with(
        RedisKeys.session_data_checkpoint("sess-1", "frontend")
    )


@pytest.mark.asyncio
async def test_client_commit_data_message_persists_checkpoint():
    mock_redis = AsyncMock()

    client = GatewayClient(redis_client=mock_redis, registry=AsyncMock())
    await client.commit_data_message(
        session_id="sess-1",
        stream_id="2-0",
        consumer_name="frontend",
    )

    mock_redis.set.assert_awaited_once_with(
        RedisKeys.session_data_checkpoint("sess-1", "frontend"),
        "2-0",
        ex=RedisKeys.DEFAULT_SESSION_TTL,
    )


@pytest.mark.asyncio
async def test_client_read_data_messages_from_checkpoint_auto_commits_last_entry():
    mock_redis = AsyncMock()
    mock_redis.get.return_value = b"1-0"
    mock_redis.xread.return_value = [
        [
            RedisKeys.session_data_stream("sess-1"),
            [
                (
                    "2-0",
                    {
                        "data": json.dumps(
                            {
                                "trace_id": "trace-1",
                                "session_id": "sess-1",
                                "event_type": "answerDelta",
                                "data": {"content": "second"},
                            }
                        )
                    },
                ),
                (
                    "3-0",
                    {
                        "data": json.dumps(
                            {
                                "trace_id": "trace-1",
                                "session_id": "sess-1",
                                "event_type": "answerDelta",
                                "data": {"content": "third"},
                            }
                        )
                    },
                ),
            ],
        ]
    ]

    client = GatewayClient(redis_client=mock_redis, registry=AsyncMock())
    entries = await client.read_data_messages_from_checkpoint(
        session_id="sess-1",
        consumer_name="frontend",
        auto_commit=True,
    )

    assert [entry.stream_id for entry in entries] == ["2-0", "3-0"]
    mock_redis.xread.assert_awaited_once_with(
        streams={RedisKeys.session_data_stream("sess-1"): "1-0"},
        count=100,
        block=0,
    )
    mock_redis.set.assert_awaited_once_with(
        RedisKeys.session_data_checkpoint("sess-1", "frontend"),
        "3-0",
        ex=RedisKeys.DEFAULT_SESSION_TTL,
    )


@pytest.mark.asyncio
async def test_client_consume_data_messages_commits_after_each_processed_entry():
    mock_redis = AsyncMock()
    mock_redis.get.return_value = "0-0"
    mock_redis.xread.side_effect = [
        [
            [
                RedisKeys.session_data_stream("sess-1"),
                [
                    (
                        "1-0",
                        {
                            "data": json.dumps(
                                {
                                    "trace_id": "trace-1",
                                    "session_id": "sess-1",
                                    "event_type": "answerDelta",
                                    "data": {"content": "first"},
                                }
                            )
                        },
                    ),
                    (
                        "2-0",
                        {
                            "data": json.dumps(
                                {
                                    "trace_id": "trace-1",
                                    "session_id": "sess-1",
                                    "event_type": "answerDelta",
                                    "data": {"content": "second"},
                                }
                            )
                        },
                    ),
                ],
            ]
        ],
        [],
    ]

    client = GatewayClient(redis_client=mock_redis, registry=AsyncMock())
    iterator = client.consume_data_messages(
        session_id="sess-1",
        consumer_name="frontend",
        block_ms=1,
        count=2,
    )

    first = await iterator.__anext__()
    assert first.stream_id == "1-0"
    mock_redis.set.assert_not_awaited()

    second = await iterator.__anext__()
    assert second.stream_id == "2-0"
    mock_redis.set.assert_awaited_once_with(
        RedisKeys.session_data_checkpoint("sess-1", "frontend"),
        "1-0",
        ex=RedisKeys.DEFAULT_SESSION_TTL,
    )

    await iterator.aclose()
    assert mock_redis.set.await_count == 1


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


@pytest.mark.asyncio
async def test_client_cancel_session_cancels_all_non_terminal_executions():
    """Test that cancel_session cancels every non-terminal execution in a
    session in one call, without needing a starting message_id."""
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
        "status": "QUEUED",
        "parent_message_id": "msg-a",
    }

    registry = AsyncMock()
    registry.get_all_session_executions.return_value = [exec_a, exec_b]
    redis = AsyncMock()
    client = GatewayClient(registry=registry, redis_client=redis)

    result = await client.cancel_session(session_id="sess-1", reason="user abort")

    assert result.success is True
    assert result.status == "CANCEL_REQUESTED"
    assert result.cancelled_count == 2
    assert result.already_finished_count == 0

    assert registry.mark_execution_cancelling.call_count == 2
    cancel_ids = [
        call.args[0] for call in registry.mark_execution_cancelling.call_args_list
    ]
    assert "exec-a" in cancel_ids
    assert "exec-b" in cancel_ids

    assert redis.xadd.call_count == 2
    target_streams = [call.args[0] for call in redis.xadd.call_args_list]
    assert any("worker-1" in s for s in target_streams)
    assert any("worker-2" in s for s in target_streams)


@pytest.mark.asyncio
async def test_client_cancel_session_flags_terminal_executions_without_cancelling():
    """Terminal executions in the session should only be flagged with
    cancel_requested (not re-marked cancelling, no CancelTaskCommand sent),
    so a still-running descendant can't wake them back up via callback."""
    exec_a = {
        "execution_id": "exec-a",
        "message_id": "msg-a",
        "session_id": "sess-1",
        "worker_id": "worker-1",
        "target_agent_type": "agent_a",
        "status": "COMPLETED",
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

    registry = AsyncMock()
    registry.get_all_session_executions.return_value = [exec_a, exec_b]
    redis = AsyncMock()
    client = GatewayClient(registry=registry, redis_client=redis)

    result = await client.cancel_session(session_id="sess-1", reason="user abort")

    assert result.success is True
    assert result.status == "CANCEL_REQUESTED"
    assert result.cancelled_count == 1
    assert result.already_finished_count == 1

    # Only the non-terminal execution gets mark_execution_cancelling
    registry.mark_execution_cancelling.assert_called_once_with(
        "exec-b", "sess-1", "user abort"
    )
    # The terminal execution only gets flagged, not re-marked cancelling
    registry.mark_cancel_requested.assert_called_once_with(
        "exec-a", "sess-1", "user abort"
    )

    # Only one CancelTaskCommand sent, to the non-terminal execution's worker
    assert redis.xadd.call_count == 1
    target_streams = [call.args[0] for call in redis.xadd.call_args_list]
    assert any("worker-2" in s for s in target_streams)
    assert not any("worker-1" in s for s in target_streams)


@pytest.mark.asyncio
async def test_client_cancel_session_all_terminal_returns_already_finished():
    """If every execution in the session is already terminal, cancel_session
    should report failure with ALREADY_FINISHED and not dispatch anything."""
    exec_a = {
        "execution_id": "exec-a",
        "message_id": "msg-a",
        "session_id": "sess-1",
        "worker_id": "worker-1",
        "target_agent_type": "agent_a",
        "status": "COMPLETED",
        "parent_message_id": "",
    }
    exec_b = {
        "execution_id": "exec-b",
        "message_id": "msg-b",
        "session_id": "sess-1",
        "worker_id": "worker-2",
        "target_agent_type": "agent_b",
        "status": "FAILED",
        "parent_message_id": "msg-a",
    }

    registry = AsyncMock()
    registry.get_all_session_executions.return_value = [exec_a, exec_b]
    redis = AsyncMock()
    client = GatewayClient(registry=registry, redis_client=redis)

    result = await client.cancel_session(session_id="sess-1", reason="user abort")

    assert result.success is False
    assert result.status == "ALREADY_FINISHED"
    assert result.cancelled_count == 0
    assert result.already_finished_count == 2

    registry.mark_execution_cancelling.assert_not_called()
    assert registry.mark_cancel_requested.call_count == 2
    redis.xadd.assert_not_called()


@pytest.mark.asyncio
async def test_client_cancel_session_empty_session_returns_not_found():
    """A session with no executions at all should return NOT_FOUND, not be
    confused with a session whose executions are all already finished."""
    registry = AsyncMock()
    registry.get_all_session_executions.return_value = []
    redis = AsyncMock()
    client = GatewayClient(registry=registry, redis_client=redis)

    result = await client.cancel_session(session_id="sess-unknown", reason="")

    assert result.success is False
    assert result.status == "NOT_FOUND"
    assert result.cancelled_count == 0
    assert result.already_finished_count == 0

    registry.mark_execution_cancelling.assert_not_called()
    registry.mark_cancel_requested.assert_not_called()
    redis.xadd.assert_not_called()


@pytest.mark.asyncio
async def test_client_cancel_session_marks_registry_when_worker_not_assigned():
    """A non-terminal execution that hasn't been claimed by a worker yet
    (no worker_id) should still be marked cancelling, but no
    CancelTaskCommand can be routed anywhere for it yet."""
    exec_a = {
        "execution_id": "exec-a",
        "message_id": "msg-a",
        "session_id": "sess-1",
        "worker_id": "",
        "target_agent_type": "agent_a",
        "status": "QUEUED",
        "parent_message_id": "",
    }

    registry = AsyncMock()
    registry.get_all_session_executions.return_value = [exec_a]
    redis = AsyncMock()
    client = GatewayClient(registry=registry, redis_client=redis)

    result = await client.cancel_session(session_id="sess-1", reason="user abort")

    assert result.success is True
    assert result.status == "CANCEL_REQUESTED"
    assert result.cancelled_count == 1

    registry.mark_execution_cancelling.assert_called_once_with(
        "exec-a", "sess-1", "user abort"
    )
    redis.xadd.assert_not_called()


@pytest.mark.asyncio
async def test_client_cancel_session_without_registry_raises():
    """cancel_session should fail the same way cancel_task does when the
    client was constructed without a WorkerRegistry."""
    redis = AsyncMock()
    client = GatewayClient(registry=None, redis_client=redis)

    with pytest.raises(ValueError):
        await client.cancel_session(session_id="sess-1")


@pytest.mark.asyncio
async def test_byai_client_cancel_session_inherited_unmodified():
    """ByaiGatewayClient only overrides send_message; cancel_session should
    be inherited from GatewayClient and behave identically."""
    exec_a = {
        "execution_id": "exec-a",
        "message_id": "msg-a",
        "session_id": "sess-1",
        "worker_id": "worker-1",
        "target_agent_type": "agent_a",
        "status": "RUNNING",
        "parent_message_id": "",
    }

    registry = AsyncMock()
    registry.get_all_session_executions.return_value = [exec_a]
    redis = AsyncMock()
    client = ByaiGatewayClient(registry=registry, redis_client=redis)

    result = await client.cancel_session(session_id="sess-1", reason="user abort")

    assert result.success is True
    assert result.status == "CANCEL_REQUESTED"
    assert result.cancelled_count == 1
    registry.mark_execution_cancelling.assert_called_once_with(
        "exec-a", "sess-1", "user abort"
    )
    assert redis.xadd.call_count == 1
