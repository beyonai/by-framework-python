import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from by_framework import AgentContext, GatewayClient
from by_framework.common.constants import RedisKeys
from by_framework.core.availability import (PendingDelivery, RoutePolicy,
                                            WakeupDecisionStatus,
                                            WakeupRequest)
from by_framework.core.delivery_gate import DeliveryGate
from by_framework.core.protocol.commands import command_from_dict
from by_framework.core.wakeup_controller import WakeupController


def test_control_plane_types_are_exported_from_top_level_package():
    from by_framework import \
        DeliveryGate as \
        ExportedDeliveryGate  # pylint: disable=import-outside-toplevel
    from by_framework import \
        RoutePolicy as \
        ExportedRoutePolicy  # pylint: disable=import-outside-toplevel
    from by_framework import \
        WakeupController as \
        ExportedWakeupController  # pylint: disable=import-outside-toplevel

    assert ExportedRoutePolicy.WAKE_AND_QUEUE == "WAKE_AND_QUEUE"
    assert ExportedWakeupController is WakeupController
    assert ExportedDeliveryGate is DeliveryGate


def test_route_policy_includes_send_anyway():
    assert RoutePolicy.SEND_ANYWAY == "SEND_ANYWAY"


def test_control_plane_redis_keys_share_namespace():
    prefix = "byai_gateway:control_plane:"

    assert RedisKeys.control_plane_wakeup_stream().startswith(prefix)
    assert RedisKeys.control_plane_wakeup_result_stream("wake-1").startswith(prefix)
    assert RedisKeys.control_plane_delivery_pending_stream().startswith(prefix)
    assert RedisKeys.control_plane_deadletter_stream().startswith(prefix)
    assert RedisKeys.control_plane_agent_availability("agent-a").startswith(prefix)
    assert RedisKeys.control_plane_agent_circuit("agent-a").startswith(prefix)
    assert RedisKeys.control_plane_agent_fallback("agent-a").startswith(prefix)
    assert RedisKeys.control_plane_user_quota("u-1").startswith(prefix)
    assert RedisKeys.control_plane_wakeup_dedupe(
        "agent-a", "u-1", "us-east"
    ).startswith(prefix)


@pytest.mark.asyncio
async def test_client_wake_and_wait_dispatches_after_ready_decision():
    redis = AsyncMock()
    registry = AsyncMock()
    registry.has_online_agent_type.side_effect = [
        (False, []),
        (True, ["worker-1"]),
    ]
    redis.xread.return_value = [
        [
            RedisKeys.control_plane_wakeup_result_stream("unused"),
            [
                (
                    b"1-0",
                    {
                        b"data": json.dumps(
                            {"status": WakeupDecisionStatus.READY}
                        ).encode()
                    },
                )
            ],
        ]
    ]

    client = GatewayClient(redis_client=redis, registry=registry)
    result = await client.send_message(
        target_agent_type="cold-agent",
        session_id="s1",
        content="hello",
        route_policy=RoutePolicy.WAKE_AND_WAIT,
        availability_timeout_ms=100,
        user_code="u-1",
        region="us-east",
        priority=7,
    )

    assert result.success is True
    assert result.status == "QUEUED"
    assert redis.xadd.await_count == 2
    wakeup_stream = redis.xadd.await_args_list[0].args[0]
    ctrl_stream = redis.xadd.await_args_list[1].args[0]
    assert wakeup_stream == RedisKeys.control_plane_wakeup_stream()
    assert ctrl_stream == RedisKeys.ctrl_stream("cold-agent")

    wakeup_payload = json.loads(redis.xadd.await_args_list[0].args[1]["data"])
    execution = registry.initialize_execution.await_args.args[0]
    assert wakeup_payload["execution_id"] == execution["execution_id"]
    assert wakeup_payload["execution_id"].startswith("exec-")
    assert wakeup_payload["target_agent_type"] == "cold-agent"
    assert wakeup_payload["user_code"] == "u-1"
    assert wakeup_payload["region"] == "us-east"
    assert wakeup_payload["priority"] == 7


@pytest.mark.asyncio
async def test_client_wake_and_wait_timeout_does_not_dispatch_control_message():
    redis = AsyncMock()
    registry = AsyncMock()
    registry.has_online_agent_type.return_value = (False, [])
    redis.xread.return_value = []

    client = GatewayClient(redis_client=redis, registry=registry)
    result = await client.send_message(
        target_agent_type="cold-agent",
        session_id="s1",
        content="hello",
        route_policy=RoutePolicy.WAKE_AND_WAIT,
        availability_timeout_ms=1,
    )

    assert result.success is False
    assert result.status == "FAILED"
    assert result.error_code == "AGENT_TYPE_UNAVAILABLE"
    assert redis.xadd.await_count == 1
    assert redis.xadd.await_args.args[0] == RedisKeys.control_plane_wakeup_stream()


@pytest.mark.asyncio
async def test_client_wake_and_queue_writes_pending_without_control_dispatch():
    redis = AsyncMock()
    registry = AsyncMock()
    registry.has_online_agent_type.return_value = (False, [])

    client = GatewayClient(redis_client=redis, registry=registry)
    result = await client.send_message(
        target_agent_type="cold-agent",
        session_id="s1",
        content="hello",
        route_policy=RoutePolicy.WAKE_AND_QUEUE,
        user_code="u-1",
    )

    assert result.success is True
    assert result.status == "QUEUED"
    assert redis.xadd.await_count == 2
    streams = [call.args[0] for call in redis.xadd.await_args_list]
    assert streams == [
        RedisKeys.control_plane_wakeup_stream(),
        RedisKeys.control_plane_delivery_pending_stream(),
    ]

    pending = json.loads(redis.xadd.await_args_list[1].args[1]["data"])
    initialized = redis.xadd.await_args_list
    del initialized
    execution = registry.initialize_execution.await_args.args[0]
    assert pending["execution_id"] == execution["execution_id"]
    assert pending["target_agent_type"] == "cold-agent"
    assert pending["user_code"] == "u-1"
    assert pending["delivery_stream"] == RedisKeys.ctrl_stream("cold-agent")
    assert pending["command_payload"]["header"]["target_agent_type"] == "cold-agent"


@pytest.mark.asyncio
async def test_client_send_anyway_route_policy_skips_online_check():
    redis = AsyncMock()
    registry = AsyncMock()

    client = GatewayClient(redis_client=redis, registry=registry)
    result = await client.send_message(
        target_agent_type="offline-agent",
        session_id="s1",
        content="hello",
        route_policy=RoutePolicy.SEND_ANYWAY,
    )

    assert result.success is True
    registry.has_online_agent_type.assert_not_called()
    assert redis.xadd.await_args.args[0] == RedisKeys.ctrl_stream("offline-agent")


@pytest.mark.asyncio
async def test_client_rejects_before_wakeup_when_tenant_quota_is_exhausted():
    redis = AsyncMock()
    redis.get.return_value = json.dumps(
        {"available": False, "reason": "tenant quota exceeded"}
    ).encode()
    registry = AsyncMock()

    client = GatewayClient(redis_client=redis, registry=registry)
    result = await client.send_message(
        target_agent_type="cold-agent",
        session_id="s1",
        content="hello",
        route_policy=RoutePolicy.WAKE_AND_QUEUE,
        user_code="u-1",
    )

    assert result.success is False
    assert result.error_code == "TENANT_QUOTA_EXCEEDED"
    assert result.error == "tenant quota exceeded"
    registry.has_online_agent_type.assert_not_called()
    redis.xadd.assert_not_called()


@pytest.mark.asyncio
async def test_client_rejects_before_wakeup_when_agent_circuit_is_open():
    redis = AsyncMock()

    async def get_state(key):
        if key == RedisKeys.control_plane_agent_circuit("cold-agent"):
            return json.dumps({"state": "OPEN", "reason": "startup failures"}).encode()
        return None

    redis.get.side_effect = get_state
    registry = AsyncMock()

    client = GatewayClient(redis_client=redis, registry=registry)
    result = await client.send_message(
        target_agent_type="cold-agent",
        session_id="s1",
        content="hello",
        route_policy=RoutePolicy.WAKE_AND_WAIT,
    )

    assert result.success is False
    assert result.error_code == "AGENT_CIRCUIT_OPEN"
    assert result.error == "startup failures"
    registry.has_online_agent_type.assert_not_called()
    redis.xadd.assert_not_called()


@pytest.mark.asyncio
async def test_client_fallback_routes_to_selected_agent_type():
    redis = AsyncMock()
    redis.get.side_effect = [
        None,
        json.dumps({"selected_agent_type": "fallback-agent"}).encode(),
    ]
    registry = AsyncMock()
    registry.has_online_agent_type.side_effect = [
        (False, []),
        (True, ["worker-fallback"]),
    ]

    client = GatewayClient(redis_client=redis, registry=registry)
    result = await client.send_message(
        target_agent_type="cold-agent",
        session_id="s1",
        content="hello",
        route_policy=RoutePolicy.FAIL_FAST,
    )

    assert result.success is True
    assert redis.xadd.await_args.args[0] == RedisKeys.ctrl_stream("fallback-agent")
    command = command_from_dict(json.loads(redis.xadd.await_args.args[1]["data"]))
    assert command.header.target_agent_type == "fallback-agent"


@pytest.mark.asyncio
async def test_context_call_agent_uses_wake_and_wait_before_dispatching():
    redis = MagicMock()
    redis.xadd = AsyncMock()
    redis.smembers = AsyncMock(side_effect=[{b"worker-1"}, {b"worker-1"}])

    async def get_state(key):
        if key == RedisKeys.worker_online_lease("worker-1"):
            if get_state.calls == 0:
                get_state.calls += 1
                return None
            return b"1"
        return None

    get_state.calls = 0
    redis.get = AsyncMock(side_effect=get_state)
    redis.xread = AsyncMock(
        return_value=[
            [
                RedisKeys.control_plane_wakeup_result_stream("unused"),
                [
                    (
                        b"1-0",
                        {
                            b"data": json.dumps(
                                {"status": WakeupDecisionStatus.READY}
                            ).encode()
                        },
                    )
                ],
            ]
        ]
    )
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    redis.pipeline.return_value = mock_pipe

    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=redis,
        current_agent_id="agent-a",
        message_id="msg-parent",
    )
    result = await ctx.call_agent(
        target_agent_type="cold-agent",
        content="hello",
        route_policy=RoutePolicy.WAKE_AND_WAIT,
        availability_timeout_ms=100,
    )

    assert result["status"] == "QUEUED"
    assert redis.xadd.await_count == 2
    assert (
        redis.xadd.await_args_list[0].args[0] == RedisKeys.control_plane_wakeup_stream()
    )
    assert redis.xadd.await_args_list[1].args[0] == RedisKeys.ctrl_stream("cold-agent")


@pytest.mark.asyncio
async def test_context_call_agent_wake_and_queue_writes_pending():
    redis = MagicMock()
    redis.xadd = AsyncMock()
    redis.pipeline.return_value = MagicMock(execute=AsyncMock(return_value=[]))
    redis.smembers = AsyncMock(return_value={b"worker-1"})
    redis.get = AsyncMock(return_value=None)

    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=redis,
        current_agent_id="agent-a",
        message_id="msg-parent",
    )
    result = await ctx.call_agent(
        target_agent_type="cold-agent",
        content="hello",
        route_policy=RoutePolicy.WAKE_AND_QUEUE,
    )

    assert result["status"] == "QUEUED"
    assert redis.xadd.await_count == 2
    assert (
        redis.xadd.await_args_list[0].args[0] == RedisKeys.control_plane_wakeup_stream()
    )
    assert (
        redis.xadd.await_args_list[1].args[0]
        == RedisKeys.control_plane_delivery_pending_stream()
    )
    pending = json.loads(redis.xadd.await_args_list[1].args[1]["data"])
    initialized = redis.pipeline.return_value.hset.call_args_list[0].args[2]
    execution = json.loads(initialized)
    assert pending["execution_id"] == execution["execution_id"]


@pytest.mark.asyncio
async def test_wakeup_controller_reads_request_and_writes_ready_decision():
    class ReadyProvider:

        async def wakeup(self, request):
            return {"status": WakeupDecisionStatus.READY, "worker_ids": ["worker-1"]}

    request = WakeupRequest(
        execution_id="wake-1",
        target_agent_type="cold-agent",
        session_id="s1",
        trace_id="t1",
        message_id="msg-1",
        source="client",
        policy=RoutePolicy.WAKE_AND_WAIT,
        timeout_ms=100,
    )
    redis = AsyncMock()
    redis.xread.return_value = [
        [
            RedisKeys.control_plane_wakeup_stream(),
            [(b"1-0", request.to_redis_payload())],
        ]
    ]

    controller = WakeupController(redis=redis, provider=ReadyProvider())
    next_id = await controller.run_once(last_id="0-0", block_ms=1)

    assert next_id == "1-0"
    redis.xadd.assert_awaited_once()
    result_stream, result_payload = redis.xadd.await_args.args
    assert result_stream == RedisKeys.control_plane_wakeup_result_stream("wake-1")
    decision = json.loads(result_payload["data"])
    assert decision["execution_id"] == "wake-1"
    assert decision["target_agent_type"] == "cold-agent"
    assert decision["status"] == WakeupDecisionStatus.READY


@pytest.mark.asyncio
async def test_wakeup_controller_dedupes_concurrent_requests():
    class CountingProvider:

        def __init__(self):
            self.calls = 0

        async def wakeup(self, request):
            self.calls += 1
            return {"status": WakeupDecisionStatus.READY}

    request = WakeupRequest(
        execution_id="wake-1",
        target_agent_type="cold-agent",
        session_id="s1",
        trace_id="t1",
        message_id="msg-1",
        source="client",
        policy=RoutePolicy.WAKE_AND_WAIT,
        timeout_ms=100,
        user_code="u-1",
        region="us-east",
    )
    redis = AsyncMock()
    redis.set.return_value = False
    redis.xread.return_value = [
        [
            RedisKeys.control_plane_wakeup_stream(),
            [(b"1-0", request.to_redis_payload())],
        ]
    ]
    provider = CountingProvider()

    controller = WakeupController(redis=redis, provider=provider)
    await controller.run_once(last_id="0-0", block_ms=1)

    assert provider.calls == 0
    redis.set.assert_awaited_once_with(
        RedisKeys.control_plane_wakeup_dedupe("cold-agent", "u-1", "us-east"),
        "wake-1",
        ex=30,
        nx=True,
    )
    decision = json.loads(redis.xadd.await_args.args[1]["data"])
    assert decision["status"] == WakeupDecisionStatus.QUEUED


@pytest.mark.asyncio
async def test_wakeup_controller_retries_provider_before_ready():
    class FlakyProvider:

        def __init__(self):
            self.calls = 0

        async def wakeup(self, request):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("transient startup failure")
            return {"status": WakeupDecisionStatus.READY}

    request = WakeupRequest(
        execution_id="wake-retry",
        target_agent_type="cold-agent",
        session_id="s1",
        trace_id="t1",
        message_id="msg-1",
        source="client",
        policy=RoutePolicy.WAKE_AND_WAIT,
        timeout_ms=100,
    )
    redis = AsyncMock()
    redis.set.return_value = True
    redis.xread.return_value = [
        [
            RedisKeys.control_plane_wakeup_stream(),
            [(b"1-0", request.to_redis_payload())],
        ]
    ]
    provider = FlakyProvider()

    controller = WakeupController(redis=redis, provider=provider, max_attempts=2)
    await controller.run_once(last_id="0-0", block_ms=1)

    assert provider.calls == 2
    decision = json.loads(redis.xadd.await_args.args[1]["data"])
    assert decision["status"] == WakeupDecisionStatus.READY


@pytest.mark.asyncio
async def test_wakeup_controller_writes_deadletter_after_final_failure():
    class FailingProvider:

        async def wakeup(self, request):
            raise RuntimeError("container failed")

    request = WakeupRequest(
        execution_id="wake-dead",
        target_agent_type="cold-agent",
        session_id="s1",
        trace_id="t1",
        message_id="msg-1",
        source="client",
        policy=RoutePolicy.WAKE_AND_WAIT,
        timeout_ms=100,
    )
    redis = AsyncMock()
    redis.set.return_value = True
    redis.xread.return_value = [
        [
            RedisKeys.control_plane_wakeup_stream(),
            [(b"1-0", request.to_redis_payload())],
        ]
    ]

    controller = WakeupController(redis=redis, provider=FailingProvider())
    await controller.run_once(last_id="0-0", block_ms=1)

    streams = [call.args[0] for call in redis.xadd.await_args_list]
    assert RedisKeys.control_plane_wakeup_result_stream("wake-dead") in streams
    assert RedisKeys.control_plane_deadletter_stream() in streams


@pytest.mark.asyncio
async def test_delivery_gate_dispatches_matching_pending_delivery():
    pending = PendingDelivery(
        execution_id="wake-1",
        message_id="msg-1",
        session_id="s1",
        trace_id="t1",
        target_agent_type="cold-agent",
        delivery_stream=RedisKeys.ctrl_stream("cold-agent"),
        command_payload={
            "action_type": "ASK_AGENT",
            "header": {
                "message_id": "msg-1",
                "session_id": "s1",
                "trace_id": "t1",
                "target_agent_type": "cold-agent",
            },
            "body": {"content": "hello", "wait_for_reply": False},
        },
    )
    redis = AsyncMock()
    redis.xread.return_value = [
        [
            RedisKeys.control_plane_delivery_pending_stream(),
            [(b"1-0", pending.to_redis_payload())],
        ]
    ]

    gate = DeliveryGate(redis=redis)
    dispatched = await gate.dispatch_ready(execution_id="wake-1")

    assert dispatched == 1
    redis.xadd.assert_awaited_once()
    stream, payload = redis.xadd.await_args.args
    assert stream == RedisKeys.ctrl_stream("cold-agent")
    assert json.loads(payload["data"])["header"]["message_id"] == "msg-1"


@pytest.mark.asyncio
async def test_delivery_gate_dispatches_higher_priority_first():
    low = PendingDelivery(
        execution_id="wake-1",
        message_id="msg-low",
        session_id="s1",
        trace_id="t1",
        target_agent_type="cold-agent",
        delivery_stream=RedisKeys.ctrl_stream("cold-agent"),
        priority=1,
        command_payload={
            "action_type": "ASK_AGENT",
            "header": {
                "message_id": "msg-low",
                "session_id": "s1",
                "trace_id": "t1",
                "target_agent_type": "cold-agent",
            },
            "body": {"content": "low", "wait_for_reply": False},
        },
    )
    high = PendingDelivery(
        execution_id="wake-1",
        message_id="msg-high",
        session_id="s1",
        trace_id="t1",
        target_agent_type="cold-agent",
        delivery_stream=RedisKeys.ctrl_stream("cold-agent"),
        priority=10,
        command_payload={
            "action_type": "ASK_AGENT",
            "header": {
                "message_id": "msg-high",
                "session_id": "s1",
                "trace_id": "t1",
                "target_agent_type": "cold-agent",
            },
            "body": {"content": "high", "wait_for_reply": False},
        },
    )
    redis = AsyncMock()
    redis.xread.return_value = [
        [
            RedisKeys.control_plane_delivery_pending_stream(),
            [
                (b"1-0", low.to_redis_payload()),
                (b"2-0", high.to_redis_payload()),
            ],
        ]
    ]

    gate = DeliveryGate(redis=redis)
    dispatched = await gate.dispatch_ready(execution_id="wake-1")

    assert dispatched == 2
    first_payload = json.loads(redis.xadd.await_args_list[0].args[1]["data"])
    assert first_payload["header"]["message_id"] == "msg-high"
