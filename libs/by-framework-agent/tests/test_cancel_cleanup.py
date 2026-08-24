# pylint: disable=redefined-outer-name
import asyncio

import pytest
from by_framework.common.constants import RedisKeys
from conftest import mark_agent_type_online
from test_ask_user_suspend_resume import (_ask_user_chunk, _execution, _resume_command)
from test_native_agent_worker import (_agent_config, _ChatWorker, _command, _registry)
from test_task_group_batching import _multi_sub_agent_turn_chunk

from by_framework_agent.testing import StubModelClient


def _set_cancel_event() -> asyncio.Event:
    event = asyncio.Event()
    event.set()
    return event


@pytest.mark.asyncio
async def test_cancelled_resume_after_ask_user_suspend_cleans_up_and_no_ops(
    mock_redis, workspace_manager
):
    worker_a = _ChatWorker(
        worker_id="worker-a",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([_agent_config()]),
        model_client=StubModelClient(turns=[[_ask_user_chunk("Which city?")]]),
    )
    suspend_result = await worker_a._handle_message(  # pylint: disable=protected-access
        _command(), execution=_execution("exec-1", "worker-a")
    )
    assert suspend_result.status == "WAITING_USER"
    assert await mock_redis.get(RedisKeys.harness_state("exec-1")) is not None

    never_called = StubModelClient(turns=[])
    worker_b = _ChatWorker(
        worker_id="worker-b",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([_agent_config()]),
        model_client=never_called,
    )
    resume_result = await worker_b._handle_message(  # pylint: disable=protected-access
        _resume_command("Tokyo"),
        cancel_event=_set_cancel_event(),
        cancel_reason="user requested cancellation",
        execution=_execution("exec-1", "worker-b"),
    )

    assert resume_result.status == "CANCELLED"
    assert never_called.calls == []
    assert await mock_redis.get(RedisKeys.harness_state("exec-1")) is None


@pytest.mark.asyncio
async def test_cancelled_resume_after_single_agent_as_tool_suspend_cleans_up(
    mock_redis, workspace_manager
):
    await mark_agent_type_online(mock_redis, "sub_assistant")
    parent = _agent_config(sub_agents=["sub_assistant"])
    sub = _agent_config(agent_id="sub_assistant")

    from test_agent_as_tool import _sub_agent_call_chunk  # pylint: disable=import-outside-toplevel

    worker_a = _ChatWorker(
        worker_id="worker-a",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([parent, sub]),
        model_client=StubModelClient(turns=[[_sub_agent_call_chunk("help")]]),
    )
    suspend_result = await worker_a._handle_message(  # pylint: disable=protected-access
        _command(), execution=_execution("exec-1", "worker-a")
    )
    # The framework overrides a suspended caller's persisted status: the
    # harness returns QUEUED so it can unwind, but the execution is parked
    # on a sub-agent reply, not queued behind a worker.
    assert suspend_result.status == "WAITING_AGENT"

    never_called = StubModelClient(turns=[])
    worker_b = _ChatWorker(
        worker_id="worker-b",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([parent, sub]),
        model_client=never_called,
    )
    resume_result = await worker_b._handle_message(  # pylint: disable=protected-access
        _resume_command("Sub-agent reply"),
        cancel_event=_set_cancel_event(),
        cancel_reason="user requested cancellation",
        execution=_execution("exec-1", "worker-b"),
    )

    assert resume_result.status == "CANCELLED"
    assert never_called.calls == []
    assert await mock_redis.get(RedisKeys.harness_state("exec-1")) is None


@pytest.mark.asyncio
async def test_cancelled_resume_after_task_group_suspend_discards_all_stray_replies(
    mock_redis, workspace_manager
):
    await mark_agent_type_online(mock_redis, "sub_a", worker_id="worker-a-online")
    await mark_agent_type_online(mock_redis, "sub_b", worker_id="worker-b-online")
    parent = _agent_config(sub_agents=["sub_a", "sub_b"])
    sub_a = _agent_config(agent_id="sub_a")
    sub_b = _agent_config(agent_id="sub_b")

    worker_1 = _ChatWorker(
        worker_id="worker-1",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([parent, sub_a, sub_b]),
        model_client=StubModelClient(turns=[[_multi_sub_agent_turn_chunk()]]),
    )
    suspend_result = await worker_1._handle_message(  # pylint: disable=protected-access
        _command(), execution=_execution("exec-1", "worker-1")
    )
    assert suspend_result.status == "WAITING_AGENT"
    assert await mock_redis.get(RedisKeys.harness_state("exec-1")) is not None

    # First straggler (sub_a) arrives after cancellation.
    reply_a = _resume_command("Answer from A")
    reply_a.header.task_group_id = "irrelevant-once-cancelled"
    reply_a.header.source_agent_type = "sub_a"
    reply_a.header.parent_message_id = "dispatch-a"
    never_called_2 = StubModelClient(turns=[])
    worker_2 = _ChatWorker(
        worker_id="worker-2",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([parent, sub_a, sub_b]),
        model_client=never_called_2,
    )
    result_a = await worker_2._handle_message(  # pylint: disable=protected-access
        reply_a,
        cancel_event=_set_cancel_event(),
        cancel_reason="user requested cancellation",
        execution=_execution("exec-1", "worker-2"),
    )
    assert result_a.status == "CANCELLED"
    assert never_called_2.calls == []
    assert await mock_redis.get(RedisKeys.harness_state("exec-1")) is None

    # Second straggler (sub_b) arrives later still — also discarded, no crash,
    # no revival of the loop, regardless of harness_state already being gone.
    reply_b = _resume_command("Answer from B")
    reply_b.header.task_group_id = "irrelevant-once-cancelled"
    reply_b.header.source_agent_type = "sub_b"
    reply_b.header.parent_message_id = "dispatch-b"
    never_called_3 = StubModelClient(turns=[])
    worker_3 = _ChatWorker(
        worker_id="worker-3",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([parent, sub_a, sub_b]),
        model_client=never_called_3,
    )
    result_b = await worker_3._handle_message(  # pylint: disable=protected-access
        reply_b,
        cancel_event=_set_cancel_event(),
        cancel_reason="user requested cancellation",
        execution=_execution("exec-1", "worker-3"),
    )
    assert result_b.status == "CANCELLED"
    assert never_called_3.calls == []


@pytest.mark.asyncio
async def test_live_task_cancellation_cleans_up_harness_state_via_on_cancel_task(
    mock_redis, workspace_manager
):
    from by_framework.core.protocol.commands import CancelTaskCommand
    from by_framework.core.protocol.message_header import MessageHeader

    await mock_redis.set(
        RedisKeys.harness_state("exec-live"), '{"messages": [], "pending": []}'
    )

    worker = _ChatWorker(
        worker_id="worker-a",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([_agent_config()]),
        model_client=StubModelClient(turns=[]),
    )

    cancel_command = CancelTaskCommand(
        header=MessageHeader(
            message_id="cancel-1",
            session_id="session-1",
            trace_id="trace-1",
            target_agent_type="assistant",
        ),
        target_message_id="msg-1",
        target_execution_id="exec-live",
        reason="user requested",
    )
    await worker.on_cancel_task(cancel_command)

    assert await mock_redis.get(RedisKeys.harness_state("exec-live")) is None
