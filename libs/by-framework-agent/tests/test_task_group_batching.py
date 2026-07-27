# pylint: disable=redefined-outer-name
import json

import pytest
from by_framework.common.constants import RedisKeys
from conftest import mark_agent_type_online
from test_ask_user_suspend_resume import _execution, _resume_command
from test_native_agent_worker import (_agent_config, _ChatWorker, _command, _registry)

from by_framework_agent.model_client import ModelChunk
from by_framework_agent.testing import StubModelClient


def _multi_sub_agent_turn_chunk() -> ModelChunk:
    return ModelChunk(
        tool_call_deltas=[
            {
                "index": 0,
                "id": "call_a",
                "function": {
                    "name": "sub_a",
                    "arguments": json.dumps({"content": "task for a"}),
                },
            },
            {
                "index": 1,
                "id": "call_b",
                "function": {
                    "name": "sub_b",
                    "arguments": json.dumps({"content": "task for b"}),
                },
            },
        ],
        is_final=True,
        finish_reason="tool_calls",
    )


@pytest.mark.asyncio
async def test_multi_target_turn_dispatches_single_task_group_not_sequential_calls(
    mock_redis, workspace_manager
):
    await mark_agent_type_online(mock_redis, "sub_a", worker_id="worker-a-online")
    await mark_agent_type_online(mock_redis, "sub_b", worker_id="worker-b-online")
    parent = _agent_config(sub_agents=["sub_a", "sub_b"])
    sub_a = _agent_config(agent_id="sub_a")
    sub_b = _agent_config(agent_id="sub_b")
    model_client = StubModelClient(turns=[[_multi_sub_agent_turn_chunk()]])
    worker = _ChatWorker(
        worker_id="worker-1",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([parent, sub_a, sub_b]),
        model_client=model_client,
    )

    result = await worker._handle_message(  # pylint: disable=protected-access
        _command(), execution=_execution("exec-1", "worker-1")
    )

    assert result.status == "QUEUED"

    # Exactly one Task Group was created (one task_group hash key), not two
    # independent call_agent dispatches.
    task_group_keys = [
        k
        for k in mock_redis.store.hashes
        if k.startswith("byai_gateway:task_group:") and not k.endswith(":results")
    ]
    assert len(task_group_keys) == 1

    dispatched_streams = [call.args[0] for call in mock_redis.xadd.call_args_list]
    assert RedisKeys.ctrl_stream("sub_a") in dispatched_streams
    assert RedisKeys.ctrl_stream("sub_b") in dispatched_streams


@pytest.mark.asyncio
async def test_single_sub_agent_call_still_uses_call_agent_not_task_group(
    mock_redis, workspace_manager
):
    await mark_agent_type_online(mock_redis, "sub_a")
    parent = _agent_config(sub_agents=["sub_a"])
    sub_a = _agent_config(agent_id="sub_a")
    model_client = StubModelClient(
        turns=[
            [
                ModelChunk(
                    tool_call_deltas=[
                        {
                            "index": 0,
                            "id": "call_a",
                            "function": {
                                "name": "sub_a",
                                "arguments": json.dumps({"content": "solo task"}),
                            },
                        }
                    ],
                    is_final=True,
                    finish_reason="tool_calls",
                )
            ]
        ]
    )
    worker = _ChatWorker(
        worker_id="worker-1",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([parent, sub_a]),
        model_client=model_client,
    )

    await worker._handle_message(  # pylint: disable=protected-access
        _command(), execution=_execution("exec-1", "worker-1")
    )

    task_group_keys = [
        k for k in mock_redis.store.hashes if k.startswith("byai_gateway:task_group:")
    ]
    assert not task_group_keys


@pytest.mark.asyncio
async def test_group_join_resumes_loop_with_each_result_mapped_to_its_tool_call(
    mock_redis, workspace_manager
):
    await mark_agent_type_online(mock_redis, "sub_a", worker_id="worker-a-online")
    await mark_agent_type_online(mock_redis, "sub_b", worker_id="worker-b-online")
    parent = _agent_config(sub_agents=["sub_a", "sub_b"])
    sub_a = _agent_config(agent_id="sub_a")
    sub_b = _agent_config(agent_id="sub_b")

    first_model_client = StubModelClient(turns=[[_multi_sub_agent_turn_chunk()]])
    worker_1 = _ChatWorker(
        worker_id="worker-1",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([parent, sub_a, sub_b]),
        model_client=first_model_client,
    )
    suspend_result = await worker_1._handle_message(  # pylint: disable=protected-access
        _command(), execution=_execution("exec-1", "worker-1")
    )
    assert suspend_result.status == "QUEUED"

    # Extract the task_group_id from the single dispatched AskAgentCommand
    # payloads so the test can simulate both siblings replying.
    task_group_keys = [
        k
        for k in mock_redis.store.hashes
        if k.startswith("byai_gateway:task_group:") and not k.endswith(":results")
    ]
    assert len(task_group_keys) == 1
    task_group_id = task_group_keys[0].split(":")[-1]

    # First sibling (sub_a) replies — worker.py's Group Join keeps this
    # execution QUEUED (waiting_for_group) without ever calling
    # process_command, since not everyone has replied yet.
    reply_a = _resume_command("Answer from A")
    reply_a.header.task_group_id = task_group_id
    reply_a.header.source_agent_type = "sub_a"
    reply_a.header.parent_message_id = "dispatch-a"
    worker_2 = _ChatWorker(
        worker_id="worker-2",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([parent, sub_a, sub_b]),
        model_client=StubModelClient(turns=[]),
    )
    partial_result = await worker_2._handle_message(  # pylint: disable=protected-access
        reply_a, execution=_execution("exec-1", "worker-2")
    )
    assert "waiting_for_group" in partial_result.status

    # Second sibling (sub_b) replies — this completes the group, so
    # process_command finally runs once, with reply_data holding both.
    reply_b = _resume_command("Answer from B")
    reply_b.header.task_group_id = task_group_id
    reply_b.header.source_agent_type = "sub_b"
    reply_b.header.parent_message_id = "dispatch-b"
    final_model_client = StubModelClient(
        turns=[[ModelChunk(content="combined", is_final=True, finish_reason="stop")]]
    )
    worker_3 = _ChatWorker(
        worker_id="worker-3",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([parent, sub_a, sub_b]),
        model_client=final_model_client,
    )
    final_result = await worker_3._handle_message(  # pylint: disable=protected-access
        reply_b, execution=_execution("exec-1", "worker-3")
    )

    assert final_result.status == "COMPLETED"
    assert final_result.content == "combined"

    sent_messages = final_model_client.calls[0]["messages"]
    tool_messages = {
        m["tool_call_id"]: m for m in sent_messages if m.get("role") == "tool"
    }
    assert tool_messages["call_a"]["content"] == "Answer from A"
    assert tool_messages["call_a"]["name"] == "sub_a"
    assert tool_messages["call_b"]["content"] == "Answer from B"
    assert tool_messages["call_b"]["name"] == "sub_b"

    assert await mock_redis.get(RedisKeys.harness_state("exec-1")) is None
