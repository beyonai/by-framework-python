# pylint: disable=redefined-outer-name
import json

import pytest
from by_framework.common.constants import RedisKeys
from conftest import mark_agent_type_online
from test_ask_user_suspend_resume import _execution, _resume_command
from test_native_agent_worker import (_agent_config, _ChatWorker, _command, _registry)

from by_framework_agent.model_client import ModelChunk
from by_framework_agent.testing import StubModelClient
from by_framework_agent.tool_spec import ToolSpec


def _sub_agent_call_chunk(content: str, call_id: str = "call_sub") -> ModelChunk:
    return ModelChunk(
        tool_call_deltas=[
            {
                "index": 0,
                "id": call_id,
                "function": {
                    "name": "sub_assistant",
                    "arguments": json.dumps({"content": content}),
                },
            }
        ],
        is_final=True,
        finish_reason="tool_calls",
    )


@pytest.mark.asyncio
async def test_sub_agent_auto_registered_as_tool_without_explicit_declaration(
    mock_redis, workspace_manager
):
    parent = _agent_config(sub_agents=["sub_assistant"])
    sub = _agent_config(agent_id="sub_assistant", description="Handles sub-tasks.")
    model_client = StubModelClient(
        turns=[[ModelChunk(content="hi", is_final=True, finish_reason="stop")]]
    )
    worker = _ChatWorker(
        worker_id="worker-a",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([parent, sub]),
        model_client=model_client,
    )

    await worker._handle_message(_command())  # pylint: disable=protected-access

    tools = model_client.calls[0]["tools"]
    sub_agent_tool = next(t for t in tools if t["function"]["name"] == "sub_assistant")
    assert sub_agent_tool["function"]["description"] == "Handles sub-tasks."


@pytest.mark.asyncio
async def test_sub_agent_tool_call_dispatches_via_call_agent(
    mock_redis, workspace_manager
):
    await mark_agent_type_online(mock_redis, "sub_assistant")
    parent = _agent_config(sub_agents=["sub_assistant"])
    sub = _agent_config(agent_id="sub_assistant")
    model_client = StubModelClient(turns=[[_sub_agent_call_chunk("Please help")]])
    worker = _ChatWorker(
        worker_id="worker-a",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([parent, sub]),
        model_client=model_client,
    )

    result = await worker._handle_message(  # pylint: disable=protected-access
        _command(), execution=_execution("exec-1", "worker-a")
    )

    assert result.status == "QUEUED"
    dispatched_streams = [call.args[0] for call in mock_redis.xadd.call_args_list]
    assert RedisKeys.ctrl_stream("sub_assistant") in dispatched_streams


@pytest.mark.asyncio
async def test_resume_after_sub_agent_reply_on_different_worker_instance(
    mock_redis, workspace_manager
):
    await mark_agent_type_online(mock_redis, "sub_assistant")
    parent = _agent_config(sub_agents=["sub_assistant"])
    sub = _agent_config(agent_id="sub_assistant")

    first_model_client = StubModelClient(turns=[[_sub_agent_call_chunk("Please help")]])
    worker_a = _ChatWorker(
        worker_id="worker-a",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([parent, sub]),
        model_client=first_model_client,
    )
    suspend_result = await worker_a._handle_message(  # pylint: disable=protected-access
        _command(), execution=_execution("exec-1", "worker-a")
    )
    assert suspend_result.status == "QUEUED"

    second_model_client = StubModelClient(
        turns=[
            [
                ModelChunk(
                    content="Delegated answer.", is_final=True, finish_reason="stop"
                )
            ]
        ]
    )
    worker_b = _ChatWorker(
        worker_id="worker-b",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([parent, sub]),
        model_client=second_model_client,
    )
    resume_result = await worker_b._handle_message(  # pylint: disable=protected-access
        _resume_command("Sub-agent's answer"),
        execution=_execution("exec-1", "worker-b"),
    )

    assert resume_result.status == "COMPLETED"
    assert resume_result.content == "Delegated answer."

    sent_messages = second_model_client.calls[0]["messages"]
    tool_result = next(m for m in sent_messages if m.get("role") == "tool")
    assert tool_result["tool_call_id"] == "call_sub"
    assert tool_result["content"] == "Sub-agent's answer"
    assert await mock_redis.get(RedisKeys.harness_state("exec-1")) is None


@pytest.mark.asyncio
async def test_mixed_local_tool_and_sub_agent_call_in_one_turn(
    mock_redis, workspace_manager
):
    await mark_agent_type_online(mock_redis, "sub_assistant")
    local_calls: list[dict] = []

    async def local_handler(context, arguments):  # pylint: disable=unused-argument
        local_calls.append(arguments)
        return "local result"

    local_tool = ToolSpec(name="local_tool", handler=local_handler)
    parent = _agent_config(
        sub_agents=["sub_assistant"], tools={"local_tool": local_tool}
    )
    sub = _agent_config(agent_id="sub_assistant")

    model_client = StubModelClient(
        turns=[
            [
                ModelChunk(
                    tool_call_deltas=[
                        {
                            "index": 0,
                            "id": "call_local",
                            "function": {"name": "local_tool", "arguments": "{}"},
                        },
                        {
                            "index": 1,
                            "id": "call_sub",
                            "function": {
                                "name": "sub_assistant",
                                "arguments": json.dumps({"content": "help"}),
                            },
                        },
                    ],
                    is_final=True,
                    finish_reason="tool_calls",
                )
            ]
        ]
    )
    worker = _ChatWorker(
        worker_id="worker-a",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([parent, sub]),
        model_client=model_client,
    )

    result = await worker._handle_message(  # pylint: disable=protected-access
        _command(), execution=_execution("exec-1", "worker-a")
    )

    assert result.status == "QUEUED"
    assert local_calls == [{}]

    raw_state = await mock_redis.get(RedisKeys.harness_state("exec-1"))
    state = json.loads(raw_state)
    roles_and_ids = [
        (m.get("role"), m.get("tool_call_id")) for m in state["messages"] if "role" in m
    ]
    # The local tool's result was persisted into the suspended state
    # alongside the pending sub-agent call, not dropped.
    assert ("tool", "call_local") in roles_and_ids
    assert state["pending"] == [
        {"tool_call_id": "call_sub", "tool_name": "sub_assistant"}
    ]


@pytest.mark.asyncio
async def test_sub_agent_dispatch_failure_surfaces_as_tool_error_and_continues(
    mock_redis, workspace_manager
):
    # sub_assistant is deliberately never marked online.
    parent = _agent_config(sub_agents=["sub_assistant"])
    sub = _agent_config(agent_id="sub_assistant")
    model_client = StubModelClient(
        turns=[
            [_sub_agent_call_chunk("help", call_id="call_sub")],
            [ModelChunk(content="fell back", is_final=True, finish_reason="stop")],
        ]
    )
    worker = _ChatWorker(
        worker_id="worker-a",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([parent, sub]),
        model_client=model_client,
    )

    result = await worker._handle_message(  # pylint: disable=protected-access
        _command(), execution=_execution("exec-1", "worker-a")
    )

    assert result.status == "COMPLETED"
    assert result.content == "fell back"
    tool_message = model_client.calls[1]["messages"][-1]
    assert tool_message["role"] == "tool"
    assert tool_message["tool_call_id"] == "call_sub"
    assert "sub_assistant" in tool_message["content"]
