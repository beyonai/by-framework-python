# pylint: disable=redefined-outer-name
import json
from unittest.mock import AsyncMock

import pytest
from by_framework import RunningExecution
from by_framework.common.constants import RedisKeys
from by_framework.core.protocol.commands import ResumeCommand
from by_framework.core.protocol.message_header import MessageHeader
from test_native_agent_worker import (_agent_config, _ChatWorker, _command, _registry)

from by_framework_agent.model_client import ModelChunk
from by_framework_agent.testing import StubModelClient


def _execution(execution_id: str, worker_id: str) -> RunningExecution:
    return RunningExecution(
        execution_id=execution_id,
        message_id="msg-1",
        session_id="session-1",
        worker_id=worker_id,
        task=AsyncMock(),
        cancel_event=AsyncMock(),
    )


def _resume_command(reply: str) -> ResumeCommand:
    return ResumeCommand(
        header=MessageHeader(
            message_id="msg-2",
            session_id="session-1",
            trace_id="trace-1",
            parent_message_id="msg-1",
            user_code="user-1",
            user_name="user-1",
            target_agent_type="assistant",
        ),
        content=reply,
    )


def _ask_user_chunk(prompt: str) -> ModelChunk:
    return ModelChunk(
        tool_call_deltas=[
            {
                "index": 0,
                "id": "call_ask",
                "function": {
                    "name": "ask_user",
                    "arguments": json.dumps({"prompt": prompt}),
                },
            }
        ],
        is_final=True,
        finish_reason="tool_calls",
    )


@pytest.mark.asyncio
async def test_ask_user_tool_suspends_and_persists_loop_state(
    mock_redis, workspace_manager
):
    model_client = StubModelClient(turns=[[_ask_user_chunk("Which city?")]])
    worker = _ChatWorker(
        worker_id="worker-a",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([_agent_config()]),
        model_client=model_client,
    )

    result = await worker._handle_message(  # pylint: disable=protected-access
        _command(), execution=_execution("exec-1", "worker-a")
    )

    assert result.status == "WAITING_USER"

    raw_state = await mock_redis.get(RedisKeys.harness_state("exec-1"))
    assert raw_state is not None
    state = json.loads(raw_state)
    assert state["pending_tool_call_id"] == "call_ask"
    assert state["pending_tool_name"] == "ask_user"
    assert state["messages"][-1]["tool_calls"][0]["function"]["name"] == "ask_user"

    pipe = mock_redis.pipeline.return_value
    payloads = [call.args[1]["data"] for call in pipe.xadd.call_args_list]
    assert any("Which city?" in payload for payload in payloads)


@pytest.mark.asyncio
async def test_resume_on_different_worker_instance_rehydrates_and_continues(
    mock_redis, workspace_manager
):
    first_model_client = StubModelClient(turns=[[_ask_user_chunk("Which city?")]])
    worker_a = _ChatWorker(
        worker_id="worker-a",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([_agent_config()]),
        model_client=first_model_client,
    )
    suspend_result = await worker_a._handle_message(  # pylint: disable=protected-access
        _command(), execution=_execution("exec-1", "worker-a")
    )
    assert suspend_result.status == "WAITING_USER"

    # A *separate* worker instance — simulating a different process/machine —
    # picks up the resume, sharing only the Redis-backed harness_state.
    second_model_client = StubModelClient(
        turns=[
            [ModelChunk(content="Tokyo it is.", is_final=True, finish_reason="stop")]
        ]
    )
    worker_b = _ChatWorker(
        worker_id="worker-b",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([_agent_config()]),
        model_client=second_model_client,
    )
    resume_result = await worker_b._handle_message(  # pylint: disable=protected-access
        _resume_command("Tokyo"), execution=_execution("exec-1", "worker-b")
    )

    assert resume_result.status == "COMPLETED"
    assert resume_result.content == "Tokyo it is."

    # The continuation's model call included the user's reply as the tool
    # result for the pending ask_user call.
    sent_messages = second_model_client.calls[0]["messages"]
    tool_result = next(m for m in sent_messages if m.get("role") == "tool")
    assert tool_result["tool_call_id"] == "call_ask"
    assert tool_result["content"] == "Tokyo"

    # harness_state was consumed and cleared once the loop reached completion.
    assert await mock_redis.get(RedisKeys.harness_state("exec-1")) is None


@pytest.mark.asyncio
async def test_stray_resume_without_harness_state_fails_loudly(
    mock_redis, workspace_manager
):
    worker = _ChatWorker(
        worker_id="worker-a",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([_agent_config()]),
        model_client=StubModelClient(turns=[]),
    )

    result = await worker._handle_message(  # pylint: disable=protected-access
        _resume_command("hello"), execution=_execution("exec-missing", "worker-a")
    )

    assert result.status == "FAILED"


@pytest.mark.asyncio
async def test_ask_user_without_tracked_execution_id_fails_loudly(
    mock_redis, workspace_manager
):
    model_client = StubModelClient(turns=[[_ask_user_chunk("Which city?")]])
    worker = _ChatWorker(
        worker_id="worker-a",
        redis_client=mock_redis,
        registry=None,
        workspace_manager=workspace_manager,
        plugin_registry=_registry([_agent_config()]),
        model_client=model_client,
    )

    # No `execution=` passed — same seam used by slices 1-2 — so
    # context.execution_id is empty; the harness must refuse to silently
    # suspend without an id it can resume against later.
    result = await worker._handle_message(_command())  # pylint: disable=protected-access

    assert result.status == "FAILED"
