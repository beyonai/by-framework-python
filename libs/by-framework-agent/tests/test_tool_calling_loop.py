# pylint: disable=redefined-outer-name
import asyncio
import json
from unittest.mock import MagicMock

import pytest
from by_framework.core.extensions.agent_config import CallbackType
from by_framework.core.runtime.history.history_manager import HistoryManager
from test_native_agent_worker import (_agent_config, _ChatWorker, _command, _registry)

from by_framework_agent.model_client import ModelChunk
from by_framework_agent.testing import StubModelClient
from by_framework_agent.tool_spec import ToolSpec


def _weather_tool_spec(handler) -> ToolSpec:
    return ToolSpec(
        name="get_weather",
        handler=handler,
        description="Get the current weather for a city.",
        parameters={
            "type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"],
        },
    )


@pytest.mark.asyncio
async def test_tool_call_then_final_answer(mock_redis, workspace_manager):
    calls: list[dict] = []

    async def get_weather(context, arguments):  # pylint: disable=unused-argument
        calls.append(arguments)
        return {"tempC": 21}

    tool = _weather_tool_spec(get_weather)
    model_client = StubModelClient(
        turns=[
            [
                ModelChunk(
                    tool_call_deltas=[
                        {
                            "index": 0,
                            "id": "call_1",
                            "type": "function",
                            "function": {
                                "name": "get_weather",
                                "arguments": '{"city": "SF"}',
                            },
                        }
                    ],
                    finish_reason="tool_calls",
                    is_final=True,
                    usage={"prompt_tokens": 10, "completion_tokens": 5},
                    model="gpt-4o-mini",
                ),
            ],
            [
                ModelChunk(
                    content="It's 21C in SF.",
                    is_final=True,
                    finish_reason="stop",
                    usage={"prompt_tokens": 20, "completion_tokens": 8},
                    model="gpt-4o-mini",
                ),
            ],
        ]
    )
    worker = _ChatWorker(
        worker_id="agent-1",
        redis_client=mock_redis,
        registry=MagicMock(),
        workspace_manager=workspace_manager,
        plugin_registry=_registry([_agent_config(tools={"get_weather": tool})]),
        model_client=model_client,
    )

    result = await worker._handle_message(_command())  # pylint: disable=protected-access

    assert result.status == "COMPLETED"
    assert result.content == "It's 21C in SF."
    assert calls == [{"city": "SF"}]

    # Two model calls: the tool_call turn, then the follow-up with the result.
    assert len(model_client.calls) == 2
    # The built-in ask_user tool is always offered alongside declared tools.
    assert tool.to_openai_schema() in model_client.calls[0]["tools"]
    second_call_messages = model_client.calls[1]["messages"]
    assert second_call_messages[-2]["tool_calls"][0]["function"]["name"] == (
        "get_weather"
    )
    assert second_call_messages[-1] == {
        "role": "tool",
        "tool_call_id": "call_1",
        "name": "get_weather",
        "content": json.dumps({"tempC": 21}),
    }


@pytest.mark.asyncio
async def test_multiple_tool_calls_in_one_turn_run_concurrently(
    mock_redis, workspace_manager
):
    order: list[str] = []

    async def slow(context, arguments):  # pylint: disable=unused-argument
        order.append("slow_start")
        await asyncio.sleep(0.05)
        order.append("slow_end")
        return "slow done"

    async def fast(context, arguments):  # pylint: disable=unused-argument
        order.append("fast_start")
        return "fast done"

    tools = {
        "slow_tool": ToolSpec(name="slow_tool", handler=slow),
        "fast_tool": ToolSpec(name="fast_tool", handler=fast),
    }
    model_client = StubModelClient(
        turns=[
            [
                ModelChunk(
                    tool_call_deltas=[
                        {
                            "index": 0,
                            "id": "call_slow",
                            "function": {"name": "slow_tool", "arguments": "{}"},
                        },
                        {
                            "index": 1,
                            "id": "call_fast",
                            "function": {"name": "fast_tool", "arguments": "{}"},
                        },
                    ],
                    is_final=True,
                    finish_reason="tool_calls",
                ),
            ],
            [ModelChunk(content="done", is_final=True, finish_reason="stop")],
        ]
    )
    worker = _ChatWorker(
        worker_id="agent-1",
        redis_client=mock_redis,
        registry=MagicMock(),
        workspace_manager=workspace_manager,
        plugin_registry=_registry([_agent_config(tools=tools)]),
        model_client=model_client,
    )

    await worker._handle_message(_command())  # pylint: disable=protected-access

    # If tools ran sequentially, fast_start couldn't happen until slow_end.
    assert order.index("fast_start") < order.index("slow_end")


@pytest.mark.asyncio
async def test_before_tool_callback_veto_prevents_handler_execution(
    mock_redis, workspace_manager
):
    handler_called = False

    async def handler(context, arguments):  # pylint: disable=unused-argument
        nonlocal handler_called
        handler_called = True
        return "should not run"

    def veto(context, payload):  # pylint: disable=unused-argument
        raise PermissionError("denied")

    tool = ToolSpec(name="dangerous_tool", handler=handler)
    config = _agent_config(
        tools={"dangerous_tool": tool},
        callbacks={CallbackType.before_tool_callback: [veto]},
    )
    model_client = StubModelClient(
        turns=[
            [
                ModelChunk(
                    tool_call_deltas=[
                        {
                            "index": 0,
                            "id": "call_1",
                            "function": {"name": "dangerous_tool", "arguments": "{}"},
                        }
                    ],
                    is_final=True,
                    finish_reason="tool_calls",
                ),
            ],
            [ModelChunk(content="ok", is_final=True, finish_reason="stop")],
        ]
    )
    worker = _ChatWorker(
        worker_id="agent-1",
        redis_client=mock_redis,
        registry=MagicMock(),
        workspace_manager=workspace_manager,
        plugin_registry=_registry([config]),
        model_client=model_client,
    )

    result = await worker._handle_message(_command())  # pylint: disable=protected-access

    assert not handler_called
    assert result.status == "COMPLETED"
    second_call_messages = model_client.calls[1]["messages"]
    tool_message = second_call_messages[-1]
    assert tool_message["role"] == "tool"
    assert "denied" in tool_message["content"]


@pytest.mark.asyncio
async def test_tool_handler_exception_surfaces_as_tool_error_and_loop_continues(
    mock_redis, workspace_manager
):
    async def broken_handler(context, arguments):  # pylint: disable=unused-argument
        raise RuntimeError("boom")

    tool = ToolSpec(name="broken_tool", handler=broken_handler)
    model_client = StubModelClient(
        turns=[
            [
                ModelChunk(
                    tool_call_deltas=[
                        {
                            "index": 0,
                            "id": "call_1",
                            "function": {"name": "broken_tool", "arguments": "{}"},
                        }
                    ],
                    is_final=True,
                    finish_reason="tool_calls",
                ),
            ],
            [ModelChunk(content="recovered", is_final=True, finish_reason="stop")],
        ]
    )
    worker = _ChatWorker(
        worker_id="agent-1",
        redis_client=mock_redis,
        registry=MagicMock(),
        workspace_manager=workspace_manager,
        plugin_registry=_registry([_agent_config(tools={"broken_tool": tool})]),
        model_client=model_client,
    )

    result = await worker._handle_message(_command())  # pylint: disable=protected-access

    assert result.status == "COMPLETED"
    assert result.content == "recovered"
    tool_message = model_client.calls[1]["messages"][-1]
    assert "boom" in tool_message["content"]


@pytest.mark.asyncio
async def test_tool_call_and_result_persisted_to_history(mock_redis, workspace_manager):
    async def handler(context, arguments):  # pylint: disable=unused-argument
        return "42"

    tool = ToolSpec(name="answer_tool", handler=handler)
    model_client = StubModelClient(
        turns=[
            [
                ModelChunk(
                    tool_call_deltas=[
                        {
                            "index": 0,
                            "id": "call_1",
                            "function": {"name": "answer_tool", "arguments": "{}"},
                        }
                    ],
                    is_final=True,
                    finish_reason="tool_calls",
                ),
            ],
            [
                ModelChunk(
                    content="the answer is 42", is_final=True, finish_reason="stop"
                )
            ],
        ]
    )
    worker = _ChatWorker(
        worker_id="agent-1",
        redis_client=mock_redis,
        registry=MagicMock(),
        workspace_manager=workspace_manager,
        plugin_registry=_registry([_agent_config(tools={"answer_tool": tool})]),
        model_client=model_client,
    )

    await worker._handle_message(_command())  # pylint: disable=protected-access

    history = await HistoryManager("session-1").get_history(limit=50)
    roles = [entry["role"] for entry in history]
    assert "tool" in roles
    tool_entry = next(entry for entry in history if entry["role"] == "tool")
    assert tool_entry["content"] == "42"
    assert tool_entry["metadata"]["tool_call_id"] == "call_1"


@pytest.mark.asyncio
async def test_tools_declared_but_no_tool_call_behaves_like_plain_chat(
    mock_redis, workspace_manager
):
    async def unused_handler(context, arguments):  # pylint: disable=unused-argument
        raise AssertionError("should never be called")

    tool = ToolSpec(name="unused_tool", handler=unused_handler)
    model_client = StubModelClient(
        turns=[
            [ModelChunk(content="no tools needed", is_final=True, finish_reason="stop")]
        ]
    )
    worker = _ChatWorker(
        worker_id="agent-1",
        redis_client=mock_redis,
        registry=MagicMock(),
        workspace_manager=workspace_manager,
        plugin_registry=_registry([_agent_config(tools={"unused_tool": tool})]),
        model_client=model_client,
    )

    result = await worker._handle_message(_command())  # pylint: disable=protected-access

    assert result.status == "COMPLETED"
    assert result.content == "no tools needed"
    assert len(model_client.calls) == 1
