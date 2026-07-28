# pylint: disable=redefined-outer-name
from unittest.mock import MagicMock

import pytest
from by_framework.core.extensions.agent_config import AgentConfig, CallbackType
from by_framework.core.extensions.registry import PluginRegistry
from by_framework.core.protocol.commands import AskAgentCommand
from by_framework.core.protocol.message_header import MessageHeader

from by_framework_agent.model_client import ModelChunk
from by_framework_agent.testing import StubModelClient
from by_framework_agent.worker import NativeAgentWorker


class _ChatWorker(NativeAgentWorker):

    def get_agent_types(self) -> list[str]:
        return ["assistant"]


def _agent_config(**overrides) -> AgentConfig:
    defaults: dict = {
        "agent_id": "assistant",
        "name": "Assistant",
        "prompts": {"system": "You are a helpful assistant."},
        "extra": {"model": "gpt-4o-mini"},
    }
    defaults.update(overrides)
    return AgentConfig(**defaults)


def _registry(configs: list[AgentConfig]) -> PluginRegistry:
    registry = PluginRegistry()
    registry._set_agent_configs(configs)  # pylint: disable=protected-access
    return registry


def _command(content: str = "Hi there") -> AskAgentCommand:
    return AskAgentCommand(
        header=MessageHeader(
            message_id="msg-1",
            session_id="session-1",
            trace_id="trace-1",
            user_code="user-1",
            user_name="user-1",
            target_agent_type="assistant",
        ),
        content=content,
    )


@pytest.mark.asyncio
async def test_plain_chat_turn_streams_and_completes(mock_redis, workspace_manager):
    model_client = StubModelClient(
        turns=[
            [
                ModelChunk(content="Hello"),
                ModelChunk(content=", world!"),
                ModelChunk(
                    is_final=True,
                    finish_reason="stop",
                    usage={"prompt_tokens": 12, "completion_tokens": 4},
                    cost=0.0002,
                    model="gpt-4o-mini",
                ),
            ]
        ]
    )
    worker = _ChatWorker(
        worker_id="agent-1",
        redis_client=mock_redis,
        registry=MagicMock(),
        workspace_manager=workspace_manager,
        plugin_registry=_registry([_agent_config()]),
        model_client=model_client,
    )

    result = await worker._handle_message(_command())  # pylint: disable=protected-access

    assert result.status == "COMPLETED"
    assert result.content == "Hello, world!"

    # Streamed deltas went out over the (mocked) data stream.
    pipe = mock_redis.pipeline.return_value
    payloads = [call.args[1]["data"] for call in pipe.xadd.call_args_list]
    assert any("Hello" in payload for payload in payloads)
    assert any(", world!" in payload for payload in payloads)

    # The model boundary is the one seam: exactly one call, with the system
    # prompt and the auto-saved user turn assembled from history.
    assert len(model_client.calls) == 1
    sent_messages = model_client.calls[0]["messages"]
    assert sent_messages[0] == {
        "role": "system",
        "content": "You are a helpful assistant.",
    }
    assert sent_messages[-1] == {"role": "user", "content": "Hi there"}
    assert model_client.calls[0]["model"] == "gpt-4o-mini"


@pytest.mark.asyncio
async def test_no_agent_config_fails_gracefully(mock_redis, workspace_manager):
    worker = _ChatWorker(
        worker_id="agent-1",
        redis_client=mock_redis,
        registry=MagicMock(),
        workspace_manager=workspace_manager,
        plugin_registry=_registry([]),
        model_client=StubModelClient(turns=[]),
    )

    result = await worker._handle_message(_command())  # pylint: disable=protected-access

    assert result.status == "FAILED"


@pytest.mark.asyncio
async def test_before_and_after_model_callbacks_fire(mock_redis, workspace_manager):
    calls: list[str] = []

    async def before(context, payload):  # pylint: disable=unused-argument
        calls.append("before")

    def after(context, payload):  # pylint: disable=unused-argument
        calls.append("after")

    config = _agent_config(
        callbacks={
            CallbackType.before_model_callback: [before],
            CallbackType.after_model_callback: [after],
        }
    )
    model_client = StubModelClient(
        turns=[[ModelChunk(content="hi", is_final=True, finish_reason="stop")]]
    )
    worker = _ChatWorker(
        worker_id="agent-1",
        redis_client=mock_redis,
        registry=MagicMock(),
        workspace_manager=workspace_manager,
        plugin_registry=_registry([config]),
        model_client=model_client,
    )

    await worker._handle_message(_command())  # pylint: disable=protected-access

    assert calls == ["before", "after"]


@pytest.mark.asyncio
async def test_token_usage_and_cost_recorded(mock_redis, workspace_manager):
    model_client = StubModelClient(
        turns=[
            [
                ModelChunk(content="hi"),
                ModelChunk(
                    is_final=True,
                    finish_reason="stop",
                    usage={"prompt_tokens": 7, "completion_tokens": 3},
                    cost=0.001,
                    model="gpt-4o-mini",
                ),
            ]
        ]
    )
    worker = _ChatWorker(
        worker_id="agent-1",
        redis_client=mock_redis,
        registry=MagicMock(),
        workspace_manager=workspace_manager,
        plugin_registry=_registry([_agent_config()]),
        model_client=model_client,
    )

    # Capture token usage by wrapping AgentContext.record_token_usage — the
    # context itself is created internally by _handle_message, so this is
    # the seam available to observe what the loop recorded on it.
    from by_framework.worker.context import AgentContext

    captured = {}
    original_record = AgentContext.record_token_usage

    def _spy(self, **kwargs):
        captured.update(kwargs)
        return original_record(self, **kwargs)

    AgentContext.record_token_usage = _spy
    try:
        await worker._handle_message(_command())  # pylint: disable=protected-access
    finally:
        AgentContext.record_token_usage = original_record

    assert captured == {
        "prompt_tokens": 7,
        "completion_tokens": 3,
        "model": "gpt-4o-mini",
        "cost": 0.001,
    }


@pytest.mark.asyncio
async def test_model_params_forwarded_to_model_client(mock_redis, workspace_manager):
    model_client = StubModelClient(
        turns=[[ModelChunk(content="ok", is_final=True, finish_reason="stop")]]
    )
    config = _agent_config(
        extra={
            "model": "gpt-4o-mini",
            "model_params": {"temperature": 0.2, "max_tokens": 500},
        }
    )
    worker = _ChatWorker(
        worker_id="agent-1",
        redis_client=mock_redis,
        registry=MagicMock(),
        workspace_manager=workspace_manager,
        plugin_registry=_registry([config]),
        model_client=model_client,
    )

    await worker._handle_message(_command())  # pylint: disable=protected-access

    assert model_client.calls[0]["params"] == {"temperature": 0.2, "max_tokens": 500}


@pytest.mark.asyncio
async def test_non_dict_model_params_fails_loudly(mock_redis, workspace_manager):
    config = _agent_config(extra={"model": "gpt-4o-mini", "model_params": "oops"})
    worker = _ChatWorker(
        worker_id="agent-1",
        redis_client=mock_redis,
        registry=MagicMock(),
        workspace_manager=workspace_manager,
        plugin_registry=_registry([config]),
        model_client=StubModelClient(turns=[]),
    )

    result = await worker._handle_message(_command())  # pylint: disable=protected-access

    assert result.status == "FAILED"
