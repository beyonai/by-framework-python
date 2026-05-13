import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from by_framework.common.emitter import (DefaultSseLayoutBuilder, GatewayDataEmitter)
from by_framework.core.protocol.content_type import SseMessageType


class CustomLayoutBuilder:

    def build(self, content, role, content_type, source_agent_type, **kwargs):
        return {
            "custom_id": "test-id",
            "text": content,
            "meta": {
                "agent": source_agent_type,
                "content_type": content_type,
                "order_id": kwargs["order_id"],
            },
        }

    def build_ask_user(
        self, prompt, source_agent_type, order_id=None, parent_order_id=None
    ):
        return {
            "custom_id": "ask-user",
            "prompt": prompt,
            "meta": {
                "agent": source_agent_type,
                "order_id": order_id,
                "parent_order_id": parent_order_id,
            },
        }


@pytest.mark.asyncio
async def test_emitter_uses_default_sse_builder():
    mock_redis = MagicMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock()
    mock_redis.pipeline.return_value = mock_pipe

    emitter = GatewayDataEmitter(redis_client=mock_redis)
    assert isinstance(emitter.layout_builder, DefaultSseLayoutBuilder)

    await emitter.emit_chunk(
        session_id="s1",
        trace_id="t1",
        event="hello",
        source_agent_type="agent-1",
        message_id="msg-1",
    )

    args, _ = mock_pipe.xadd.call_args
    payload = json.loads(args[1]["data"])
    data = payload["data"]

    assert "choices" in data
    assert data["orderId"] == "msg-1"
    assert data["choices"][0]["delta"]["content"] == "hello"


@pytest.mark.asyncio
async def test_emitter_uses_custom_layout_builder_hook():
    mock_redis = MagicMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock()
    mock_redis.pipeline.return_value = mock_pipe

    custom_builder = CustomLayoutBuilder()
    emitter = GatewayDataEmitter(redis_client=mock_redis, layout_builder=custom_builder)

    await emitter.emit_chunk(
        session_id="s1",
        trace_id="t1",
        event="hello custom",
        source_agent_type="agent-custom",
        message_id="msg-custom",
    )

    args, _ = mock_pipe.xadd.call_args
    payload = json.loads(args[1]["data"])
    data = payload["data"]

    assert data == {
        "custom_id": "test-id",
        "text": "hello custom",
        "meta": {
            "agent": "agent-custom",
            "content_type": SseMessageType.text.value,
            "order_id": "msg-custom",
        },
    }


@pytest.mark.asyncio
async def test_default_sse_builder_owns_ask_user_form_payload():
    mock_redis = MagicMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock()
    mock_redis.pipeline.return_value = mock_pipe

    emitter = GatewayDataEmitter(redis_client=mock_redis)

    await emitter.ask_user(
        session_id="s1",
        trace_id="t1",
        event="Need input",
        source_agent_type="agent-1",
        message_id="msg-ask",
    )

    args, _ = mock_pipe.xadd.call_args
    payload = json.loads(args[1]["data"])
    data = payload["data"]
    input_form = json.loads(data["choices"][0]["delta"]["content"])

    assert data["contentType"] == "3013"
    assert input_form == {
        "formStatus": 0,
        "pluginMachineFields": [
            {
                "formType": "textarea",
                "fieldName": "user_input",
                "fieldCode": "user_input",
                "description": "Need input",
                "required": True,
            }
        ],
    }


@pytest.mark.asyncio
async def test_ask_user_uses_custom_layout_builder_hook():
    mock_redis = MagicMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock()
    mock_redis.pipeline.return_value = mock_pipe

    emitter = GatewayDataEmitter(
        redis_client=mock_redis, layout_builder=CustomLayoutBuilder()
    )

    await emitter.ask_user(
        session_id="s1",
        trace_id="t1",
        event="Custom prompt",
        source_agent_type="agent-custom",
        message_id="msg-custom",
        parent_message_id="parent-custom",
    )

    args, _ = mock_pipe.xadd.call_args
    payload = json.loads(args[1]["data"])
    data = payload["data"]

    assert data == {
        "custom_id": "ask-user",
        "prompt": "Custom prompt",
        "meta": {
            "agent": "agent-custom",
            "order_id": "msg-custom",
            "parent_order_id": "parent-custom",
        },
    }
