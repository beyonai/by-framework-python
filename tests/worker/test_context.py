import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from by_framework import AgentContext
from by_framework.core.extensions.plugin import Plugin, PluginManifest
from by_framework.core.extensions.registry import PluginRegistry
from by_framework.core.protocol.byai_codec import ByaiContentCodec
from by_framework.core.protocol.commands import (AskAgentCommand, command_from_dict)
from by_framework.core.protocol.message import (BaiYingMessage, BaiYingMessageRole)


class RecordingCallAgentPlugin(Plugin):

    def __init__(self):
        super().__init__(PluginManifest(plugin_id="recording-call-agent"))
        self.events: list[tuple[str, str, str]] = []

    async def register_agent_configs(self, build_context):
        return None

    async def on_call_agent_start(self, context, command):
        self.events.append(
            ("start", command.header.target_agent_type, command.header.message_id)
        )

    async def on_call_agent_complete(self, context, command, result):
        self.events.append(("complete", result["status"], command.header.message_id))

    async def on_call_agent_error(self, context, command, error):
        self.events.append(("error", str(error), command.header.message_id))


class DenyAllPolicy:

    def check(
        self,
        operation: str,
        path: str,
        *,
        session_id: str,
        user_code: str,
    ) -> str | None:
        return f"blocked {operation} for {path} in {user_code}/{session_id}"


@pytest.mark.asyncio
async def test_context_call_agent_with_metadata():
    """Test that call_agent passes metadata to emitted command."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    # xadd is a true async method (await self.redis.xadd(...))
    mock_redis.xadd = AsyncMock()
    # pipeline() is a sync method, returning a Pipeline object
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    mock_redis.pipeline.return_value = mock_pipe
    # Mock for agent-type probing
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.zrangebyscore = AsyncMock(return_value=[b"worker-1"])
    mock_redis.get = AsyncMock(return_value=b"1")

    ctx = AgentContext(session_id="s1", trace_id="t1", redis_client=mock_redis)
    await ctx.call_agent(
        target_agent_type="test", content="hello", metadata={"ctx": "val"}
    )
    args, _ = mock_redis.xadd.call_args
    data = json.loads(args[1]["data"])
    command = command_from_dict(data)
    assert command.header.metadata == {"ctx": "val"}


@pytest.mark.asyncio
async def test_context_call_agent_emits_message_decodable_as_command():
    """Test that call_agent emits AskAgentCommand decodable from Redis."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    mock_redis.pipeline.return_value = mock_pipe
    # Mock for agent-type probing
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.zrangebyscore = AsyncMock(return_value=[b"worker-1"])
    mock_redis.get = AsyncMock(return_value=b"1")

    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        parent_message_id="msg-parent",
    )

    await ctx.call_agent(
        target_agent_type="agent-b",
        content="hello",
        extra_payload={"history": ["m1"]},
        wait_for_reply=True,
    )

    args, _ = mock_redis.xadd.call_args
    raw = json.loads(args[1]["data"])
    command = command_from_dict(raw)

    assert isinstance(command, AskAgentCommand)
    assert command.content == "hello"
    assert command.wait_for_reply is True
    assert command.extra_payload["history"] == ["m1"]


@pytest.mark.asyncio
async def test_context_call_agent_rejects_domain_content_without_codec():
    """Test that call_agent requires a codec for non-wire domain content."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    mock_redis.pipeline.return_value = mock_pipe
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.zrangebyscore = AsyncMock(return_value=[b"worker-1"])
    mock_redis.get = AsyncMock(return_value=b"1")

    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
    )

    with pytest.raises(TypeError, match="content codec"):
        await ctx.call_agent(
            target_agent_type="agent-b",
            content=BaiYingMessage(role=BaiYingMessageRole.USER, content="hello"),
        )


@pytest.mark.asyncio
async def test_context_call_agent_serializes_baiying_message_with_codec():
    """Test that call_agent serializes BaiYingMessage through the configured codec."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    mock_redis.pipeline.return_value = mock_pipe
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.zrangebyscore = AsyncMock(return_value=[b"worker-1"])
    mock_redis.get = AsyncMock(return_value=b"1")

    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        content_codec=ByaiContentCodec(),
    )

    await ctx.call_agent(
        target_agent_type="agent-b",
        content=BaiYingMessage(role=BaiYingMessageRole.USER, content="hello"),
    )

    args, _ = mock_redis.xadd.call_args
    raw = json.loads(args[1]["data"])
    command = command_from_dict(raw)

    assert isinstance(command, AskAgentCommand)
    assert command.content == [{"role": "user", "content": "hello"}]


@pytest.mark.asyncio
async def test_context_dispatch_group_serializes_baiying_message_with_codec():
    """Test that dispatch_group serializes BaiYingMessage via codec."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_redis.hset = AsyncMock()
    mock_redis.expire = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    mock_redis.pipeline.return_value = mock_pipe

    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
        content_codec=ByaiContentCodec(),
    )

    await ctx.dispatch_group(
        tasks=[
            {
                "target_agent_type": "agent-b",
                "content": BaiYingMessage(
                    role=BaiYingMessageRole.USER,
                    content="hello group",
                ),
            }
        ],
        wait_for_reply=False,
    )

    args, _ = mock_redis.xadd.call_args
    raw = json.loads(args[1]["data"])
    command = command_from_dict(raw)

    assert isinstance(command, AskAgentCommand)
    assert command.content == [{"role": "user", "content": "hello group"}]


@pytest.mark.asyncio
async def test_context_call_agent_triggers_plugin_lifecycle_hooks():
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    mock_redis.pipeline.return_value = mock_pipe
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.zrangebyscore = AsyncMock(return_value=[b"worker-1"])
    mock_redis.get = AsyncMock(return_value=b"1")

    plugin = RecordingCallAgentPlugin()
    registry = PluginRegistry()
    registry.register_bundle(plugin)

    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        plugin_registry=registry,
    )

    result = await ctx.call_agent(target_agent_type="agent-b", content="hello")

    assert result["status"]
    assert [event[0] for event in plugin.events] == ["start", "complete"]
    assert plugin.events[0][1] == "agent-b"
    assert plugin.events[1][1] == result["status"]


@pytest.mark.asyncio
async def test_context_call_agent_triggers_error_hook_on_dispatch_failure():
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock(side_effect=RuntimeError("redis down"))
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    mock_redis.pipeline.return_value = mock_pipe
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.zrangebyscore = AsyncMock(return_value=[b"worker-1"])
    mock_redis.get = AsyncMock(return_value=b"1")

    plugin = RecordingCallAgentPlugin()
    registry = PluginRegistry()
    registry.register_bundle(plugin)

    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        plugin_registry=registry,
    )

    with pytest.raises(RuntimeError, match="redis down"):
        await ctx.call_agent(target_agent_type="agent-b", content="hello")

    assert [event[0] for event in plugin.events] == ["start", "error"]
    assert "redis down" in plugin.events[1][1]


def test_context_reports_no_cancel_by_default():
    """Test that is_cancel_requested returns False when no cancel event is set."""
    ctx = AgentContext(session_id="s1", trace_id="t1")
    assert ctx.is_cancel_requested() is False


@pytest.mark.asyncio
async def test_context_check_cancelled_raises_when_event_set():
    """Test that check_cancelled raises CancelledError when cancel event is set."""
    event = asyncio.Event()
    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        cancel_event=event,
        cancel_reason="user aborted",
    )
    event.set()

    with pytest.raises(asyncio.CancelledError):
        await ctx.check_cancelled()


@pytest.mark.asyncio
async def test_context_injects_custom_file_permission_policy(tmp_path):
    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        workspace_dir=str(tmp_path),
        permission_policy=DenyAllPolicy(),
    )

    result = await ctx.agent_runtime_state.session_manager.file_manager.write_file(
        "sessions/s1/docs/guide.md",
        "# hello\n",
    )

    assert result["success"] is False
    assert "blocked write" in result["error"]
