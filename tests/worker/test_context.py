import asyncio
import json
import sys
import types
from unittest.mock import AsyncMock

import pytest

import by_framework.worker.context as context_module
from by_framework import AgentContext
from by_framework.common.constants import TASK_GROUP_FIELD_ABORTED, RedisKeys
from by_framework.core.extensions.plugin import Plugin, PluginManifest
from by_framework.core.extensions.registry import PluginRegistry
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.protocol.byai_codec import ByaiContentCodec
from by_framework.core.protocol.commands import (AskAgentCommand, command_from_dict)
from by_framework.core.protocol.message import (BaiYingMessage, BaiYingMessageRole)
from by_framework.trace.span_recorder import str_to_uint64


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
    assert command.header.metadata["ctx"] == "val"
    assert command.header.metadata["framework_parent_span_id"] == (
        f"{command.header.message_id}:client.dispatch"
    )
    assert command.header.metadata["trace_parent_span_id"] == (
        command.header.trace_parent_span_id
    )


@pytest.mark.asyncio
async def test_context_call_agent_propagates_langfuse_observation_id():
    """Test that call_agent propagates _langfuse_observation id if present."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    mock_redis.pipeline.return_value = mock_pipe
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.zrangebyscore = AsyncMock(return_value=[b"worker-1"])
    mock_redis.get = AsyncMock(return_value=b"1")

    ctx = AgentContext(session_id="s1", trace_id="t1", redis_client=mock_redis)

    class DummyObservation:
        id = "dummy-obs-id-123"

    ctx._langfuse_observation = DummyObservation()

    await ctx.call_agent(
        target_agent_type="test", content="hello", metadata={"ctx": "val"}
    )
    args, _ = mock_redis.xadd.call_args
    data = json.loads(args[1]["data"])
    command = command_from_dict(data)
    assert command.header.langfuse_parent_observation_id == "dummy-obs-id-123"
    assert command.header.metadata["ctx"] == "val"


@pytest.mark.asyncio
async def test_context_call_agent_prefers_langfuse_call_parent_observation_id():
    """Async child calls should parent to the durable workflow observation."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    mock_redis.pipeline.return_value = mock_pipe
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.zrangebyscore = AsyncMock(return_value=[b"worker-1"])
    mock_redis.get = AsyncMock(return_value=b"1")

    ctx = AgentContext(session_id="s1", trace_id="t1", redis_client=mock_redis)

    class TaskObservation:
        id = "agent-task-obs"

    class WorkflowObservation:
        id = "workflow-obs"

    ctx._langfuse_observation = TaskObservation()
    ctx._langfuse_call_parent_observation = WorkflowObservation()

    await ctx.call_agent(target_agent_type="test", content="hello")
    args, _ = mock_redis.xadd.call_args
    data = json.loads(args[1]["data"])
    command = command_from_dict(data)
    assert command.header.langfuse_parent_observation_id == "workflow-obs"


@pytest.mark.asyncio
async def test_context_call_agent_prefers_current_langfuse_tool_observation(
    monkeypatch,
):
    """LangGraph tool calls should parent remote calls to the active tool span."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    mock_redis.pipeline.return_value = mock_pipe
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.zrangebyscore = AsyncMock(return_value=[b"worker-1"])
    mock_redis.get = AsyncMock(return_value=b"1")

    ctx = AgentContext(session_id="s1", trace_id="trace-tool", redis_client=mock_redis)

    class FakeLangfuseClient:

        @staticmethod
        def get_current_observation_id():
            return "obs-query-weather"

    class FakeSpanContext:
        is_valid = True
        span_id = str_to_uint64("langgraph-parent")

    class FakeSpan:

        @staticmethod
        def get_span_context():
            return FakeSpanContext()

    mock_trace = types.ModuleType("opentelemetry.trace")
    mock_trace.get_current_span = FakeSpan
    mock_otel_module = types.ModuleType("opentelemetry")
    mock_otel_module.trace = mock_trace
    mock_langfuse = types.ModuleType("langfuse")

    def get_langfuse_client():
        return FakeLangfuseClient()

    mock_langfuse.get_client = get_langfuse_client

    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
    monkeypatch.setenv("LANGFUSE_BASE_URL", "http://localhost:3000")
    monkeypatch.setattr(context_module, "_LANGFUSE_CURRENT_OBSERVATION_GETTER", None)
    monkeypatch.setitem(sys.modules, "opentelemetry", mock_otel_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.trace", mock_trace)
    monkeypatch.setitem(sys.modules, "langfuse", mock_langfuse)

    await ctx.call_agent(target_agent_type="weather-agent", content="weather")

    args, _ = mock_redis.xadd.call_args
    data = json.loads(args[1]["data"])
    command = command_from_dict(data)
    assert command.header.langfuse_parent_observation_id == "obs-query-weather"


def test_context_current_langfuse_observation_getter_is_cached(monkeypatch):
    """Langfuse module/client lookup is cached across calls."""
    get_client_calls = 0
    get_observation_calls = 0

    class FakeLangfuseClient:

        @staticmethod
        def get_current_observation_id():
            nonlocal get_observation_calls
            get_observation_calls += 1
            return "obs-tool"

    mock_langfuse = types.ModuleType("langfuse")

    def get_langfuse_client():
        nonlocal get_client_calls
        get_client_calls += 1
        return FakeLangfuseClient()

    mock_langfuse.get_client = get_langfuse_client

    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
    monkeypatch.setenv("LANGFUSE_BASE_URL", "http://localhost:3000")
    monkeypatch.setattr(context_module, "_LANGFUSE_CURRENT_OBSERVATION_GETTER", None)
    monkeypatch.setitem(sys.modules, "langfuse", mock_langfuse)

    assert AgentContext._current_langfuse_observation_id() == "obs-tool"
    assert AgentContext._current_langfuse_observation_id() == "obs-tool"
    assert get_client_calls == 1
    assert get_observation_calls == 2


@pytest.mark.asyncio
async def test_context_call_agent_prefers_metadata_langfuse_parent():
    """Tool integrations can explicitly pass the exact Langfuse parent id."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    mock_redis.pipeline.return_value = mock_pipe
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.zrangebyscore = AsyncMock(return_value=[b"worker-1"])
    mock_redis.get = AsyncMock(return_value=b"1")

    ctx = AgentContext(session_id="s1", trace_id="trace-tool", redis_client=mock_redis)

    await ctx.call_agent(
        target_agent_type="weather-agent",
        content="weather",
        metadata={"langfuse_parent_observation_id": "obs-query-weather"},
    )

    args, _ = mock_redis.xadd.call_args
    data = json.loads(args[1]["data"])
    command = command_from_dict(data)
    assert command.header.langfuse_parent_observation_id == "obs-query-weather"
    assert command.header.metadata["langfuse_parent_observation_id"] == (
        "obs-query-weather"
    )


@pytest.mark.asyncio
async def test_context_call_agent_propagates_current_otel_span_id(monkeypatch):
    """External commands receive current OTel span id for generic APM joins."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    mock_redis.pipeline.return_value = mock_pipe
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.zrangebyscore = AsyncMock(return_value=[b"worker-1"])
    mock_redis.get = AsyncMock(return_value=b"1")

    ctx = AgentContext(session_id="s1", trace_id="trace-otel", redis_client=mock_redis)
    span_id = str_to_uint64("exec-parent:worker.execute")

    class FakeSpanContext:
        is_valid = True

        def __init__(self, span_id_value):
            self.span_id = span_id_value

    class FakeSpan:

        def get_span_context(self):
            return FakeSpanContext(span_id)

    mock_trace = types.ModuleType("opentelemetry.trace")
    mock_trace.get_current_span = FakeSpan
    mock_otel_module = types.ModuleType("opentelemetry")
    mock_otel_module.trace = mock_trace
    monkeypatch.setitem(sys.modules, "opentelemetry", mock_otel_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.trace", mock_trace)

    await ctx.call_agent(target_agent_type="test", content="hello")

    args, _ = mock_redis.xadd.call_args
    data = json.loads(args[1]["data"])
    command = command_from_dict(data)
    assert command.header.trace_parent_span_id == (f"{span_id:016x}")


@pytest.mark.asyncio
async def test_context_dispatch_group_propagates_langfuse_observation_id():
    """Test that dispatch_group propagates _langfuse_observation id if present."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_redis.hset = AsyncMock()
    mock_redis.expire = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    mock_redis.pipeline.return_value = mock_pipe
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.get = AsyncMock(return_value=b"1")

    ctx = AgentContext(session_id="s1", trace_id="t1", redis_client=mock_redis)

    class DummyObservation:
        id = "dummy-obs-id-456"

    ctx._langfuse_observation = DummyObservation()

    await ctx.dispatch_group(
        tasks=[
            {
                "target_agent_type": "agent-b",
                "content": "hello group",
                "metadata": {"custom": "x"},
            }
        ],
        wait_for_reply=False,
    )

    args, _ = mock_redis.xadd.call_args
    data = json.loads(args[1]["data"])
    command = command_from_dict(data)
    assert command.header.langfuse_parent_observation_id == "dummy-obs-id-456"
    assert command.header.metadata["custom"] == "x"


@pytest.mark.asyncio
async def test_context_dispatch_group_prefers_metadata_langfuse_parent():
    """Each fan-out task can explicitly choose its Langfuse parent."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_redis.hset = AsyncMock()
    mock_redis.expire = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    mock_redis.pipeline.return_value = mock_pipe
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.get = AsyncMock(return_value=b"1")

    ctx = AgentContext(session_id="s1", trace_id="t1", redis_client=mock_redis)

    class DummyObservation:
        id = "context-parent"

    ctx._langfuse_observation = DummyObservation()

    await ctx.dispatch_group(
        tasks=[
            {
                "target_agent_type": "agent-b",
                "content": "hello group",
                "metadata": {"langfuse_parent_observation_id": "explicit-parent"},
            }
        ],
        wait_for_reply=False,
    )

    args, _ = mock_redis.xadd.call_args
    data = json.loads(args[1]["data"])
    command = command_from_dict(data)
    assert command.header.langfuse_parent_observation_id == "explicit-parent"
    assert command.header.metadata["langfuse_parent_observation_id"] == (
        "explicit-parent"
    )


@pytest.mark.asyncio
async def test_context_dispatch_group_triggers_plugin_lifecycle_hooks():
    """Parallel fan-out creates per-child call observations through plugins."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_redis.hset = AsyncMock()
    mock_redis.expire = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    mock_redis.pipeline.return_value = mock_pipe
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.get = AsyncMock(return_value=b"1")

    plugin = RecordingCallAgentPlugin()
    registry = PluginRegistry()
    registry.register_bundle(plugin)

    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
        plugin_registry=registry,
    )

    result = await ctx.dispatch_group(
        tasks=[
            {"target_agent_type": "agent-b", "content": "one"},
            {"target_agent_type": "agent-c", "content": "two"},
        ],
    )

    assert result["status"] == AgentState.QUEUED.value
    assert [event[0] for event in plugin.events] == [
        "start",
        "complete",
        "start",
        "complete",
    ]
    assert {event[1] for event in plugin.events if event[0] == "start"} == {
        "agent-b",
        "agent-c",
    }


@pytest.mark.asyncio
async def test_context_call_agents_dispatches_like_dispatch_group():
    """call_agents is call_agent's plural: same dispatch, same status vocabulary."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_redis.hset = AsyncMock()
    mock_redis.expire = AsyncMock()
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.get = AsyncMock(return_value=b"1")

    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
    )

    result = await ctx.call_agents(
        tasks=[
            {"target_agent_type": "agent-b", "content": "one"},
            {"target_agent_type": "agent-c", "content": "two"},
        ],
    )

    assert result["status"] == AgentState.QUEUED.value
    assert len(result["dispatched_tasks"]) == 2
    assert mock_redis.xadd.await_count == 2


@pytest.mark.asyncio
async def test_context_call_agents_marks_unavailable_task_failed():
    """An offline target agent type fails fast as one group member, the
    rest of the batch still dispatches — and the failure is compensated by a
    reply, never by the dispatcher booking the group itself."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_redis.hset = AsyncMock()
    mock_redis.expire = AsyncMock()
    mock_redis.hincrby = AsyncMock(return_value=1)
    mock_redis.zadd = AsyncMock()
    mock_redis.delete = AsyncMock()

    async def smembers_side_effect(name):
        if name == RedisKeys.agent_type_members("agent-b"):
            return {b"worker-1"}
        return set()

    mock_redis.smembers = AsyncMock(side_effect=smembers_side_effect)
    mock_redis.get = AsyncMock(return_value=b"1")

    plugin = RecordingCallAgentPlugin()
    registry = PluginRegistry()
    registry.register_bundle(plugin)

    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
        plugin_registry=registry,
    )

    result = await ctx.call_agents(
        tasks=[
            {"target_agent_type": "agent-b", "content": "one"},
            {"target_agent_type": "agent-c", "content": "two"},
        ],
    )

    assert result["status"] == AgentState.QUEUED.value
    assert len(result["dispatched_tasks"]) == 2
    task_group_id = result["task_group_id"]

    # Only the available target actually got dispatched.
    assert mock_redis.xadd.await_count == 1
    xadd_args, _ = mock_redis.xadd.call_args
    dispatched_command = command_from_dict(json.loads(xadd_args[1]["data"]))
    assert dispatched_command.header.target_agent_type == "agent-b"

    # The dispatcher books NOTHING for the unavailable task: no result, no
    # `completed` increment. A second writer of the group's accounting is what
    # hangs the caller when its increment is the one that reaches `total` and
    # no reply is left to run the join.
    assert mock_redis.hincrby.await_count == 0
    results_writes = [c for c in mock_redis.hset.await_args_list if len(c.args) >= 3]
    assert results_writes == []

    # Instead, the reply a sub-agent would have sent is queued for the worker
    # to flush once process_command returns.
    assert len(ctx._pending_group_replies) == 1
    stand_in = ctx._pending_group_replies[0]
    # Addressed at the caller's suspended execution, identifying the sub-task.
    assert stand_in.header.message_id == "parent-msg"
    assert (
        stand_in.header.parent_message_id == result["dispatched_tasks"][1]["message_id"]
    )
    assert stand_in.header.task_group_id == task_group_id
    assert stand_in.header.target_agent_type == "agent-a"
    assert stand_in.header.source_agent_type == "agent-c"
    assert stand_in.status == AgentState.FAILED.value
    # Failure detail where a sub-agent that ran and failed puts it, so callers
    # cannot tell "failed" from "never got to fail".
    assert stand_in.reply_data["error_code"] == "AGENT_TYPE_UNAVAILABLE"

    # The sub-task is registered in the wait index like any other, so a sweep
    # can still compensate the group if the stand-in is never delivered.
    registered = {
        (member.child_message_id, member.task_group_id)
        for member, score in _wait_entries(mock_redis)
    }
    assert (stand_in.header.parent_message_id, task_group_id) in registered

    # Same plugin hooks call_agent's own availability failure fires: a
    # "start"/"error" pair for the unavailable agent-c, alongside the
    # normal "start"/"complete" pair for the dispatched agent-b.
    assert [event[0] for event in plugin.events] == [
        "start",
        "complete",
        "start",
        "error",
    ]
    assert {e[1] for e in plugin.events if e[0] == "start"} == {"agent-b", "agent-c"}
    error_event = next(e for e in plugin.events if e[0] == "error")
    assert stand_in.reply_data["error"] in error_event[1]


@pytest.mark.asyncio
async def test_context_dispatch_group_rejects_empty_task_list():
    """An empty task list is a caller bug, not a valid no-op group."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.hset = AsyncMock()
    mock_redis.expire = AsyncMock()
    mock_redis.xadd = AsyncMock()

    ctx = AgentContext(session_id="s1", trace_id="t1", redis_client=mock_redis)

    with pytest.raises(ValueError):
        await ctx.dispatch_group(tasks=[])

    mock_redis.hset.assert_not_called()
    mock_redis.xadd.assert_not_called()


@pytest.mark.asyncio
async def test_context_dispatch_group_triggers_error_hook_on_dispatch_failure():
    """Fan-out dispatch failures end the per-child plugin observation as an error."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock(side_effect=RuntimeError("redis down"))
    mock_redis.hset = AsyncMock()
    mock_redis.expire = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    mock_redis.pipeline.return_value = mock_pipe
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.get = AsyncMock(return_value=b"1")

    plugin = RecordingCallAgentPlugin()
    registry = PluginRegistry()
    registry.register_bundle(plugin)

    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
        plugin_registry=registry,
    )

    with pytest.raises(RuntimeError, match="redis down"):
        await ctx.dispatch_group(
            tasks=[{"target_agent_type": "agent-b", "content": "one"}],
        )

    assert [event[0] for event in plugin.events] == ["start", "error"]
    assert plugin.events[0][1] == "agent-b"
    assert "redis down" in plugin.events[1][1]


@pytest.mark.asyncio
async def test_context_call_agents_marks_group_aborted_on_mid_dispatch_failure():
    """A dispatch-time infra error stops the batch and marks the Task Group
    aborted, so already-sent siblings' replies won't later resume a caller
    execution that already ended in failure."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock(side_effect=[None, RuntimeError("redis down")])
    mock_redis.hset = AsyncMock()
    mock_redis.expire = AsyncMock()
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.get = AsyncMock(return_value=b"1")

    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
    )

    with pytest.raises(RuntimeError, match="redis down"):
        await ctx.call_agents(
            tasks=[
                {"target_agent_type": "agent-b", "content": "one"},
                {"target_agent_type": "agent-c", "content": "two"},
                {"target_agent_type": "agent-d", "content": "three"},
            ],
        )

    # Only the two tasks up to (and including) the failing one were attempted.
    assert mock_redis.xadd.await_count == 2

    abort_writes = [
        c
        for c in mock_redis.hset.await_args_list
        if len(c.args) >= 2 and c.args[1] == TASK_GROUP_FIELD_ABORTED
    ]
    assert len(abort_writes) == 1
    assert abort_writes[0].args[2] == "1"


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
async def test_context_call_agent_records_dispatch_span():
    """Nested agent dispatch writes a child dispatch span into the same trace."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    mock_redis.pipeline.return_value = mock_pipe
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.zrangebyscore = AsyncMock(return_value=[b"worker-1"])
    mock_redis.get = AsyncMock(return_value=b"1")
    span_recorder = AsyncMock()

    ctx = AgentContext(
        session_id="s1",
        trace_id="trace-call",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
        execution_id="exec-parent",
        span_recorder=span_recorder,
    )

    await ctx.call_agent(
        target_agent_type="agent-b",
        content="hello",
        wait_for_reply=True,
        message_id="child-msg",
    )

    span_recorder.record_span.assert_awaited_once()
    span = span_recorder.record_span.await_args.args[0]
    assert span.trace_id == "trace-call"
    assert span.span_id == "child-msg:client.dispatch"
    assert span.parent_span_id == "exec-parent:worker.execute"
    assert span.operation == "client.dispatch"
    assert span.component == "agent_context"
    assert span.session_id == "s1"
    assert span.message_id == "child-msg"
    assert span.parent_message_id == "parent-msg"
    assert span.source_agent_type == "agent-a"
    assert span.target_agent_type == "agent-b"
    assert span.status == "COMPLETED"


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
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.get = AsyncMock(return_value=b"1")

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
async def test_context_dispatch_group_records_dispatch_spans():
    """Scatter-gather dispatch writes one dispatch span per child task
    plus one aggregate.
    """
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_redis.hset = AsyncMock()
    mock_redis.expire = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    mock_redis.pipeline.return_value = mock_pipe
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.get = AsyncMock(return_value=b"1")
    span_recorder = AsyncMock()

    ctx = AgentContext(
        session_id="s1",
        trace_id="trace-group",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
        execution_id="exec-parent",
        span_recorder=span_recorder,
    )

    await ctx.dispatch_group(
        [
            {"target_agent_type": "agent-b", "content": "one"},
            {"target_agent_type": "agent-c", "content": "two"},
        ]
    )

    # 2 child dispatch spans + 1 aggregate agent.dispatch_group span
    assert span_recorder.record_span.await_count == 3
    spans = [call.args[0] for call in span_recorder.record_span.await_args_list]
    dispatch_spans = [s for s in spans if s.operation == "client.dispatch"]
    group_spans = [s for s in spans if s.operation == "agent.dispatch_group"]
    assert len(dispatch_spans) == 2
    assert len(group_spans) == 1
    assert {s.target_agent_type for s in dispatch_spans} == {"agent-b", "agent-c"}
    assert all(s.parent_span_id == "exec-parent:worker.execute" for s in dispatch_spans)
    assert group_spans[0].parent_span_id == "exec-parent:worker.execute"
    assert group_spans[0].metadata["task_count"] == 2


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


@pytest.mark.asyncio
async def test_context_rolls_back_suspend_flag_when_dispatch_is_rejected():
    """A dispatch the availability check rejected never suspends the caller.

    _is_suspended is set optimistically before the availability check, and
    the framework reads it to decide whether to close the caller's output
    stream (worker.py's should_emit_stream_end). Leaving it set after a
    FAILED route means a caller that is in fact still running is treated as
    suspended, and its stream is never closed.
    """
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_redis.hset = AsyncMock()
    mock_redis.expire = AsyncMock()
    mock_redis.smembers = AsyncMock(return_value=set())
    mock_redis.get = AsyncMock(return_value=None)

    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
    )

    result = await ctx.call_agent("offline-agent", "hello")

    assert result["status"] == AgentState.FAILED.value
    assert ctx._is_suspended is False
    assert ctx._permission_transferred is False


@pytest.mark.asyncio
async def test_context_rejected_dispatch_preserves_an_earlier_suspend():
    """Roll back to the value on entry, not to False.

    The same context may already be legitimately suspended by an earlier
    call_agent; a later rejected dispatch must not un-suspend it.
    """
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_redis.hset = AsyncMock()
    mock_redis.expire = AsyncMock()

    async def smembers_side_effect(name):
        if name == RedisKeys.agent_type_members("online-agent"):
            return {b"worker-1"}
        return set()

    mock_redis.smembers = AsyncMock(side_effect=smembers_side_effect)
    mock_redis.get = AsyncMock(return_value=b"1")

    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
    )

    first = await ctx.call_agent("online-agent", "hello")
    assert first["status"] == AgentState.QUEUED.value
    assert ctx._is_suspended is True

    second = await ctx.call_agent("offline-agent", "hello")
    assert second["status"] == AgentState.FAILED.value
    assert ctx._is_suspended is True


@pytest.mark.asyncio
async def test_context_rejected_fire_and_forget_keeps_stream_permission():
    """wait_for_reply=False hands the output stream to the callee — but only
    if the dispatch actually happened."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_redis.hset = AsyncMock()
    mock_redis.expire = AsyncMock()
    mock_redis.smembers = AsyncMock(return_value=set())
    mock_redis.get = AsyncMock(return_value=None)

    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
    )

    result = await ctx.call_agent("offline-agent", "hello", wait_for_reply=False)

    assert result["status"] == AgentState.FAILED.value
    assert ctx._permission_transferred is False
    assert ctx._is_suspended is False


def _online_redis():
    """Redis double whose availability probes report every agent type online."""
    from unittest.mock import MagicMock

    mock_redis = MagicMock()
    mock_redis.xadd = AsyncMock()
    mock_redis.hset = AsyncMock()
    mock_redis.expire = AsyncMock()
    mock_redis.hincrby = AsyncMock()
    mock_redis.zadd = AsyncMock()
    mock_redis.delete = AsyncMock()
    mock_redis.smembers = AsyncMock(return_value={b"worker-1"})
    mock_redis.get = AsyncMock(return_value=b"1")
    mock_redis.pipeline = MagicMock(
        return_value=MagicMock(
            xadd=MagicMock(), expire=MagicMock(), execute=AsyncMock(return_value=[])
        )
    )
    return mock_redis


def _wait_entries(mock_redis, session_id="s1"):
    """Every (member, score) pair written to this session's wait-index shard."""
    from by_framework.core.wait_index import decode_member, wait_index_key

    entries = []
    for call in mock_redis.zadd.await_args_list:
        if call.args[0] != wait_index_key(session_id):
            continue
        for member, score in call.args[1].items():
            entries.append((decode_member(member), score))
    return entries


@pytest.mark.asyncio
async def test_call_agent_registers_wait_index_entry():
    """A suspended caller must be findable by something other than the reply
    it is waiting for; the wait index is that something."""
    import time

    from by_framework.common.constants import DEFAULT_REPLY_TIMEOUT_MS

    mock_redis = _online_redis()
    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
    )

    before_ms = int(time.time() * 1000)
    result = await ctx.call_agent("agent-b", "hello")

    entries = _wait_entries(mock_redis)
    assert len(entries) == 1
    member, score = entries[0]
    assert member.session_id == "s1"
    # The caller's own id — what the reply carries as header.message_id and
    # what the suspended execution is reattached by.
    assert member.parent_message_id == "parent-msg"
    assert member.child_message_id == result["message_id"]
    assert member.task_group_id == ""
    assert (
        before_ms + DEFAULT_REPLY_TIMEOUT_MS
        <= score
        <= (int(time.time() * 1000) + DEFAULT_REPLY_TIMEOUT_MS)
    )


@pytest.mark.asyncio
async def test_call_agent_reply_timeout_ms_overrides_the_default():
    import time

    mock_redis = _online_redis()
    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
    )

    await ctx.call_agent("agent-b", "hello", reply_timeout_ms=5_000)

    _, score = _wait_entries(mock_redis)[0]
    assert score <= int(time.time() * 1000) + 5_000


@pytest.mark.asyncio
async def test_fire_and_forget_call_registers_no_wait_entry():
    """wait_for_reply=False never suspends, so nothing is waiting."""
    mock_redis = _online_redis()
    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
    )

    await ctx.call_agent("agent-b", "hello", wait_for_reply=False)

    assert _wait_entries(mock_redis) == []


@pytest.mark.asyncio
async def test_wait_index_write_failure_does_not_break_dispatch():
    """Bookkeeping is not the thing the caller is waiting on: a failed
    registration costs the safety net, never the dispatch itself."""
    mock_redis = _online_redis()
    mock_redis.zadd = AsyncMock(side_effect=RuntimeError("redis down"))
    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
    )

    result = await ctx.call_agent("agent-b", "hello")

    assert result["status"] == AgentState.QUEUED.value
    assert mock_redis.xadd.await_count == 1


@pytest.mark.asyncio
async def test_registering_a_wait_voids_the_previous_consumed_verdict():
    """Consecutive ask_user rounds in one execution encode to the SAME
    member (there is no sub-task id to tell them apart), so round 1's
    "already consumed" marker outlives round 1. Leaving it in place would
    let the gate drop round 2's answer the moment its entry goes missing —
    the one direction this subsystem must never fail in."""
    from by_framework.core.wait_gate import consumed_marker_key
    from by_framework.core.wait_index import encode_member

    mock_redis = _online_redis()
    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="caller-msg",
    )

    await ctx.ask_user("what colour?")

    member = encode_member("s1", "caller-msg", "", "")
    mock_redis.delete.assert_awaited_once_with(consumed_marker_key("s1", member))


@pytest.mark.asyncio
async def test_clearing_the_consumed_marker_cannot_break_registration():
    """The entry is what matters; while it exists the marker is never read."""
    mock_redis = _online_redis()
    mock_redis.delete = AsyncMock(side_effect=RuntimeError("redis down"))
    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
    )

    await ctx.call_agent("agent-b", "hello")

    assert len(_wait_entries(mock_redis)) == 1


@pytest.mark.asyncio
async def test_call_agents_registers_one_wait_entry_per_sub_task():
    """Each group member can go missing on its own, so each needs its own
    entry — and each must carry the group id so an orphan can be resolved
    through the group's join accounting instead of waking the caller."""
    mock_redis = _online_redis()
    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
    )

    group = await ctx.call_agents(
        tasks=[
            {"target_agent_type": "agent-b", "content": "one"},
            {"target_agent_type": "agent-c", "content": "two"},
        ],
    )

    entries = _wait_entries(mock_redis)
    assert len(entries) == 2
    assert {member.task_group_id for member, _ in entries} == {group["task_group_id"]}
    assert {member.child_message_id for member, _ in entries} == {
        task["message_id"] for task in group["dispatched_tasks"]
    }
    assert {member.parent_message_id for member, _ in entries} == {"parent-msg"}


@pytest.mark.asyncio
async def test_call_agents_refuses_to_pin_one_message_id_across_a_batch():
    """A group's per-sibling identity IS its sub-task message_id.

    Pinning one across the fan-out collapses every sibling onto the same
    task_group_results key AND the same wait-index member, so their results
    overwrite each other and the idempotency gate — correctly — claims the
    single entry once and drops the rest, leaving `completed` short of
    `total` and the caller suspended forever. It has always been broken for a
    batch; the gate only upgraded scrambled results into a hang. Minting ids
    silently instead would swap one invisible behaviour for another.
    """
    mock_redis = _online_redis()
    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
    )

    with pytest.raises(ValueError, match="cannot pin message_id"):
        await ctx.call_agents(
            tasks=[
                {"target_agent_type": "agent-b", "content": "one"},
                {"target_agent_type": "agent-c", "content": "two"},
            ],
            message_id="pinned-msg",
        )

    # Rejected before anything was written: no group tracker, no dispatch,
    # no wait entry to leak.
    mock_redis.xadd.assert_not_called()
    mock_redis.hset.assert_not_called()
    assert _wait_entries(mock_redis) == []


@pytest.mark.asyncio
async def test_dispatch_group_alias_rejects_a_pinned_message_id_too():
    """dispatch_group shares call_agents' implementation, so it must share
    the guard — an alias that skipped it would be the way around it."""
    mock_redis = _online_redis()
    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
    )

    with pytest.raises(ValueError, match="cannot pin message_id"):
        await ctx.dispatch_group(
            tasks=[
                {"target_agent_type": "agent-b", "content": "one"},
                {"target_agent_type": "agent-c", "content": "two"},
            ],
            message_id="pinned-msg",
        )


@pytest.mark.asyncio
async def test_call_agents_still_accepts_a_message_id_for_a_single_task():
    """One id for one sub-task is exactly right — nothing collides — so the
    guard has to be about the batch, not about the parameter."""
    mock_redis = _online_redis()
    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
    )

    group = await ctx.call_agents(
        tasks=[{"target_agent_type": "agent-b", "content": "one"}],
        message_id="chosen-msg",
    )

    assert [task["message_id"] for task in group["dispatched_tasks"]] == ["chosen-msg"]
    entries = _wait_entries(mock_redis)
    assert [member.child_message_id for member, _ in entries] == ["chosen-msg"]


@pytest.mark.asyncio
async def test_ask_user_registers_wait_entry_with_its_own_timeout():
    """ask_user waits on a human, so it must not inherit call_agent's
    machine-scale deadline; its member has no sub-task id by convention."""
    import time

    from by_framework.common.constants import (
        DEFAULT_ASK_USER_TIMEOUT_MS,
        DEFAULT_REPLY_TIMEOUT_MS,
    )

    mock_redis = _online_redis()
    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
    )

    result = await ctx.ask_user("what next?")

    assert result["status"] == AgentState.WAITING_USER.value
    member, score = _wait_entries(mock_redis)[0]
    assert member.parent_message_id == "parent-msg"
    assert member.child_message_id == ""
    assert member.task_group_id == ""
    assert score > int(time.time() * 1000) + DEFAULT_REPLY_TIMEOUT_MS
    assert score <= int(time.time() * 1000) + DEFAULT_ASK_USER_TIMEOUT_MS


@pytest.mark.asyncio
async def test_suspended_state_records_what_the_execution_waits_on():
    """The framework persists this, not the business status: WAITING_AGENT and
    WAITING_USER must be distinguishable, and a rejected dispatch must leave
    neither behind."""
    mock_redis = _online_redis()
    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
    )
    assert ctx._suspended_state == ""

    await ctx.call_agent("agent-b", "hello")
    assert ctx._suspended_state == AgentState.WAITING_AGENT.value

    offline = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=_online_redis(),
        current_agent_id="agent-a",
        message_id="parent-msg",
    )
    offline.redis.smembers = AsyncMock(return_value=set())
    offline.redis.get = AsyncMock(return_value=None)
    assert (await offline.call_agent("agent-b", "hi"))["status"] == (
        AgentState.FAILED.value
    )
    assert offline._suspended_state == ""

    asked = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=_online_redis(),
        current_agent_id="agent-a",
        message_id="parent-msg",
    )
    await asked.ask_user("prompt")
    assert asked._suspended_state == AgentState.WAITING_USER.value


@pytest.mark.asyncio
async def test_dispatch_records_task_group_id_on_the_execution():
    """A sub-task that itself suspends rebuilds its caller (and its group)
    from this record — the resume message only describes the hop below it."""
    from unittest.mock import patch

    mock_redis = _online_redis()
    ctx = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=mock_redis,
        current_agent_id="agent-a",
        message_id="parent-msg",
    )

    with patch(
        "by_framework.core.registry.WorkerRegistry.initialize_execution",
        new_callable=AsyncMock,
    ) as init_execution:
        group = await ctx.call_agents(
            tasks=[{"target_agent_type": "agent-b", "content": "one"}],
        )

    payload = init_execution.await_args.args[0]
    assert payload["task_group_id"] == group["task_group_id"]
    assert payload["source_agent_type"] == "agent-a"
