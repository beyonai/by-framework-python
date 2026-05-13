"""Tests for adapter and worker modules."""

import sys
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from by_framework.core.protocol.commands import AskAgentCommand, ResumeCommand
from by_framework.core.protocol.message_header import MessageHeader

from by_framework_langgraph.adapter import LangGraphAdapter
from by_framework_langgraph.worker import LangGraphWorker


def _make_mock_context(session_id: str = "test-session"):
    """Create a mock AgentContext."""
    ctx = MagicMock()
    ctx.session_id = session_id
    ctx.trace_id = "trace-ctx"
    ctx.message_id = "msg-ctx"
    ctx.parent_message_id = "parent-ctx"
    ctx.current_agent_id = "planner"
    ctx.redis = AsyncMock()
    ctx.emit_chunk = AsyncMock()
    ctx.ask_user = AsyncMock()
    ctx.call_agent = AsyncMock()
    ctx.current_command = MagicMock(
        header=MessageHeader(
            message_id="msg-ctx",
            session_id=session_id,
            trace_id="trace-ctx",
            target_agent_type="planner",
            parent_message_id="parent-ctx",
            user_code="user-1",
            user_name="Alice",
            metadata={"source": "test"},
        )
    )
    ctx._langfuse_observation = SimpleNamespace(id="obs-framework")  # pylint: disable=protected-access
    return ctx


def _make_header(**kwargs):
    """Create a MessageHeader with default values overridden by kwargs."""
    defaults = {
        "message_id": "msg-001",
        "session_id": "sess-001",
        "trace_id": "trace-001",
    }
    defaults.update(kwargs)
    return MessageHeader(**defaults)


class TestLangGraphAdapterInit:
    """Tests for LangGraphAdapter initialization."""

    def test_default_thread_id_uses_session_id(self):
        """Verify adapter falls back to session_id when no thread_id given."""
        ctx = _make_mock_context(session_id="my-session")
        graph = MagicMock()
        adapter = LangGraphAdapter(graph, ctx)
        assert adapter._thread_id == "my-session"  # pylint: disable=protected-access

    def test_custom_thread_id(self):
        """Verify adapter uses explicit thread_id when provided."""
        ctx = _make_mock_context()
        graph = MagicMock()
        adapter = LangGraphAdapter(graph, ctx, thread_id="custom-thread")
        assert adapter._thread_id == "custom-thread"  # pylint: disable=protected-access


class TestIsGraphSuspended:
    """Tests for _is_graph_suspended."""

    def test_suspended_when_next_is_nonempty(self):
        """Verify graph is detected as suspended when snapshot.next is non-empty."""
        ctx = _make_mock_context()
        graph = MagicMock()
        snapshot = MagicMock()
        snapshot.next = ("tools",)
        graph.get_state.return_value = snapshot

        adapter = LangGraphAdapter(graph, ctx)
        assert adapter._is_graph_suspended() is True  # pylint: disable=protected-access

    def test_not_suspended_when_next_is_empty(self):
        """Verify graph is not suspended when snapshot.next is empty."""
        ctx = _make_mock_context()
        graph = MagicMock()
        snapshot = MagicMock()
        snapshot.next = ()
        graph.get_state.return_value = snapshot

        adapter = LangGraphAdapter(graph, ctx)
        assert adapter._is_graph_suspended() is False  # pylint: disable=protected-access

    def test_not_suspended_on_error(self):
        """Verify graph is not suspended when get_state raises an exception."""
        ctx = _make_mock_context()
        graph = MagicMock()
        graph.get_state.side_effect = RuntimeError("no checkpoint")

        adapter = LangGraphAdapter(graph, ctx)
        assert adapter._is_graph_suspended() is False  # pylint: disable=protected-access


class TestAdapterRun:
    """Tests for adapter.run() dispatching."""

    @pytest.mark.asyncio
    async def test_resume_command_calls_handle_resume(self):
        """Verify ResumeCommand resumes the graph and returns the final answer."""
        ctx = _make_mock_context()
        graph = MagicMock()
        graph.ainvoke = AsyncMock(
            return_value={"messages": [MagicMock(content="done")]}
        )
        # Not suspended
        snapshot = MagicMock()
        snapshot.next = ()
        graph.get_state.return_value = snapshot

        adapter = LangGraphAdapter(graph, ctx, stream=False)

        cmd = ResumeCommand(
            header=_make_header(),
            content="user reply",
            status="COMPLETED",
        )
        result = await adapter.run(cmd)

        # Should have called ainvoke (not with initial state)
        graph.ainvoke.assert_called_once()
        assert result == "done"

    @pytest.mark.asyncio
    async def test_initial_command_calls_handle_initial(self):
        """Verify AskAgentCommand invokes the graph with initial input."""
        ctx = _make_mock_context()
        graph = MagicMock()
        graph.ainvoke = AsyncMock(
            return_value={"messages": [MagicMock(content="hello")]}
        )
        snapshot = MagicMock()
        snapshot.next = ()
        graph.get_state.return_value = snapshot

        adapter = LangGraphAdapter(graph, ctx, stream=False)

        cmd = AskAgentCommand(
            header=_make_header(),
            content="write a poem",
        )
        result = await adapter.run(cmd)

        graph.ainvoke.assert_called_once()
        assert result == "hello"

    @pytest.mark.asyncio
    async def test_returns_queued_when_suspended(self):
        """Verify a suspended graph returns QUEUED status dict."""
        ctx = _make_mock_context()
        graph = MagicMock()
        graph.ainvoke = AsyncMock(
            return_value={"messages": [MagicMock(content="partial")]}
        )
        snapshot = MagicMock()
        snapshot.next = ("tools",)
        graph.get_state.return_value = snapshot

        adapter = LangGraphAdapter(graph, ctx, stream=False)

        cmd = AskAgentCommand(
            header=_make_header(),
            content="write a poem",
        )
        result = await adapter.run(cmd)

        assert isinstance(result, dict)
        assert result["status"] == "QUEUED"

    @pytest.mark.asyncio
    async def test_includes_langfuse_callbacks_and_parent_trace_context(
        self, monkeypatch
    ):
        """Verify adapter wires Langfuse callback handler into LangGraph config."""
        ctx = _make_mock_context()
        graph = MagicMock()
        graph.ainvoke = AsyncMock(
            return_value={"messages": [MagicMock(content="hello")]}
        )
        snapshot = MagicMock()
        snapshot.next = ()
        graph.get_state.return_value = snapshot

        observation_calls: list[dict] = []
        callback_instances: list[object] = []

        @contextmanager
        def fake_observation_scope(**kwargs):
            observation_calls.append(kwargs)
            yield SimpleNamespace(id="obs-langgraph")

        class FakeCallbackHandler:  # pylint: disable=too-few-public-methods
            """Minimal callback handler stub for adapter config assertions."""

            def __init__(self):
                callback_instances.append(self)

        fake_langfuse_client = SimpleNamespace(
            start_as_current_observation=fake_observation_scope
        )

        monkeypatch.setitem(
            sys.modules,
            "langfuse",
            SimpleNamespace(get_client=MagicMock(return_value=fake_langfuse_client)),
        )
        monkeypatch.setitem(
            sys.modules,
            "langfuse.langchain",
            SimpleNamespace(CallbackHandler=FakeCallbackHandler),
        )
        monkeypatch.setitem(
            sys.modules,
            "by_framework_trace_langfuse",
            SimpleNamespace(LangfuseConfig=MagicMock()),
        )
        sys.modules[
            "by_framework_trace_langfuse"
        ].LangfuseConfig.from_env.return_value = object()
        monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")
        monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
        monkeypatch.setenv("LANGFUSE_BASE_URL", "http://localhost:3000")

        adapter = LangGraphAdapter(graph, ctx, stream=False)

        cmd = AskAgentCommand(
            header=_make_header(),
            content="write a poem",
        )
        result = await adapter.run(cmd)

        assert result == "hello"
        graph.ainvoke.assert_called_once()
        _, kwargs = graph.ainvoke.call_args
        config = kwargs["config"]
        assert config["run_name"] == "planner:langgraph"
        assert config["metadata"]["langfuse_session_id"] == "test-session"
        assert config["metadata"]["langfuse_user_id"] == "user-1"
        assert config["metadata"]["by_framework_message_id"] == "msg-ctx"
        assert config["metadata"]["langgraph_thread_id"] == "test-session"
        assert config["callbacks"] == callback_instances
        assert len(observation_calls) == 1
        assert observation_calls[0]["trace_context"] == {
            "trace_id": "trace-ctx",
            "parent_span_id": "obs-framework",
        }
        assert observation_calls[0]["name"] == "planner:langgraph"

    @pytest.mark.asyncio
    async def test_skips_langfuse_tracing_silently_when_not_configured(
        self, monkeypatch, caplog
    ):
        """Verify missing Langfuse env disables inner tracing without warnings."""
        ctx = _make_mock_context()
        graph = MagicMock()
        graph.ainvoke = AsyncMock(
            return_value={"messages": [MagicMock(content="hello")]}
        )
        snapshot = MagicMock()
        snapshot.next = ()
        graph.get_state.return_value = snapshot

        monkeypatch.delenv("LANGFUSE_SECRET_KEY", raising=False)
        monkeypatch.delenv("LANGFUSE_PUBLIC_KEY", raising=False)
        monkeypatch.delenv("LANGFUSE_BASE_URL", raising=False)
        monkeypatch.setitem(
            sys.modules,
            "by_framework_trace_langfuse",
            SimpleNamespace(LangfuseConfig=MagicMock()),
        )
        sys.modules[
            "by_framework_trace_langfuse"
        ].LangfuseConfig.from_env.return_value = None

        adapter = LangGraphAdapter(graph, ctx, stream=False)

        cmd = AskAgentCommand(
            header=_make_header(),
            content="write a poem",
        )
        result = await adapter.run(cmd)

        assert result == "hello"
        _, kwargs = graph.ainvoke.call_args
        config = kwargs["config"]
        assert "callbacks" not in config
        assert "Langfuse" not in caplog.text

    @pytest.mark.asyncio
    async def test_skips_langfuse_tracing_when_explicitly_disabled(
        self, monkeypatch, caplog
    ):
        """Verify BYAI_LANGFUSE_ENABLED=false silences inner tracing as well."""
        ctx = _make_mock_context()
        graph = MagicMock()
        graph.ainvoke = AsyncMock(
            return_value={"messages": [MagicMock(content="hello")]}
        )
        snapshot = MagicMock()
        snapshot.next = ()
        graph.get_state.return_value = snapshot

        monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")
        monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
        monkeypatch.setenv("LANGFUSE_BASE_URL", "http://localhost:3000")
        monkeypatch.setenv("BYAI_LANGFUSE_ENABLED", "false")
        monkeypatch.setitem(
            sys.modules,
            "by_framework_trace_langfuse",
            SimpleNamespace(LangfuseConfig=MagicMock()),
        )
        sys.modules[
            "by_framework_trace_langfuse"
        ].LangfuseConfig.from_env.return_value = None

        adapter = LangGraphAdapter(graph, ctx, stream=False)

        cmd = AskAgentCommand(
            header=_make_header(),
            content="write a poem",
        )
        result = await adapter.run(cmd)

        assert result == "hello"
        _, kwargs = graph.ainvoke.call_args
        config = kwargs["config"]
        assert "callbacks" not in config
        assert "Langfuse" not in caplog.text


class TestLangGraphWorkerHooks:  # pylint: disable=too-few-public-methods
    """Tests for LangGraphWorker tracing config hooks."""

    @pytest.mark.asyncio
    async def test_process_command_passes_langgraph_config_to_adapter(
        self, monkeypatch
    ):
        """Verify worker hooks are forwarded into LangGraphAdapter."""

        captured: dict[str, object] = {}

        class DemoWorker(LangGraphWorker):  # pylint: disable=too-few-public-methods
            """Concrete worker used to verify LangGraph hook plumbing."""

            def get_agent_types(self):
                return ["demo"]

            def build_graph(self, context, command):
                del context, command
                return MagicMock()

            def get_langgraph_run_name(self, context, command):
                del context, command
                return "custom-run"

            def get_langgraph_metadata(self, context, command):
                del context, command
                return {"team": "alpha"}

            def get_langgraph_callbacks(self, context, command):
                del context, command
                return ["cb-1"]

        class FakeAdapter:  # pylint: disable=too-few-public-methods
            """Adapter stub that captures constructor kwargs."""

            def __init__(self, graph, context, **kwargs):
                del graph, context
                captured.update(kwargs)

            async def run(self, command):
                """Return a fixed result for process_command assertions."""
                del command
                return "ok"

        monkeypatch.setattr(
            "by_framework_langgraph.worker.LangGraphAdapter",
            FakeAdapter,
        )

        worker = DemoWorker(worker_id="demo-worker")
        ctx = _make_mock_context()
        cmd = AskAgentCommand(
            header=_make_header(),
            content="hello",
        )

        result = await worker.process_command(cmd, ctx)

        assert result == "ok"
        assert captured["thread_id"] == "test-session"
        assert captured["stream"] is True
        assert captured["run_name"] == "custom-run"
        assert captured["metadata"] == {"team": "alpha"}
        assert captured["callbacks"] == ["cb-1"]
