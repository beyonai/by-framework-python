import sys
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Optional

import by_framework_trace_langfuse.langfuse as langfuse_module
import pytest
from by_framework_trace_langfuse import (
    LangfuseConfig,
    LangfusePlugin,
    LangfuseTraceProviderFactory,
)

from by_framework import (
    AgentContext,
    AskAgentCommand,
    CancelTaskCommand,
    MessageHeader,
    ResumeCommand,
)


@dataclass
class FakeObservation:
    id: str
    updates: list[dict[str, Any]] = field(default_factory=list)
    ended_with: Optional[dict[str, Any]] = None

    def update(self, **kwargs: Any) -> None:
        self.updates.append(kwargs)

    def end(self, **kwargs: Any) -> None:
        self.ended_with = kwargs


class FakeTracer:
    """Simple tracer test double that records observation start payloads."""

    def __init__(self):
        self.start_calls: list[dict[str, Any]] = []
        self.shutdown_called = False

    def start_observation(self, request: Any) -> FakeObservation:
        self.start_calls.append(
            {
                "trace_id": request.trace_id,
                "name": request.name,
                "input": request.observation_input,
                "observation_input": request.observation_input,
                "metadata": request.metadata,
                "parent_observation_id": request.parent_observation_id,
                "span_id": request.span_id,
                "as_root": request.as_root,
            }
        )
        return FakeObservation(id=f"obs-{len(self.start_calls)}")

    def shutdown(self) -> None:
        self.shutdown_called = True


class EndWithoutKwargsObservation:
    """Observation double whose end() does not accept keyword arguments."""

    def __init__(self):
        self.id = "obs-fallback"
        self.updates: list[dict[str, Any]] = []
        self.end_calls = 0

    def update(self, **kwargs: Any) -> None:
        self.updates.append(kwargs)

    def end(self) -> None:
        self.end_calls += 1


class FakeObservationStore:
    """In-memory observation mapping used by the plugin tests."""

    def __init__(self):
        self.mapping: dict[tuple[str, str], str] = {}

    async def get_observation_id(
        self, session_id: str, message_id: str
    ) -> Optional[str]:
        return self.mapping.get((session_id, message_id))

    async def set_observation_id(
        self, session_id: str, message_id: str, observation_id: str
    ) -> None:
        self.mapping[(session_id, message_id)] = observation_id


class FakeOtelSpan:
    """Minimal OTel span double for Langfuse SDK adapter tests."""

    def __init__(self):
        self.attributes: dict[str, Any] = {}

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value


class FakeSdkObservation:
    """Observation double with an attached OTel span."""

    def __init__(self):
        self.id = "sdk-obs"
        self._otel_span = FakeOtelSpan()


class FakeSdkClient:
    """Langfuse client double used by _SdkLangfuseTracer tests."""

    def __init__(self):
        self.calls: list[dict[str, Any]] = []
        self.observations: list[FakeSdkObservation] = []
        self.trace_calls: list[dict[str, Any]] = []

    def start_observation(self, **kwargs: Any) -> FakeSdkObservation:
        self.calls.append(kwargs)
        observation = FakeSdkObservation()
        self.observations.append(observation)
        return observation

    def trace(self, **kwargs: Any) -> None:
        self.trace_calls.append(kwargs)


def _build_context(
    *,
    message_id: str = "msg-1",
    parent_message_id: str = "",
    trace_id: str = "12345678901234567890123456789012",
    session_id: str = "session-1",
    current_agent_id: str = "planner",
    content: Any = "hello",
    metadata: Optional[dict[str, Any]] = None,
    langfuse_parent_observation_id: str = "",
    trace_parent_span_id: str = "",
) -> AgentContext:
    command = AskAgentCommand(
        header=MessageHeader(
            message_id=message_id,
            session_id=session_id,
            trace_id=trace_id,
            target_agent_type=current_agent_id,
            parent_message_id=parent_message_id,
            user_code="user-1",
            user_name="Alice",
            metadata=metadata or {},
            langfuse_parent_observation_id=langfuse_parent_observation_id,
            trace_parent_span_id=trace_parent_span_id,
        ),
        content=content,
    )
    return AgentContext(
        session_id=session_id,
        trace_id=trace_id,
        redis_client=object(),
        current_agent_id=current_agent_id,
        message_id=message_id,
        parent_message_id=parent_message_id,
        current_command=command,
        user_code="user-1",
        user_name="Alice",
    )


@pytest.mark.asyncio
async def test_langfuse_plugin_starts_observation_and_persists_mapping():
    tracer = FakeTracer()
    store = FakeObservationStore()
    store.mapping[("session-1", "msg-parent")] = "obs-parent"
    plugin = LangfusePlugin(tracer=tracer, observation_store=store)
    context = _build_context(message_id="msg-child", parent_message_id="msg-parent")

    await plugin.on_task_start(context)

    # Two native observations are created: worker.execute, then the agent task.
    assert len(tracer.start_calls) == 2
    worker_execute_call = tracer.start_calls[0]
    agent_call = tracer.start_calls[1]

    # worker.execute hangs under the parent task (store lookup for sub-agents).
    assert worker_execute_call["name"] == "worker.execute"
    assert worker_execute_call["parent_observation_id"] == "obs-parent"
    assert worker_execute_call["trace_id"] == "12345678901234567890123456789012"

    # The agent task nests under this execution's worker.execute observation.
    assert agent_call["name"] == "planner"
    assert agent_call["parent_observation_id"] == "obs-1"  # worker.execute obs id
    assert agent_call["input"] == "hello"
    assert agent_call["metadata"]["message_id"] == "msg-child"
    # The agent task observation id is what children look up as their parent.
    assert store.mapping[("session-1", "msg-child")] == "obs-2"


@pytest.mark.asyncio
async def test_langfuse_plugin_top_level_nests_under_worker_execute():
    """Top-level chain: client.dispatch -> worker.execute -> agent task.

    Regression: worker.execute is materialised as a native Langfuse observation
    so it reliably appears (raw by-framework OTel spans are dropped by Langfuse's
    default filter in the worker process); the agent task nests under it, and
    worker.execute itself parents to client.dispatch.
    """
    from by_framework.observability.span_recorder import str_to_uint64

    tracer = FakeTracer()
    store = FakeObservationStore()
    plugin = LangfusePlugin(tracer=tracer, observation_store=store)
    # No execution_id on the context -> anchor falls back to message_id.
    context = _build_context(message_id="msg-1", parent_message_id="")

    await plugin.on_task_start(context)

    assert len(tracer.start_calls) == 2
    worker_execute_call = tracer.start_calls[0]
    agent_call = tracer.start_calls[1]

    worker_execute_span_id = str_to_uint64("msg-1:worker.execute")
    agent_task_span_id = str_to_uint64("msg-1:agent.task")

    # worker.execute is its own node. With no propagated client parent it has no
    # synthetic client root in the worker process.
    assert worker_execute_call["name"] == "worker.execute"
    assert worker_execute_call["span_id"] == worker_execute_span_id
    assert worker_execute_call["parent_observation_id"] == ""
    assert worker_execute_call["as_root"] is False

    # Agent task nests under worker.execute, with a distinct span id.
    assert agent_call["span_id"] == agent_task_span_id
    assert agent_call["span_id"] != worker_execute_span_id
    assert agent_call["parent_observation_id"] == "obs-1"  # worker.execute obs id
    assert agent_call["as_root"] is False


@pytest.mark.asyncio
async def test_langfuse_plugin_top_level_uses_client_dispatch_parent_from_metadata():
    """Top-level worker spans nest under the client-created Langfuse root."""
    tracer = FakeTracer()
    store = FakeObservationStore()
    plugin = LangfusePlugin(tracer=tracer, observation_store=store)
    context = _build_context(
        message_id="msg-1",
        parent_message_id="",
        metadata={"langfuse_parent_observation_id": "obs-client-dispatch"},
    )

    await plugin.on_task_start(context)

    assert len(tracer.start_calls) == 2
    worker_execute_call = tracer.start_calls[0]
    agent_call = tracer.start_calls[1]

    assert worker_execute_call["name"] == "worker.execute"
    assert worker_execute_call["parent_observation_id"] == "obs-client-dispatch"
    assert worker_execute_call["as_root"] is False
    assert agent_call["parent_observation_id"] == "obs-1"


@pytest.mark.asyncio
async def test_langfuse_plugin_top_level_parent_from_header_attr():
    """Top-level worker spans nest under client-created Langfuse root
    using header attr.
    """
    tracer = FakeTracer()
    store = FakeObservationStore()
    plugin = LangfusePlugin(tracer=tracer, observation_store=store)
    context = _build_context(
        message_id="msg-1",
        parent_message_id="",
        langfuse_parent_observation_id="obs-client-dispatch",
    )

    await plugin.on_task_start(context)

    assert len(tracer.start_calls) == 2
    worker_execute_call = tracer.start_calls[0]
    agent_call = tracer.start_calls[1]

    assert worker_execute_call["name"] == "worker.execute"
    assert worker_execute_call["parent_observation_id"] == "obs-client-dispatch"
    assert worker_execute_call["as_root"] is False
    assert agent_call["parent_observation_id"] == "obs-1"


@pytest.mark.asyncio
async def test_child_agent_prefers_root_parent_from_metadata():
    """Child agent spans nest under root dispatch ID if metadata exists."""
    tracer = FakeTracer()
    store = FakeObservationStore()
    store.mapping[("session-1", "msg-parent")] = "obs-parent-agent"
    plugin = LangfusePlugin(tracer=tracer, observation_store=store)
    context = _build_context(
        message_id="msg-child",
        parent_message_id="msg-parent",
        metadata={"langfuse_parent_observation_id": "obs-root-client-dispatch"},
    )

    await plugin.on_task_start(context)

    assert len(tracer.start_calls) == 2
    worker_execute_call = tracer.start_calls[0]
    assert worker_execute_call["parent_observation_id"] == "obs-root-client-dispatch"


@pytest.mark.asyncio
async def test_resume_ignores_parent_observation_id_metadata():
    """Top-level resumed task spans ignore parent ID in metadata to avoid self loops."""
    tracer = FakeTracer()
    store = FakeObservationStore()
    plugin = LangfusePlugin(tracer=tracer, observation_store=store)

    command = ResumeCommand(
        header=MessageHeader(
            message_id="msg-resume",
            session_id="session-1",
            trace_id="trace-1",
            target_agent_type="planner",
            parent_message_id="",
            metadata={"langfuse_parent_observation_id": "some-old-stage-id"},
        ),
        status="success",
        reply_data=None,
    )

    context = AgentContext(
        session_id="session-1",
        trace_id="trace-1",
        redis_client=object(),
        current_agent_id="planner",
        message_id="msg-resume",
        parent_message_id="",
        current_command=command,
    )

    await plugin.on_task_start(context)

    assert len(tracer.start_calls) == 2
    worker_execute_call = tracer.start_calls[0]
    assert worker_execute_call["parent_observation_id"] == ""


@pytest.mark.asyncio
async def test_langfuse_plugin_child_task_does_not_become_trace_root():
    """Child task observations stay nested under the parent task trace root."""
    tracer = FakeTracer()
    store = FakeObservationStore()
    store.mapping[("session-1", "msg-parent")] = "obs-parent"
    plugin = LangfusePlugin(tracer=tracer, observation_store=store)
    context = _build_context(message_id="msg-child", parent_message_id="msg-parent")

    await plugin.on_task_start(context)

    worker_execute_call = tracer.start_calls[0]
    agent_call = tracer.start_calls[1]

    assert worker_execute_call["name"] == "worker.execute"
    assert worker_execute_call["parent_observation_id"] == "obs-parent"
    assert worker_execute_call["as_root"] is False
    assert agent_call["as_root"] is False


def test_sdk_tracer_preserves_root_observation_for_trace_name():
    """SDK adapter only clears Langfuse root promotion for nested observations."""
    client = FakeSdkClient()
    tracer = langfuse_module._SdkLangfuseTracer(client)  # pylint: disable=protected-access

    root_observation = tracer.start_observation(
        langfuse_module._ObservationStartRequest(  # pylint: disable=protected-access
            trace_id="12345678901234567890123456789012",
            name="client.dispatch:planner",
            observation_input="hello",
            metadata={"session_id": "session-1", "user_code": "user-1"},
            as_root=True,
        )
    )
    nested_observation = tracer.start_observation(
        langfuse_module._ObservationStartRequest(  # pylint: disable=protected-access
            trace_id="12345678901234567890123456789012",
            name="planner",
            observation_input="hello",
            metadata={},
            parent_observation_id="sdk-obs",
            as_root=False,
        )
    )

    assert client.calls[0]["name"] == "client.dispatch:planner"
    assert (
        root_observation._otel_span.attributes["langfuse.trace.name"]
        == "client.dispatch:planner"
    )
    assert root_observation._otel_span.attributes["session.id"] == "session-1"
    assert root_observation._otel_span.attributes["user.id"] == "user-1"
    assert "langfuse.internal.as_root" not in root_observation._otel_span.attributes
    assert (
        nested_observation._otel_span.attributes["langfuse.internal.as_root"] is False
    )

    # Verify that client.trace was called when as_root is True
    assert len(client.trace_calls) == 1
    assert client.trace_calls[0] == {
        "id": "12345678901234567890123456789012",
        "name": "client.dispatch:planner",
        "session_id": "session-1",
        "user_id": "user-1",
    }


@pytest.mark.asyncio
async def test_langfuse_plugin_ends_observation_with_result_output():
    tracer = FakeTracer()
    store = FakeObservationStore()
    plugin = LangfusePlugin(tracer=tracer, observation_store=store)
    context = _build_context()

    await plugin.on_task_start(context)
    await plugin.on_task_complete(context, {"status": "COMPLETED", "answer": "done"})

    observation = context._langfuse_observation  # pylint: disable=protected-access
    assert observation.ended_with == {
        "output": {"status": "COMPLETED", "answer": "done"}
    }


@pytest.mark.asyncio
async def test_langfuse_plugin_marks_errors_on_task_failure():
    tracer = FakeTracer()
    store = FakeObservationStore()
    plugin = LangfusePlugin(tracer=tracer, observation_store=store)
    context = _build_context()

    await plugin.on_task_start(context)
    await plugin.on_task_error(context, RuntimeError("boom"))

    observation = context._langfuse_observation  # pylint: disable=protected-access
    assert observation.updates[-1]["level"] == "ERROR"
    assert observation.updates[-1]["status_message"] == "boom"
    assert observation.ended_with == {"output": {"error": "boom"}}


@pytest.mark.asyncio
async def test_langfuse_plugin_marks_cancellation_and_ends_observation():
    tracer = FakeTracer()
    store = FakeObservationStore()
    plugin = LangfusePlugin(tracer=tracer, observation_store=store)
    context = _build_context()
    cancel_command = CancelTaskCommand(
        header=MessageHeader(
            message_id="cancel-1",
            session_id="session-1",
            trace_id="trace-1",
        ),
        target_message_id="msg-1",
        reason="user cancelled",
    )

    await plugin.on_task_start(context)
    await plugin.on_task_cancel(context, cancel_command)

    observation = context._langfuse_observation  # pylint: disable=protected-access
    assert observation.updates[-1]["level"] == "WARNING"
    assert observation.updates[-1]["status_message"] == "user cancelled"
    assert observation.ended_with == {
        "output": {"cancelled": True, "reason": "user cancelled"}
    }


def test_langfuse_plugin_builds_sdk_client_with_constructor(monkeypatch):
    captured: dict[str, Any] = {}

    class FakeLangfuseClient:

        def __init__(self, **kwargs: Any):
            captured.update(kwargs)

    fake_module = SimpleNamespace(Langfuse=FakeLangfuseClient)
    monkeypatch.setitem(sys.modules, "langfuse", fake_module)
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
    monkeypatch.setenv("LANGFUSE_BASE_URL", "http://localhost:3000")

    plugin = LangfusePlugin()
    tracer = plugin._build_default_tracer()  # pylint: disable=protected-access

    assert tracer is not None
    assert captured == {
        "public_key": "pk-test",
        "secret_key": "sk-test",
        "base_url": "http://localhost:3000",
    }


def test_langfuse_config_prefers_clean_base_url_value():
    config = LangfuseConfig(
        secret_key="sk-test",
        public_key="pk-test",
        base_url=LangfuseConfig._clean_env_value("“http://localhost:3000”"),  # pylint: disable=protected-access
    )

    assert config.base_url == "http://localhost:3000"


def test_langfuse_plugin_end_falls_back_to_update_then_plain_end():
    plugin = LangfusePlugin()
    observation = EndWithoutKwargsObservation()
    context = SimpleNamespace(_langfuse_observation=observation)

    plugin._end_observation(context, output={"status": "ok"})  # pylint: disable=protected-access

    assert observation.updates[-1] == {"output": {"status": "ok"}}
    assert observation.end_calls == 1


def test_langfuse_trace_provider_factory_builds_plugin_from_env(monkeypatch):
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
    monkeypatch.setenv("LANGFUSE_BASE_URL", "http://localhost:3000")

    plugin = LangfuseTraceProviderFactory().build_plugin_from_env()

    assert isinstance(plugin, LangfusePlugin)
