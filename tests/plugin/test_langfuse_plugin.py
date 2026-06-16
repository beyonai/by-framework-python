import sys
import time
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Optional

import by_framework_trace_langfuse.langfuse as langfuse_module
import pytest
from by_framework_trace_langfuse import (
    LangfuseConfig,
    LangfusePlugin,
    LangfuseTraceProviderFactory,
    build_langchain_callback,
)

from by_framework import (
    AgentContext,
    AskAgentCommand,
    CancelTaskCommand,
    MessageHeader,
    ResumeCommand,
)
from by_framework.trace.span_recorder import str_to_uint128


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
        self.trace_output_updates: list[dict[str, Any]] = []
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

    def update_trace_output(self, trace_id: str, output: Any) -> None:
        self.trace_output_updates.append({"trace_id": trace_id, "output": output})


class SlowTraceOutputTracer(FakeTracer):
    """Tracer double whose trace output update would block if called inline."""

    def update_trace_output(self, trace_id: str, output: Any) -> None:
        time.sleep(0.2)
        super().update_trace_output(trace_id, output)


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


def test_build_langchain_callback_uses_trace_context_for_current_sdk(monkeypatch):
    """Langfuse v4 CallbackHandler accepts trace_context, not trace_id kwargs."""
    captured: dict[str, object] = {}

    class FakeCallbackHandler:  # pylint: disable=too-few-public-methods

        def __init__(self, *, public_key=None, trace_context=None):
            captured["public_key"] = public_key
            captured["trace_context"] = trace_context
            self._runs: dict[str, Any] = {}

        def on_chain_start(self, serialized, inputs, *, run_id, **kwargs):
            del serialized, inputs, kwargs
            observation = SimpleNamespace(_otel_span=FakeOtelSpan())
            observation._otel_span.set_attribute("langfuse.internal.as_root", True)
            self._runs[run_id] = observation

    monkeypatch.setitem(
        sys.modules,
        "langfuse.langchain",
        SimpleNamespace(CallbackHandler=FakeCallbackHandler),
    )
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
    monkeypatch.setenv("LANGFUSE_BASE_URL", "http://localhost:3000")

    trace_id_val = "trace-langfuse"
    handler = build_langchain_callback(
        trace_id=trace_id_val,
        parent_observation_id="obs-parent",
    )

    assert isinstance(handler, FakeCallbackHandler)
    assert captured == {
        "public_key": "pk-test",
        "trace_context": {
            "trace_id": f"{str_to_uint128(trace_id_val):032x}",
            "parent_span_id": "obs-parent",
        },
    }

    handler.on_chain_start(None, {}, run_id="langgraph-root")
    root_observation = handler._runs["langgraph-root"]  # pylint: disable=protected-access
    assert root_observation._otel_span.attributes["langfuse.internal.as_root"] is False


def test_build_langchain_callback_injects_worker_id_into_run_metadata(monkeypatch):
    """LangChain child observations receive framework worker metadata."""
    captured_metadata: list[dict[str, Any]] = []

    class FakeCallbackHandler:  # pylint: disable=too-few-public-methods

        def __init__(self, *, public_key=None, trace_context=None):
            del public_key, trace_context
            self._runs: dict[str, Any] = {}

        def on_chain_start(
            self, serialized, inputs, *, run_id, metadata=None, **kwargs
        ):
            del serialized, inputs, kwargs
            captured_metadata.append(metadata or {})
            observation = SimpleNamespace(_otel_span=FakeOtelSpan())
            self._runs[run_id] = observation

        def on_chat_model_start(
            self, serialized, messages, *, run_id, metadata=None, **kwargs
        ):
            del serialized, messages, run_id, kwargs
            captured_metadata.append(metadata or {})

        def on_tool_start(
            self, serialized, input_str, *, run_id, metadata=None, **kwargs
        ):
            del serialized, input_str, run_id, kwargs
            captured_metadata.append(metadata or {})

    monkeypatch.setitem(
        sys.modules,
        "langfuse.langchain",
        SimpleNamespace(CallbackHandler=FakeCallbackHandler),
    )
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
    monkeypatch.setenv("LANGFUSE_BASE_URL", "http://localhost:3000")

    handler = build_langchain_callback(
        trace_id="trace-langfuse",
        parent_observation_id="obs-parent",
        metadata={"worker_id": "worker-callback-1"},
    )

    handler.on_chain_start(None, {}, run_id="chain-run", metadata={"node": "root"})
    handler.on_chat_model_start(None, [], run_id="chat-run")
    handler.on_tool_start(None, "{}", run_id="tool-run", metadata={"tool": "calc"})

    assert captured_metadata == [
        {"worker_id": "worker-callback-1", "node": "root"},
        {"worker_id": "worker-callback-1"},
        {"worker_id": "worker-callback-1", "tool": "calc"},
    ]


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
    worker_id: str = "worker-langfuse-1",
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
        worker_id=worker_id,
    )


@pytest.mark.asyncio
async def test_langfuse_plugin_call_agent_observation_parents_child_task():
    tracer = FakeTracer()
    store = FakeObservationStore()
    plugin = LangfusePlugin(tracer=tracer, observation_store=store)
    context = _build_context()
    command = AskAgentCommand(
        header=MessageHeader(
            message_id="msg-child-call",
            session_id="session-1",
            trace_id="12345678901234567890123456789012",
            source_agent_type="planner",
            target_agent_type="weather-agent",
            parent_message_id="msg-1",
            langfuse_parent_observation_id="obs-tool-call",
            metadata={"framework_parent_span_id": "msg-child-call:client.dispatch"},
        ),
        content="weather?",
    )

    await plugin.on_call_agent_start(context, command)
    await plugin.on_call_agent_complete(context, command, {"status": "QUEUED"})

    call = tracer.start_calls[-1]
    assert call["name"] == "agent.call_agent:weather-agent"
    assert call["parent_observation_id"] == "obs-tool-call"
    assert command.header.langfuse_parent_observation_id == "obs-1"
    assert command.header.metadata["langfuse_parent_observation_id"] == "obs-1"
    assert ("session-1", "msg-child-call") not in store.mapping
    assert getattr(command, "_langfuse_call_observation").ended_with == {
        "output": {"status": "QUEUED"}
    }


@pytest.mark.asyncio
async def test_langfuse_plugin_agent_return_observation_parents_resume_task():
    from by_framework.trace.span_recorder import str_to_uint64

    tracer = FakeTracer()
    store = FakeObservationStore()
    plugin = LangfusePlugin(tracer=tracer, observation_store=store)
    context = _build_context(message_id="msg-b", current_agent_id="weather-agent")
    command = AskAgentCommand(
        header=MessageHeader(
            message_id="msg-b",
            session_id="session-1",
            trace_id="12345678901234567890123456789012",
            source_agent_type="planner",
            target_agent_type="weather-agent",
            parent_message_id="msg-a",
        ),
        content="weather?",
    )
    callback_command = ResumeCommand(
        header=MessageHeader(
            message_id="msg-a",
            session_id="session-1",
            trace_id="12345678901234567890123456789012",
            source_agent_type="weather-agent",
            target_agent_type="planner",
            parent_message_id="msg-b",
            langfuse_parent_observation_id="obs-weather-task",
            metadata={"framework_parent_span_id": "exec-b:agent.return"},
        ),
        status="COMPLETED",
        reply_data={"answer": "sunny"},
    )

    await plugin.on_agent_return_start(context, command, callback_command)
    await plugin.on_agent_return_complete(context, command, callback_command)

    return_call = tracer.start_calls[-1]
    assert return_call["name"] == "agent.return"
    assert return_call["parent_observation_id"] == "obs-weather-task"
    assert return_call["span_id"] == str_to_uint64("exec-b:agent.return")
    assert return_call["metadata"]["return_route"] == "weather-agent->planner"
    assert callback_command.header.langfuse_parent_observation_id == "obs-1"
    assert callback_command.header.metadata["langfuse_parent_observation_id"] == (
        "obs-1"
    )
    assert getattr(callback_command, "_langfuse_return_observation").ended_with == {
        "output": {"status": "COMPLETED", "reply_data": {"answer": "sunny"}}
    }


@pytest.mark.asyncio
async def test_langfuse_plugin_starts_observation_and_persists_mapping():
    tracer = FakeTracer()
    store = FakeObservationStore()
    store.mapping[("session-1", "msg-parent")] = "obs-parent"
    plugin = LangfusePlugin(tracer=tracer, observation_store=store)
    context = _build_context(message_id="msg-child", parent_message_id="msg-parent")

    await plugin.on_task_start(context)

    # Three native observations are created: workflow, worker.execute, agent task.
    assert len(tracer.start_calls) == 3
    workflow_call = tracer.start_calls[0]
    worker_execute_call = tracer.start_calls[1]
    agent_call = tracer.start_calls[2]

    # workflow hangs under the parent task; worker.execute is one execution segment.
    assert workflow_call["name"] == "agent.workflow:planner"
    assert workflow_call["parent_observation_id"] == "obs-parent"
    assert worker_execute_call["name"] == "worker.execute"
    assert worker_execute_call["parent_observation_id"] == "obs-1"
    assert worker_execute_call["trace_id"] == "12345678901234567890123456789012"
    assert workflow_call["metadata"]["worker_id"] == "worker-langfuse-1"
    assert worker_execute_call["metadata"]["worker_id"] == "worker-langfuse-1"
    assert agent_call["metadata"]["worker_id"] == "worker-langfuse-1"

    # The agent task nests under this execution's worker.execute observation.
    assert agent_call["name"] == "planner"
    assert agent_call["parent_observation_id"] == "obs-2"  # worker.execute obs id
    assert agent_call["input"] == "hello"
    assert agent_call["metadata"]["message_id"] == "msg-child"
    assert context.get_trace_parent_observation_id() == "obs-3"
    # The workflow observation id is what children look up as their parent.
    assert store.mapping[("session-1", "msg-child")] == "obs-1"


@pytest.mark.asyncio
async def test_langfuse_plugin_propagates_worker_id_during_task(monkeypatch):
    """Native LangGraph calls inside process_command inherit worker metadata."""
    tracer = FakeTracer()
    plugin = LangfusePlugin(tracer=tracer, observation_store=FakeObservationStore())
    context = _build_context(worker_id="native-langgraph-worker")
    propagation_active = False
    propagation_events: list[tuple[str, dict[str, str]]] = []

    class FakePropagation:

        def __init__(self, metadata):
            self.metadata = metadata

        def __enter__(self):
            nonlocal propagation_active
            propagation_active = True
            propagation_events.append(("enter", self.metadata))

        def __exit__(self, exc_type, exc_val, exc_tb):
            nonlocal propagation_active
            propagation_events.append(("exit", self.metadata))
            propagation_active = False

    def fake_propagate_attributes(**kwargs):
        return FakePropagation(metadata=kwargs["metadata"])

    monkeypatch.setitem(
        sys.modules,
        "langfuse",
        SimpleNamespace(propagate_attributes=fake_propagate_attributes),
    )

    await plugin.on_task_start(context)

    assert propagation_active is True
    assert propagation_events == [("enter", {"worker_id": "native-langgraph-worker"})]

    await plugin.on_task_complete(context, {"status": "COMPLETED"})

    assert propagation_active is False
    assert propagation_events == [
        ("enter", {"worker_id": "native-langgraph-worker"}),
        ("exit", {"worker_id": "native-langgraph-worker"}),
    ]


@pytest.mark.asyncio
async def test_langfuse_plugin_top_level_nests_under_worker_execute():
    """Top-level chain: client.dispatch -> worker.execute -> agent task.

    Regression: worker.execute is materialised as a native Langfuse observation
    so it reliably appears (raw by-framework OTel spans are dropped by Langfuse's
    default filter in the worker process); the agent task nests under it, and
    worker.execute itself parents to client.dispatch.
    """
    from by_framework.trace.span_recorder import str_to_uint64

    tracer = FakeTracer()
    store = FakeObservationStore()
    plugin = LangfusePlugin(tracer=tracer, observation_store=store)
    # No execution_id on the context -> anchor falls back to message_id.
    context = _build_context(message_id="msg-1", parent_message_id="")

    await plugin.on_task_start(context)

    assert len(tracer.start_calls) == 3
    workflow_call = tracer.start_calls[0]
    worker_execute_call = tracer.start_calls[1]
    agent_call = tracer.start_calls[2]

    workflow_span_id = str_to_uint64("msg-1:agent.workflow")
    worker_execute_span_id = str_to_uint64("msg-1:worker.execute")
    agent_task_span_id = str_to_uint64("msg-1:agent.task")

    assert workflow_call["name"] == "agent.workflow:planner"
    assert workflow_call["span_id"] == workflow_span_id
    assert workflow_call["parent_observation_id"] == ""
    assert workflow_call["as_root"] is False

    # worker.execute is its own execution segment under the durable workflow.
    assert worker_execute_call["name"] == "worker.execute"
    assert worker_execute_call["span_id"] == worker_execute_span_id
    assert worker_execute_call["parent_observation_id"] == "obs-1"
    assert worker_execute_call["as_root"] is False

    # Agent task nests under worker.execute, with a distinct span id.
    assert agent_call["span_id"] == agent_task_span_id
    assert agent_call["span_id"] != worker_execute_span_id
    assert agent_call["parent_observation_id"] == "obs-2"  # worker.execute obs id
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

    assert len(tracer.start_calls) == 3
    workflow_call = tracer.start_calls[0]
    worker_execute_call = tracer.start_calls[1]
    agent_call = tracer.start_calls[2]

    assert workflow_call["name"] == "agent.workflow:planner"
    assert workflow_call["parent_observation_id"] == "obs-client-dispatch"
    assert worker_execute_call["name"] == "worker.execute"
    assert worker_execute_call["parent_observation_id"] == "obs-1"
    assert worker_execute_call["as_root"] is False
    assert agent_call["parent_observation_id"] == "obs-2"


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

    assert len(tracer.start_calls) == 3
    workflow_call = tracer.start_calls[0]
    worker_execute_call = tracer.start_calls[1]
    agent_call = tracer.start_calls[2]

    assert workflow_call["parent_observation_id"] == "obs-client-dispatch"
    assert worker_execute_call["name"] == "worker.execute"
    assert worker_execute_call["parent_observation_id"] == "obs-1"
    assert worker_execute_call["as_root"] is False
    assert agent_call["parent_observation_id"] == "obs-2"


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

    assert len(tracer.start_calls) == 3
    workflow_call = tracer.start_calls[0]
    worker_execute_call = tracer.start_calls[1]
    assert workflow_call["parent_observation_id"] == "obs-root-client-dispatch"
    assert worker_execute_call["parent_observation_id"] == "obs-1"


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

    assert len(tracer.start_calls) == 3
    workflow_call = tracer.start_calls[0]
    worker_execute_call = tracer.start_calls[1]
    assert workflow_call["parent_observation_id"] == ""
    assert worker_execute_call["parent_observation_id"] == "obs-1"


@pytest.mark.asyncio
async def test_resume_uses_distinct_span_ids_to_avoid_parent_cycles():
    """Resume stages must not reuse the original agent.task observation id."""
    from by_framework.trace.span_recorder import str_to_uint64

    tracer = FakeTracer()
    store = FakeObservationStore()
    plugin = LangfusePlugin(tracer=tracer, observation_store=store)

    initial_context = _build_context(
        message_id="msg-parent",
        parent_message_id="",
        current_agent_id="planner",
        langfuse_parent_observation_id="obs-client-dispatch",
    )
    initial_context.execution_id = "exec-1"
    await plugin.on_task_start(initial_context)

    resume_command = ResumeCommand(
        header=MessageHeader(
            message_id="msg-parent",
            session_id="session-1",
            trace_id="trace-1",
            target_agent_type="planner",
            parent_message_id="msg-child",
            langfuse_parent_observation_id="obs-previous-agent",
        ),
        status="success",
        reply_data=None,
    )
    resume_context = AgentContext(
        session_id="session-1",
        trace_id="trace-1",
        redis_client=object(),
        current_agent_id="planner",
        message_id="msg-parent",
        parent_message_id="msg-child",
        current_command=resume_command,
        execution_id="exec-1",
    )
    await plugin.on_task_start(resume_context)

    initial_agent_call = tracer.start_calls[2]
    resume_worker_call = tracer.start_calls[3]
    resume_agent_call = tracer.start_calls[4]

    assert initial_agent_call["span_id"] == str_to_uint64("exec-1:agent.task")
    assert resume_worker_call["span_id"] == str_to_uint64(
        "exec-1:resume:msg-child:worker.execute"
    )
    assert resume_agent_call["span_id"] == str_to_uint64(
        "exec-1:resume:msg-child:agent.task"
    )
    assert resume_agent_call["span_id"] != initial_agent_call["span_id"]
    assert resume_worker_call["parent_observation_id"] == "obs-1"
    assert resume_agent_call["parent_observation_id"] == "obs-4"


@pytest.mark.asyncio
async def test_resume_uses_callback_parent_when_context_parent_is_root():
    """Resume callbacks should use the callback header parent for trace nesting.

    WorkerRunner restores the original execution's parent_message_id onto
    AgentContext. For a top-level suspended task that value is empty, while the
    ResumeCommand header still carries the child message that returned. Langfuse
    needs the header parent to avoid placing the resume stage beside the initial
    worker.execute.
    """
    tracer = FakeTracer()
    plugin = LangfusePlugin(tracer=tracer, observation_store=FakeObservationStore())

    resume_command = ResumeCommand(
        header=MessageHeader(
            message_id="msg-parent",
            session_id="session-1",
            trace_id="trace-1",
            target_agent_type="planner",
            parent_message_id="msg-child",
            langfuse_parent_observation_id="obs-original-agent",
        ),
        status="success",
        reply_data="weather result",
    )
    resume_context = AgentContext(
        session_id="session-1",
        trace_id="trace-1",
        redis_client=object(),
        current_agent_id="planner",
        message_id="msg-parent",
        parent_message_id="",
        current_command=resume_command,
        execution_id="exec-1",
    )

    await plugin.on_task_start(resume_context)

    workflow_call = tracer.start_calls[0]
    resume_worker_call = tracer.start_calls[1]
    assert workflow_call["parent_observation_id"] == "obs-original-agent"
    assert resume_worker_call["parent_observation_id"] == "obs-1"
    assert resume_worker_call["metadata"]["parent_message_id"] == "msg-child"


@pytest.mark.asyncio
async def test_langfuse_workflow_stays_open_while_task_is_queued_then_ends_on_resume():
    """Logical workflow duration spans async child execution and final resume."""
    tracer = FakeTracer()
    plugin = LangfusePlugin(tracer=tracer, observation_store=FakeObservationStore())

    initial_context = _build_context(
        message_id="msg-parent",
        parent_message_id="",
        current_agent_id="planner",
        langfuse_parent_observation_id="obs-client-dispatch",
    )
    initial_context.execution_id = "exec-1"
    await plugin.on_task_start(initial_context)

    workflow = initial_context._langfuse_workflow_observation  # pylint: disable=protected-access
    worker_execute = initial_context._langfuse_worker_execute_observation  # pylint: disable=protected-access
    agent_task = initial_context._langfuse_observation  # pylint: disable=protected-access

    await plugin.on_task_complete(initial_context, {"status": "QUEUED"})

    assert workflow.ended_with is None
    assert worker_execute.ended_with == {"output": {"status": "QUEUED"}}
    assert agent_task.ended_with == {"output": {"status": "QUEUED"}}

    resume_command = ResumeCommand(
        header=MessageHeader(
            message_id="msg-parent",
            session_id="session-1",
            trace_id="trace-1",
            target_agent_type="planner",
            parent_message_id="msg-child",
            langfuse_parent_observation_id=workflow.id,
        ),
        status="success",
        reply_data="weather result",
    )
    resume_context = AgentContext(
        session_id="session-1",
        trace_id="trace-1",
        redis_client=object(),
        current_agent_id="planner",
        message_id="msg-parent",
        parent_message_id="",
        current_command=resume_command,
        execution_id="exec-1",
    )
    await plugin.on_task_start(resume_context)

    assert resume_context._langfuse_workflow_observation is workflow  # pylint: disable=protected-access
    resume_worker_call = tracer.start_calls[3]
    assert resume_worker_call["parent_observation_id"] == workflow.id

    await plugin.on_task_complete(resume_context, "final answer")

    assert workflow.ended_with == {"output": "final answer"}


@pytest.mark.asyncio
async def test_langfuse_resume_worker_execute_parents_to_agent_return():
    """Resume execution segment should follow the B -> A return causality."""
    tracer = FakeTracer()
    plugin = LangfusePlugin(tracer=tracer, observation_store=FakeObservationStore())

    initial_context = _build_context(
        message_id="msg-parent",
        parent_message_id="",
        current_agent_id="planner",
        langfuse_parent_observation_id="obs-client-dispatch",
    )
    initial_context.execution_id = "exec-a-1"
    await plugin.on_task_start(initial_context)
    workflow = initial_context._langfuse_workflow_observation  # pylint: disable=protected-access
    await plugin.on_task_complete(initial_context, {"status": "QUEUED"})

    resume_command = ResumeCommand(
        header=MessageHeader(
            message_id="msg-parent",
            session_id="session-1",
            trace_id="trace-1",
            source_agent_type="weather-agent",
            target_agent_type="planner",
            parent_message_id="msg-weather",
            langfuse_parent_observation_id="obs-agent-return",
            metadata={
                "framework_parent_span_id": "exec-weather:agent.return",
                "langfuse_parent_observation_id": "obs-agent-return",
            },
        ),
        status="success",
        reply_data="weather result",
    )
    resume_context = AgentContext(
        session_id="session-1",
        trace_id="trace-1",
        redis_client=object(),
        current_agent_id="planner",
        message_id="msg-parent",
        parent_message_id="",
        current_command=resume_command,
        execution_id="exec-a-2",
    )
    await plugin.on_task_start(resume_context)

    assert resume_context._langfuse_workflow_observation is workflow  # pylint: disable=protected-access
    resume_worker_call = tracer.start_calls[3]
    resume_agent_call = tracer.start_calls[4]
    assert resume_worker_call["name"] == "worker.execute"
    assert resume_worker_call["parent_observation_id"] == "obs-agent-return"
    assert resume_worker_call["metadata"]["resume_via"] == "agent.return"
    assert resume_worker_call["metadata"]["resume_from_agent_type"] == "weather-agent"
    assert resume_worker_call["metadata"]["resume_to_agent_type"] == "planner"
    assert resume_worker_call["metadata"]["resume_return_span_id"] == (
        "exec-weather:agent.return"
    )
    assert resume_agent_call["parent_observation_id"] == "obs-4"


@pytest.mark.asyncio
async def test_langfuse_plugin_child_task_does_not_become_trace_root():
    """Child task observations stay nested under the parent task trace root."""
    tracer = FakeTracer()
    store = FakeObservationStore()
    store.mapping[("session-1", "msg-parent")] = "obs-parent"
    plugin = LangfusePlugin(tracer=tracer, observation_store=store)
    context = _build_context(message_id="msg-child", parent_message_id="msg-parent")

    await plugin.on_task_start(context)

    workflow_call = tracer.start_calls[0]
    worker_execute_call = tracer.start_calls[1]
    agent_call = tracer.start_calls[2]

    assert workflow_call["name"] == "agent.workflow:planner"
    assert workflow_call["parent_observation_id"] == "obs-parent"
    assert worker_execute_call["name"] == "worker.execute"
    assert worker_execute_call["parent_observation_id"] == "obs-1"
    assert worker_execute_call["as_root"] is False
    assert agent_call["as_root"] is False


def test_sdk_tracer_preserves_root_observation_for_trace_name():
    """SDK adapter only clears Langfuse root promotion for nested observations."""
    pytest.importorskip("langfuse")
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
    assert tracer.trace_output_updates == [
        {
            "trace_id": "12345678901234567890123456789012",
            "output": {"status": "COMPLETED", "answer": "done"},
        }
    ]


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
    assert tracer.trace_output_updates == [
        {
            "trace_id": "12345678901234567890123456789012",
            "output": {"error": "boom"},
        }
    ]


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
    assert tracer.trace_output_updates == [
        {
            "trace_id": "12345678901234567890123456789012",
            "output": {"cancelled": True, "reason": "user cancelled"},
        }
    ]


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


@pytest.mark.asyncio
async def test_langfuse_trace_output_update_does_not_block_task_complete():
    """Trace-level output writes should not block the async worker event loop."""
    tracer = SlowTraceOutputTracer()
    plugin = LangfusePlugin(tracer=tracer, observation_store=FakeObservationStore())
    context = _build_context()

    await plugin.on_task_start(context)
    started = time.perf_counter()
    await plugin.on_task_complete(context, {"answer": "done"})
    elapsed = time.perf_counter() - started

    assert elapsed < 0.1


def test_start_client_dispatch_observation_reuses_langfuse_client(monkeypatch):
    """Client dispatch tracing should not instantiate a Langfuse SDK per message."""
    instances: list[Any] = []

    class FakeLangfuseClient:

        def __init__(self, **kwargs: Any):
            self.kwargs = kwargs
            instances.append(self)

        def start_observation(self, **kwargs: Any):
            del kwargs
            return FakeSdkObservation()

    fake_module = SimpleNamespace(Langfuse=FakeLangfuseClient)
    monkeypatch.setitem(sys.modules, "langfuse", fake_module)
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
    monkeypatch.setenv("LANGFUSE_BASE_URL", "http://localhost:3000")

    for idx in range(2):
        langfuse_module.start_client_dispatch_observation(
            trace_id="trace-client",
            message_id=f"msg-{idx}",
            target_agent_type="planner",
            session_id="session-1",
            content="hello",
        )

    assert len(instances) == 1


@pytest.mark.asyncio
async def test_langfuse_plugin_limits_active_workflow_cache():
    """Long-running workers should cap active workflow handles."""
    tracer = FakeTracer()
    plugin = LangfusePlugin(
        tracer=tracer,
        observation_store=FakeObservationStore(),
        max_active_workflows=2,
    )

    for idx in range(3):
        context = _build_context(message_id=f"msg-{idx}")
        await plugin.on_task_start(context)
        await plugin.on_task_complete(context, {"status": "QUEUED"})

    assert len(plugin._active_workflows) == 2  # pylint: disable=protected-access
    assert ("session-1", "msg-0") not in plugin._active_workflows  # pylint: disable=protected-access


def test_langfuse_trace_provider_factory_builds_plugin_from_env(monkeypatch):
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
    monkeypatch.setenv("LANGFUSE_BASE_URL", "http://localhost:3000")

    plugin = LangfuseTraceProviderFactory().build_plugin_from_env()

    assert isinstance(plugin, LangfusePlugin)
