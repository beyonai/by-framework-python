"""Tests for external app trace context helpers."""

from by_framework.core.protocol.commands import AskAgentCommand
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.observability.external_trace import (
    build_langfuse_trace_context,
    extract_external_trace_context,
    start_langfuse_observation,
)
from by_framework.observability.span_recorder import str_to_uint128


class FakeLangfuseClient:
    """Small Langfuse client double that records start_observation arguments."""

    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.observation = FakeObservation()

    def start_observation(self, **kwargs):
        self.calls.append(kwargs)
        self.observation.name = kwargs["name"]
        return self.observation


class FakeOtelSpan:
    """Records attributes set on the underlying OTel span."""

    def __init__(self) -> None:
        self.attributes: dict[str, object] = {}

    def set_attribute(self, key: str, value: object) -> None:
        self.attributes[key] = value


class FakeObservation:
    """Observation double shaped like Langfuse SDK wrappers."""

    def __init__(self) -> None:
        self.name = ""
        self._otel_span = FakeOtelSpan()


def test_extract_external_trace_context_from_command_dict():
    """External apps can read all trace parent values from plain command dicts."""
    trace_id_str = "trace-plain"
    command = AskAgentCommand(
        header=MessageHeader(
            message_id="msg-1",
            session_id="sess-1",
            trace_id=trace_id_str,
            target_agent_type="external",
            trace_parent_span_id="0123456789abcdef",
            langfuse_parent_observation_id="obs-parent",
        ),
        content="hello",
    )

    context = extract_external_trace_context(command.to_dict())

    assert context.framework_trace_id == trace_id_str
    assert context.langfuse_trace_id == f"{str_to_uint128(trace_id_str):032x}"
    assert context.langfuse_parent_observation_id == "obs-parent"
    assert context.trace_parent_span_id == "0123456789abcdef"
    assert context.session_id == "sess-1"
    assert context.message_id == "msg-1"
    assert context.target_agent_type == "external"


def test_build_langfuse_trace_context_uses_observation_parent():
    """Langfuse trace_context uses converted trace id and parent observation id."""
    trace_id_str = "trace-plain"
    command = AskAgentCommand(
        header=MessageHeader(
            message_id="msg-1",
            session_id="sess-1",
            trace_id=trace_id_str,
            langfuse_parent_observation_id="obs-parent",
        ),
        content="hello",
    )

    assert build_langfuse_trace_context(command) == {
        "trace_id": f"{str_to_uint128(trace_id_str):032x}",
        "parent_span_id": "obs-parent",
    }


def test_start_langfuse_observation_populates_parent_and_metadata():
    """Helper starts a Langfuse observation attached to the framework parent."""
    client = FakeLangfuseClient()
    command = AskAgentCommand(
        header=MessageHeader(
            message_id="msg-1",
            session_id="sess-1",
            trace_id="trace-plain",
            source_agent_type="caller",
            target_agent_type="external",
            langfuse_parent_observation_id="obs-parent",
        ),
        content="hello",
    )

    trace_id_str = "trace-plain"
    observation = start_langfuse_observation(
        client,
        command,
        name="external_plain_app",
        as_type="span",
        input_data="hello",
        metadata={"integration": "plain"},
    )

    assert observation is client.observation
    assert observation.name == "external_plain_app"
    assert observation._otel_span.attributes == {"langfuse.internal.as_root": False}
    assert client.calls == [
        {
            "name": "external_plain_app",
            "as_type": "span",
            "trace_context": {
                "trace_id": f"{str_to_uint128(trace_id_str):032x}",
                "parent_span_id": "obs-parent",
            },
            "input": "hello",
            "metadata": {
                "integration": "plain",
                "session_id": "sess-1",
                "message_id": "msg-1",
                "source_agent_type": "caller",
                "target_agent_type": "external",
            },
        }
    ]
