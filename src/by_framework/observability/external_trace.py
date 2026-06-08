"""Helpers for external applications joining by-framework traces."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from by_framework.core.protocol.commands import BaseCommand
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.observability.span_recorder import str_to_uint128


@dataclass(frozen=True)
class ExternalTraceContext:
    """Trace values an external app needs to join framework observability."""

    framework_trace_id: str
    langfuse_trace_id: str
    langfuse_parent_observation_id: str
    trace_parent_span_id: str
    session_id: str = ""
    message_id: str = ""
    parent_message_id: str = ""
    source_agent_type: str = ""
    target_agent_type: str = ""
    metadata: dict[str, Any] | None = None


def to_langfuse_trace_id(framework_trace_id: str) -> str:
    """Convert a framework trace id to the 32-char hex id Langfuse uses."""
    return f"{str_to_uint128(framework_trace_id):032x}"


def extract_external_trace_context(source: Any) -> ExternalTraceContext:
    """Extract trace join values from a command, header, or plain command dict."""
    header = _coerce_header_dict(source)
    metadata = dict(header.get("metadata", {}) or {})
    framework_trace_id = str(header.get("trace_id", ""))
    if not framework_trace_id:
        raise ValueError("trace_id is required to join an external trace")

    langfuse_parent_observation_id = str(
        header.get("langfuse_parent_observation_id")
        or metadata.get("langfuse_parent_observation_id")
        or ""
    )
    trace_parent_span_id = str(
        header.get("trace_parent_span_id") or metadata.get("trace_parent_span_id") or ""
    )
    return ExternalTraceContext(
        framework_trace_id=framework_trace_id,
        langfuse_trace_id=to_langfuse_trace_id(framework_trace_id),
        langfuse_parent_observation_id=langfuse_parent_observation_id,
        trace_parent_span_id=trace_parent_span_id,
        session_id=str(header.get("session_id", "")),
        message_id=str(header.get("message_id", "")),
        parent_message_id=str(header.get("parent_message_id", "")),
        source_agent_type=str(header.get("source_agent_type", "")),
        target_agent_type=str(header.get("target_agent_type", "")),
        metadata=metadata,
    )


def build_langfuse_trace_context(source: Any) -> dict[str, str]:
    """Return Langfuse ``trace_context`` for start_observation."""
    context = extract_external_trace_context(source)
    if not context.langfuse_parent_observation_id:
        raise ValueError("langfuse_parent_observation_id is required for Langfuse")
    return {
        "trace_id": context.langfuse_trace_id,
        "parent_span_id": context.langfuse_parent_observation_id,
    }


def start_langfuse_observation(
    langfuse_client: Any,
    source: Any,
    *,
    name: str,
    as_type: str = "span",
    input_data: Any = None,
    metadata: dict[str, Any] | None = None,
    **kwargs: Any,
) -> Any:
    """Start a Langfuse observation attached to the framework parent."""
    context = extract_external_trace_context(source)
    merged_metadata = {
        **dict(metadata or {}),
        "session_id": context.session_id,
        "message_id": context.message_id,
        "source_agent_type": context.source_agent_type,
        "target_agent_type": context.target_agent_type,
    }
    observation = langfuse_client.start_observation(
        name=name,
        as_type=as_type,
        trace_context=build_langfuse_trace_context(source),
        input=input_data,
        metadata=merged_metadata,
        **kwargs,
    )
    _mark_langfuse_observation_as_non_root(observation)
    return observation


def build_otel_parent_context(source: Any) -> Any:
    """Build an OpenTelemetry parent context from command trace fields."""
    context = extract_external_trace_context(source)
    if not context.trace_parent_span_id:
        raise ValueError("trace_parent_span_id is required for OpenTelemetry")

    from opentelemetry import trace
    from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

    parent_span_context = SpanContext(
        trace_id=str_to_uint128(context.framework_trace_id),
        span_id=int(context.trace_parent_span_id, 16),
        is_remote=True,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )
    return trace.set_span_in_context(NonRecordingSpan(parent_span_context))


def _coerce_header_dict(source: Any) -> dict[str, Any]:
    if isinstance(source, MessageHeader):
        return source.to_dict()
    if isinstance(source, BaseCommand):
        return source.header.to_dict()
    if isinstance(source, dict):
        if "header" in source and isinstance(source["header"], dict):
            return dict(source["header"])
        if "message_id" in source and "trace_id" in source:
            return dict(source)
    header = getattr(source, "header", None)
    if isinstance(header, MessageHeader):
        return header.to_dict()
    raise TypeError("expected MessageHeader, command, command dict, or header dict")


def _mark_langfuse_observation_as_non_root(observation: Any) -> None:
    """Prevent external child observations from renaming the Langfuse trace root."""
    otel_span = getattr(observation, "_otel_span", None)
    if otel_span is None or not hasattr(otel_span, "set_attribute"):
        return
    try:
        otel_span.set_attribute("langfuse.internal.as_root", False)
    except Exception:  # pylint: disable=broad-exception-caught
        return
