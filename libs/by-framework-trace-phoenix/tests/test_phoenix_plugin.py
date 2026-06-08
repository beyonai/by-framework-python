"""Tests for Arize Phoenix Trace plugin."""

# pylint: disable=protected-access,too-many-arguments

from typing import Any, Optional
from unittest.mock import MagicMock

import pytest
from by_framework import (AgentContext, AskAgentCommand, MessageHeader, ResumeCommand)
from opentelemetry import trace

from by_framework_trace_phoenix.phoenix import PhoenixConfig, PhoenixPlugin


def _build_context(
    *,
    message_id: str = "msg-1",
    parent_message_id: str = "",
    trace_id: str = "12345678901234567890123456789012",
    session_id: str = "session-1",
    current_agent_id: str = "planner",
    metadata: Optional[dict[str, Any]] = None,
    command_class=AskAgentCommand,
    trace_parent_span_id: str = "",
) -> AgentContext:
    if command_class == ResumeCommand:
        command = ResumeCommand(
            header=MessageHeader(
                message_id=message_id,
                session_id=session_id,
                trace_id=trace_id,
                target_agent_type=current_agent_id,
                parent_message_id=parent_message_id,
                metadata=metadata or {},
                trace_parent_span_id=trace_parent_span_id,
            ),
            status="success",
            reply_data=None,
        )
    else:
        command = AskAgentCommand(
            header=MessageHeader(
                message_id=message_id,
                session_id=session_id,
                trace_id=trace_id,
                target_agent_type=current_agent_id,
                parent_message_id=parent_message_id,
                metadata=metadata or {},
                trace_parent_span_id=trace_parent_span_id,
            ),
            content="hello",
        )
    return AgentContext(
        session_id=session_id,
        trace_id=trace_id,
        redis_client=object(),
        current_agent_id=current_agent_id,
        message_id=message_id,
        parent_message_id=parent_message_id,
        current_command=command,
    )


@pytest.mark.asyncio
async def test_phoenix_plugin_child_agent_uses_parent_span_id_from_metadata():
    """Child task spans nest under the trace_parent_span_id in metadata if present."""
    mock_tracer = MagicMock()
    plugin = PhoenixPlugin(config=PhoenixConfig(enabled=True))
    plugin._tracer = mock_tracer

    context = _build_context(
        message_id="msg-child",
        parent_message_id="msg-parent",
        metadata={"trace_parent_span_id": "0000000000003039"},
    )

    await plugin.on_task_start(context)

    mock_tracer.start_span.assert_called_once()
    call_kwargs = mock_tracer.start_span.call_args.kwargs
    parent_context = call_kwargs.get("context")
    assert parent_context is not None

    extracted_span = trace.get_current_span(parent_context)
    span_context = extracted_span.get_span_context()
    assert span_context.span_id == 12345


@pytest.mark.asyncio
async def test_phoenix_plugin_child_agent_parent_span_id_from_header_attr():
    """Child task spans nest under trace_parent_span_id in header attr if present."""
    mock_tracer = MagicMock()
    plugin = PhoenixPlugin(config=PhoenixConfig(enabled=True))
    plugin._tracer = mock_tracer

    context = _build_context(
        message_id="msg-child",
        parent_message_id="msg-parent",
        trace_parent_span_id="0000000000003039",
    )

    await plugin.on_task_start(context)

    mock_tracer.start_span.assert_called_once()
    call_kwargs = mock_tracer.start_span.call_args.kwargs
    parent_context = call_kwargs.get("context")
    assert parent_context is not None

    extracted_span = trace.get_current_span(parent_context)
    span_context = extracted_span.get_span_context()
    assert span_context.span_id == 12345


@pytest.mark.asyncio
async def test_phoenix_plugin_top_level_resume_ignores_parent_span_id_from_metadata():
    """Top-level resumed tasks ignore trace_parent_span_id to avoid self-parent loops."""
    mock_tracer = MagicMock()
    plugin = PhoenixPlugin(config=PhoenixConfig(enabled=True))
    plugin._tracer = mock_tracer

    context = _build_context(
        message_id="msg-resume",
        parent_message_id="",
        metadata={"trace_parent_span_id": "0000000000003039"},
        command_class=ResumeCommand,
    )

    await plugin.on_task_start(context)

    mock_tracer.start_span.assert_called_once()
    call_kwargs = mock_tracer.start_span.call_args.kwargs
    parent_context = call_kwargs.get("context")
    assert parent_context is None


@pytest.mark.asyncio
async def test_phoenix_plugin_fallback_to_parent_message_id_hash():
    """If no trace_parent_span_id is in metadata, fallback to parent_message_id hash."""
    mock_tracer = MagicMock()
    plugin = PhoenixPlugin(config=PhoenixConfig(enabled=True))
    plugin._tracer = mock_tracer

    context = _build_context(
        message_id="msg-child",
        parent_message_id="parent-msg-id-xyz",
        metadata={},
    )

    await plugin.on_task_start(context)

    mock_tracer.start_span.assert_called_once()
    call_kwargs = mock_tracer.start_span.call_args.kwargs
    parent_context = call_kwargs.get("context")
    assert parent_context is not None

    extracted_span = trace.get_current_span(parent_context)
    span_context = extracted_span.get_span_context()
    expected_span_id = plugin._str_to_uint64("parent-msg-id-xyz")
    assert span_context.span_id == expected_span_id
