import sys
from unittest.mock import MagicMock, patch

import pytest

from by_framework.observability.span_recorder import (
    ContextIdGenerator,
    OTelSpanExporter,
    TraceSpan,
    current_span_id_var,
    current_trace_id_var,
    live_execution_otel_span,
    str_to_uint64,
    str_to_uint128,
)


def test_id_converters():
    """Trace and span IDs are deterministically converted to integers."""
    trace_id_1 = "test-trace-id-123"
    trace_id_2 = "test-trace-id-123"
    trace_id_3 = "test-trace-id-456"

    val_1 = str_to_uint128(trace_id_1)
    val_2 = str_to_uint128(trace_id_2)
    val_3 = str_to_uint128(trace_id_3)

    assert isinstance(val_1, int)
    assert val_1 == val_2
    assert val_1 != val_3
    assert val_1 > 0

    span_id_1 = "test-span-id-123"
    span_id_2 = "test-span-id-123"
    span_id_3 = "test-span-id-456"

    s_val_1 = str_to_uint64(span_id_1)
    s_val_2 = str_to_uint64(span_id_2)
    s_val_3 = str_to_uint64(span_id_3)

    assert isinstance(s_val_1, int)
    assert s_val_1 == s_val_2
    assert s_val_1 != s_val_3
    assert s_val_1 > 0


def test_context_id_generator_with_values():
    """ContextIdGenerator returns values explicitly set in context vars."""
    generator = ContextIdGenerator()

    # Simulate explicit context values.
    t_token = current_trace_id_var.set(12345678901234567890123456789012)
    s_token = current_span_id_var.set(9876543210987654)
    try:
        assert generator.generate_trace_id() == 12345678901234567890123456789012
        assert generator.generate_span_id() == 9876543210987654
    finally:
        current_trace_id_var.reset(t_token)
        current_span_id_var.reset(s_token)


def test_context_id_generator_fallback():
    """ContextIdGenerator falls back to random non-zero IDs."""
    generator = ContextIdGenerator()

    t_token = current_trace_id_var.set(None)
    s_token = current_span_id_var.set(None)
    try:
        id_1 = generator.generate_trace_id()
        id_2 = generator.generate_trace_id()
        assert id_1 != id_2
        assert id_1 > 0

        sid_1 = generator.generate_span_id()
        sid_2 = generator.generate_span_id()
        assert sid_1 != sid_2
        assert sid_1 > 0
    finally:
        current_trace_id_var.reset(t_token)
        current_span_id_var.reset(s_token)


@pytest.mark.asyncio
async def test_otel_span_exporter_missing_dep():
    """OTelSpanExporter stays silent when OTel is unavailable."""
    # Simulate import failure.
    with patch("opentelemetry.trace", None):
        exporter = OTelSpanExporter()
        assert exporter._tracer is None

        # Calling export_span should remain a no-op.
        span = TraceSpan(
            trace_id="t-1",
            span_id="s-1",
            parent_span_id="",
            operation="op",
            component="c",
            start_ts=1000,
            end_ts=2000,
            status="COMPLETED",
        )
        await exporter.export_span(span)


@pytest.mark.asyncio
async def test_otel_span_exporter_success():
    """OTelSpanExporter exports spans when OTel is available."""
    mock_trace = MagicMock()
    mock_tracer = MagicMock()
    mock_span = MagicMock()

    mock_trace.get_tracer.return_value = mock_tracer
    mock_tracer.start_span.return_value = mock_span

    # Mock the OTel API surface.
    mock_trace.SpanContext = MagicMock()
    mock_trace.NonRecordingSpan = MagicMock()
    mock_trace.set_span_in_context = MagicMock()

    mock_trace_flags = MagicMock()
    mock_trace_flags.SAMPLED = 1
    mock_trace.TraceFlags = mock_trace_flags

    # Mock imports.
    modules = {
        "opentelemetry": mock_trace,
        "opentelemetry.trace": mock_trace,
        "opentelemetry.sdk": MagicMock(),
        "opentelemetry.sdk.trace": MagicMock(),
    }

    with patch.dict(sys.modules, modules):
        exporter = OTelSpanExporter()
        # Manually bind attributes to simulate a loaded exporter.
        exporter.trace_mod = mock_trace
        exporter._tracer = mock_tracer

        span = TraceSpan(
            trace_id="trace-abc",
            span_id="span-123",
            parent_span_id="parent-456",
            operation="test.op",
            component="worker",
            start_ts=1717320000000,  # Millisecond timestamp.
            end_ts=1717320001000,
            status="FAILED",
            error_message="Test failure error",
        )

        await exporter.export_span(span)

        # Verify start_span call arguments.
        mock_tracer.start_span.assert_called_once()
        args, kwargs = mock_tracer.start_span.call_args
        assert kwargs["name"] == "test.op"
        assert kwargs["start_time"] == 1717320000000 * 1_000_000

        # Verify status and end calls.
        mock_span.set_status.assert_called_once()
        mock_span.end.assert_called_once_with(end_time=1717320001000 * 1_000_000)


@pytest.mark.asyncio
async def test_live_execution_span_makes_worker_execute_current_parent():
    """Child spans created inside worker.execute inherit it without id collisions."""
    trace_sdk = pytest.importorskip("opentelemetry.sdk.trace")
    otel_trace = pytest.importorskip("opentelemetry.trace")
    in_memory = pytest.importorskip(
        "opentelemetry.sdk.trace.export.in_memory_span_exporter"
    )
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor

    provider = trace_sdk.TracerProvider()
    exporter = in_memory.InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    # Save/restore the raw module attribute (not the proxy from
    # get_tracer_provider) so we don't leave a self-delegating proxy behind.
    previous = otel_trace._TRACER_PROVIDER
    otel_trace._TRACER_PROVIDER = provider  # force global provider for the test

    try:
        trace_id = "live-trace-1"
        worker_span_str = "exec-1:worker.execute"
        async with live_execution_otel_span(
            trace_id=trace_id,
            span_id=worker_span_str,
            parent_span_id="msg-1:client.dispatch",
            operation="worker.execute",
            attributes={"component": "worker"},
            start_ts=1_000,
            otel_enabled=True,
        ):
            # ID injection vars must be reset inside the body so child spans do not
            # reuse the parent span id.
            assert current_span_id_var.get() is None
            assert current_trace_id_var.get() is None

            child = provider.get_tracer("agent").start_span("llm.call")
            child.end()
    finally:
        otel_trace._TRACER_PROVIDER = previous

    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert "worker.execute" in spans
    assert "llm.call" in spans

    worker_span = spans["worker.execute"]
    child_span = spans["llm.call"]

    # worker.execute uses a deterministic span_id.
    assert worker_span.context.span_id == str_to_uint64(worker_span_str)
    # The child span is parented to worker.execute and shares the same trace.
    assert child_span.parent is not None
    assert child_span.parent.span_id == str_to_uint64(worker_span_str)
    assert child_span.context.trace_id == str_to_uint128(trace_id)
    # The child span has its own span_id and does not collide with the parent.
    assert child_span.context.span_id != worker_span.context.span_id


@pytest.mark.asyncio
async def test_live_execution_span_is_disabled_without_otel_opt_in():
    """live_execution_otel_span skips span creation when otel_enabled=False."""
    trace_sdk = pytest.importorskip("opentelemetry.sdk.trace")
    otel_trace = pytest.importorskip("opentelemetry.trace")
    in_memory = pytest.importorskip(
        "opentelemetry.sdk.trace.export.in_memory_span_exporter"
    )
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor

    provider = trace_sdk.TracerProvider()
    exporter = in_memory.InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    previous = otel_trace._TRACER_PROVIDER
    otel_trace._TRACER_PROVIDER = provider

    try:
        async with live_execution_otel_span(
            trace_id="live-trace-disabled",
            span_id="exec-disabled:worker.execute",
            parent_span_id="",
            operation="worker.execute",
            attributes={"component": "worker"},
            start_ts=1_000,
            otel_enabled=False,
        ):
            child = provider.get_tracer("agent").start_span("llm.call")
            child.end()
    finally:
        otel_trace._TRACER_PROVIDER = previous

    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert "worker.execute" not in spans
