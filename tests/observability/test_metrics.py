"""Unit tests for observability metrics integration."""

from __future__ import annotations

from by_framework.observability.metrics import (
    PROMETHEUS_AVAILABLE,
    build_observability_diagnostics_metrics,
    generate_latest_metrics,
    get_registry,
    record_execution_metrics,
)
from by_framework.observability.span_recorder import (
    TraceSpan,
    get_observability_diagnostics,
    reset_observability_diagnostics,
)


def test_metrics_recording_does_not_raise_exception():
    """Metrics record helper functions under standard / dummy modes do not crash."""
    record_execution_metrics(
        status="COMPLETED",
        agent_type="dummy_agent",
        worker_id="test_worker_1",
        execution_ms=125.0,
        queue_wait_ms_val=45.0,
    )


def test_generate_latest_metrics_output():
    """Generating latest metrics produces expected output formats."""
    text = generate_latest_metrics()
    if PROMETHEUS_AVAILABLE:
        assert isinstance(text, str)
        # Verify the key metrics appear in registry outputs
        assert "by_framework_execution_status_total" in text
        assert "by_framework_execution_latency_ms" in text
        assert "by_framework_queue_wait_ms" in text
        assert 'status="COMPLETED"' in text
        assert 'agent_type="dummy_agent"' in text
    else:
        assert text == ""


def test_get_registry():
    """Active Prometheus collector registry mapping."""
    registry = get_registry()
    if PROMETHEUS_AVAILABLE:
        assert registry is not None
    else:
        assert registry is None


def test_build_observability_diagnostics_metrics_exports_drop_and_failure_counts():
    """Trace exporter self-diagnostics are exported as Prometheus text."""
    reset_observability_diagnostics()

    # Mutate diagnostics through public record behavior.
    from by_framework.observability.span_recorder import SpanRecorder

    recorder = SpanRecorder(exporters=[])
    span = TraceSpan(
        trace_id="trace-diag",
        span_id="span-1",
        parent_span_id="",
        operation="agent.emit_chunk",
        component="agent_context",
        start_ts=1,
        end_ts=2,
        status="COMPLETED",
    )

    import asyncio

    asyncio.run(recorder.record_span(span))
    diagnostics = get_observability_diagnostics()

    metrics = build_observability_diagnostics_metrics(diagnostics)

    assert "by_framework_observability_dropped_spans_total 1" in metrics
    assert (
        'by_framework_observability_dropped_spans_by_reason_total{reason="disabled"} 1'
        in metrics
    )
