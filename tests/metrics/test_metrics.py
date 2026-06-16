"""Unit tests for observability metrics integration."""

from __future__ import annotations

import by_framework.metrics as metrics_module
from by_framework.metrics import (
    PROMETHEUS_AVAILABLE,
    build_observability_diagnostics_metrics,
    generate_latest_metrics,
    get_registry,
    record_execution_metrics,
    record_execution_started_metrics,
)
from by_framework.trace.span_recorder import (
    TraceSpan,
    get_observability_diagnostics,
    reset_observability_diagnostics,
)


def test_metrics_recording_does_not_raise_exception():
    """Metrics record helper functions under standard / dummy modes do not crash."""
    record_execution_started_metrics(agent_type="dummy_agent")
    record_execution_metrics(
        status="COMPLETED",
        agent_type="dummy_agent",
        worker_id="test_worker_1",
        execution_ms=125.0,
        queue_wait_ms_val=45.0,
    )


class FakeMetric:
    """Small metric fake that records label calls and increments."""

    def __init__(self):
        self.calls = []

    def labels(self, **kwargs):
        self.calls.append({"labels": kwargs, "inc": 0, "observe": []})
        return self

    def inc(self, amount=1.0):
        self.calls[-1]["inc"] += amount

    def observe(self, amount):
        self.calls[-1]["observe"].append(amount)


def test_started_counter_is_recorded_only_for_start_events(monkeypatch):
    """Started and terminal counters should represent different lifecycle events."""
    started = FakeMetric()
    completed = FakeMetric()
    failed = FakeMetric()
    status_total = FakeMetric()
    latency_ms = FakeMetric()
    run_seconds = FakeMetric()
    total_seconds = FakeMetric()

    monkeypatch.setattr(metrics_module, "executions_started_total", started)
    monkeypatch.setattr(metrics_module, "executions_completed_total", completed)
    monkeypatch.setattr(metrics_module, "executions_failed_total", failed)
    monkeypatch.setattr(metrics_module, "execution_status_total", status_total)
    monkeypatch.setattr(metrics_module, "execution_latency_ms", latency_ms)
    monkeypatch.setattr(metrics_module, "execution_run_duration_seconds", run_seconds)
    monkeypatch.setattr(
        metrics_module, "execution_total_duration_seconds", total_seconds
    )

    record_execution_started_metrics(agent_type="planner")
    record_execution_metrics(
        status="COMPLETED",
        agent_type="planner",
        worker_id="worker-1",
        execution_ms=250,
    )

    assert len(started.calls) == 1
    assert started.calls[0]["labels"] == {"agent_type": "planner"}
    assert completed.calls[0]["labels"] == {
        "status": "COMPLETED",
        "agent_type": "planner",
    }


def test_generate_latest_metrics_output():
    """Generating latest metrics produces expected output formats."""
    text = generate_latest_metrics()
    if PROMETHEUS_AVAILABLE:
        assert isinstance(text, str)
        # Verify the key metrics appear in registry outputs
        assert "by_framework_execution_status_total" in text
        assert "by_framework_execution_latency_ms" in text
        assert "by_framework_queue_wait_ms" in text
        assert "by_framework_executions_started_total" in text
        assert "by_framework_executions_completed_total" in text
        assert "by_framework_execution_queue_duration_seconds" in text
        assert "by_framework_execution_run_duration_seconds" in text
        assert "by_framework_execution_total_duration_seconds" in text
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
    from by_framework.trace.span_recorder import SpanRecorder

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
