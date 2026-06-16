"""Metric catalog for by-framework observability.

The catalog is the source of truth for normalized metric names, units, labels,
and human interpretation. Runtime instrumentation and snapshot exporters may
carry legacy metrics for compatibility, but new dashboard and Prometheus
surfaces should prefer definitions from this module.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class MetricKind(str, Enum):
    """Prometheus-compatible metric kinds."""

    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"


class MetricUnit(str, Enum):
    """Canonical units used by normalized metrics."""

    RATIO = "ratio"
    MILLISECONDS = "milliseconds"
    SECONDS = "seconds"
    MESSAGES = "messages"
    EXECUTIONS = "executions"
    WORKERS = "workers"
    NONE = "none"


@dataclass(frozen=True)
class MetricDefinition:
    """Metadata describing one observable metric contract."""

    name: str
    kind: MetricKind
    unit: MetricUnit
    labels: tuple[str, ...] = ()
    description: str = ""
    interpretation: str = ""
    legacy_names: tuple[str, ...] = field(default_factory=tuple)
    debug_only: bool = False


_CATALOG: dict[str, MetricDefinition] = {
    "by_framework_executions_started_total": MetricDefinition(
        name="by_framework_executions_started_total",
        kind=MetricKind.COUNTER,
        unit=MetricUnit.EXECUTIONS,
        labels=("agent_type",),
        description="Executions accepted by the framework by target agent type.",
        interpretation="Traffic signal: sustained growth shows incoming work volume.",
    ),
    "by_framework_executions_completed_total": MetricDefinition(
        name="by_framework_executions_completed_total",
        kind=MetricKind.COUNTER,
        unit=MetricUnit.EXECUTIONS,
        labels=("status", "agent_type"),
        description="Executions that reached a terminal status.",
        interpretation="Traffic and availability signal for completed framework work.",
    ),
    "by_framework_executions_failed_total": MetricDefinition(
        name="by_framework_executions_failed_total",
        kind=MetricKind.COUNTER,
        unit=MetricUnit.EXECUTIONS,
        labels=("agent_type", "error_type"),
        description="Terminal failed executions by agent type and error type.",
        interpretation="Error signal: use with total completions for failure ratio.",
    ),
    "by_framework_execution_queue_duration_seconds": MetricDefinition(
        name="by_framework_execution_queue_duration_seconds",
        kind=MetricKind.HISTOGRAM,
        unit=MetricUnit.SECONDS,
        labels=("agent_type",),
        description="Time an execution waited before worker processing started.",
        interpretation="Latency signal for Redis Streams queue wait.",
        legacy_names=("by_framework_queue_wait_ms",),
    ),
    "by_framework_execution_run_duration_seconds": MetricDefinition(
        name="by_framework_execution_run_duration_seconds",
        kind=MetricKind.HISTOGRAM,
        unit=MetricUnit.SECONDS,
        labels=("status", "agent_type"),
        description="Worker processing duration for terminal executions.",
        interpretation="Latency signal for worker and agent execution.",
    ),
    "by_framework_execution_total_duration_seconds": MetricDefinition(
        name="by_framework_execution_total_duration_seconds",
        kind=MetricKind.HISTOGRAM,
        unit=MetricUnit.SECONDS,
        labels=("status", "agent_type"),
        description="End-to-end execution duration from enqueue to terminal state.",
        interpretation="Primary user-visible latency SLI.",
        legacy_names=("by_framework_execution_latency_ms",),
    ),
    "by_framework_execution_status_total": MetricDefinition(
        name="by_framework_execution_status_total",
        kind=MetricKind.COUNTER,
        unit=MetricUnit.EXECUTIONS,
        labels=("status", "agent_type", "worker_id"),
        description=(
            "Legacy terminal execution count by status, agent type, and worker."
        ),
        interpretation=(
            "Debug-only worker-level execution count; avoid long-retention "
            "aggregation by worker_id."
        ),
        debug_only=True,
    ),
    "by_framework_execution_latency_ms": MetricDefinition(
        name="by_framework_execution_latency_ms",
        kind=MetricKind.HISTOGRAM,
        unit=MetricUnit.MILLISECONDS,
        labels=("status", "agent_type", "worker_id"),
        description="Legacy worker execution latency histogram in milliseconds.",
        interpretation=(
            "Debug-only worker-level latency; prefer "
            "by_framework_execution_run_duration_seconds."
        ),
        debug_only=True,
    ),
    "by_framework_queue_wait_ms": MetricDefinition(
        name="by_framework_queue_wait_ms",
        kind=MetricKind.HISTOGRAM,
        unit=MetricUnit.MILLISECONDS,
        labels=("agent_type",),
        description="Legacy queue wait histogram in milliseconds.",
        interpretation=(
            "Compatibility metric; prefer "
            "by_framework_execution_queue_duration_seconds."
        ),
    ),
    "by_framework_availability_routing_ms": MetricDefinition(
        name="by_framework_availability_routing_ms",
        kind=MetricKind.HISTOGRAM,
        unit=MetricUnit.MILLISECONDS,
        labels=("agent_type", "policy", "status"),
        description=(
            "Availability routing latency before message dispatch in milliseconds."
        ),
        interpretation="Routing latency signal for scheduler decisions.",
    ),
    "by_framework_stream_depth": MetricDefinition(
        name="by_framework_stream_depth",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.MESSAGES,
        labels=("queue_type", "name"),
        description="Redis Stream length by framework queue.",
        interpretation="Saturation signal: backlog waiting to be consumed.",
        legacy_names=("by_framework_queue_depth",),
    ),
    "by_framework_stream_pending_messages": MetricDefinition(
        name="by_framework_stream_pending_messages",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.MESSAGES,
        labels=("queue_type", "name", "group"),
        description="Messages pending acknowledgment in a consumer group.",
        interpretation="Saturation signal: work owned by consumers but not acked.",
    ),
    "by_framework_stream_oldest_pending_age_seconds": MetricDefinition(
        name="by_framework_stream_oldest_pending_age_seconds",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.SECONDS,
        labels=("queue_type", "name", "group"),
        description="Idle age for the oldest pending message in a group.",
        interpretation="Saturation signal: high values indicate stuck delivery.",
    ),
    "by_framework_stream_max_delivery_count": MetricDefinition(
        name="by_framework_stream_max_delivery_count",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.NONE,
        labels=("queue_type", "name", "group"),
        description="Maximum pending delivery attempt count in a Redis Stream group.",
        interpretation=(
            "Retry pressure signal: high values indicate messages repeatedly "
            "redelivered before ack."
        ),
    ),
    "by_framework_workers_online": MetricDefinition(
        name="by_framework_workers_online",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.WORKERS,
        description="Online workers discovered by the registry.",
        interpretation="Freshness and capacity signal for active workers.",
    ),
    "by_framework_agent_types": MetricDefinition(
        name="by_framework_agent_types",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.NONE,
        description="Known online agent types currently served by workers.",
        interpretation="Capacity breadth signal for available agent coverage.",
    ),
    "by_framework_active_executions": MetricDefinition(
        name="by_framework_active_executions",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.EXECUTIONS,
        description="Active executions across all observable workers.",
        interpretation="Saturation signal for currently running work.",
    ),
    "by_framework_tracked_executions": MetricDefinition(
        name="by_framework_tracked_executions",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.EXECUTIONS,
        description="Executions tracked across worker active and history stores.",
        interpretation="Debugging scope signal for retained execution visibility.",
    ),
    "by_framework_execution_status_current": MetricDefinition(
        name="by_framework_execution_status_current",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.EXECUTIONS,
        labels=("status",),
        description="Current execution counts by lifecycle status.",
        interpretation="Availability and saturation signal by execution state.",
    ),
    "by_framework_queue_depth": MetricDefinition(
        name="by_framework_queue_depth",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.MESSAGES,
        labels=("queue_type", "name", "stream"),
        description="Legacy Redis Stream length by queue.",
        interpretation="Legacy saturation signal; prefer by_framework_stream_depth.",
    ),
    "by_framework_agent_queue_depth": MetricDefinition(
        name="by_framework_agent_queue_depth",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.MESSAGES,
        labels=("agent_type",),
        description="Redis control queue depth by agent type.",
        interpretation="Saturation signal for work awaiting a matching worker.",
    ),
    "by_framework_agent_workers": MetricDefinition(
        name="by_framework_agent_workers",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.WORKERS,
        labels=("agent_type",),
        description="Online worker count supporting each agent type.",
        interpretation="Capacity signal for agent-type level routing.",
    ),
    "by_framework_agent_recent_failed_executions": MetricDefinition(
        name="by_framework_agent_recent_failed_executions",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.EXECUTIONS,
        labels=("agent_type",),
        description="Recent failed executions grouped by agent type.",
        interpretation="Error signal for agent-type level health.",
    ),
    "by_framework_stream_consumer_lag": MetricDefinition(
        name="by_framework_stream_consumer_lag",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.MESSAGES,
        labels=("queue_type", "name", "group"),
        description="Redis Stream consumer group lag when Redis reports it.",
        interpretation="Saturation signal for unread messages behind a group.",
    ),
    "by_framework_execution_recent_failures": MetricDefinition(
        name="by_framework_execution_recent_failures",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.EXECUTIONS,
        labels=("error_type",),
        description="Recent failed executions by error class.",
        interpretation="Error signal for dominant failure modes.",
    ),
    "by_framework_alerts_current": MetricDefinition(
        name="by_framework_alerts_current",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.NONE,
        labels=("severity",),
        description="Current derived health alerts by severity.",
        interpretation="Aggregated alert pressure signal for operators.",
    ),
    "by_framework_execution_latency_avg_ms": MetricDefinition(
        name="by_framework_execution_latency_avg_ms",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.MILLISECONDS,
        description="Average completed worker run latency in milliseconds.",
        interpretation="Legacy latency signal; prefer seconds duration metrics.",
    ),
    "by_framework_execution_latency_p95_ms": MetricDefinition(
        name="by_framework_execution_latency_p95_ms",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.MILLISECONDS,
        description="P95 completed worker run latency in milliseconds.",
        interpretation="Legacy latency signal; prefer seconds duration metrics.",
    ),
    "by_framework_execution_queue_latency_p95_ms": MetricDefinition(
        name="by_framework_execution_queue_latency_p95_ms",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.MILLISECONDS,
        description="P95 queue wait latency in milliseconds.",
        interpretation="Legacy queue latency signal; prefer seconds duration metrics.",
    ),
    "by_framework_execution_total_latency_p95_ms": MetricDefinition(
        name="by_framework_execution_total_latency_p95_ms",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.MILLISECONDS,
        description="P95 end-to-end execution latency in milliseconds.",
        interpretation=(
            "Legacy user-visible latency signal; prefer seconds duration metrics."
        ),
    ),
    "by_framework_execution_queue_duration_p95_seconds": MetricDefinition(
        name="by_framework_execution_queue_duration_p95_seconds",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.SECONDS,
        description="Current snapshot P95 queue wait duration in seconds.",
        interpretation=(
            "Latency signal for queue wait over the retained snapshot window."
        ),
    ),
    "by_framework_execution_run_duration_p95_seconds": MetricDefinition(
        name="by_framework_execution_run_duration_p95_seconds",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.SECONDS,
        description="Current snapshot P95 worker run duration in seconds.",
        interpretation=(
            "Latency signal for worker processing over the retained snapshot " "window."
        ),
    ),
    "by_framework_execution_total_duration_p95_seconds": MetricDefinition(
        name="by_framework_execution_total_duration_p95_seconds",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.SECONDS,
        description="Current snapshot P95 end-to-end execution duration in seconds.",
        interpretation="User-visible latency SLI over the retained snapshot window.",
    ),
    "by_framework_slo_burn_rate": MetricDefinition(
        name="by_framework_slo_burn_rate",
        kind=MetricKind.GAUGE,
        unit=MetricUnit.RATIO,
        labels=("sli", "window"),
        description="How fast an SLI is consuming its configured error budget.",
        interpretation="Values above 1 mean the SLO is currently violated.",
    ),
    "by_framework_observability_dropped_spans_total": MetricDefinition(
        name="by_framework_observability_dropped_spans_total",
        kind=MetricKind.COUNTER,
        unit=MetricUnit.NONE,
        description="Trace spans dropped before export.",
        interpretation="Instrumentation reliability signal for trace data loss.",
    ),
    "by_framework_observability_dropped_spans_by_reason_total": MetricDefinition(
        name="by_framework_observability_dropped_spans_by_reason_total",
        kind=MetricKind.COUNTER,
        unit=MetricUnit.NONE,
        labels=("reason",),
        description="Trace spans dropped before export by reason.",
        interpretation="Instrumentation reliability signal explaining trace data loss.",
    ),
    "by_framework_observability_export_failures_total": MetricDefinition(
        name="by_framework_observability_export_failures_total",
        kind=MetricKind.COUNTER,
        unit=MetricUnit.NONE,
        description="Trace exporter failures.",
        interpretation="Instrumentation reliability signal for exporter health.",
    ),
    "by_framework_observability_export_failures_by_exporter_total": MetricDefinition(
        name="by_framework_observability_export_failures_by_exporter_total",
        kind=MetricKind.COUNTER,
        unit=MetricUnit.NONE,
        labels=("exporter",),
        description="Trace exporter failures by exporter.",
        interpretation=(
            "Instrumentation reliability signal pinpointing exporter failures."
        ),
    ),
}


def get_metric_catalog() -> dict[str, MetricDefinition]:
    """Return the normalized metric catalog keyed by metric name."""
    return dict(_CATALOG)


def get_metric_definition(name: str) -> MetricDefinition:
    """Return one metric definition, raising KeyError for unknown metrics."""
    return _CATALOG[name]


def metric_definition_to_dict(definition: MetricDefinition) -> dict[str, object]:
    """Serialize a metric definition for API and documentation surfaces."""
    return {
        "name": definition.name,
        "kind": definition.kind.value,
        "unit": definition.unit.value,
        "labels": list(definition.labels),
        "description": definition.description,
        "interpretation": definition.interpretation,
        "legacy_names": list(definition.legacy_names),
        "debug_only": definition.debug_only,
    }


def get_metric_catalog_payload() -> dict[str, object]:
    """Return API-friendly catalog metadata with core/debug counts."""
    metrics = {
        name: metric_definition_to_dict(definition)
        for name, definition in sorted(_CATALOG.items())
    }
    debug_count = sum(1 for definition in _CATALOG.values() if definition.debug_only)
    return {
        "total": len(metrics),
        "core_count": len(metrics) - debug_count,
        "debug_count": debug_count,
        "metrics": metrics,
    }
