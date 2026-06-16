"""Tests for the observability metric catalog."""

from by_framework.metrics import (
    PROMETHEUS_AVAILABLE,
    build_observability_diagnostics_metrics,
    generate_latest_metrics,
    record_availability_metrics,
    record_execution_metrics,
    record_execution_started_metrics,
)
from by_framework.metrics.catalog import (
    MetricKind,
    MetricUnit,
    get_metric_catalog,
    get_metric_definition,
)
from by_framework.metrics.snapshot import (
    build_demo_observability_snapshot,
    build_prometheus_metrics,
)


def test_metric_catalog_defines_core_signals_with_units_and_labels():
    """Catalog documents the normalized metrics contract."""
    catalog = get_metric_catalog()

    total_duration = catalog["by_framework_execution_total_duration_seconds"]
    assert total_duration.kind is MetricKind.HISTOGRAM
    assert total_duration.unit is MetricUnit.SECONDS
    assert total_duration.labels == ("status", "agent_type")
    assert total_duration.description
    assert total_duration.interpretation
    assert total_duration.legacy_names == ("by_framework_execution_latency_ms",)

    started = get_metric_definition("by_framework_executions_started_total")
    assert started.kind is MetricKind.COUNTER
    assert started.unit is MetricUnit.EXECUTIONS
    assert started.labels == ("agent_type",)


def test_metric_catalog_exposes_dashboard_safe_low_cardinality_metrics():
    """Core catalog should prefer aggregate dimensions over instance IDs."""
    catalog = get_metric_catalog()

    for definition in catalog.values():
        if not definition.debug_only:
            assert "worker_id" not in definition.labels

    assert "by_framework_stream_oldest_pending_age_seconds" in catalog
    assert "by_framework_slo_burn_rate" in catalog


def test_snapshot_prometheus_exporter_matches_metric_catalog_types():
    """Every snapshot-exported metric has catalog metadata and a consistent type."""
    catalog = get_metric_catalog()
    exported_types: dict[str, str] = {}
    for line in build_prometheus_metrics(
        build_demo_observability_snapshot()
    ).splitlines():
        if line.startswith("# TYPE "):
            _, _, name, metric_type = line.split()
            exported_types[name] = metric_type

    assert exported_types
    for name, metric_type in exported_types.items():
        assert name in catalog
        assert catalog[name].kind.value == metric_type


def test_runtime_prometheus_metrics_have_catalog_metadata():
    """Runtime and diagnostics metrics include legacy debug metadata."""
    if not PROMETHEUS_AVAILABLE:
        return
    record_execution_started_metrics(agent_type="catalog-agent")
    record_execution_metrics(
        status="COMPLETED",
        agent_type="catalog-agent",
        worker_id="catalog-worker",
        execution_ms=125,
        queue_wait_ms_val=25,
    )
    record_availability_metrics(
        agent_type="catalog-agent",
        policy="least_pending",
        status="selected",
        routing_ms=5,
    )
    metrics_text = "\n".join(
        [
            generate_latest_metrics(),
            build_observability_diagnostics_metrics(
                {
                    "dropped_spans_total": 1,
                    "dropped_spans_by_reason": {"disabled": 1},
                    "export_failures_total": 1,
                    "export_failures_by_exporter": {"otlp": 1},
                }
            ),
        ]
    )
    exported_types: dict[str, str] = {}
    for line in metrics_text.splitlines():
        if line.startswith("# TYPE by_framework_"):
            _, _, name, metric_type = line.split()
            if not name.endswith("_created"):
                exported_types[name] = metric_type

    catalog = get_metric_catalog()
    assert exported_types
    for name, metric_type in exported_types.items():
        assert name in catalog
        assert catalog[name].kind.value == metric_type
