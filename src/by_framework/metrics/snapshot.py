"""Build monitoring snapshots for the observability dashboard."""

# pylint: disable=line-too-long,inconsistent-quotes,invalid-name

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any, Optional

from by_framework.common.constants import RedisKeys
from by_framework.common.redis_client import Redis, get_redis
from by_framework.core.registry import WorkerRegistry

STATUS_ORDER = ("QUEUED", "RUNNING", "CANCELLING", "COMPLETED", "FAILED", "CANCELLED")

REDIS_HISTORY_KEY = "by_framework:obs:history"
REDIS_HISTORY_TTL_MS = 2 * 60 * 60 * 1000  # Keep two hours of trend data.
SLO_RUNBOOKS: dict[str, dict[str, Any]] = {
    "slo-success-ratio": {
        "id": "slo-success-ratio",
        "title": "Restore execution success ratio",
        "actions": [
            "Open recent failed executions and group by error type.",
            "Check Redis Streams pending/deadletter queues for stuck messages.",
            "Inspect worker logs with trace_id/session_id around the burn window.",
        ],
    },
    "slo-latency-p95": {
        "id": "slo-latency-p95",
        "title": "Reduce end-to-end latency P95",
        "actions": [
            "Compare queue wait P95 with worker run P95 to locate the bottleneck.",
            "Check stream depth, pending age, and max delivery count for retry pressure.",
            "Scale or unblock workers for the affected agent types.",
        ],
    },
    "slo-deadletter": {
        "id": "slo-deadletter",
        "title": "Drain and explain deadletters",
        "actions": [
            "Inspect deadletter payloads and correlate by message_id/execution_id.",
            "Identify the producing agent type and error type.",
            "Replay or quarantine after the handler defect is understood.",
        ],
    },
    "slo-worker-freshness": {
        "id": "slo-worker-freshness",
        "title": "Restore worker heartbeat freshness",
        "actions": [
            "List worker snapshots and find stale or evicted workers.",
            "Check worker process health and Redis connectivity.",
            "Restart or replace stale workers before accepting new traffic.",
        ],
    },
}


@dataclass(frozen=True)
class SLOPolicy:
    """Service-level objectives used to derive SLI-aware health alerts."""

    success_ratio_target: float = 0.99
    total_latency_p95_ms: int = 30_000
    deadletter_threshold: int = 0
    freshness_max_age_ms: int = 15_000
    window: str = "5m"


@dataclass(frozen=True)
class AlertPolicy:
    """Thresholds used to derive dashboard health alerts."""

    queue_backlog_threshold: int = 100
    delivery_pending_threshold: int = 0
    consumer_pending_threshold: int = 0
    failed_execution_threshold: int = 0
    slo_policy: SLOPolicy | None = None


async def save_history_point_to_redis(
    redis_client: Optional[Redis],
    point: dict[str, int],
) -> None:
    """Persist a trend point to Redis for history across process restarts."""
    if redis_client is None:
        return
    zadd = getattr(redis_client, "zadd", None)
    if not callable(zadd):
        return
    score = int(point.get("generated_at", 0) or 0)
    if not score:
        return
    try:
        await zadd(
            REDIS_HISTORY_KEY,
            {json.dumps(point, separators=(",", ":")): score},
        )
        zremrangebyscore = getattr(redis_client, "zremrangebyscore", None)
        if callable(zremrangebyscore):
            cutoff = score - REDIS_HISTORY_TTL_MS
            await zremrangebyscore(REDIS_HISTORY_KEY, "-inf", cutoff)
    except Exception:  # pylint: disable=broad-exception-caught
        pass


async def load_history_from_redis(
    redis_client: Optional[Redis],
    limit: int = 120,
) -> list[dict[str, int]]:
    """Load trend history from Redis sorted by time ascending."""
    if redis_client is None:
        return []
    zrange = getattr(redis_client, "zrange", None)
    if not callable(zrange):
        return []
    try:
        raw_entries = await zrange(REDIS_HISTORY_KEY, -max(limit, 1), -1)
        points: list[dict[str, int]] = []
        for raw in raw_entries:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            try:
                points.append(json.loads(raw))
            except (TypeError, ValueError):
                pass
        return points
    except Exception:  # pylint: disable=broad-exception-caught
        return []


async def build_observability_snapshot(
    redis_client: Optional[Redis] = None,
    *,
    active_limit: int = 100,
    history_limit: int = 20,
    include_consumer_details: bool = False,
    worker_scan_limit: int = 300,
    queue_backlog_threshold: int = 100,
    alert_policy: AlertPolicy | None = None,
) -> dict[str, Any]:
    """Return a dashboard-friendly snapshot of cluster health and executions."""
    redis = redis_client or get_redis()
    registry = WorkerRegistry(redis)

    workers, worker_scan = await _get_observable_workers(redis, worker_scan_limit)
    worker_summaries = await asyncio.gather(
        *[
            registry.get_worker_execution_summary(
                worker_id,
                active_limit=active_limit,
                history_limit=history_limit,
                workers=workers,
            )
            for worker_id in sorted(workers)
        ]
    )

    status_counts = _aggregate_status_counts(worker_summaries)
    agent_types = sorted(
        {
            agent_type
            for summary in worker_summaries
            for agent_type in summary.get("agent_types", [])
        }
    )

    agent_queue_rows = await asyncio.gather(
        *[
            _agent_type_stream_snapshot(
                redis, agent_type, include_consumer_details=include_consumer_details
            )
            for agent_type in agent_types
        ]
    )
    control_plane_rows = await asyncio.gather(
        _named_stream_length(
            redis,
            RedisKeys.control_plane_wakeup_stream(),
            include_consumer_details=include_consumer_details,
        ),
        _named_stream_length(
            redis,
            RedisKeys.control_plane_delivery_pending_stream(),
            include_consumer_details=include_consumer_details,
        ),
        _named_stream_length(
            redis,
            RedisKeys.control_plane_deadletter_stream(),
            include_consumer_details=include_consumer_details,
        ),
    )

    snapshot = {
        "generated_at": int(time.time() * 1000),
        "totals": {
            "workers_online": len(worker_summaries),
            "agent_types": len(agent_types),
            "active_executions": sum(
                int(summary.get("active_count", 0)) for summary in worker_summaries
            ),
            "tracked_executions": sum(
                int(summary.get("total_tracked", 0)) for summary in worker_summaries
            ),
        },
        "status_counts": status_counts,
        "workers": worker_summaries,
        "queues": {
            "agent_type_streams": list(agent_queue_rows),
            "control_plane": {
                "wakeup": control_plane_rows[0],
                "delivery_pending": control_plane_rows[1],
                "deadletter": control_plane_rows[2],
            },
        },
        "recent_executions": _merge_recent_executions(worker_summaries, history_limit),
        "worker_scan": worker_scan,
    }
    return _enrich_snapshot(
        snapshot,
        alert_policy=_resolve_alert_policy(
            alert_policy, queue_backlog_threshold=queue_backlog_threshold
        ),
    )


async def build_worker_observability_snapshot(
    redis_client: Optional[Redis] = None,
    *,
    worker_scan_limit: int = 300,
    alert_policy: AlertPolicy | None = None,
) -> dict[str, Any]:
    """Return lightweight worker liveness and counter health without history scans."""
    redis = redis_client or get_redis()
    registry = WorkerRegistry(redis)
    workers, worker_scan = await _get_observable_workers(redis, worker_scan_limit)
    admin_worker_ids, admin_scan = await _get_admin_managed_worker_ids(
        redis, worker_scan_limit
    )
    sorted_worker_ids = sorted(set(workers) | set(admin_worker_ids))
    admin_states = await _batch_fetch_admin_states(redis, sorted_worker_ids)
    worker_summaries = await asyncio.gather(
        *[
            _build_lightweight_worker_summary(
                registry, workers, worker_id, admin_states.get(worker_id, {})
            )
            for worker_id in sorted_worker_ids
        ]
    )
    status_counts = _aggregate_status_counts(worker_summaries)
    agent_types = sorted(
        {
            agent_type
            for summary in worker_summaries
            for agent_type in summary.get("agent_types", [])
        }
    )
    snapshot = {
        "generated_at": int(time.time() * 1000),
        "totals": {
            "workers_online": len(workers),
            "workers_managed": len(worker_summaries),
            "agent_types": len(agent_types),
            "active_executions": sum(
                int(summary.get("active_count", 0)) for summary in worker_summaries
            ),
            "tracked_executions": sum(
                int(summary.get("total_tracked", 0)) for summary in worker_summaries
            ),
        },
        "status_counts": status_counts,
        "workers": worker_summaries,
        "agent_types": agent_types,
        "worker_scan": {**worker_scan, "admin": admin_scan},
    }
    snapshot["alerts"] = _build_worker_alerts(
        snapshot, alert_policy=_resolve_alert_policy(alert_policy)
    )
    snapshot["health"] = _build_health_summary(snapshot.get("alerts", []))
    return snapshot


async def build_execution_observability_snapshot(
    redis_client: Optional[Redis] = None,
    *,
    history_limit: int = 20,
    worker_scan_limit: int = 300,
    alert_policy: AlertPolicy | None = None,
) -> dict[str, Any]:
    """Return recent execution, latency, and failure health."""
    redis = redis_client or get_redis()
    registry = WorkerRegistry(redis)
    workers, worker_scan = await _get_observable_workers(redis, worker_scan_limit)
    worker_summaries = await asyncio.gather(
        *[
            registry.get_worker_execution_summary(
                worker_id,
                active_limit=0,
                history_limit=history_limit,
                workers=workers,
            )
            for worker_id in sorted(workers)
        ]
    )
    status_counts = _aggregate_status_counts(worker_summaries)
    agent_types = sorted(
        {
            agent_type
            for summary in worker_summaries
            for agent_type in summary.get("agent_types", [])
        }
    )
    snapshot = {
        "generated_at": int(time.time() * 1000),
        "totals": {
            "workers_online": len(worker_summaries),
            "agent_types": len(agent_types),
            "active_executions": sum(
                int(summary.get("active_count", 0)) for summary in worker_summaries
            ),
            "tracked_executions": sum(
                int(summary.get("total_tracked", 0)) for summary in worker_summaries
            ),
        },
        "status_counts": status_counts,
        "workers": worker_summaries,
        "recent_executions": _merge_recent_executions(worker_summaries, history_limit),
        "agent_types": agent_types,
        "worker_scan": worker_scan,
    }
    snapshot["recent_executions"] = [
        _enrich_execution_timing(execution)
        for execution in snapshot.get("recent_executions", [])
    ]
    snapshot["latency"] = _calculate_latency(snapshot.get("recent_executions", []))
    snapshot["failures"] = _build_failure_summary(snapshot.get("recent_executions", []))
    snapshot["agent_health"] = _build_agent_health(snapshot)
    snapshot["alerts"] = _build_worker_alerts(
        snapshot, alert_policy=_resolve_alert_policy(alert_policy)
    )
    snapshot["health"] = _build_health_summary(snapshot.get("alerts", []))
    return snapshot


async def build_queue_observability_snapshot(
    redis_client: Optional[Redis] = None,
    *,
    agent_types: Optional[list[str]] = None,
    include_consumer_details: bool = False,
    worker_scan_limit: int = 300,
    queue_backlog_threshold: int = 100,
    alert_policy: AlertPolicy | None = None,
) -> dict[str, Any]:
    """Return Redis Stream queue and consumer-group health only."""
    redis = redis_client or get_redis()
    if agent_types is None:
        workers, _ = await _get_observable_workers(redis, worker_scan_limit)
        agent_types = sorted(
            {
                agent_type
                for worker in workers.values()
                for agent_type in worker.get("agent_types", [])
            }
        )

    agent_queue_rows = await asyncio.gather(
        *[
            _agent_type_stream_snapshot(
                redis, agent_type, include_consumer_details=include_consumer_details
            )
            for agent_type in sorted(set(agent_types))
        ]
    )
    control_plane_rows = await asyncio.gather(
        _named_stream_length(
            redis,
            RedisKeys.control_plane_wakeup_stream(),
            include_consumer_details=include_consumer_details,
        ),
        _named_stream_length(
            redis,
            RedisKeys.control_plane_delivery_pending_stream(),
            include_consumer_details=include_consumer_details,
        ),
        _named_stream_length(
            redis,
            RedisKeys.control_plane_deadletter_stream(),
            include_consumer_details=include_consumer_details,
        ),
    )
    snapshot = {
        "generated_at": int(time.time() * 1000),
        "queues": {
            "agent_type_streams": list(agent_queue_rows),
            "control_plane": {
                "wakeup": control_plane_rows[0],
                "delivery_pending": control_plane_rows[1],
                "deadletter": control_plane_rows[2],
            },
        },
    }
    snapshot["alerts"] = _build_queue_alerts(
        snapshot,
        alert_policy=_resolve_alert_policy(
            alert_policy, queue_backlog_threshold=queue_backlog_threshold
        ),
    )
    snapshot["health"] = _build_health_summary(snapshot.get("alerts", []))
    return snapshot


async def build_session_observability_snapshot(
    redis_client: Optional[Redis],
    session_id: str,
    *,
    trace_id: str = "",
    event_limit: int = 50,
) -> dict[str, Any]:
    """Return execution tree and recent data events for a single session."""
    redis = redis_client or get_redis()
    registry = WorkerRegistry(redis)
    executions = await registry.get_all_session_executions(session_id)
    if trace_id:
        executions = [
            execution
            for execution in executions
            if str(execution.get("trace_id", "")) == trace_id
        ]
    executions = [_enrich_execution_timing(execution) for execution in executions]
    executions.sort(key=lambda item: int(item.get("created_at", 0) or 0))
    recent_events = await _read_recent_session_events(
        redis, session_id, trace_id=trace_id, limit=event_limit
    )
    return {
        "generated_at": int(time.time() * 1000),
        "session_id": session_id,
        "trace_id": trace_id,
        "totals": {
            "executions": len(executions),
            "events": len(recent_events),
        },
        "status_counts": _count_execution_statuses(executions),
        "executions": executions,
        "execution_tree": _build_execution_tree(executions),
        "timeline": _build_session_timeline(executions, recent_events),
        "recent_events": recent_events,
    }


async def build_trace_observability_snapshot(
    redis_client: Optional[Redis],
    trace_id: str,
    *,
    session_id: str = "",
    event_limit: int = 100,
) -> dict[str, Any]:
    """Return a trace-centric span tree and waterfall-ready timeline."""
    redis = redis_client or get_redis()
    stored_spans = await _read_stored_trace_spans(redis, trace_id)
    if stored_spans:
        session_id = session_id or str(stored_spans[0].get("session_id", ""))
        return _build_trace_snapshot(trace_id, session_id, stored_spans)
    if not session_id:
        return _empty_trace_snapshot(trace_id)
    session_snapshot = await build_session_observability_snapshot(
        redis,
        session_id,
        trace_id=trace_id,
        event_limit=event_limit,
    )
    return _build_trace_from_session_snapshot(session_snapshot, trace_id)


def build_prometheus_metrics(snapshot: dict[str, Any]) -> str:
    """Render an observability snapshot as Prometheus text exposition."""
    totals = snapshot.get("totals", {})
    workers_online = int(totals.get("workers_online", 0))
    agent_type_count = int(totals.get("agent_types", 0))
    active_execution_count = int(totals.get("active_executions", 0))
    tracked_execution_count = int(totals.get("tracked_executions", 0))
    lines = [
        "# HELP by_framework_workers_online Online workers discovered by registry.",
        "# TYPE by_framework_workers_online gauge",
        f"by_framework_workers_online {workers_online}",
        "# HELP by_framework_agent_types Known online agent types.",
        "# TYPE by_framework_agent_types gauge",
        f"by_framework_agent_types {agent_type_count}",
        "# HELP by_framework_active_executions Active executions across workers.",
        "# TYPE by_framework_active_executions gauge",
        f"by_framework_active_executions {active_execution_count}",
        "# HELP by_framework_tracked_executions Tracked executions across workers.",
        "# TYPE by_framework_tracked_executions gauge",
        f"by_framework_tracked_executions {tracked_execution_count}",
    ]
    lines.extend(
        [
            "# HELP by_framework_execution_status_current Current executions by status.",
            "# TYPE by_framework_execution_status_current gauge",
        ]
    )
    for status, value in sorted(snapshot.get("status_counts", {}).items()):
        lines.append(
            "by_framework_execution_status_current"
            f'{{status="{_escape_label(status)}"}} {int(value)}'
        )

    lines.extend(
        [
            "# HELP by_framework_queue_depth Redis stream depth.",
            "# TYPE by_framework_queue_depth gauge",
            "# HELP by_framework_stream_depth Redis stream depth.",
            "# TYPE by_framework_stream_depth gauge",
        ]
    )
    queues = snapshot.get("queues", {})
    for queue in queues.get("agent_type_streams", []):
        _append_queue_metric(
            lines, "agent_type", str(queue.get("agent_type", "")), queue
        )
    for name, queue in queues.get("control_plane", {}).items():
        _append_queue_metric(lines, "control_plane", str(name), queue)

    lines.extend(
        [
            "# HELP by_framework_agent_queue_depth "
            "Redis control queue depth by agent type.",
            "# TYPE by_framework_agent_queue_depth gauge",
            "# HELP by_framework_agent_workers "
            "Online workers supporting each agent type.",
            "# TYPE by_framework_agent_workers gauge",
            "# HELP by_framework_agent_recent_failed_executions "
            "Recent failed executions by agent type.",
            "# TYPE by_framework_agent_recent_failed_executions gauge",
        ]
    )
    for agent in snapshot.get("agent_health", []):
        agent_type = _escape_label(str(agent.get("agent_type", "")))
        failed_count = int(agent.get("recent_failed_executions", 0))
        lines.append(
            "by_framework_agent_queue_depth"
            f'{{agent_type="{agent_type}"}} {int(agent.get("queue_depth", 0))}'
        )
        lines.append(
            "by_framework_agent_workers"
            f'{{agent_type="{agent_type}"}} {int(agent.get("worker_count", 0))}'
        )
        lines.append(
            "by_framework_agent_recent_failed_executions"
            f'{{agent_type="{agent_type}"}} {failed_count}'
        )

    lines.extend(
        [
            "# HELP by_framework_stream_pending_messages "
            "Pending Redis Stream messages by consumer group.",
            "# TYPE by_framework_stream_pending_messages gauge",
            "# HELP by_framework_stream_consumer_lag "
            "Redis Stream consumer group lag when reported by Redis.",
            "# TYPE by_framework_stream_consumer_lag gauge",
            "# HELP by_framework_stream_oldest_pending_age_seconds "
            "Idle age of the oldest pending Redis Stream message.",
            "# TYPE by_framework_stream_oldest_pending_age_seconds gauge",
            "# HELP by_framework_stream_max_delivery_count "
            "Maximum Redis Stream pending delivery count by consumer group.",
            "# TYPE by_framework_stream_max_delivery_count gauge",
        ]
    )
    for queue in _iter_queues(snapshot):
        for group in queue.get("consumer_groups", []):
            labels = (
                f'queue_type="{_escape_label(str(queue.get("queue_type", "")))}",'
                f'name="{_escape_label(str(queue.get("name", "")))}",'
                f'group="{_escape_label(str(group.get("name", "")))}"'
            )
            lines.append(
                "by_framework_stream_pending_messages"
                f"{{{labels}}} {int(group.get('pending', 0) or 0)}"
            )
            lag = group.get("lag")
            if lag is not None:
                lines.append(
                    f"by_framework_stream_consumer_lag{{{labels}}} {int(lag or 0)}"
                )
            oldest_pending_age = group.get("oldest_pending_age_seconds")
            if oldest_pending_age is not None:
                lines.append(
                    "by_framework_stream_oldest_pending_age_seconds"
                    f"{{{labels}}} {int(oldest_pending_age or 0)}"
                )
            max_delivery_count = group.get("max_delivery_count")
            if max_delivery_count is not None:
                lines.append(
                    "by_framework_stream_max_delivery_count"
                    f"{{{labels}}} {int(max_delivery_count or 0)}"
                )

    lines.extend(
        [
            "# HELP by_framework_execution_recent_failures "
            "Recent failed executions by error type.",
            "# TYPE by_framework_execution_recent_failures gauge",
        ]
    )
    for error_type, value in sorted(
        snapshot.get("failures", {}).get("by_error_type", {}).items()
    ):
        lines.append(
            "by_framework_execution_recent_failures"
            f'{{error_type="{_escape_label(str(error_type))}"}} {int(value)}'
        )

    lines.extend(
        [
            "# HELP by_framework_alerts_current "
            "Current derived health alerts by severity.",
            "# TYPE by_framework_alerts_current gauge",
        ]
    )
    alert_counts = _count_alerts_by_severity(snapshot.get("alerts", []))
    for severity, value in sorted(alert_counts.items()):
        severity_label = _escape_label(severity)
        lines.append(
            f'by_framework_alerts_current{{severity="{severity_label}"}} {value}'
        )

    latency = snapshot.get("latency", {})
    run_latency = latency.get("run", {})
    queue_latency = latency.get("queue", {})
    total_latency = latency.get("total", {})
    avg_latency_ms = int(run_latency.get("avg_ms", latency.get("avg_ms", 0)) or 0)
    p95_latency_ms = int(run_latency.get("p95_ms", latency.get("p95_ms", 0)) or 0)
    lines.extend(
        [
            "# HELP by_framework_execution_latency_avg_ms "
            "Average completed execution latency in milliseconds.",
            "# TYPE by_framework_execution_latency_avg_ms gauge",
            f"by_framework_execution_latency_avg_ms {avg_latency_ms}",
            "# HELP by_framework_execution_latency_p95_ms "
            "P95 completed execution latency in milliseconds.",
            "# TYPE by_framework_execution_latency_p95_ms gauge",
            f"by_framework_execution_latency_p95_ms {p95_latency_ms}",
            "# HELP by_framework_execution_queue_latency_p95_ms "
            "P95 queue wait latency in milliseconds.",
            "# TYPE by_framework_execution_queue_latency_p95_ms gauge",
            "by_framework_execution_queue_latency_p95_ms "
            f"{int(queue_latency.get('p95_ms', 0) or 0)}",
            "# HELP by_framework_execution_total_latency_p95_ms "
            "P95 end-to-end execution latency in milliseconds.",
            "# TYPE by_framework_execution_total_latency_p95_ms gauge",
            "by_framework_execution_total_latency_p95_ms "
            f"{int(total_latency.get('p95_ms', 0) or 0)}",
            "# HELP by_framework_execution_queue_duration_p95_seconds "
            "P95 queue wait duration in seconds.",
            "# TYPE by_framework_execution_queue_duration_p95_seconds gauge",
            "by_framework_execution_queue_duration_p95_seconds "
            f"{_ms_to_seconds(queue_latency.get('p95_ms', 0))}",
            "# HELP by_framework_execution_run_duration_p95_seconds "
            "P95 worker run duration in seconds.",
            "# TYPE by_framework_execution_run_duration_p95_seconds gauge",
            "by_framework_execution_run_duration_p95_seconds "
            f"{_ms_to_seconds(run_latency.get('p95_ms', 0))}",
            "# HELP by_framework_execution_total_duration_p95_seconds "
            "P95 end-to-end execution duration in seconds.",
            "# TYPE by_framework_execution_total_duration_p95_seconds gauge",
            "by_framework_execution_total_duration_p95_seconds "
            f"{_ms_to_seconds(total_latency.get('p95_ms', 0))}",
        ]
    )
    slo = snapshot.get("slo", {})
    if slo:
        lines.extend(
            [
                "# HELP by_framework_slo_burn_rate "
                "Current SLO burn rate by SLI and window.",
                "# TYPE by_framework_slo_burn_rate gauge",
                "by_framework_slo_burn_rate"
                f'{{sli="execution_success_ratio",'
                f'window="{_escape_label(str(slo.get("window", "5m")))}"}} '
                f'{float(slo.get("burn_rate", 0) or 0)}',
            ]
        )
    return "\n".join(lines) + "\n"


def build_history_point(snapshot: dict[str, Any]) -> dict[str, int]:
    """Extract compact trend values from a full observability snapshot."""
    status_counts = snapshot.get("status_counts", {})
    totals = snapshot.get("totals", {})
    latency = snapshot.get("latency", {})
    slo = snapshot.get("slo", {})
    completed_executions = int(status_counts.get("COMPLETED", 0) or 0)
    failed_executions = int(status_counts.get("FAILED", 0) or 0)
    cancelled_executions = int(status_counts.get("CANCELLED", 0) or 0)
    return {
        "generated_at": int(snapshot.get("generated_at", 0) or 0),
        "workers_online": int(totals.get("workers_online", 0) or 0),
        "active_executions": int(totals.get("active_executions", 0) or 0),
        "queued_executions": int(status_counts.get("QUEUED", 0) or 0),
        "completed_executions": completed_executions,
        "failed_executions": failed_executions,
        "cancelled_executions": cancelled_executions,
        "terminal_executions": (
            completed_executions + failed_executions + cancelled_executions
        ),
        "queue_depth_total": _total_queue_depth(snapshot),
        "consumer_pending_total": _total_consumer_pending(snapshot),
        "max_delivery_count": _max_delivery_count(snapshot),
        "alert_count": len(snapshot.get("alerts", [])),
        "latency_p95_ms": int(latency.get("p95_ms", 0) or 0),
        "queue_latency_p95_ms": int(latency.get("queue", {}).get("p95_ms", 0) or 0),
        "total_latency_p95_ms": int(latency.get("total", {}).get("p95_ms", 0) or 0),
        "success_ratio_ppm": int(slo.get("success_ratio_ppm", 1_000_000) or 0),
        "deadletter_count": int(slo.get("deadletter_count", 0) or 0),
        "freshness_age_ms": int(slo.get("freshness_age_ms", 0) or 0),
    }


def build_demo_observability_history(samples: int = 18) -> list[dict[str, int]]:
    """Return deterministic trend points for dashboard previews."""
    now = int(time.time() * 1000)
    count = max(1, samples)
    points = []
    for index in range(count):
        age = count - index - 1
        points.append(
            {
                "generated_at": now - age * 5000,
                "workers_online": 2,
                "active_executions": 2 + (index % 4),
                "queued_executions": index % 3,
                "completed_executions": 120 + index * 8,
                "failed_executions": 1 if index < count - 4 else 3,
                "cancelled_executions": index // 8,
                "terminal_executions": 121 + index * 8 + (index // 8),
                "queue_depth_total": 4 + ((index * 2) % 7),
                "consumer_pending_total": 1 + (index % 4),
                "alert_count": 1 if index < count - 5 else 2,
                "latency_p95_ms": 6000 + ((index % 5) * 900),
                "queue_latency_p95_ms": 400 + ((index % 4) * 150),
                "total_latency_p95_ms": 6700 + ((index % 5) * 1000),
                "success_ratio_ppm": 980000 if index < count - 4 else 960000,
                "deadletter_count": 0 if index < count - 4 else 2,
                "freshness_age_ms": 2500 + ((index % 5) * 900),
                "max_delivery_count": 1 if index < count - 4 else 2,
            }
        )
    return points


def build_demo_observability_snapshot(
    alert_policy: AlertPolicy | None = None,
) -> dict[str, Any]:
    """Return a deterministic sample snapshot for dashboard previews."""
    now = int(time.time() * 1000)
    workers = [
        {
            "worker_id": "worker-planner-1",
            "online": True,
            "agent_types": ["planner", "researcher"],
            "last_seen": now - 2000,
            "counts": {
                "total": 42,
                "active": 3,
                "queued": 1,
                "running": 2,
                "cancelling": 0,
                "completed": 36,
                "failed": 2,
                "cancelled": 1,
            },
            "active_count": 3,
            "total_tracked": 42,
            "last_updated_at": now - 1200,
            "last_started_at": now - 18000,
            "last_finished_at": now - 4000,
            "status_counts": {
                "QUEUED": 1,
                "RUNNING": 2,
                "COMPLETED": 36,
                "FAILED": 2,
                "CANCELLED": 1,
            },
            "active_executions": [],
            "recent_executions": [
                _demo_execution(
                    "exec-demo-running",
                    "msg-demo-running",
                    "sess-demo",
                    "worker-planner-1",
                    "planner",
                    "RUNNING",
                    now - 1200,
                ),
                _demo_execution(
                    "exec-demo-completed",
                    "msg-demo-completed",
                    "sess-demo",
                    "worker-planner-1",
                    "researcher",
                    "COMPLETED",
                    now - 4000,
                ),
                _demo_execution(
                    "exec-demo-failed",
                    "msg-demo-failed",
                    "sess-demo",
                    "worker-planner-1",
                    "planner",
                    "FAILED",
                    now - 6500,
                ),
            ],
        },
        {
            "worker_id": "worker-writer-1",
            "online": True,
            "agent_types": ["writer"],
            "last_seen": now - 3500,
            "counts": {
                "total": 27,
                "active": 1,
                "queued": 0,
                "running": 1,
                "cancelling": 0,
                "completed": 24,
                "failed": 1,
                "cancelled": 1,
            },
            "active_count": 1,
            "total_tracked": 27,
            "last_updated_at": now - 2200,
            "last_started_at": now - 9000,
            "last_finished_at": now - 7000,
            "status_counts": {
                "RUNNING": 1,
                "COMPLETED": 24,
                "FAILED": 1,
                "CANCELLED": 1,
            },
            "active_executions": [],
            "recent_executions": [
                _demo_execution(
                    "exec-demo-writer",
                    "msg-demo-writer",
                    "sess-demo",
                    "worker-writer-1",
                    "writer",
                    "RUNNING",
                    now - 2200,
                )
            ],
        },
    ]
    snapshot = {
        "generated_at": now,
        "totals": {
            "workers_online": 2,
            "agent_types": 3,
            "active_executions": 4,
            "tracked_executions": 69,
        },
        "status_counts": {
            "QUEUED": 1,
            "RUNNING": 3,
            "COMPLETED": 60,
            "FAILED": 3,
            "CANCELLED": 2,
        },
        "workers": workers,
        "queues": {
            "agent_type_streams": [
                {
                    "agent_type": "planner",
                    "stream": RedisKeys.ctrl_stream("planner"),
                    "length": 4,
                    "consumer_groups": [
                        _demo_consumer_group("agent_engines", pending=1, lag=2)
                    ],
                },
                {
                    "agent_type": "researcher",
                    "stream": RedisKeys.ctrl_stream("researcher"),
                    "length": 1,
                    "consumer_groups": [
                        _demo_consumer_group("agent_engines", pending=0, lag=0)
                    ],
                },
                {
                    "agent_type": "writer",
                    "stream": RedisKeys.ctrl_stream("writer"),
                    "length": 2,
                    "consumer_groups": [
                        _demo_consumer_group("agent_engines", pending=2, lag=1)
                    ],
                },
            ],
            "control_plane": {
                "wakeup": {
                    "stream": RedisKeys.control_plane_wakeup_stream(),
                    "length": 0,
                    "consumer_groups": [],
                },
                "delivery_pending": {
                    "stream": RedisKeys.control_plane_delivery_pending_stream(),
                    "length": 2,
                    "consumer_groups": [],
                },
                "deadletter": {
                    "stream": RedisKeys.control_plane_deadletter_stream(),
                    "length": 0,
                    "consumer_groups": [],
                },
            },
        },
        "recent_executions": _merge_recent_executions(workers, 20),
    }
    return _enrich_snapshot(snapshot, alert_policy=_resolve_alert_policy(alert_policy))


def build_demo_session_observability_snapshot() -> dict[str, Any]:
    """Return deterministic session drilldown data for dashboard previews."""
    now = int(time.time() * 1000)
    executions = [
        _demo_execution(
            "exec-demo-root",
            "msg-demo-root",
            "sess-demo",
            "worker-planner-1",
            "planner",
            "RUNNING",
            now - 3000,
        ),
        {
            **_demo_execution(
                "exec-demo-child",
                "msg-demo-child",
                "sess-demo",
                "worker-writer-1",
                "writer",
                "COMPLETED",
                now - 6000,
            ),
            "parent_message_id": "msg-demo-root",
        },
    ]
    recent_events = [
        {
            "stream_id": "2-0",
            "trace_id": "trace-demo",
            "session_id": "sess-demo",
            "event_type": "ANSWER_DELTA",
            "source_agent_type": "planner",
            "message_id": "msg-demo-root",
            "parent_message_id": "",
            "timestamp": now - 1500,
            "data": {"content": "Planning response"},
            "metadata": {},
        },
        {
            "stream_id": "1-0",
            "trace_id": "trace-demo",
            "session_id": "sess-demo",
            "event_type": "REASONING_LOG_DELTA",
            "source_agent_type": "writer",
            "message_id": "msg-demo-child",
            "parent_message_id": "msg-demo-root",
            "timestamp": now - 5000,
            "data": {"content": "Draft completed"},
            "metadata": {},
        },
    ]
    return {
        "generated_at": now,
        "session_id": "sess-demo",
        "trace_id": "trace-demo",
        "totals": {
            "executions": len(executions),
            "events": 2,
        },
        "status_counts": _count_execution_statuses(executions),
        "executions": executions,
        "execution_tree": _build_execution_tree(executions),
        "timeline": _build_session_timeline(executions, recent_events),
        "recent_events": recent_events,
    }


def build_demo_trace_observability_snapshot() -> dict[str, Any]:
    """Return deterministic trace drilldown data for dashboard previews."""
    now = int(time.time() * 1000)
    spans = [
        _trace_span(
            trace_id="trace-demo",
            span_id="span-client-dispatch",
            parent_span_id="",
            operation="client.dispatch",
            component="client",
            start_ts=now - 15000,
            end_ts=now - 14940,
            status="COMPLETED",
            session_id="sess-demo",
            message_id="msg-demo-root",
            target_agent_type="planner",
        ),
        _trace_span(
            trace_id="trace-demo",
            span_id="span-queue-wait",
            parent_span_id="span-client-dispatch",
            operation="queue.wait",
            component="redis",
            start_ts=now - 14940,
            end_ts=now - 12000,
            status="COMPLETED",
            session_id="sess-demo",
            message_id="msg-demo-root",
            target_agent_type="planner",
            queue_wait_ms=2940,
        ),
        _trace_span(
            trace_id="trace-demo",
            span_id="span-worker-execute",
            parent_span_id="span-queue-wait",
            operation="worker.execute",
            component="worker",
            start_ts=now - 12000,
            end_ts=now - 3000,
            status="RUNNING",
            session_id="sess-demo",
            execution_id="exec-demo-root",
            message_id="msg-demo-root",
            worker_id="worker-planner-1",
            target_agent_type="planner",
        ),
        _trace_span(
            trace_id="trace-demo",
            span_id="span-agent-process",
            parent_span_id="span-worker-execute",
            operation="agent.process",
            component="agent_context",
            start_ts=now - 11800,
            end_ts=now - 3200,
            status="RUNNING",
            session_id="sess-demo",
            execution_id="exec-demo-root",
            message_id="msg-demo-root",
            worker_id="worker-planner-1",
            target_agent_type="planner",
            chunk_count=2,
        ),
        _trace_span(
            trace_id="trace-demo",
            span_id="span-agent-emit-chunk",
            parent_span_id="span-agent-process",
            operation="agent.emit_chunk",
            component="agent_context",
            start_ts=now - 5000,
            end_ts=now - 4999,
            status="COMPLETED",
            session_id="sess-demo",
            message_id="msg-demo-root",
            target_agent_type="planner",
            event_type="ANSWER_DELTA",
        ),
    ]
    return _build_trace_snapshot("trace-demo", "sess-demo", spans)


def _demo_execution(
    execution_id: str,
    message_id: str,
    session_id: str,
    worker_id: str,
    agent_type: str,
    status: str,
    updated_at: int,
) -> dict[str, Any]:
    return {
        "execution_id": execution_id,
        "message_id": message_id,
        "session_id": session_id,
        "worker_id": worker_id,
        "target_agent_type": agent_type,
        "status": status,
        "created_at": updated_at - 12000,
        "started_at": updated_at - 9000,
        "finished_at": 0 if status == "RUNNING" else updated_at,
        "updated_at": updated_at,
        "parent_message_id": "",
        "cancel_requested": False,
        "cancel_reason": "",
        "route_policy": "FAIL_FAST",
        "route_status": "DELIVER_NOW",
        "selected_agent_type": "",
        "availability_error_code": "",
        "availability_error": "",
        "error_type": "RuntimeError" if status == "FAILED" else "",
        "error_message": "demo failure" if status == "FAILED" else "",
        "error_code": "",
        "failed_stage": "process_command" if status == "FAILED" else "",
        "retryable": False,
        "timeline": [
            {"status": "QUEUED", "timestamp": updated_at - 12000},
            {"status": "RUNNING", "timestamp": updated_at - 9000},
            {"status": status, "timestamp": updated_at},
        ],
    }


def _demo_consumer_group(name: str, *, pending: int, lag: int) -> dict[str, Any]:
    return {
        "name": name,
        "pending": pending,
        "lag": lag,
        "last_delivered_id": "0-0",
        "pending_total": pending,
        "pending_oldest_id": "1710000000000-0" if pending else "",
        "oldest_pending_idle_ms": 4200 if pending else 0,
        "oldest_pending_age_seconds": 4 if pending else 0,
        "pending_owner": "worker-demo" if pending else "",
        "max_delivery_count": 2 if pending else 0,
        "consumers": [
            {
                "name": "worker-demo",
                "pending": pending,
                "idle_ms": 1200 if pending else 0,
            }
        ],
    }


def _aggregate_status_counts(worker_summaries: list[dict[str, Any]]) -> dict[str, int]:
    counts = {status: 0 for status in STATUS_ORDER}
    for summary in worker_summaries:
        for status, value in summary.get("status_counts", {}).items():
            counts[status] = counts.get(status, 0) + int(value)
    return {status: value for status, value in counts.items() if value}


async def _get_observable_workers(
    redis: Redis,
    scan_limit: int,
    *,
    concurrency: int = 100,
) -> tuple[dict[str, Any], dict[str, Any]]:
    online_worker_ids, online_scan_supported = await _scan_online_worker_ids(
        redis, scan_limit
    )
    if online_scan_supported:
        worker_ids = online_worker_ids
        total_known = len(worker_ids)
        source = "online_lease_scan"
    else:
        worker_ids_raw = await redis.smembers(RedisKeys.known_workers())
        worker_ids = sorted(
            worker_id.decode("utf-8")
            if isinstance(worker_id, bytes)
            else str(worker_id)
            for worker_id in worker_ids_raw
        )
        total_known = len(worker_ids)
        source = "known_workers_fallback"

    limited_ids = worker_ids[: max(scan_limit, 0)] if scan_limit else worker_ids
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def load_worker(worker_id: str) -> tuple[str, dict[str, Any]] | None:
        async with semaphore:
            presence = await redis.get(RedisKeys.worker_online_lease(worker_id))
            last_seen, is_legacy = _decode_worker_presence_for_snapshot(presence)
            if presence is None or (not is_legacy and last_seen <= 0):
                return None
            agent_types_raw = await redis.smembers(
                RedisKeys.worker_declared_agent_types(worker_id)
            )
            agent_types = sorted(
                agent_type.decode("utf-8")
                if isinstance(agent_type, bytes)
                else str(agent_type)
                for agent_type in agent_types_raw
            )
            return (
                worker_id,
                {
                    "agent_types": agent_types,
                    "last_seen": int(time.time() * 1000) if is_legacy else last_seen,
                },
            )

    loaded = await asyncio.gather(
        *(load_worker(worker_id) for worker_id in limited_ids)
    )
    workers = {worker_id: data for item in loaded if item for worker_id, data in [item]}
    return workers, {
        "source": source,
        "known_workers": total_known,
        "scanned_workers": len(limited_ids),
        "truncated": total_known > len(limited_ids),
    }


async def _scan_online_worker_ids(
    redis: Redis, scan_limit: int
) -> tuple[list[str], bool]:
    prefix = RedisKeys.worker_online_lease("")
    pattern = f"{prefix}*"
    limit = max(scan_limit, 0)
    worker_ids: list[str] = []
    scan_iter = getattr(redis, "scan_iter", None)
    if callable(scan_iter):
        async for key in scan_iter(match=pattern, count=max(limit or 100, 100)):
            key_text = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            if key_text.startswith(prefix):
                worker_ids.append(key_text.removeprefix(prefix))
            if limit and len(worker_ids) >= limit:
                break
        return sorted(set(worker_ids)), True

    scan = getattr(redis, "scan", None)
    if not callable(scan):
        return [], False

    cursor: int | str = 0
    while True:
        cursor, keys = await scan(
            cursor=cursor, match=pattern, count=max(limit or 100, 100)
        )
        for key in keys:
            key_text = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            if key_text.startswith(prefix):
                worker_ids.append(key_text.removeprefix(prefix))
            if limit and len(worker_ids) >= limit:
                return sorted(set(worker_ids)), True
        if str(cursor) == "0":
            break
    return sorted(set(worker_ids)), True


async def _get_admin_managed_worker_ids(
    redis: Redis,
    scan_limit: int,
) -> tuple[list[str], dict[str, Any]]:
    """Return workers with explicit admin lifecycle state, including offline ones."""
    indexed_raw = await redis.smembers(RedisKeys.admin_workers())
    indexed_ids = {
        item.decode("utf-8") if isinstance(item, bytes) else str(item)
        for item in indexed_raw
    }
    scanned_ids, scan_supported = await _scan_worker_admin_ids(redis, scan_limit)
    worker_ids = sorted(indexed_ids | set(scanned_ids))
    limited_ids = worker_ids[: max(scan_limit, 0)] if scan_limit else worker_ids
    return limited_ids, {
        "source": "admin_index_and_scan" if scan_supported else "admin_index",
        "indexed_workers": len(indexed_ids),
        "scanned_workers": len(scanned_ids),
        "managed_workers": len(worker_ids),
        "returned_workers": len(limited_ids),
        "truncated": len(worker_ids) > len(limited_ids),
    }


async def _scan_worker_admin_ids(
    redis: Redis,
    scan_limit: int,
) -> tuple[list[str], bool]:
    prefix = RedisKeys.worker_admin("")
    pattern = f"{prefix}*"
    limit = max(scan_limit, 0)
    worker_ids: list[str] = []
    scan_iter = getattr(redis, "scan_iter", None)
    if callable(scan_iter):
        async for key in scan_iter(match=pattern, count=max(limit or 100, 100)):
            key_text = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            if key_text.startswith(prefix):
                worker_ids.append(key_text.removeprefix(prefix))
            if limit and len(worker_ids) >= limit:
                break
        return sorted(set(worker_ids)), True

    scan = getattr(redis, "scan", None)
    if not callable(scan):
        return [], False

    cursor: int | str = 0
    while True:
        cursor, keys = await scan(
            cursor=cursor, match=pattern, count=max(limit or 100, 100)
        )
        for key in keys:
            key_text = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            if key_text.startswith(prefix):
                worker_ids.append(key_text.removeprefix(prefix))
            if limit and len(worker_ids) >= limit:
                return sorted(set(worker_ids)), True
        if str(cursor) == "0":
            break
    return sorted(set(worker_ids)), True


def _decode_worker_presence_for_snapshot(raw: Any) -> tuple[int, bool]:
    if raw is None:
        return 0, False
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return 0, True
    if isinstance(payload, dict):
        return int(payload.get("last_seen", 0) or 0), False
    if payload == 1:
        return 0, True
    return 0, True


async def _batch_fetch_admin_states(
    redis: Redis, worker_ids: list[str]
) -> dict[str, dict[str, Any]]:
    """Pipeline-fetch admin state HASHes for all workers in one round-trip."""
    if not worker_ids:
        return {}
    pipe = redis.pipeline()
    for wid in worker_ids:
        pipe.hgetall(RedisKeys.worker_admin(wid))
    results = await pipe.execute()
    return {
        worker_id: _decode_hash_mapping(raw)
        for worker_id, raw in zip(worker_ids, results)
    }


def _decode_hash_mapping(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    decoded: dict[str, Any] = {}
    for key, value in raw.items():
        key_text = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        if isinstance(value, bytes):
            decoded[key_text] = value.decode("utf-8")
        else:
            decoded[key_text] = value
    return decoded


async def _build_lightweight_worker_summary(
    registry: WorkerRegistry,
    workers: dict[str, Any],
    worker_id: str,
    admin_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw_counts = await registry.redis.hgetall(RedisKeys.worker_status(worker_id))
    counts = {
        "total": _get_hash_int(raw_counts, "total_count"),
        "active": _get_hash_int(raw_counts, "active_count"),
        "queued": _get_hash_int(raw_counts, "queued_count"),
        "running": _get_hash_int(raw_counts, "running_count"),
        "cancelling": _get_hash_int(raw_counts, "cancelling_count"),
        "completed": _get_hash_int(raw_counts, "completed_count"),
        "failed": _get_hash_int(raw_counts, "failed_count"),
        "cancelled": _get_hash_int(raw_counts, "cancelled_count"),
    }
    status_counts = {
        "QUEUED": counts["queued"],
        "RUNNING": counts["running"],
        "CANCELLING": counts["cancelling"],
        "COMPLETED": counts["completed"],
        "FAILED": counts["failed"],
        "CANCELLED": counts["cancelled"],
    }
    worker_info = workers.get(worker_id, {})
    admin = admin_state or {}
    return {
        "worker_id": worker_id,
        "online": worker_id in workers,
        "agent_types": sorted(worker_info.get("agent_types", [])),
        "last_seen": int(worker_info.get("last_seen", 0)),
        "ip_address": worker_info.get("ip_address", ""),
        "lifecycle": admin.get("lifecycle", "") or "active",
        "lifecycle_reason": admin.get("reason", "") or "",
        "counts": counts,
        "active_count": counts["active"],
        "total_tracked": counts["total"],
        "last_updated_at": _get_hash_int(raw_counts, "last_updated_at"),
        "last_started_at": _get_hash_int(raw_counts, "last_started_at"),
        "last_finished_at": _get_hash_int(raw_counts, "last_finished_at"),
        "status_counts": {key: value for key, value in status_counts.items() if value},
    }


def _get_hash_int(data: dict[Any, Any], key: str) -> int:
    value = data.get(key)
    if value is None:
        value = data.get(key.encode("utf-8"))
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    return int(value or 0)


def _enrich_snapshot(
    snapshot: dict[str, Any], *, alert_policy: AlertPolicy | None = None
) -> dict[str, Any]:
    policy = _resolve_alert_policy(alert_policy)
    snapshot["recent_executions"] = [
        _enrich_execution_timing(execution)
        for execution in snapshot.get("recent_executions", [])
    ]
    snapshot["latency"] = _calculate_latency(snapshot.get("recent_executions", []))
    snapshot["failures"] = _build_failure_summary(snapshot.get("recent_executions", []))
    snapshot["slo"] = _build_slo_summary(snapshot, policy.slo_policy)
    snapshot["alerts"] = _build_alerts(snapshot, alert_policy=policy)
    snapshot["health"] = _build_health_summary(snapshot.get("alerts", []))
    snapshot["agent_health"] = _build_agent_health(snapshot)
    snapshot["data_flow"] = _build_data_flow(snapshot)
    return snapshot


def _calculate_latency(executions: list[dict[str, Any]]) -> dict[str, Any]:
    queue_durations = _collect_duration(executions, "queue_latency_ms")
    run_durations = _collect_duration(executions, "run_latency_ms")
    total_durations = _collect_duration(executions, "total_latency_ms")
    run_stats = _duration_stats(run_durations)
    return {
        **run_stats,
        "queue": _duration_stats(queue_durations),
        "run": run_stats,
        "total": _duration_stats(total_durations),
    }


def _collect_duration(executions: list[dict[str, Any]], key: str) -> list[int]:
    return sorted(
        int(execution.get(key, 0) or 0)
        for execution in executions
        if int(execution.get(key, 0) or 0) > 0
    )


def _duration_stats(durations: list[int]) -> dict[str, int]:
    if not durations:
        return {
            "completed_count": 0,
            "avg_ms": 0,
            "p50_ms": 0,
            "p95_ms": 0,
            "max_ms": 0,
        }
    return {
        "completed_count": len(durations),
        "avg_ms": int(sum(durations) / len(durations)),
        "p50_ms": _percentile(durations, 50),
        "p95_ms": _percentile(durations, 95),
        "max_ms": durations[-1],
    }


def _enrich_execution_timing(execution: dict[str, Any]) -> dict[str, Any]:
    created_at = int(execution.get("created_at", 0) or 0)
    started_at = int(execution.get("started_at", 0) or 0)
    finished_at = int(execution.get("finished_at", 0) or 0)
    enriched = dict(execution)
    enriched["queue_latency_ms"] = (
        started_at - created_at if started_at > 0 and started_at >= created_at else 0
    )
    enriched["run_latency_ms"] = (
        finished_at - started_at
        if finished_at > 0 and started_at > 0 and finished_at >= started_at
        else 0
    )
    enriched["total_latency_ms"] = (
        finished_at - created_at
        if finished_at > 0 and created_at > 0 and finished_at >= created_at
        else 0
    )
    return enriched


def _build_failure_summary(executions: list[dict[str, Any]]) -> dict[str, Any]:
    failed = [
        execution
        for execution in executions
        if str(execution.get("status", "")) == "FAILED"
    ]
    by_error_type: dict[str, int] = {}
    recent = []
    for execution in failed:
        error_type = str(execution.get("error_type") or "unknown")
        by_error_type[error_type] = by_error_type.get(error_type, 0) + 1
        recent.append(
            {
                "execution_id": execution.get("execution_id", ""),
                "message_id": execution.get("message_id", ""),
                "worker_id": execution.get("worker_id", ""),
                "target_agent_type": execution.get("target_agent_type", ""),
                "error_type": error_type,
                "error_code": execution.get("error_code", ""),
                "error_message": execution.get("error_message", ""),
                "failed_stage": execution.get("failed_stage", ""),
                "updated_at": int(execution.get("updated_at", 0) or 0),
            }
        )
    return {
        "total": len(failed),
        "by_error_type": dict(sorted(by_error_type.items())),
        "recent": recent[:10],
    }


def _percentile(sorted_values: list[int], percentile: int) -> int:
    if not sorted_values:
        return 0
    index = int(round((percentile / 100) * (len(sorted_values) - 1)))
    return sorted_values[max(0, min(index, len(sorted_values) - 1))]


def _resolve_alert_policy(
    alert_policy: AlertPolicy | None,
    *,
    queue_backlog_threshold: int | None = None,
) -> AlertPolicy:
    if alert_policy is None:
        policy = AlertPolicy()
    else:
        policy = AlertPolicy(
            queue_backlog_threshold=max(0, alert_policy.queue_backlog_threshold),
            delivery_pending_threshold=max(0, alert_policy.delivery_pending_threshold),
            consumer_pending_threshold=max(0, alert_policy.consumer_pending_threshold),
            failed_execution_threshold=max(0, alert_policy.failed_execution_threshold),
            slo_policy=_resolve_slo_policy(alert_policy.slo_policy),
        )
    if queue_backlog_threshold is None:
        return policy
    return AlertPolicy(
        queue_backlog_threshold=max(0, queue_backlog_threshold),
        delivery_pending_threshold=max(0, policy.delivery_pending_threshold),
        consumer_pending_threshold=max(0, policy.consumer_pending_threshold),
        failed_execution_threshold=max(0, policy.failed_execution_threshold),
        slo_policy=policy.slo_policy,
    )


def _resolve_slo_policy(slo_policy: SLOPolicy | None) -> SLOPolicy | None:
    if slo_policy is None:
        return None
    return SLOPolicy(
        success_ratio_target=min(1.0, max(0.0, slo_policy.success_ratio_target)),
        total_latency_p95_ms=max(0, int(slo_policy.total_latency_p95_ms)),
        deadletter_threshold=max(0, int(slo_policy.deadletter_threshold)),
        freshness_max_age_ms=max(0, int(slo_policy.freshness_max_age_ms)),
        window=slo_policy.window or "5m",
    )


def _build_alerts(
    snapshot: dict[str, Any], *, alert_policy: AlertPolicy
) -> list[dict[str, Any]]:
    return (
        _build_worker_alerts(snapshot, alert_policy=alert_policy)
        + _build_queue_alerts(snapshot, alert_policy=alert_policy)
        + _build_slo_alerts(snapshot, alert_policy=alert_policy)
    )


def _build_health_summary(alerts: list[dict[str, Any]]) -> dict[str, Any]:
    critical_alerts = sum(
        1 for alert in alerts if str(alert.get("severity", "")) == "critical"
    )
    warning_alerts = sum(
        1 for alert in alerts if str(alert.get("severity", "")) == "warning"
    )
    score = max(0, 100 - critical_alerts * 40 - warning_alerts * 10)
    if critical_alerts:
        status = "critical"
        summary = (
            f"{critical_alerts} critical alerts and "
            f"{warning_alerts} warning alerts active."
        )
    elif warning_alerts:
        status = "warning"
        summary = f"{warning_alerts} warning alert{'s' if warning_alerts != 1 else ''} active."
    else:
        status = "healthy"
        summary = "No active health alerts."
    return {
        "status": status,
        "score": score,
        "critical_alerts": critical_alerts,
        "warning_alerts": warning_alerts,
        "summary": summary,
    }


def _build_worker_alerts(
    snapshot: dict[str, Any], *, alert_policy: AlertPolicy | None = None
) -> list[dict[str, Any]]:
    policy = _resolve_alert_policy(alert_policy)
    alerts = []
    totals = snapshot.get("totals", {})
    if int(totals.get("workers_online", 0) or 0) == 0:
        alerts.append(
            {
                "code": "NO_WORKERS_ONLINE",
                "severity": "critical",
                "message": "No online workers discovered.",
                "value": 0,
                "threshold": 1,
            }
        )

    failed_count = int(snapshot.get("status_counts", {}).get("FAILED", 0) or 0)
    if failed_count > policy.failed_execution_threshold:
        alerts.append(
            {
                "code": "FAILED_EXECUTIONS",
                "severity": "warning",
                "message": f"{failed_count} failed executions recorded.",
                "value": failed_count,
                "threshold": policy.failed_execution_threshold,
            }
        )
    return alerts


def _build_queue_alerts(
    snapshot: dict[str, Any], *, alert_policy: AlertPolicy | None = None
) -> list[dict[str, Any]]:
    policy = _resolve_alert_policy(alert_policy)
    alerts = []

    delivery_pending = (
        snapshot.get("queues", {})
        .get("control_plane", {})
        .get("delivery_pending", {})
        .get("length", 0)
    )
    delivery_pending_count = int(delivery_pending or 0)
    if delivery_pending_count > policy.delivery_pending_threshold:
        alerts.append(
            {
                "code": "PENDING_DELIVERIES",
                "severity": "warning",
                "message": (
                    f"{delivery_pending_count} pending control-plane deliveries."
                ),
                "value": delivery_pending_count,
                "threshold": policy.delivery_pending_threshold,
            }
        )

    for queue in snapshot.get("queues", {}).get("agent_type_streams", []):
        length = int(queue.get("length", 0) or 0)
        if length >= policy.queue_backlog_threshold:
            agent_type = queue.get("agent_type", "")
            alerts.append(
                {
                    "code": "QUEUE_BACKLOG",
                    "severity": "warning",
                    "message": (
                        f"{length} messages queued for agent type {agent_type}."
                    ),
                    "value": length,
                    "threshold": policy.queue_backlog_threshold,
                }
            )

    pending_total = _total_consumer_pending(snapshot)
    if pending_total > policy.consumer_pending_threshold:
        alerts.append(
            {
                "code": "CONSUMER_PENDING",
                "severity": "warning",
                "message": f"{pending_total} messages pending in consumer groups.",
                "value": pending_total,
                "threshold": policy.consumer_pending_threshold,
            }
        )
    return alerts


def _build_slo_summary(
    snapshot: dict[str, Any], slo_policy: SLOPolicy | None
) -> dict[str, Any]:
    status_counts = snapshot.get("status_counts", {})
    completed = int(status_counts.get("COMPLETED", 0) or 0)
    failed = int(status_counts.get("FAILED", 0) or 0)
    cancelled = int(status_counts.get("CANCELLED", 0) or 0)
    terminal = completed + failed + cancelled
    success_ratio = completed / terminal if terminal else 1.0
    latency_p95 = int(
        snapshot.get("latency", {}).get("total", {}).get("p95_ms", 0) or 0
    )
    deadletter_count = int(
        snapshot.get("queues", {})
        .get("control_plane", {})
        .get("deadletter", {})
        .get("length", 0)
        or 0
    )
    generated_at = int(snapshot.get("generated_at", 0) or 0)
    newest_seen = max(
        [int(worker.get("last_seen", 0) or 0) for worker in snapshot.get("workers", [])]
        or [generated_at]
    )
    freshness_age_ms = max(0, generated_at - newest_seen) if generated_at else 0
    target = slo_policy.success_ratio_target if slo_policy else 0.99
    budget = max(0.000001, 1.0 - target)
    burn_rate = max(0.0, (target - success_ratio) / budget)
    return {
        "window": slo_policy.window if slo_policy else "5m",
        "success_ratio": success_ratio,
        "success_ratio_ppm": int(success_ratio * 1_000_000),
        "success_ratio_target_ppm": int(target * 1_000_000),
        "burn_rate": round(burn_rate, 3),
        "terminal_executions": terminal,
        "total_latency_p95_ms": latency_p95,
        "total_latency_p95_target_ms": (
            slo_policy.total_latency_p95_ms if slo_policy else 30_000
        ),
        "deadletter_count": deadletter_count,
        "deadletter_threshold": slo_policy.deadletter_threshold if slo_policy else 0,
        "freshness_age_ms": freshness_age_ms,
        "freshness_max_age_ms": (
            slo_policy.freshness_max_age_ms if slo_policy else 15_000
        ),
    }


def _build_slo_alerts(
    snapshot: dict[str, Any], *, alert_policy: AlertPolicy | None = None
) -> list[dict[str, Any]]:
    policy = _resolve_alert_policy(alert_policy)
    slo_policy = policy.slo_policy
    if slo_policy is None:
        return []
    slo = snapshot.get("slo") or _build_slo_summary(snapshot, slo_policy)
    alerts = []
    success_ratio = float(slo.get("success_ratio", 1.0) or 1.0)
    if success_ratio < slo_policy.success_ratio_target:
        alerts.append(
            _slo_alert(
                code="SLO_SUCCESS_RATIO",
                severity="critical"
                if float(slo.get("burn_rate", 0)) >= 2
                else "warning",
                message=(
                    "Execution success ratio "
                    f"{success_ratio:.2%} is below target "
                    f"{slo_policy.success_ratio_target:.2%}."
                ),
                value=int(slo.get("success_ratio_ppm", 0) or 0),
                threshold=int(slo_policy.success_ratio_target * 1_000_000),
                sli="execution_success_ratio",
                window=slo_policy.window,
                burn_rate=float(slo.get("burn_rate", 0) or 0),
                runbook_id="slo-success-ratio",
            )
        )
    latency_p95 = int(slo.get("total_latency_p95_ms", 0) or 0)
    if latency_p95 > slo_policy.total_latency_p95_ms:
        alerts.append(
            _slo_alert(
                code="SLO_LATENCY_P95",
                severity="warning",
                message=(
                    f"End-to-end P95 latency {latency_p95} ms exceeds "
                    f"{slo_policy.total_latency_p95_ms} ms."
                ),
                value=latency_p95,
                threshold=slo_policy.total_latency_p95_ms,
                sli="execution_total_latency_p95",
                window=slo_policy.window,
                burn_rate=round(
                    latency_p95 / max(slo_policy.total_latency_p95_ms, 1), 3
                ),
                runbook_id="slo-latency-p95",
            )
        )
    deadletters = int(slo.get("deadletter_count", 0) or 0)
    if deadletters > slo_policy.deadletter_threshold:
        alerts.append(
            _slo_alert(
                code="SLO_DEADLETTER",
                severity="critical",
                message=f"{deadletters} deadletter messages exceed SLO threshold.",
                value=deadletters,
                threshold=slo_policy.deadletter_threshold,
                sli="deadletter_count",
                window=slo_policy.window,
                burn_rate=deadletters / max(slo_policy.deadletter_threshold, 1),
                runbook_id="slo-deadletter",
            )
        )
    freshness_age_ms = int(slo.get("freshness_age_ms", 0) or 0)
    if freshness_age_ms > slo_policy.freshness_max_age_ms:
        alerts.append(
            _slo_alert(
                code="SLO_FRESHNESS",
                severity="warning",
                message=(
                    f"Newest worker heartbeat is {freshness_age_ms} ms old, "
                    f"above {slo_policy.freshness_max_age_ms} ms."
                ),
                value=freshness_age_ms,
                threshold=slo_policy.freshness_max_age_ms,
                sli="worker_freshness",
                window=slo_policy.window,
                burn_rate=round(
                    freshness_age_ms / max(slo_policy.freshness_max_age_ms, 1), 3
                ),
                runbook_id="slo-worker-freshness",
            )
        )
    return alerts


def _slo_alert(
    *,
    code: str,
    severity: str,
    message: str,
    value: int,
    threshold: int,
    sli: str,
    window: str,
    burn_rate: float,
    runbook_id: str,
) -> dict[str, Any]:
    runbook = SLO_RUNBOOKS.get(
        runbook_id,
        {
            "id": runbook_id,
            "title": "Investigate SLO alert",
            "actions": ["Inspect correlated metrics, traces, logs, and queue state."],
        },
    )
    return {
        "code": code,
        "severity": severity,
        "message": message,
        "value": value,
        "threshold": threshold,
        "sli": sli,
        "window": window,
        "burn_rate": round(float(burn_rate), 3),
        "runbook_id": runbook_id,
        "runbook": runbook,
    }


def _count_alerts_by_severity(alerts: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for alert in alerts:
        severity = str(alert.get("severity", "unknown"))
        counts[severity] = counts.get(severity, 0) + 1
    return counts


def _build_data_flow(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Build a frontend-ready model of the runtime data path."""
    totals = snapshot.get("totals", {})
    status_counts = snapshot.get("status_counts", {})
    latency = snapshot.get("latency", {})
    agent_queue_depth = sum(
        int(queue.get("length", 0) or 0)
        for queue in snapshot.get("queues", {}).get("agent_type_streams", [])
    )
    control_plane_depth = sum(
        int(queue.get("length", 0) or 0)
        for queue in snapshot.get("queues", {}).get("control_plane", {}).values()
    )
    consumer_pending = _total_consumer_pending(snapshot)
    failed_executions = int(status_counts.get("FAILED", 0) or 0)
    workers_online = int(totals.get("workers_online", 0) or 0)
    active_executions = int(totals.get("active_executions", 0) or 0)
    tracked_executions = int(totals.get("tracked_executions", 0) or 0)
    pending_deliveries = int(
        snapshot.get("queues", {})
        .get("control_plane", {})
        .get("delivery_pending", {})
        .get("length", 0)
        or 0
    )
    deadletters = int(
        snapshot.get("queues", {})
        .get("control_plane", {})
        .get("deadletter", {})
        .get("length", 0)
        or 0
    )
    data_events = sum(
        1
        for execution in snapshot.get("recent_executions", [])
        if execution.get("session_id")
    )
    total_latency_p95_ms = int(latency.get("total", {}).get("p95_ms", 0) or 0)
    websocket_observable = False

    queue_status = _flow_status(
        critical=False,
        warning=agent_queue_depth > 0 or consumer_pending > 0,
    )
    worker_status = _flow_status(
        critical=workers_online == 0,
        warning=failed_executions > 0,
    )
    control_plane_status = _flow_status(
        critical=deadletters > 0,
        warning=pending_deliveries > 0,
    )
    data_stream_status = _flow_status(
        critical=deadletters > 0,
        warning=False,
    )

    nodes = [
        {
            "id": "client",
            "label": "Client / GatewayClient",
            "kind": "ingress",
            "status": "healthy",
            "metrics": {
                "tracked_executions": tracked_executions,
            },
        },
        {
            "id": "control_queues",
            "label": "Redis Input MQ",
            "kind": "queue",
            "status": queue_status,
            "metrics": {
                "queue_depth": agent_queue_depth,
                "consumer_pending": consumer_pending,
                "queue_latency_p95_ms": int(
                    latency.get("queue", {}).get("p95_ms", 0) or 0
                ),
            },
        },
        {
            "id": "workers",
            "label": "GatewayWorker Pool",
            "kind": "worker",
            "status": worker_status,
            "metrics": {
                "workers_online": workers_online,
                "active_executions": active_executions,
                "failed_executions": failed_executions,
                "run_latency_p95_ms": int(latency.get("run", {}).get("p95_ms", 0) or 0),
            },
        },
        {
            "id": "data_stream",
            "label": "Redis Data Stream",
            "kind": "stream",
            "status": data_stream_status,
            "metrics": {
                "recent_events": data_events,
                "total_latency_p95_ms": total_latency_p95_ms,
            },
        },
        {
            "id": "websocket_backend",
            "label": "WebSocket Backend",
            "kind": "egress",
            "status": "unknown",
            "metrics": {
                "fanout_observable": int(websocket_observable),
            },
        },
        {
            "id": "control_plane",
            "label": "Control Plane",
            "kind": "control",
            "status": control_plane_status,
            "metrics": {
                "pending_deliveries": pending_deliveries,
                "deadletters": deadletters,
                "control_queue_depth": control_plane_depth,
            },
        },
    ]
    edges = [
        {
            "id": "client-to-control-queues",
            "source": "client",
            "target": "control_queues",
            "label": "AskAgentCommand / ResumeCommand",
            "metric_label": "queued",
            "metric_value": agent_queue_depth,
            "status": queue_status,
        },
        {
            "id": "control-queues-to-workers",
            "source": "control_queues",
            "target": "workers",
            "label": "Redis Stream consumer groups",
            "metric_label": "pending",
            "metric_value": consumer_pending,
            "status": queue_status,
        },
        {
            "id": "workers-to-data-stream",
            "source": "workers",
            "target": "data_stream",
            "label": "StreamChunk / State / Artifact events",
            "metric_label": "recent",
            "metric_value": data_events,
            "status": data_stream_status,
        },
        {
            "id": "data-stream-to-websocket",
            "source": "data_stream",
            "target": "websocket_backend",
            "label": "Session data stream fan-out",
            "metric_label": "fanout observable",
            "metric_value": int(websocket_observable),
            "status": "unknown",
        },
        {
            "id": "control-plane-to-workers",
            "source": "control_plane",
            "target": "workers",
            "label": "wakeups / pending delivery / deadletter",
            "metric_label": "pending",
            "metric_value": pending_deliveries,
            "status": control_plane_status,
        },
    ]
    return {
        "summary": {
            "queue_depth_total": agent_queue_depth + control_plane_depth,
            "consumer_pending_total": consumer_pending,
            "workers_online": workers_online,
            "active_executions": active_executions,
            "failed_executions": failed_executions,
            "queue_latency_p95_ms": int(latency.get("queue", {}).get("p95_ms", 0) or 0),
            "run_latency_p95_ms": int(latency.get("run", {}).get("p95_ms", 0) or 0),
            "total_latency_p95_ms": int(latency.get("total", {}).get("p95_ms", 0) or 0),
        },
        "nodes": nodes,
        "edges": edges,
    }


def _flow_status(*, critical: bool, warning: bool) -> str:
    if critical:
        return "critical"
    if warning:
        return "warning"
    return "healthy"


def _total_queue_depth(snapshot: dict[str, Any]) -> int:
    queues = snapshot.get("queues", {})
    total = sum(
        int(queue.get("length", 0) or 0)
        for queue in queues.get("agent_type_streams", [])
    )
    total += sum(
        int(queue.get("length", 0) or 0)
        for queue in queues.get("control_plane", {}).values()
    )
    return total


def _iter_queues(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    queues = snapshot.get("queues", {})
    rows = [
        {
            **queue,
            "queue_type": "agent_type",
            "name": str(queue.get("agent_type", "")),
        }
        for queue in queues.get("agent_type_streams", [])
    ]
    rows.extend(
        {
            **queue,
            "queue_type": "control_plane",
            "name": str(name),
        }
        for name, queue in queues.get("control_plane", {}).items()
    )
    return rows


def _total_consumer_pending(snapshot: dict[str, Any]) -> int:
    return sum(
        int(group.get("pending", 0) or 0)
        for queue in _iter_queues(snapshot)
        for group in queue.get("consumer_groups", [])
    )


def _max_delivery_count(snapshot: dict[str, Any]) -> int:
    return max(
        (
            int(group.get("max_delivery_count", 0) or 0)
            for queue in _iter_queues(snapshot)
            for group in queue.get("consumer_groups", [])
        ),
        default=0,
    )


def _build_agent_health(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    agent_types = sorted(
        {
            agent_type
            for worker in snapshot.get("workers", [])
            for agent_type in worker.get("agent_types", [])
        }
        | {
            str(queue.get("agent_type", ""))
            for queue in snapshot.get("queues", {}).get("agent_type_streams", [])
            if queue.get("agent_type")
        }
        | {
            str(execution.get("target_agent_type", ""))
            for execution in snapshot.get("recent_executions", [])
            if execution.get("target_agent_type")
        }
    )
    queue_depth_by_agent = {
        str(queue.get("agent_type", "")): int(queue.get("length", 0) or 0)
        for queue in snapshot.get("queues", {}).get("agent_type_streams", [])
    }
    health = []
    for agent_type in agent_types:
        recent_executions = [
            execution
            for execution in snapshot.get("recent_executions", [])
            if str(execution.get("target_agent_type", "")) == agent_type
        ]
        recent_status_counts = _count_execution_statuses(recent_executions)
        health.append(
            {
                "agent_type": agent_type,
                "worker_count": sum(
                    1
                    for worker in snapshot.get("workers", [])
                    if agent_type in worker.get("agent_types", [])
                ),
                "queue_depth": queue_depth_by_agent.get(agent_type, 0),
                "recent_executions": len(recent_executions),
                "recent_active_executions": sum(
                    1
                    for execution in recent_executions
                    if execution.get("status") in ("QUEUED", "RUNNING", "CANCELLING")
                ),
                "recent_failed_executions": int(
                    recent_status_counts.get("FAILED", 0) or 0
                ),
                "recent_status_counts": recent_status_counts,
            }
        )
    return health


def _count_execution_statuses(executions: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for execution in executions:
        status = str(execution.get("status", ""))
        if status:
            counts[status] = counts.get(status, 0) + 1
    return dict(sorted(counts.items()))


def _build_execution_tree(executions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    nodes = [
        {
            **execution,
            "children": [],
        }
        for execution in executions
    ]
    by_message_id = {str(node.get("message_id", "")): node for node in nodes}
    roots = []
    for node in nodes:
        parent_message_id = str(node.get("parent_message_id", ""))
        parent = by_message_id.get(parent_message_id)
        if parent is None:
            roots.append(node)
            continue
        parent["children"].append(node)
    return roots


def _build_session_timeline(
    executions: list[dict[str, Any]], events: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    timeline = []
    execution_by_message = {
        str(execution.get("message_id", "")): execution for execution in executions
    }
    for execution in executions:
        for item in execution.get("timeline", []):
            timestamp = int(item.get("timestamp", 0) or 0)
            if not timestamp:
                continue
            timeline.append(
                {
                    "timestamp": timestamp,
                    "kind": "execution_status",
                    "status": str(item.get("status", "")),
                    "execution_id": execution.get("execution_id", ""),
                    "message_id": execution.get("message_id", ""),
                    "target_agent_type": execution.get("target_agent_type", ""),
                    "worker_id": execution.get("worker_id", ""),
                }
            )
    for event in events:
        message_id = str(event.get("message_id", ""))
        execution = execution_by_message.get(message_id, {})
        timeline.append(
            {
                "timestamp": int(event.get("timestamp", 0) or 0),
                "kind": "data_event",
                "event_type": event.get("event_type", ""),
                "stream_id": event.get("stream_id", ""),
                "message_id": message_id,
                "target_agent_type": execution.get("target_agent_type", ""),
                "source_agent_type": event.get("source_agent_type", ""),
            }
        )
    timeline.sort(key=lambda item: int(item.get("timestamp", 0) or 0))
    return timeline


def _build_trace_from_session_snapshot(
    session_snapshot: dict[str, Any], trace_id: str
) -> dict[str, Any]:
    spans = []
    worker_span_by_message = {}
    for execution in session_snapshot.get("executions", []):
        parent_message_id = str(execution.get("parent_message_id", ""))
        dispatch_parent_span_id = worker_span_by_message.get(parent_message_id, "")
        dispatch_span, queue_span, worker_span = _execution_to_trace_spans(
            trace_id,
            execution,
            dispatch_parent_span_id=dispatch_parent_span_id,
        )
        if dispatch_span is not None:
            spans.append(dispatch_span)
        if queue_span is not None:
            spans.append(queue_span)
        if worker_span is not None:
            spans.append(worker_span)
            worker_span_by_message[str(execution.get("message_id", ""))] = worker_span[
                "span_id"
            ]
    for event in session_snapshot.get("recent_events", []):
        span = _event_to_trace_span(
            trace_id,
            event,
            parent_span_id=worker_span_by_message.get(
                str(event.get("message_id", "")), ""
            ),
        )
        if span is not None:
            spans.append(span)
    return _build_trace_snapshot(
        trace_id,
        str(session_snapshot.get("session_id", "")),
        spans,
    )


async def _read_stored_trace_spans(redis: Redis, trace_id: str) -> list[dict[str, Any]]:
    lrange = getattr(redis, "lrange", None)
    if not callable(lrange):
        return []
    try:
        raw_spans = await lrange(RedisKeys.trace_spans(trace_id), 0, -1)
    except Exception:  # pylint: disable=broad-exception-caught
        return []
    spans = []
    for raw in raw_spans:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            span = json.loads(raw)
        except (TypeError, ValueError):
            continue
        if isinstance(span, dict):
            spans.append(span)
    return spans


def _execution_to_trace_spans(
    trace_id: str,
    execution: dict[str, Any],
    *,
    dispatch_parent_span_id: str = "",
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None]:
    created_at = int(execution.get("created_at", 0) or 0)
    started_at = int(execution.get("started_at", 0) or 0)
    finished_at = int(execution.get("finished_at", 0) or 0)
    updated_at = int(execution.get("updated_at", 0) or 0)
    status = str(execution.get("status", "") or "UNKNOWN")
    execution_id = str(execution.get("execution_id", ""))
    message_id = str(execution.get("message_id", ""))
    session_id = str(execution.get("session_id", ""))
    parent_message_id = str(execution.get("parent_message_id", ""))
    source_agent_type = str(execution.get("source_agent_type", ""))
    target_agent_type = str(execution.get("target_agent_type", ""))
    route_policy = str(execution.get("route_policy", ""))
    route_status = str(execution.get("route_status", ""))
    dispatch_start = created_at or started_at or updated_at
    dispatch_span = None
    if dispatch_start and message_id:
        dispatch_span = _trace_span(
            trace_id=trace_id,
            span_id=f"{message_id}:client.dispatch",
            parent_span_id=dispatch_parent_span_id,
            operation="client.dispatch",
            component="client" if not dispatch_parent_span_id else "agent_context",
            start_ts=dispatch_start,
            end_ts=dispatch_start,
            status="COMPLETED",
            session_id=session_id,
            execution_id=execution_id,
            message_id=message_id,
            parent_message_id=parent_message_id,
            source_agent_type=source_agent_type or "client",
            target_agent_type=target_agent_type,
            route_policy=route_policy,
            route_status=route_status,
        )
    queue_span = None
    if created_at and started_at and started_at >= created_at:
        queue_span = _trace_span(
            trace_id=trace_id,
            span_id=f"{execution_id}:queue.wait",
            parent_span_id=dispatch_span["span_id"] if dispatch_span else "",
            operation="queue.wait",
            component="redis",
            start_ts=created_at,
            end_ts=started_at,
            status="COMPLETED",
            session_id=session_id,
            execution_id=execution_id,
            message_id=message_id,
            parent_message_id=parent_message_id,
            source_agent_type=source_agent_type,
            target_agent_type=target_agent_type,
            queue_wait_ms=max(0, started_at - created_at),
            route_policy=route_policy,
            route_status=route_status,
        )
    worker_start = started_at or created_at or updated_at
    worker_end = finished_at or updated_at or worker_start
    worker_span = None
    if worker_start:
        parent_span_id = ""
        if queue_span:
            parent_span_id = queue_span["span_id"]
        elif dispatch_span:
            parent_span_id = dispatch_span["span_id"]
        worker_span = _trace_span(
            trace_id=trace_id,
            span_id=f"{execution_id}:worker.execute",
            parent_span_id=parent_span_id,
            operation="worker.execute",
            component="worker",
            start_ts=worker_start,
            end_ts=max(worker_start, worker_end),
            status=status,
            session_id=session_id,
            execution_id=execution_id,
            message_id=message_id,
            parent_message_id=parent_message_id,
            worker_id=str(execution.get("worker_id", "")),
            source_agent_type=source_agent_type,
            target_agent_type=target_agent_type,
            error_type=str(execution.get("error_type", "")),
            error_message=str(execution.get("error_message", "")),
            error_code=str(execution.get("error_code", "")),
            failed_stage=str(execution.get("failed_stage", "")),
            retryable=bool(execution.get("retryable", False)),
            route_policy=route_policy,
            route_status=route_status,
        )
    return dispatch_span, queue_span, worker_span


def _event_to_trace_span(
    trace_id: str, event: dict[str, Any], *, parent_span_id: str
) -> dict[str, Any] | None:
    timestamp = int(event.get("timestamp", 0) or 0)
    if not timestamp:
        return None
    event_type = str(event.get("event_type", ""))
    operation = (
        "agent.emit_chunk"
        if event_type.endswith("_DELTA") or event_type.endswith("CHUNK")
        else "agent.emit_event"
    )
    return _trace_span(
        trace_id=trace_id,
        span_id=f"{event.get('stream_id', '')}:{operation}",
        parent_span_id=parent_span_id,
        operation=operation,
        component="agent_context",
        start_ts=timestamp,
        end_ts=timestamp + 1,
        status="COMPLETED",
        session_id=str(event.get("session_id", "")),
        message_id=str(event.get("message_id", "")),
        parent_message_id=str(event.get("parent_message_id", "")),
        source_agent_type=str(event.get("source_agent_type", "")),
        target_agent_type=str(event.get("source_agent_type", "")),
        event_type=event_type,
    )


def _trace_span(
    *,
    trace_id: str,
    span_id: str,
    parent_span_id: str,
    operation: str,
    component: str,
    start_ts: int,
    end_ts: int,
    status: str,
    **fields: Any,
) -> dict[str, Any]:
    clean_fields = {
        key: value
        for key, value in fields.items()
        if value not in ("", None) and value is not False
    }
    end_ts = max(start_ts, int(end_ts or start_ts))
    return {
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": parent_span_id,
        "operation": operation,
        "component": component,
        "start_ts": int(start_ts or 0),
        "end_ts": end_ts,
        "duration_ms": max(0, end_ts - int(start_ts or 0)),
        "status": status,
        **clean_fields,
    }


def _build_trace_snapshot(
    trace_id: str, session_id: str, spans: list[dict[str, Any]]
) -> dict[str, Any]:
    ordered_spans = sorted(spans, key=lambda span: int(span.get("start_ts", 0) or 0))
    if not ordered_spans:
        return _empty_trace_snapshot(trace_id, session_id=session_id)
    start_ts = min(int(span.get("start_ts", 0) or 0) for span in ordered_spans)
    end_ts = max(int(span.get("end_ts", 0) or 0) for span in ordered_spans)
    status = _trace_status(ordered_spans)
    return {
        "generated_at": int(time.time() * 1000),
        "trace_id": trace_id,
        "session_id": session_id,
        "status": status,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "duration_ms": max(0, end_ts - start_ts),
        "span_count": len(ordered_spans),
        "spans": ordered_spans,
        "tree": _build_span_tree(ordered_spans),
        "timeline": _build_trace_timeline(ordered_spans),
    }


def _trace_status(spans: list[dict[str, Any]]) -> str:
    statuses = {str(span.get("status", "")) for span in spans}
    if "FAILED" in statuses:
        return "FAILED"
    if "RUNNING" in statuses or "QUEUED" in statuses:
        return "RUNNING"
    if statuses:
        return "COMPLETED"
    return "UNKNOWN"


def _build_span_tree(spans: list[dict[str, Any]]) -> list[dict[str, Any]]:
    nodes = [{**span, "children": []} for span in spans]
    by_span_id = {str(node.get("span_id", "")): node for node in nodes}
    roots = []
    for node in nodes:
        parent = by_span_id.get(str(node.get("parent_span_id", "")))
        if parent is None:
            roots.append(node)
            continue
        parent["children"].append(node)
    return roots


def _build_trace_timeline(spans: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not spans:
        return []
    trace_start = min(int(span.get("start_ts", 0) or 0) for span in spans)
    return [
        {
            **span,
            "offset_ms": max(0, int(span.get("start_ts", 0) or 0) - trace_start),
            "duration_ms": int(span.get("duration_ms", 0) or 0),
        }
        for span in sorted(spans, key=lambda item: int(item.get("start_ts", 0) or 0))
    ]


def _empty_trace_snapshot(trace_id: str, *, session_id: str = "") -> dict[str, Any]:
    return {
        "generated_at": int(time.time() * 1000),
        "trace_id": trace_id,
        "session_id": session_id,
        "status": "UNKNOWN",
        "start_ts": 0,
        "end_ts": 0,
        "duration_ms": 0,
        "span_count": 0,
        "spans": [],
        "tree": [],
        "timeline": [],
    }


async def _read_recent_session_events(
    redis: Redis,
    session_id: str,
    *,
    trace_id: str = "",
    limit: int = 50,
) -> list[dict[str, Any]]:
    xrevrange = getattr(redis, "xrevrange", None)
    if not callable(xrevrange):
        return []
    stream_name = RedisKeys.session_data_stream(session_id)
    entries = await xrevrange(stream_name, count=max(limit, 0))
    events = []
    for stream_id, fields in entries:
        event = _decode_data_stream_event(stream_id, fields)
        if event is None:
            continue
        if trace_id and event.get("trace_id") != trace_id:
            continue
        events.append(event)
    return events


def _decode_data_stream_event(
    stream_id: Any, fields: dict[Any, Any]
) -> dict[str, Any] | None:
    raw = fields.get("data")
    if raw is None:
        raw = fields.get(b"data")
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    payload = json.loads(raw)
    return {
        "stream_id": stream_id.decode("utf-8")
        if isinstance(stream_id, bytes)
        else str(stream_id),
        "trace_id": str(payload.get("trace_id", "")),
        "session_id": str(payload.get("session_id", "")),
        "event_type": str(payload.get("event_type", "")),
        "source_agent_type": str(payload.get("source_agent_type", "")),
        "message_id": str(payload.get("message_id", "")),
        "parent_message_id": str(payload.get("parent_message_id", "")),
        "timestamp": int(payload.get("timestamp", 0) or 0),
        "data": payload.get("data", {}),
        "metadata": payload.get("metadata", {}),
    }


def _merge_recent_executions(
    worker_summaries: list[dict[str, Any]], limit: int
) -> list[dict[str, Any]]:
    executions = [
        execution
        for summary in worker_summaries
        for execution in summary.get("recent_executions", [])
    ]
    executions.sort(key=lambda item: int(item.get("updated_at", 0) or 0), reverse=True)
    return executions[: max(limit, 0)]


async def _agent_type_stream_snapshot(
    redis: Redis, agent_type: str, *, include_consumer_details: bool
) -> dict[str, Any]:
    stream_name = RedisKeys.ctrl_stream(agent_type)
    return {
        "agent_type": agent_type,
        "stream": stream_name,
        "length": await _stream_length(redis, stream_name),
        "consumer_groups": await _stream_consumer_group_health(
            redis, stream_name, include_consumer_details=include_consumer_details
        ),
    }


async def _named_stream_length(
    redis: Redis, stream_name: str, *, include_consumer_details: bool = False
) -> dict[str, Any]:
    return {
        "stream": stream_name,
        "length": await _stream_length(redis, stream_name),
        "consumer_groups": await _stream_consumer_group_health(
            redis, stream_name, include_consumer_details=include_consumer_details
        ),
    }


async def _stream_length(redis: Redis, stream_name: str) -> int | None:
    xlen = getattr(redis, "xlen", None)
    if not callable(xlen):
        return None
    return int(await xlen(stream_name))


async def _stream_consumer_group_health(
    redis: Redis, stream_name: str, *, include_consumer_details: bool = False
) -> list[dict[str, Any]]:
    xinfo_groups = getattr(redis, "xinfo_groups", None)
    if not callable(xinfo_groups):
        return []
    try:
        raw_groups = await xinfo_groups(stream_name)
    except Exception:  # pylint: disable=broad-exception-caught
        return []

    groups = []
    for raw_group in raw_groups or []:
        group = _normalize_redis_mapping(raw_group)
        group_name = str(group.get("name", ""))
        pending = int(group.get("pending", 0) or 0)
        pending_detail = (
            await _stream_pending_detail(redis, stream_name, group_name)
            if include_consumer_details
            else {}
        )
        health = {
            "name": group_name,
            "pending": pending,
            "lag": _optional_int(group.get("lag")),
            "last_delivered_id": str(group.get("last-delivered-id", "")),
            "consumers": await _stream_consumer_health(redis, stream_name, group_name)
            if include_consumer_details
            else [],
            **pending_detail,
        }
        groups.append(health)
    return groups


async def _stream_consumer_health(
    redis: Redis, stream_name: str, group_name: str
) -> list[dict[str, Any]]:
    xinfo_consumers = getattr(redis, "xinfo_consumers", None)
    if not callable(xinfo_consumers) or not group_name:
        return []
    try:
        raw_consumers = await xinfo_consumers(stream_name, group_name)
    except Exception:  # pylint: disable=broad-exception-caught
        return []
    consumers = []
    for raw_consumer in raw_consumers or []:
        consumer = _normalize_redis_mapping(raw_consumer)
        consumers.append(
            {
                "name": str(consumer.get("name", "")),
                "pending": int(consumer.get("pending", 0) or 0),
                "idle_ms": int(consumer.get("idle", 0) or 0),
            }
        )
    return consumers


async def _stream_pending_detail(
    redis: Redis, stream_name: str, group_name: str
) -> dict[str, Any]:
    xpending = getattr(redis, "xpending", None)
    if not callable(xpending) or not group_name:
        return {}
    try:
        raw_pending = await xpending(stream_name, group_name)
    except Exception:  # pylint: disable=broad-exception-caught
        return {}
    detail = _normalize_pending_summary(raw_pending)
    if not detail:
        return {}
    return detail


def _normalize_pending_summary(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    pending = int(raw.get("pending", 0) or 0)
    entries = raw.get("entries", []) or []
    oldest_entry = entries[0] if entries else {}
    if isinstance(oldest_entry, dict):
        oldest_entry = _normalize_redis_mapping(oldest_entry)
    else:
        oldest_entry = {}
    oldest_id = str(
        oldest_entry.get("message_id")
        or oldest_entry.get("message-id")
        or raw.get("min")
        or ""
    )
    oldest_idle_ms = int(
        oldest_entry.get("idle_ms")
        or oldest_entry.get("idle")
        or raw.get("oldest_pending_idle_ms")
        or 0
    )
    consumers = [
        _normalize_redis_mapping(consumer)
        for consumer in raw.get("consumers", []) or []
        if isinstance(consumer, dict)
    ]
    pending_owner = str(
        oldest_entry.get("consumer")
        or oldest_entry.get("consumer_name")
        or (consumers[0].get("name") if consumers else "")
        or ""
    )
    max_delivery_count = int(
        oldest_entry.get("delivery_count")
        or oldest_entry.get("times_delivered")
        or raw.get("max_delivery_count")
        or 0
    )
    return {
        "pending_total": pending,
        "pending_oldest_id": oldest_id,
        "oldest_pending_idle_ms": oldest_idle_ms,
        "oldest_pending_age_seconds": int(oldest_idle_ms / 1000),
        "pending_owner": pending_owner,
        "max_delivery_count": max_delivery_count,
    }


def _normalize_redis_mapping(raw: dict[Any, Any]) -> dict[str, Any]:
    normalized = {}
    for key, value in raw.items():
        normalized_key = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        normalized[normalized_key] = value
    return normalized


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _append_queue_metric(
    lines: list[str], queue_type: str, name: str, queue: dict[str, Any]
) -> None:
    length = queue.get("length")
    if length is None:
        return
    lines.append(
        "by_framework_queue_depth"
        f'{{queue_type="{_escape_label(queue_type)}",'
        f'name="{_escape_label(name)}",'
        f'stream="{_escape_label(str(queue.get("stream", "")))}"}} {int(length)}'
    )
    lines.append(
        "by_framework_stream_depth"
        f'{{queue_type="{_escape_label(queue_type)}",'
        f'name="{_escape_label(name)}"}} {int(length)}'
    )


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _ms_to_seconds(value: Any) -> str:
    return f"{(float(value or 0) / 1000):.6g}"
