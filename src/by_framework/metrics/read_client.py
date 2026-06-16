"""Read SDK for correlating metrics history with trace time windows."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Optional

from by_framework.common.redis_client import Redis, get_redis
from by_framework.metrics.snapshot import (
    REDIS_HISTORY_KEY,
    SLOPolicy,
    build_history_point,
    build_observability_snapshot,
    load_history_from_redis,
)


@dataclass(frozen=True)
class MetricsDiagnostic:
    """A metrics read diagnostic."""

    code: str
    message: str
    severity: str = "info"

    def to_dict(self) -> dict[str, Any]:
        return {
            key: value for key, value in asdict(self).items() if value not in ("", None)
        }


@dataclass(frozen=True)
class MetricsWindow:
    """A time window used to correlate metrics with trace spans."""

    start_ts: int
    end_ts: int
    buffer_ms: int = 0

    def expanded(self) -> "MetricsWindow":
        """Return this window with buffer applied on both sides."""
        return MetricsWindow(
            start_ts=max(0, self.start_ts - self.buffer_ms),
            end_ts=max(self.start_ts, self.end_ts + self.buffer_ms),
            buffer_ms=self.buffer_ms,
        )

    def to_dict(self) -> dict[str, int]:
        return asdict(self)


@dataclass(frozen=True)
class MetricsReadResult:
    """Metrics samples and compact summary for a time window."""

    window: MetricsWindow
    samples: list[dict[str, int]] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)
    diagnostics: list[MetricsDiagnostic] = field(default_factory=list)
    status: str = "ok"

    def to_dict(self) -> dict[str, Any]:
        return {
            "window": self.window.to_dict(),
            "samples": self.samples,
            "summary": self.summary,
            "diagnostics": [diagnostic.to_dict() for diagnostic in self.diagnostics],
            "status": self.status,
        }


class MetricsReadClient:
    """Read metrics snapshots and history from by-framework Redis storage."""

    def __init__(self, redis_client: Optional[Redis] = None) -> None:
        self.redis = redis_client or get_redis()

    async def get_snapshot(self) -> dict[str, Any]:
        """Return the current observability snapshot."""
        return await build_observability_snapshot(self.redis)

    async def get_history(
        self,
        *,
        start_ts: int,
        end_ts: int,
        limit: int = 120,
    ) -> list[dict[str, int]]:
        """Return compact metrics history points that overlap a time window."""
        window = MetricsWindow(start_ts=start_ts, end_ts=end_ts).expanded()
        if window.end_ts <= 0:
            return []
        samples = await self._load_history_between(
            window.start_ts,
            window.end_ts,
            limit=max(1, limit),
        )
        return sorted(samples, key=lambda item: int(item.get("generated_at", 0) or 0))

    async def explain_window(
        self,
        *,
        start_ts: int,
        end_ts: int,
        buffer_ms: int = 5_000,
        limit: int = 120,
        slo_policy: SLOPolicy | None = None,
    ) -> MetricsReadResult:
        """Summarize metrics conditions around a trace/span time window."""
        window = MetricsWindow(
            start_ts=int(start_ts or 0),
            end_ts=max(int(start_ts or 0), int(end_ts or 0)),
            buffer_ms=max(0, int(buffer_ms or 0)),
        ).expanded()
        diagnostics: list[MetricsDiagnostic] = []
        samples = await self.get_history(
            start_ts=window.start_ts,
            end_ts=window.end_ts,
            limit=limit,
        )
        if not samples:
            diagnostics.append(
                MetricsDiagnostic(
                    code="metrics_history_missing",
                    message="No metrics history points were found for this window.",
                    severity="warning",
                )
            )
            try:
                current_point = build_history_point(await self.get_snapshot())
                if self._point_in_window(current_point, window):
                    samples = [current_point]
                    diagnostics.append(
                        MetricsDiagnostic(
                            code="metrics_current_snapshot_used",
                            message="Current metrics snapshot was used as fallback.",
                        )
                    )
            except Exception as err:  # pylint: disable=broad-exception-caught
                diagnostics.append(
                    MetricsDiagnostic(
                        code="metrics_snapshot_failed",
                        message=f"Current metrics snapshot fallback failed: {err}",
                        severity="warning",
                    )
                )
        summary = self._summarize(samples)
        if slo_policy is not None and samples:
            summary["slo_window"] = self._summarize_slo_window(samples, slo_policy)
        diagnostics.extend(self._diagnose_summary(summary))
        diagnostics.extend(self._diagnose_slo_window(summary.get("slo_window")))
        status = "partial" if diagnostics else "ok"
        return MetricsReadResult(
            window=window,
            samples=samples,
            summary=summary,
            diagnostics=diagnostics,
            status=status,
        )

    async def _load_history_between(
        self,
        start_ts: int,
        end_ts: int,
        *,
        limit: int,
    ) -> list[dict[str, int]]:
        zrangebyscore = getattr(self.redis, "zrangebyscore", None)
        if callable(zrangebyscore):
            try:
                raw_entries = await zrangebyscore(
                    REDIS_HISTORY_KEY,
                    start_ts,
                    end_ts,
                    start=0,
                    num=limit,
                )
                return [
                    point for point in map(self._decode_point, raw_entries) if point
                ]
            except TypeError:
                pass
        points = await load_history_from_redis(self.redis, limit=limit)
        return [
            point
            for point in points
            if start_ts <= int(point.get("generated_at", 0) or 0) <= end_ts
        ]

    @staticmethod
    def _decode_point(raw: Any) -> dict[str, int]:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                return {}
        return raw if isinstance(raw, dict) else {}

    @staticmethod
    def _point_in_window(point: dict[str, int], window: MetricsWindow) -> bool:
        generated_at = int(point.get("generated_at", 0) or 0)
        return window.start_ts <= generated_at <= window.end_ts

    @staticmethod
    def _summarize(samples: list[dict[str, int]]) -> dict[str, Any]:
        if not samples:
            return {"sample_count": 0}
        fields = (
            "workers_online",
            "active_executions",
            "queued_executions",
            "failed_executions",
            "queue_depth_total",
            "consumer_pending_total",
            "alert_count",
            "latency_p95_ms",
            "queue_latency_p95_ms",
            "total_latency_p95_ms",
            "success_ratio_ppm",
            "deadletter_count",
            "freshness_age_ms",
            "oldest_pending_age_seconds",
            "max_delivery_count",
        )
        summary: dict[str, Any] = {"sample_count": len(samples)}
        for field_name in fields:
            values = [int(sample.get(field_name, 0) or 0) for sample in samples]
            summary[field_name] = {
                "min": min(values),
                "max": max(values),
                "last": values[-1],
            }
        summary["signal_explain"] = MetricsReadClient._build_signal_explain(summary)
        return summary

    @staticmethod
    def _summary_max(summary: dict[str, Any], field_name: str) -> int:
        value = summary.get(field_name, {})
        return int(value.get("max", 0) or 0) if isinstance(value, dict) else 0

    @staticmethod
    def _summary_last(summary: dict[str, Any], field_name: str) -> int:
        value = summary.get(field_name, {})
        return int(value.get("last", 0) or 0) if isinstance(value, dict) else 0

    @staticmethod
    def _build_signal_explain(summary: dict[str, Any]) -> list[dict[str, Any]]:
        queue_depth = MetricsReadClient._summary_max(summary, "queue_depth_total")
        pending = MetricsReadClient._summary_max(summary, "consumer_pending_total")
        oldest_pending_age = MetricsReadClient._summary_max(
            summary, "oldest_pending_age_seconds"
        )
        max_delivery_count = MetricsReadClient._summary_max(
            summary, "max_delivery_count"
        )
        workers_online = MetricsReadClient._summary_last(summary, "workers_online")
        active_executions = MetricsReadClient._summary_max(summary, "active_executions")
        failed_executions = MetricsReadClient._summary_max(summary, "failed_executions")
        deadletter_count = MetricsReadClient._summary_max(summary, "deadletter_count")
        alert_count = MetricsReadClient._summary_max(summary, "alert_count")

        return [
            {
                "category": "queue",
                "severity": (
                    "warning"
                    if (
                        queue_depth > 0
                        or pending > 0
                        or oldest_pending_age > 0
                        or max_delivery_count > 1
                    )
                    else "ok"
                ),
                "message": (
                    "Queue backlog or pending delivery overlapped this trace window."
                    if (
                        queue_depth > 0
                        or pending > 0
                        or oldest_pending_age > 0
                        or max_delivery_count > 1
                    )
                    else "No queue backlog was observed in this trace window."
                ),
                "metrics": {
                    "queue_depth_total": queue_depth,
                    "consumer_pending_total": pending,
                    "oldest_pending_age_seconds": oldest_pending_age,
                    "max_delivery_count": max_delivery_count,
                },
            },
            {
                "category": "worker",
                "severity": (
                    "warning"
                    if workers_online <= 0 or active_executions > workers_online
                    else "ok"
                ),
                "message": (
                    "Worker capacity was absent or saturated during this trace window."
                    if workers_online <= 0 or active_executions > workers_online
                    else "Worker capacity was available during this trace window."
                ),
                "metrics": {
                    "workers_online": workers_online,
                    "active_executions": active_executions,
                },
            },
            {
                "category": "errors",
                "severity": (
                    "warning"
                    if failed_executions > 0 or deadletter_count > 0 or alert_count > 0
                    else "ok"
                ),
                "message": (
                    "Failures, deadletters, or alerts overlapped this trace window."
                    if failed_executions > 0 or deadletter_count > 0 or alert_count > 0
                    else "No failure signal was observed in this trace window."
                ),
                "metrics": {
                    "failed_executions": failed_executions,
                    "deadletter_count": deadletter_count,
                    "alert_count": alert_count,
                },
            },
        ]

    @staticmethod
    def _diagnose_summary(summary: dict[str, Any]) -> list[MetricsDiagnostic]:
        diagnostics: list[MetricsDiagnostic] = []
        if not summary or int(summary.get("sample_count", 0) or 0) <= 0:
            return diagnostics
        checks = (
            ("queue_depth_total", "metrics_queue_backlog", "Queue depth was non-zero."),
            (
                "consumer_pending_total",
                "metrics_consumer_pending",
                "Consumer pending messages were non-zero.",
            ),
            ("alert_count", "metrics_alerts_present", "System alerts were present."),
            (
                "failed_executions",
                "metrics_failures_present",
                "Failed executions were present.",
            ),
            (
                "deadletter_count",
                "metrics_deadletters_present",
                "Deadletter messages were present.",
            ),
            (
                "oldest_pending_age_seconds",
                "metrics_pending_age_high",
                "Pending messages had non-zero idle age.",
            ),
            (
                "max_delivery_count",
                "metrics_stream_redelivery_high",
                "Pending messages had repeated delivery attempts.",
            ),
            (
                "freshness_age_ms",
                "metrics_freshness_age_high",
                "Worker or metrics freshness age was non-zero.",
            ),
        )
        for field_name, code, message in checks:
            max_value = int(summary.get(field_name, {}).get("max", 0) or 0)
            if max_value > 0:
                diagnostics.append(
                    MetricsDiagnostic(
                        code=code,
                        message=message,
                        severity="warning",
                    )
                )
        success_ratio = summary.get("success_ratio_ppm", {})
        if success_ratio and int(success_ratio.get("min", 1_000_000) or 0) < 990_000:
            diagnostics.append(
                MetricsDiagnostic(
                    code="metrics_slo_success_ratio",
                    message="Execution success ratio was below 99%.",
                    severity="warning",
                )
            )
        return diagnostics

    @staticmethod
    def _summarize_slo_window(
        samples: list[dict[str, int]], slo_policy: SLOPolicy
    ) -> dict[str, Any]:
        first = samples[0]
        last = samples[-1]

        successful_executions = max(
            0,
            int(last.get("completed_executions", 0) or 0)
            - int(first.get("completed_executions", 0) or 0),
        )
        terminal_executions = max(
            0,
            int(last.get("terminal_executions", 0) or 0)
            - int(first.get("terminal_executions", 0) or 0),
        )
        if terminal_executions <= 0:
            failed_delta = max(
                0,
                int(last.get("failed_executions", 0) or 0)
                - int(first.get("failed_executions", 0) or 0),
            )
            cancelled_delta = max(
                0,
                int(last.get("cancelled_executions", 0) or 0)
                - int(first.get("cancelled_executions", 0) or 0),
            )
            terminal_executions = successful_executions + failed_delta + cancelled_delta

        success_ratio = (
            successful_executions / terminal_executions
            if terminal_executions > 0
            else 1.0
        )
        target = min(1.0, max(0.0, float(slo_policy.success_ratio_target)))
        error_budget = max(1.0 - target, 0.000001)
        burn_rate = max(0.0, (target - success_ratio) / error_budget)
        total_latency_p95_ms = max(
            int(sample.get("total_latency_p95_ms", 0) or 0) for sample in samples
        )
        deadletter_count = max(
            int(sample.get("deadletter_count", 0) or 0) for sample in samples
        )
        freshness_age_ms = max(
            int(sample.get("freshness_age_ms", 0) or 0) for sample in samples
        )

        return {
            "window": slo_policy.window,
            "successful_executions": successful_executions,
            "terminal_executions": terminal_executions,
            "success_ratio_ppm": int(success_ratio * 1_000_000),
            "success_ratio_target_ppm": int(target * 1_000_000),
            "success_ratio_objective_met": success_ratio >= target,
            "burn_rate": round(burn_rate, 3),
            "total_latency_p95_ms": total_latency_p95_ms,
            "latency_objective_met": (
                total_latency_p95_ms <= int(slo_policy.total_latency_p95_ms)
            ),
            "deadletter_count": deadletter_count,
            "deadletter_objective_met": (
                deadletter_count <= int(slo_policy.deadletter_threshold)
            ),
            "freshness_age_ms": freshness_age_ms,
            "freshness_objective_met": (
                freshness_age_ms <= int(slo_policy.freshness_max_age_ms)
            ),
        }

    @staticmethod
    def _diagnose_slo_window(
        slo_window: dict[str, Any] | None,
    ) -> list[MetricsDiagnostic]:
        diagnostics: list[MetricsDiagnostic] = []
        if not slo_window:
            return diagnostics
        checks = (
            (
                "success_ratio_objective_met",
                "metrics_slo_window_success_ratio",
                "Window execution success ratio missed the configured SLO.",
            ),
            (
                "latency_objective_met",
                "metrics_slo_window_latency",
                "Window total latency P95 missed the configured SLO.",
            ),
            (
                "deadletter_objective_met",
                "metrics_slo_window_deadletter",
                "Window deadletter count missed the configured SLO.",
            ),
            (
                "freshness_objective_met",
                "metrics_slo_window_freshness",
                "Window metrics or heartbeat freshness missed the configured SLO.",
            ),
        )
        for key, code, message in checks:
            if slo_window.get(key) is False:
                diagnostics.append(
                    MetricsDiagnostic(
                        code=code,
                        message=message,
                        severity="warning",
                    )
                )
        return diagnostics
