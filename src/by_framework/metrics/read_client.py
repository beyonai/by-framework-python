"""Read SDK for correlating metrics history with trace time windows."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Optional

from by_framework.common.redis_client import Redis, get_redis
from by_framework.metrics.snapshot import (
    REDIS_HISTORY_KEY,
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
        diagnostics.extend(self._diagnose_summary(summary))
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
        )
        summary: dict[str, Any] = {"sample_count": len(samples)}
        for field_name in fields:
            values = [int(sample.get(field_name, 0) or 0) for sample in samples]
            summary[field_name] = {
                "min": min(values),
                "max": max(values),
                "last": values[-1],
            }
        return summary

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
        return diagnostics
