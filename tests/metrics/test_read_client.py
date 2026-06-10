"""Tests for metrics history read SDK."""

import json

import pytest

from by_framework.metrics import MetricsReadClient
from by_framework.metrics.snapshot import REDIS_HISTORY_KEY


class MetricsRedis:
    """Small Redis fake for metrics history reads."""

    def __init__(self):
        self.data = {}

    async def zadd(self, name, mapping):
        self.data.setdefault(name, {}).update(mapping)

    async def zrangebyscore(self, name, min, max, start=None, num=None):  # pylint: disable=redefined-builtin
        values = [
            item
            for item, score in self.data.get(name, {}).items()
            if int(min) <= int(score) <= int(max)
        ]
        values.sort(key=lambda raw: json.loads(raw)["generated_at"])
        if start is not None and num is not None:
            return values[start : start + num]
        return values


@pytest.mark.asyncio
async def test_metrics_read_client_reads_and_summarizes_time_window():
    """MetricsReadClient returns history samples that overlap a trace window."""
    redis = MetricsRedis()
    await redis.zadd(
        REDIS_HISTORY_KEY,
        {
            json.dumps(
                {
                    "generated_at": 100,
                    "workers_online": 1,
                    "active_executions": 0,
                    "queued_executions": 0,
                    "failed_executions": 0,
                    "queue_depth_total": 0,
                    "consumer_pending_total": 0,
                    "alert_count": 0,
                    "latency_p95_ms": 20,
                    "queue_latency_p95_ms": 0,
                    "total_latency_p95_ms": 20,
                }
            ): 100,
            json.dumps(
                {
                    "generated_at": 160,
                    "workers_online": 1,
                    "active_executions": 2,
                    "queued_executions": 1,
                    "failed_executions": 1,
                    "queue_depth_total": 9,
                    "consumer_pending_total": 3,
                    "alert_count": 1,
                    "latency_p95_ms": 80,
                    "queue_latency_p95_ms": 15,
                    "total_latency_p95_ms": 95,
                }
            ): 160,
        },
    )

    result = await MetricsReadClient(redis).explain_window(
        start_ts=120,
        end_ts=150,
        buffer_ms=20,
    )

    assert result.status == "partial"
    assert [sample["generated_at"] for sample in result.samples] == [100, 160]
    assert result.summary["queue_depth_total"]["max"] == 9
    assert {diagnostic.code for diagnostic in result.diagnostics} >= {
        "metrics_queue_backlog",
        "metrics_consumer_pending",
    }


@pytest.mark.asyncio
async def test_metrics_read_client_reports_missing_history():
    """Missing history returns a partial result instead of failing."""
    result = await MetricsReadClient(MetricsRedis()).explain_window(
        start_ts=120,
        end_ts=150,
        buffer_ms=0,
    )

    assert result.status == "partial"
    assert result.summary == {"sample_count": 0}
    assert [diagnostic.code for diagnostic in result.diagnostics] == [
        "metrics_history_missing",
        "metrics_snapshot_failed",
    ]
