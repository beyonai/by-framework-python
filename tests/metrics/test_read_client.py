"""Tests for metrics history read SDK."""

import asyncio
import json

import pytest

from by_framework.metrics import MetricsReadClient
from by_framework.metrics.snapshot import REDIS_HISTORY_KEY, SLOPolicy


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


def test_metrics_read_client_summarizes_slo_and_stream_age_fields():
    """Window explain includes SLO and Redis Streams saturation fields."""

    async def run_case():
        redis = MetricsRedis()
        await redis.zadd(
            REDIS_HISTORY_KEY,
            {
                json.dumps(
                    {
                        "generated_at": 100,
                        "success_ratio_ppm": 920000,
                        "deadletter_count": 2,
                        "freshness_age_ms": 18000,
                        "oldest_pending_age_seconds": 45,
                    }
                ): 100
            },
        )
        return await MetricsReadClient(redis).explain_window(
            start_ts=90,
            end_ts=110,
            buffer_ms=0,
        )

    result = asyncio.run(run_case())

    assert result.summary["success_ratio_ppm"]["last"] == 920000
    assert result.summary["deadletter_count"]["max"] == 2
    assert result.summary["freshness_age_ms"]["max"] == 18000
    assert result.summary["oldest_pending_age_seconds"]["max"] == 45
    assert {diagnostic.code for diagnostic in result.diagnostics} >= {
        "metrics_slo_success_ratio",
        "metrics_deadletters_present",
        "metrics_pending_age_high",
    }


def test_metrics_read_client_computes_slo_window_from_history_deltas():
    """Window explain computes SLOs from samples in the requested time window."""

    async def run_case():
        redis = MetricsRedis()
        await redis.zadd(
            REDIS_HISTORY_KEY,
            {
                json.dumps(
                    {
                        "generated_at": 100,
                        "completed_executions": 90,
                        "failed_executions": 5,
                        "cancelled_executions": 0,
                        "terminal_executions": 95,
                        "total_latency_p95_ms": 800,
                        "deadletter_count": 0,
                        "freshness_age_ms": 500,
                    }
                ): 100,
                json.dumps(
                    {
                        "generated_at": 200,
                        "completed_executions": 95,
                        "failed_executions": 10,
                        "cancelled_executions": 0,
                        "terminal_executions": 105,
                        "total_latency_p95_ms": 2400,
                        "deadletter_count": 2,
                        "freshness_age_ms": 2200,
                    }
                ): 200,
            },
        )
        return await MetricsReadClient(redis).explain_window(
            start_ts=100,
            end_ts=200,
            buffer_ms=0,
            slo_policy=SLOPolicy(
                success_ratio_target=0.9,
                total_latency_p95_ms=1000,
                deadletter_threshold=0,
                freshness_max_age_ms=1000,
                window="100ms",
            ),
        )

    result = asyncio.run(run_case())

    assert result.summary["slo_window"]["window"] == "100ms"
    assert result.summary["slo_window"]["terminal_executions"] == 10
    assert result.summary["slo_window"]["successful_executions"] == 5
    assert result.summary["slo_window"]["success_ratio_ppm"] == 500000
    assert result.summary["slo_window"]["burn_rate"] > 1
    assert result.summary["slo_window"]["latency_objective_met"] is False
    assert result.summary["slo_window"]["deadletter_objective_met"] is False
    assert result.summary["slo_window"]["freshness_objective_met"] is False
    assert {diagnostic.code for diagnostic in result.diagnostics} >= {
        "metrics_slo_window_success_ratio",
        "metrics_slo_window_latency",
        "metrics_slo_window_deadletter",
        "metrics_slo_window_freshness",
    }


def test_metrics_read_client_explains_trace_window_by_signal_category():
    """Trace-window summaries explain queue, worker, and error conditions."""

    async def run_case():
        redis = MetricsRedis()
        await redis.zadd(
            REDIS_HISTORY_KEY,
            {
                json.dumps(
                    {
                        "generated_at": 100,
                        "workers_online": 0,
                        "active_executions": 3,
                        "queue_depth_total": 12,
                        "consumer_pending_total": 4,
                        "oldest_pending_age_seconds": 30,
                        "max_delivery_count": 5,
                        "failed_executions": 2,
                        "deadletter_count": 1,
                        "alert_count": 2,
                    }
                ): 100
            },
        )
        return await MetricsReadClient(redis).explain_window(
            start_ts=90,
            end_ts=110,
            buffer_ms=0,
        )

    result = asyncio.run(run_case())

    signals = result.summary["signal_explain"]
    assert {signal["category"] for signal in signals} == {"queue", "worker", "errors"}
    queue = next(signal for signal in signals if signal["category"] == "queue")
    worker = next(signal for signal in signals if signal["category"] == "worker")
    errors = next(signal for signal in signals if signal["category"] == "errors")
    assert queue["severity"] == "warning"
    assert queue["metrics"]["queue_depth_total"] == 12
    assert queue["metrics"]["oldest_pending_age_seconds"] == 30
    assert queue["metrics"]["max_delivery_count"] == 5
    assert worker["metrics"]["workers_online"] == 0
    assert errors["metrics"]["deadletter_count"] == 1


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
