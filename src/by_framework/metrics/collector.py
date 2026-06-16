"""Background metrics collector with distributed lock.

Only one worker process at a time will write history points.  The lock is
a simple Redis SET NX with an expiry that is renewed on every successful
collection cycle, so the lock outlives any individual iteration failure.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Optional

from by_framework.common.logger import logger, observability_log_extra
from by_framework.common.redis_client import Redis, get_redis
from by_framework.metrics.snapshot import (
    build_history_point,
    build_observability_snapshot,
    save_history_point_to_redis,
)

COLLECTOR_LOCK_KEY = "by_framework:obs:collector_lock"
# Lock TTL must exceed the collection interval so it is still held between cycles.
_LOCK_TTL_MULTIPLIER = 3

try:
    from prometheus_client import Counter, Gauge, Histogram  # type: ignore

    _collector_cycles_total = Counter(
        "by_framework_metrics_collector_cycles_total",
        "Metrics collector cycles by result.",
        ["result"],
    )
    _collector_snapshot_duration_ms = Histogram(
        "by_framework_metrics_collector_snapshot_duration_ms",
        "Metrics collector snapshot and history write duration in milliseconds.",
        buckets=(10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000),
    )
    _collector_lock_held = Gauge(
        "by_framework_metrics_collector_lock_held",
        "Whether this process currently holds the metrics collector lock.",
    )
    _collector_last_success_timestamp_ms = Gauge(
        "by_framework_metrics_collector_last_success_timestamp_ms",
        "Unix timestamp in milliseconds for the last successful metrics collection.",
    )
except ImportError:

    class _NoopMetric:
        """No-op metric used when prometheus-client is not installed."""

        def labels(self, *args: Any, **kwargs: Any) -> "_NoopMetric":
            del args, kwargs
            return self

        def inc(self, amount: float = 1.0) -> None:
            del amount

        def observe(self, amount: float) -> None:
            del amount

        def set(self, value: float) -> None:
            del value

    _collector_cycles_total = _NoopMetric()
    _collector_snapshot_duration_ms = _NoopMetric()
    _collector_lock_held = _NoopMetric()
    _collector_last_success_timestamp_ms = _NoopMetric()


def _env_int(name: str, default: int) -> int:
    try:
        return int((os.environ.get(name) or "").strip() or default)
    except ValueError:
        return default


def _env_bool(name: str, *, default: bool) -> bool:
    val = (os.environ.get(name) or "").strip().lower()
    if val in {"1", "true", "yes", "on", "enabled"}:
        return True
    if val in {"0", "false", "no", "off", "disabled"}:
        return False
    return default


class MetricsCollector:
    """Periodically snapshots metrics and saves history points to Redis.

    Uses a Redis distributed lock so only one worker process writes history
    at a time — avoids duplicate entries when many workers run in parallel.

    The lock is automatically acquired and renewed each cycle.  If this
    worker loses the lock (e.g. Redis timeout, restart), it backs off and
    retries on the next cycle without raising an error.

    Configuration via environment variables:
        BY_FRAMEWORK_METRICS_HISTORY_ENABLED  — default: true
        BY_FRAMEWORK_METRICS_HISTORY_INTERVAL_SECONDS — default: 5
    """

    def __init__(
        self,
        redis_client: Optional[Redis] = None,
        *,
        worker_id: str = "",
        interval_seconds: Optional[int] = None,
        enabled: Optional[bool] = None,
    ) -> None:
        self.redis = redis_client or get_redis()
        self.worker_id = worker_id or f"collector-{id(self)}"
        self.interval_seconds = (
            interval_seconds
            if interval_seconds is not None
            else (_env_int("BY_FRAMEWORK_METRICS_HISTORY_INTERVAL_SECONDS", 5))
        )
        self.enabled = (
            enabled
            if enabled is not None
            else (
                _env_bool("BY_FRAMEWORK_METRICS_HISTORY_ENABLED", default=True)
                and _env_bool("BY_FRAMEWORK_OBSERVABILITY_ENABLED", default=True)
            )
        )
        self._lock_ttl_seconds = max(self.interval_seconds * _LOCK_TTL_MULTIPLIER, 15)

    async def run(self) -> None:
        """Run the collection loop until cancelled."""
        if not self.enabled:
            logger.debug("MetricsCollector disabled, not starting.")
            return
        logger.debug(
            "MetricsCollector started (worker_id=%s, interval=%ds)",
            self.worker_id,
            self.interval_seconds,
        )
        try:
            while True:
                await self._collect_once()
                await asyncio.sleep(self.interval_seconds)
        except asyncio.CancelledError:
            pass
        finally:
            await self._release_lock()
            logger.debug("MetricsCollector stopped (worker_id=%s)", self.worker_id)

    async def _collect_once(self) -> None:
        """Attempt to acquire the lock and write one history point."""
        if not self.enabled:
            _collector_cycles_total.labels(result="disabled").inc()
            return
        if not await self._acquire_or_renew_lock():
            _collector_lock_held.set(0)
            _collector_cycles_total.labels(result="lock_skipped").inc()
            return
        _collector_lock_held.set(1)
        started_at = time.perf_counter()
        try:
            snapshot = await build_observability_snapshot(self.redis)
            point = build_history_point(snapshot)
            await save_history_point_to_redis(self.redis, point)
            _collector_snapshot_duration_ms.observe(
                max(0.0, (time.perf_counter() - started_at) * 1000)
            )
            _collector_last_success_timestamp_ms.set(
                int(point.get("generated_at", 0) or int(time.time() * 1000))
            )
            _collector_cycles_total.labels(result="success").inc()
        except Exception as err:  # pylint: disable=broad-exception-caught
            _collector_cycles_total.labels(result="snapshot_failed").inc()
            logger.debug(
                "MetricsCollector snapshot failed: %s",
                err,
                **observability_log_extra(worker_id=self.worker_id),
            )

    async def _acquire_or_renew_lock(self) -> bool:
        """Return True if this worker now holds the collector lock.

        Tries to SET NX first; if the key already exists, checks whether *this*
        worker owns it and, if so, refreshes the TTL.
        """
        try:
            acquired = await self.redis.set(
                COLLECTOR_LOCK_KEY,
                self.worker_id,
                nx=True,
                ex=self._lock_ttl_seconds,
            )
            if acquired:
                return True
            # Already exists — check ownership and refresh
            current = await self.redis.get(COLLECTOR_LOCK_KEY)
            if isinstance(current, bytes):
                current = current.decode()
            if current == self.worker_id:
                await self.redis.expire(COLLECTOR_LOCK_KEY, self._lock_ttl_seconds)
                return True
            return False
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.debug(
                "MetricsCollector lock acquire failed: %s",
                err,
                **observability_log_extra(worker_id=self.worker_id),
            )
            return False

    async def _release_lock(self) -> None:
        """Release the lock only if this worker still owns it."""
        try:
            current = await self.redis.get(COLLECTOR_LOCK_KEY)
            if isinstance(current, bytes):
                current = current.decode()
            if current == self.worker_id:
                await self.redis.delete(COLLECTOR_LOCK_KEY)
                _collector_lock_held.set(0)
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.debug("MetricsCollector lock release failed: %s", err)

    def snapshot(self) -> dict[str, Any]:
        """Synchronous diagnostic snapshot for health checks."""
        return {
            "worker_id": self.worker_id,
            "enabled": self.enabled,
            "interval_seconds": self.interval_seconds,
            "lock_key": COLLECTOR_LOCK_KEY,
        }
