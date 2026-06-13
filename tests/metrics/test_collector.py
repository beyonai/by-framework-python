"""Tests for MetricsCollector background task with distributed lock."""

from __future__ import annotations

import asyncio

import pytest

from by_framework.metrics.collector import COLLECTOR_LOCK_KEY, MetricsCollector
from by_framework.metrics.snapshot import REDIS_HISTORY_KEY


class FakeRedis:
    """Minimal in-memory Redis fake for collector tests."""

    def __init__(self):
        self.store: dict = {}
        self.zsets: dict[str, dict] = {}
        self.set_calls: list = []

    async def set(self, key, value, *, nx=False, ex=None):
        self.set_calls.append((key, value, nx, ex))
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    async def get(self, key):
        return self.store.get(key)

    async def expire(self, key, seconds):
        pass

    async def delete(self, key):
        self.store.pop(key, None)

    async def zadd(self, name, mapping):
        self.zsets.setdefault(name, {}).update(mapping)

    async def zremrangebyscore(self, name, min_score, max_score):
        pass

    async def zrange(self, name, start, end):
        zset = self.zsets.get(name, {})
        items = sorted(zset.items(), key=lambda x: x[1])
        if end < 0:
            end = len(items) + end + 1
        return [k.encode() if isinstance(k, str) else k for k, _ in items[start:end]]

    # snapshot helpers need these
    async def smembers(self, key):
        return set()

    async def hgetall(self, key):
        return {}

    async def xpending_range(self, *args, **kwargs):
        return []

    async def xlen(self, key):
        return 0

    async def pipeline(self):
        return _FakePipeline(self)


class _FakePipeline:

    def __init__(self, redis):
        self._redis = redis
        self._cmds = []

    def __getattr__(self, name):
        def _cmd(*args, **kwargs):
            self._cmds.append((name, args, kwargs))
            return self

        return _cmd

    async def execute(self):
        return []


@pytest.mark.asyncio
async def test_collector_acquires_lock_and_writes_history():
    """MetricsCollector writes a history point when it acquires the lock."""
    redis = FakeRedis()
    collector = MetricsCollector(
        redis, worker_id="w1", interval_seconds=1, enabled=True
    )
    await collector._collect_once()

    assert COLLECTOR_LOCK_KEY in redis.store
    assert redis.store[COLLECTOR_LOCK_KEY] == "w1"
    assert REDIS_HISTORY_KEY in redis.zsets
    assert len(redis.zsets[REDIS_HISTORY_KEY]) >= 1


@pytest.mark.asyncio
async def test_collector_lock_exclusion():
    """Second collector does not overwrite history when first holds the lock."""
    redis = FakeRedis()
    # Manually pre-fill lock as owned by another worker
    redis.store[COLLECTOR_LOCK_KEY] = "w-other"
    zsets_before = dict(redis.zsets)

    collector = MetricsCollector(
        redis, worker_id="w2", interval_seconds=1, enabled=True
    )
    await collector._collect_once()

    # History key should NOT have grown because w2 did not hold the lock
    assert redis.zsets.get(REDIS_HISTORY_KEY) == zsets_before.get(REDIS_HISTORY_KEY)


@pytest.mark.asyncio
async def test_collector_renews_own_lock():
    """Collector renews the TTL when it already holds the lock."""
    redis = FakeRedis()
    redis.store[COLLECTOR_LOCK_KEY] = "w1"
    expire_calls = []
    original_expire = redis.expire

    async def tracking_expire(key, seconds):
        expire_calls.append((key, seconds))
        await original_expire(key, seconds)

    redis.expire = tracking_expire

    collector = MetricsCollector(
        redis, worker_id="w1", interval_seconds=1, enabled=True
    )
    await collector._collect_once()

    assert any(k == COLLECTOR_LOCK_KEY for k, _ in expire_calls)


@pytest.mark.asyncio
async def test_collector_releases_lock_on_stop():
    """Lock is released when the collector task is cancelled."""
    redis = FakeRedis()
    collector = MetricsCollector(
        redis, worker_id="w1", interval_seconds=60, enabled=True
    )

    task = asyncio.create_task(collector.run())
    await asyncio.sleep(0.05)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert COLLECTOR_LOCK_KEY not in redis.store


@pytest.mark.asyncio
async def test_collector_disabled_does_not_write():
    """When disabled the collector writes nothing."""
    redis = FakeRedis()
    collector = MetricsCollector(
        redis, worker_id="w1", interval_seconds=1, enabled=False
    )
    await collector._collect_once()

    assert REDIS_HISTORY_KEY not in redis.zsets
    assert COLLECTOR_LOCK_KEY not in redis.store


def test_collector_snapshot_returns_config():
    """snapshot() reports current configuration."""
    collector = MetricsCollector(None, worker_id="w-snap", interval_seconds=10)
    info = collector.snapshot()
    assert info["worker_id"] == "w-snap"
    assert info["interval_seconds"] == 10
    assert "lock_key" in info
