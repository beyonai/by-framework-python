"""Tests for observability dashboard snapshots."""

import asyncio
import fnmatch
from unittest.mock import patch

import pytest

from by_framework import RedisKeys, WorkerRegistry
from by_framework.core.protocol.data_message import DataMessage
from by_framework.metrics.snapshot import (
    AlertPolicy,
    SLOPolicy,
    build_demo_observability_history,
    build_demo_observability_snapshot,
    build_demo_session_observability_snapshot,
    build_demo_trace_observability_snapshot,
    build_execution_observability_snapshot,
    build_history_point,
    build_observability_snapshot,
    build_prometheus_metrics,
    build_queue_observability_snapshot,
    build_session_observability_snapshot,
    build_trace_observability_snapshot,
    build_worker_observability_snapshot,
)
from by_framework.trace.span_recorder import SpanRecorder, TraceSpan


class MockPipeline:
    """Mock Redis pipeline for snapshot tests."""

    def __init__(self, redis):
        self.redis = redis
        self.commands = []

    def hset(self, name, key, value):
        self.commands.append(("hset", name, key, value))
        return self

    def hdel(self, name, *keys):
        self.commands.append(("hdel", name, keys))
        return self

    def zadd(self, name, mapping):
        self.commands.append(("zadd", name, mapping))
        return self

    def zrem(self, name, *values):
        self.commands.append(("zrem", name, values))
        return self

    def rpush(self, name, value):
        self.commands.append(("rpush", name, value))
        return self

    def hincrby(self, name, key, amount=1):
        self.commands.append(("hincrby", name, key, amount))
        return self

    def expire(self, name, ttl):
        self.commands.append(("expire", name, ttl))
        return self

    def hgetall(self, name):
        self.commands.append(("hgetall", name))
        return self

    def sadd(self, name, value):
        self.commands.append(("sadd", name, value))
        return self

    def srem(self, name, value):
        self.commands.append(("srem", name, value))
        return self

    def delete(self, name):
        self.commands.append(("delete", name))
        return self

    async def execute(self):
        results = []
        for command in self.commands:
            if command[0] == "hset":
                await self.redis.hset(command[1], {command[2]: command[3]})
                results.append(None)
            elif command[0] == "hdel":
                await self.redis.hdel(command[1], *command[2])
                results.append(None)
            elif command[0] == "zadd":
                await self.redis.zadd(command[1], command[2])
                results.append(None)
            elif command[0] == "zrem":
                await self.redis.zrem(command[1], *command[2])
                results.append(None)
            elif command[0] == "rpush":
                await self.redis.rpush(command[1], command[2])
                results.append(None)
            elif command[0] == "hincrby":
                await self.redis.hincrby(command[1], command[2], command[3])
                results.append(None)
            elif command[0] == "expire":
                await self.redis.expire(command[1], command[2])
                results.append(None)
            elif command[0] == "hgetall":
                result = await self.redis.hgetall(command[1])
                results.append(result)
            elif command[0] == "sadd":
                await self.redis.sadd(command[1], command[2])
                results.append(None)
            elif command[0] == "srem":
                await self.redis.srem(command[1], command[2])
                results.append(None)
            elif command[0] == "delete":
                await self.redis.delete(command[1])
                results.append(None)
            else:
                results.append(None)
        return results


class StreamAwareRedis:
    """Mock Redis client with minimal stream length support."""

    def __init__(self):
        self.data = {}
        self.kv = {}
        self.expires = {}
        self.stream_lengths = {}
        self.stream_entries = {}
        self.stream_groups = {}
        self.stream_consumers = {}
        self.pending_summaries = {}
        self.xinfo_consumers_calls = []
        self.xpending_calls = []

    async def zadd(self, name, mapping):
        self.data.setdefault(name, {}).update(mapping)

    async def zrem(self, name, *values):
        bucket = self.data.get(name, {})
        for value in values:
            bucket.pop(value, None)

    async def zrevrange(self, name, start, end):
        items = sorted(self.data.get(name, {}).items(), key=lambda item: item[1])
        items.reverse()
        return [item[0] for item in items[start : end + 1]]

    async def rpush(self, name, value):
        self.data.setdefault(name, []).append(value)

    async def lrange(self, name, start, end):
        values = self.data.get(name, [])
        if end == -1:
            end = len(values) - 1
        return values[start : end + 1]

    async def sadd(self, name, value):
        self.data.setdefault(name, set()).add(value)

    async def srem(self, name, value):
        self.data.get(name, set()).discard(value)

    async def smembers(self, name):
        return self.data.get(name, set())

    async def sismember(self, name, value):
        return value in self.data.get(name, set())

    async def set(self, name, value, nx=False, ex=None):
        if nx and name in self.kv:
            return False
        self.kv[name] = value
        if ex is not None:
            self.expires[name] = ex
        return True

    async def get(self, name):
        return self.kv.get(name)

    async def delete(self, name):
        self.kv.pop(name, None)
        self.data.pop(name, None)

    async def hset(self, name, mapping=None, key=None, value=None):
        self.data.setdefault(name, {})
        if mapping:
            self.data[name].update(mapping)
        else:
            self.data[name][key] = value

    async def hget(self, name, key):
        return self.data.get(name, {}).get(key)

    async def hgetall(self, name):
        return self.data.get(name, {})

    async def hmget(self, name, keys):
        bucket = self.data.get(name, {})
        return [bucket.get(key) for key in keys]

    async def hincrby(self, name, key, amount=1):
        self.data.setdefault(name, {})
        self.data[name][key] = int(self.data[name].get(key, 0)) + amount

    async def hdel(self, name, *keys):
        bucket = self.data.get(name, {})
        for key in keys:
            bucket.pop(key, None)

    async def expire(self, name, ttl):
        self.expires[name] = ttl
        return 1

    async def xlen(self, name):
        return self.stream_lengths.get(name, 0)

    async def xrevrange(self, name, max="+", min="-", count=None):  # pylint: disable=redefined-builtin
        del max, min
        entries = list(reversed(self.stream_entries.get(name, [])))
        if count is not None:
            return entries[:count]
        return entries

    async def xinfo_groups(self, name):
        return self.stream_groups.get(name, [])

    async def xinfo_consumers(self, name, groupname):
        self.xinfo_consumers_calls.append((name, groupname))
        return self.stream_consumers.get((name, groupname), [])

    async def xpending(self, name, groupname):
        self.xpending_calls.append((name, groupname))
        return self.pending_summaries.get((name, groupname), {})

    async def eval(self, script, numkeys, *keys_and_args):
        """Simulate Lua registry scripts in Python for unit tests.

        Dispatches by argv count:
          3 args → _HEARTBEAT_CAS_SCRIPT  (token, new_value, ttl)
          2 args → _REFRESH_LOCK_SCRIPT   (token, ttl)
          1 arg  → _RELEASE_LOCK_SCRIPT   (token_or_empty)
        """
        import json as _json

        keys = list(keys_and_args[:numkeys])
        args = list(keys_and_args[numkeys:])
        lease_key = keys[0]

        def _parse_token(raw):
            if raw is None:
                return None
            try:
                data = _json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
                if not isinstance(data, dict):
                    return None if data == 1 else str(data)
                return data.get("token")
            except Exception:  # pylint: disable=broad-exception-caught
                return "__unparseable__"

        if len(args) == 3:
            # _HEARTBEAT_CAS_SCRIPT
            token_arg, new_value, ttl = str(args[0]), args[1], int(args[2])
            raw = self.kv.get(lease_key)
            if token_arg != "":
                if raw is None:
                    self.kv[lease_key] = new_value
                    self.expires[lease_key] = ttl
                    return 1
                stored = _parse_token(raw)
                if stored == "__unparseable__":
                    return -1
                if stored is None or str(stored) != token_arg:
                    return 0
                self.kv[lease_key] = new_value
                self.expires[lease_key] = ttl
                return 1
            if raw is not None:
                stored = _parse_token(raw)
                if stored == "__unparseable__" or stored is not None:
                    return 0
            self.kv[lease_key] = new_value
            self.expires[lease_key] = ttl
            return 1

        if len(args) == 2:
            # _REFRESH_LOCK_SCRIPT
            token, ttl = str(args[0]), int(args[1])
            raw = self.kv.get(lease_key)
            stored = _parse_token(raw)
            if stored is None or stored == "__unparseable__" or str(stored) != token:
                return 0
            self.expires[lease_key] = ttl
            return 1

        # len(args) == 1: _RELEASE_LOCK_SCRIPT
        token_arg = str(args[0])
        raw = self.kv.get(lease_key)
        if raw is None:
            return 1 if token_arg == "" else 0
        if token_arg == "":
            self.kv.pop(lease_key, None)
            return 1
        stored = _parse_token(raw)
        if stored == "__unparseable__" or stored is None or str(stored) != token_arg:
            return 0
        self.kv.pop(lease_key, None)
        return 1

    def pipeline(self):
        return MockPipeline(self)


@pytest.mark.asyncio
async def test_build_observability_snapshot_aggregates_workers_and_queues():
    """Snapshot summarizes worker state, status counts, and queue depths."""
    redis = StreamAwareRedis()
    registry = WorkerRegistry(redis)

    await registry.register_worker_membership("worker-1", ["planner", "writer"])
    await registry.heartbeat_worker("worker-1")
    await registry.save_execution(
        {
            "execution_id": "exec-completed",
            "message_id": "msg-completed",
            "session_id": "sess-1",
            "worker_id": "worker-1",
            "target_agent_type": "planner",
            "status": "RUNNING",
            "created_at": 100,
            "started_at": 200,
        }
    )
    await registry.mark_execution_finished("exec-completed", "sess-1", "COMPLETED")
    await registry.save_execution(
        {
            "execution_id": "exec-running",
            "message_id": "msg-running",
            "session_id": "sess-2",
            "worker_id": "worker-1",
            "target_agent_type": "writer",
            "status": "RUNNING",
        }
    )
    redis.stream_lengths[RedisKeys.ctrl_stream("planner")] = 3
    redis.stream_lengths[RedisKeys.ctrl_stream("writer")] = 1
    redis.stream_lengths[RedisKeys.control_plane_delivery_pending_stream()] = 2
    redis.stream_groups[RedisKeys.ctrl_stream("planner")] = [
        {
            "name": "agent_engines",
            "pending": 2,
            "lag": 5,
            "last-delivered-id": "1-0",
        }
    ]
    redis.stream_consumers[(RedisKeys.ctrl_stream("planner"), "agent_engines")] = [
        {"name": "worker-1", "pending": 2, "idle": 1200}
    ]

    snapshot = await build_observability_snapshot(redis)

    assert snapshot["totals"]["workers_online"] == 1
    assert snapshot["totals"]["active_executions"] == 1
    assert snapshot["totals"]["tracked_executions"] == 2
    assert snapshot["status_counts"] == {"COMPLETED": 1, "RUNNING": 1}
    assert [worker["worker_id"] for worker in snapshot["workers"]] == ["worker-1"]
    assert snapshot["workers"][0]["agent_types"] == ["planner", "writer"]
    assert [item["execution_id"] for item in snapshot["recent_executions"]] == [
        "exec-running",
        "exec-completed",
    ]
    assert snapshot["queues"]["agent_type_streams"] == [
        {
            "agent_type": "planner",
            "stream": RedisKeys.ctrl_stream("planner"),
            "length": 3,
            "consumer_groups": [
                {
                    "name": "agent_engines",
                    "pending": 2,
                    "lag": 5,
                    "last_delivered_id": "1-0",
                    "consumers": [],
                }
            ],
        },
        {
            "agent_type": "writer",
            "stream": RedisKeys.ctrl_stream("writer"),
            "length": 1,
            "consumer_groups": [],
        },
    ]
    assert snapshot["queues"]["control_plane"]["delivery_pending"]["length"] == 2
    assert redis.xinfo_consumers_calls == []
    assert {
        "code": "CONSUMER_PENDING",
        "severity": "warning",
        "message": "2 messages pending in consumer groups.",
        "value": 2,
        "threshold": 0,
    } in snapshot["alerts"]
    assert snapshot["latency"]["queue"]["completed_count"] >= 1
    assert snapshot["latency"]["run"]["completed_count"] >= 1
    assert snapshot["latency"]["total"]["completed_count"] >= 1
    assert snapshot["agent_health"] == [
        {
            "agent_type": "planner",
            "worker_count": 1,
            "queue_depth": 3,
            "recent_executions": 1,
            "recent_active_executions": 0,
            "recent_failed_executions": 0,
            "recent_status_counts": {"COMPLETED": 1},
        },
        {
            "agent_type": "writer",
            "worker_count": 1,
            "queue_depth": 1,
            "recent_executions": 1,
            "recent_active_executions": 1,
            "recent_failed_executions": 0,
            "recent_status_counts": {"RUNNING": 1},
        },
    ]
    assert snapshot["data_flow"]["summary"] == {
        "queue_depth_total": 6,
        "consumer_pending_total": 2,
        "workers_online": 1,
        "active_executions": 1,
        "failed_executions": 0,
        "queue_latency_p95_ms": snapshot["latency"]["queue"]["p95_ms"],
        "run_latency_p95_ms": snapshot["latency"]["run"]["p95_ms"],
        "total_latency_p95_ms": snapshot["latency"]["total"]["p95_ms"],
    }
    assert [node["id"] for node in snapshot["data_flow"]["nodes"]] == [
        "client",
        "control_queues",
        "workers",
        "data_stream",
        "websocket_backend",
        "control_plane",
    ]
    assert snapshot["data_flow"]["nodes"][1]["metrics"]["queue_depth"] == 4
    assert snapshot["data_flow"]["nodes"][1]["metrics"]["consumer_pending"] == 2
    assert snapshot["data_flow"]["nodes"][2]["status"] == "healthy"
    assert snapshot["data_flow"]["nodes"][3]["status"] == "healthy"
    assert snapshot["data_flow"]["nodes"][4]["metrics"]["fanout_observable"] == 0
    assert snapshot["data_flow"]["nodes"][5]["metrics"]["pending_deliveries"] == 2
    assert snapshot["data_flow"]["edges"][0] == {
        "id": "client-to-control-queues",
        "source": "client",
        "target": "control_queues",
        "label": "AskAgentCommand / ResumeCommand",
        "metric_label": "queued",
        "metric_value": 4,
        "status": "warning",
    }


@pytest.mark.asyncio
async def test_split_worker_snapshot_omits_queue_scans():
    """Worker endpoint data avoids Redis Stream queue inspection."""
    redis = StreamAwareRedis()
    registry = WorkerRegistry(redis)
    await registry.register_worker_membership("worker-1", ["planner"])
    await registry.heartbeat_worker("worker-1")
    await registry.save_execution(
        {
            "execution_id": "exec-1",
            "message_id": "msg-1",
            "session_id": "sess-1",
            "worker_id": "worker-1",
            "target_agent_type": "planner",
            "status": "RUNNING",
        }
    )

    snapshot = await build_worker_observability_snapshot(redis)

    assert snapshot["totals"]["workers_online"] == 1
    assert snapshot["agent_types"] == ["planner"]
    assert "queues" not in snapshot
    assert "recent_executions" not in snapshot
    assert "latency" not in snapshot
    assert redis.xinfo_consumers_calls == []


@pytest.mark.asyncio
async def test_split_worker_snapshot_bounds_known_worker_scan():
    """Worker endpoint reports when known worker scanning is bounded."""
    redis = StreamAwareRedis()
    registry = WorkerRegistry(redis)
    await registry.register_worker_membership("worker-1", ["planner"])
    await registry.register_worker_membership("worker-2", ["writer"])
    await registry.heartbeat_worker("worker-1")
    await registry.heartbeat_worker("worker-2")

    snapshot = await build_worker_observability_snapshot(redis, worker_scan_limit=1)

    assert snapshot["worker_scan"]["source"] == "known_workers_fallback"
    assert snapshot["worker_scan"]["known_workers"] == 2
    assert snapshot["worker_scan"]["scanned_workers"] == 1
    assert snapshot["worker_scan"]["truncated"] is True
    assert snapshot["worker_scan"]["admin"]["managed_workers"] == 0
    assert snapshot["totals"]["workers_online"] == 1


@pytest.mark.asyncio
async def test_split_worker_snapshot_prefers_online_lease_scan():
    """Worker endpoint avoids stale known-worker scans when Redis SCAN is available."""
    redis = StreamAwareRedis()
    registry = WorkerRegistry(redis)
    await registry.register_worker_membership("worker-online", ["planner"])
    await registry.register_worker_membership("worker-stale", ["writer"])
    await registry.heartbeat_worker("worker-online")

    async def scan_iter(match=None, count=None):
        del match, count
        yield RedisKeys.worker_online_lease("worker-online")

    redis.scan_iter = scan_iter

    snapshot = await build_worker_observability_snapshot(redis)

    assert snapshot["worker_scan"]["source"] == "online_lease_scan"
    assert snapshot["worker_scan"]["known_workers"] == 1
    assert snapshot["workers"][0]["worker_id"] == "worker-online"
    assert snapshot["agent_types"] == ["planner"]


@pytest.mark.asyncio
async def test_worker_snapshot_discovers_admin_worker_via_scan():
    """Admin-managed workers are discoverable via the cluster-aware scan
    helper too, not only via the admin_workers() index set — exercising
    the same scan_iter path _scan_online_worker_ids already uses."""
    redis = StreamAwareRedis()

    async def scan_iter(match=None, count=None):
        del match, count
        yield RedisKeys.worker_admin("worker-scanned")

    redis.scan_iter = scan_iter
    await redis.hset(
        RedisKeys.worker_admin("worker-scanned"),
        mapping={"lifecycle": "suspended", "reason": "scanned"},
    )

    snapshot = await build_worker_observability_snapshot(redis)

    assert snapshot["worker_scan"]["admin"]["source"] == "admin_index_and_scan"
    summaries = {worker["worker_id"]: worker for worker in snapshot["workers"]}
    assert summaries["worker-scanned"]["lifecycle"] == "suspended"


class _FnmatchScanRedis(StreamAwareRedis):
    """StreamAwareRedis variant whose scan_iter actually respects the MATCH
    glob pattern (like real Redis SCAN), instead of ignoring it and always
    yielding a hardcoded key. Needed to catch bugs in the scan pattern
    itself, not just in client-side key parsing."""

    async def scan_iter(self, match=None, count=None):
        del count
        for key in set(self.kv.keys()) | set(self.data.keys()):
            if match is None or fnmatch.fnmatchcase(key, match):
                yield key


@pytest.mark.asyncio
async def test_worker_snapshot_discovers_online_worker_via_scan_under_v2():
    """Regression: the online-lease scan pattern must actually match real
    v2-format keys (worker_id wrapped in a Cluster hash tag), both as a
    SCAN MATCH glob and for client-side worker_id extraction — not just
    happen to work under the default v1 schema."""
    with patch.dict("os.environ", {"REDIS_KEY_SCHEMA_VERSION": "v2"}, clear=False):
        redis = _FnmatchScanRedis()
        registry = WorkerRegistry(redis)
        await registry.register_worker_membership("worker-online", ["planner"])
        await registry.heartbeat_worker("worker-online")

        snapshot = await build_worker_observability_snapshot(redis)

    assert snapshot["worker_scan"]["source"] == "online_lease_scan"
    assert snapshot["workers"][0]["worker_id"] == "worker-online"


@pytest.mark.asyncio
async def test_worker_snapshot_discovers_admin_worker_via_scan_under_v2():
    """Same regression, for the admin-worker scan path. Writes the
    worker_admin hash directly (bypassing the admin_workers() index) so
    this test only passes if scan discovery itself actually works."""
    with patch.dict("os.environ", {"REDIS_KEY_SCHEMA_VERSION": "v2"}, clear=False):
        redis = _FnmatchScanRedis()
        await redis.hset(
            RedisKeys.worker_admin("worker-scanned-v2"),
            mapping={"lifecycle": "suspended", "reason": "scanned"},
        )

        snapshot = await build_worker_observability_snapshot(redis)

    assert snapshot["worker_scan"]["admin"]["source"] == "admin_index_and_scan"
    assert snapshot["worker_scan"]["admin"]["scanned_workers"] >= 1
    summaries = {worker["worker_id"]: worker for worker in snapshot["workers"]}
    assert summaries["worker-scanned-v2"]["lifecycle"] == "suspended"


@pytest.mark.asyncio
async def test_worker_snapshot_includes_offline_evicted_admin_worker():
    """Worker endpoint includes evicted workers even after online lease is gone."""
    redis = StreamAwareRedis()
    registry = WorkerRegistry(redis)
    await registry.set_worker_admin_state("worker-evicted", "evicted", "decommission")

    snapshot = await build_worker_observability_snapshot(redis)

    assert snapshot["totals"]["workers_online"] == 0
    assert snapshot["totals"]["workers_managed"] == 1
    worker = snapshot["workers"][0]
    assert worker["worker_id"] == "worker-evicted"
    assert worker["online"] is False
    assert worker["lifecycle"] == "evicted"
    assert worker["lifecycle_reason"] == "decommission"
    assert worker["last_seen"] == 0
    assert worker["agent_types"] == []


@pytest.mark.asyncio
async def test_worker_snapshot_survives_one_worker_admin_state_read_failure():
    """A read failure for one worker's worker_admin(id) hash must not
    prevent the other workers' admin states (or the whole snapshot) from
    being returned. worker_admin(id) keys belong to different worker
    entities, so batch-fetching them must not be one all-or-nothing
    pipeline (a cross-entity CROSSSLOT hazard under Cluster) — each
    worker's read is independent."""

    class FailingAdminStateRedis(StreamAwareRedis):

        async def hgetall(self, name):
            if name == RedisKeys.worker_admin("worker-bad"):
                raise ConnectionError("simulated worker_admin read failure")
            return await super().hgetall(name)

    redis = FailingAdminStateRedis()
    registry = WorkerRegistry(redis)
    await registry.set_worker_admin_state("worker-ok", "suspended", reason="test")
    await registry.set_worker_admin_state("worker-bad", "suspended", reason="test")

    snapshot = await build_worker_observability_snapshot(redis)

    summaries = {worker["worker_id"]: worker for worker in snapshot["workers"]}
    assert summaries["worker-ok"]["lifecycle"] == "suspended"
    # worker-bad's read failed: falls back to the default "active" lifecycle
    # rather than taking down the whole snapshot.
    assert summaries["worker-bad"]["lifecycle"] == "active"


@pytest.mark.asyncio
async def test_split_execution_snapshot_contains_recent_execution_detail():
    """Execution endpoint owns heavier recent execution and latency scans."""
    redis = StreamAwareRedis()
    registry = WorkerRegistry(redis)
    await registry.register_worker_membership("worker-1", ["planner"])
    await registry.heartbeat_worker("worker-1")
    await registry.save_execution(
        {
            "execution_id": "exec-1",
            "message_id": "msg-1",
            "session_id": "sess-1",
            "worker_id": "worker-1",
            "target_agent_type": "planner",
            "status": "RUNNING",
            "created_at": 100,
            "started_at": 200,
        }
    )
    await registry.mark_execution_finished("exec-1", "sess-1", "COMPLETED")

    snapshot = await build_execution_observability_snapshot(redis)

    assert snapshot["recent_executions"][0]["execution_id"] == "exec-1"
    assert snapshot["latency"]["queue"]["completed_count"] == 1
    assert snapshot["agent_health"][0]["agent_type"] == "planner"


@pytest.mark.asyncio
async def test_split_queue_snapshot_uses_provided_agent_types():
    """Queue endpoint can avoid worker scans by accepting agent types."""
    redis = StreamAwareRedis()
    stream_name = RedisKeys.ctrl_stream("planner")
    redis.stream_lengths[stream_name] = 7
    redis.stream_groups[stream_name] = [{"name": "agent_engines", "pending": 1}]

    snapshot = await build_queue_observability_snapshot(redis, agent_types=["planner"])

    assert snapshot["queues"]["agent_type_streams"][0]["length"] == 7
    assert (
        snapshot["queues"]["agent_type_streams"][0]["consumer_groups"][0]["pending"]
        == 1
    )
    assert "workers" not in snapshot


@pytest.mark.asyncio
async def test_build_observability_snapshot_can_include_consumer_details():
    """Consumer details are opt-in because they add Redis calls per group."""
    redis = StreamAwareRedis()
    registry = WorkerRegistry(redis)
    await registry.register_worker_membership("worker-1", ["planner"])
    await registry.heartbeat_worker("worker-1")
    stream_name = RedisKeys.ctrl_stream("planner")
    redis.stream_groups[stream_name] = [{"name": "agent_engines", "pending": 1}]
    redis.stream_consumers[(stream_name, "agent_engines")] = [
        {"name": "worker-1", "pending": 1, "idle": 2500}
    ]

    snapshot = await build_observability_snapshot(redis, include_consumer_details=True)

    assert redis.xinfo_consumers_calls == [(stream_name, "agent_engines")]
    assert snapshot["queues"]["agent_type_streams"][0]["consumer_groups"][0][
        "consumers"
    ] == [{"name": "worker-1", "pending": 1, "idle_ms": 2500}]


def test_queue_snapshot_includes_pending_ownership_and_age_details():
    """Redis Streams details include pending owner, retry count, and age."""
    redis = StreamAwareRedis()
    stream_name = RedisKeys.ctrl_stream("planner")
    redis.stream_lengths[stream_name] = 3
    redis.stream_groups[stream_name] = [
        {"name": "agent_engines", "pending": 2, "lag": 5}
    ]
    redis.stream_consumers[(stream_name, "agent_engines")] = [
        {"name": "worker-1", "pending": 2, "idle": 4500}
    ]
    redis.pending_summaries[(stream_name, "agent_engines")] = {
        "pending": 2,
        "min": "1710000000000-0",
        "max": "1710000005000-0",
        "consumers": [{"name": "worker-1", "pending": 2}],
        "entries": [
            {
                "message_id": "1710000000000-0",
                "consumer": "worker-1",
                "idle_ms": 9000,
                "delivery_count": 3,
            }
        ],
    }

    snapshot = asyncio.run(
        build_queue_observability_snapshot(
            redis,
            agent_types=["planner"],
            include_consumer_details=True,
        )
    )

    group = snapshot["queues"]["agent_type_streams"][0]["consumer_groups"][0]
    assert redis.xpending_calls == [(stream_name, "agent_engines")]
    assert group["pending_oldest_id"] == "1710000000000-0"
    assert group["oldest_pending_idle_ms"] == 9000
    assert group["oldest_pending_age_seconds"] == 9
    assert group["pending_owner"] == "worker-1"
    assert group["max_delivery_count"] == 3
    assert group["consumers"][0]["idle_ms"] == 4500


def test_build_demo_observability_snapshot_has_visualization_data():
    """Demo snapshot provides stable data for local dashboard previews."""
    snapshot = build_demo_observability_snapshot()

    assert snapshot["totals"]["workers_online"] >= 2
    assert snapshot["status_counts"]["RUNNING"] > 0
    assert snapshot["workers"]
    assert snapshot["queues"]["agent_type_streams"]
    assert snapshot["data_flow"]["nodes"]
    assert snapshot["data_flow"]["edges"]
    assert snapshot["data_flow"]["summary"]["queue_depth_total"] == 9
    assert snapshot["recent_executions"]
    assert snapshot["agent_health"]
    assert snapshot["agent_health"][0]["agent_type"] == "planner"
    assert snapshot["agent_health"][0]["worker_count"] == 1
    assert snapshot["agent_health"][0]["queue_depth"] == 4
    assert snapshot["latency"]["completed_count"] > 0
    assert snapshot["latency"]["avg_ms"] > 0
    assert snapshot["latency"]["p95_ms"] >= snapshot["latency"]["avg_ms"]
    assert snapshot["latency"]["queue"]["p95_ms"] > 0
    assert snapshot["latency"]["total"]["p95_ms"] > 0
    assert snapshot["failures"]["total"] > 0
    assert snapshot["failures"]["by_error_type"]["RuntimeError"] == 1
    assert snapshot["health"] == {
        "status": "warning",
        "score": 70,
        "critical_alerts": 0,
        "warning_alerts": 3,
        "summary": "3 warning alerts active.",
    }
    assert {
        "code": "FAILED_EXECUTIONS",
        "severity": "warning",
        "message": "3 failed executions recorded.",
        "value": 3,
        "threshold": 0,
    } in snapshot["alerts"]
    assert {
        "code": "PENDING_DELIVERIES",
        "severity": "warning",
        "message": "2 pending control-plane deliveries.",
        "value": 2,
        "threshold": 0,
    } in snapshot["alerts"]


def test_build_history_point_extracts_trend_values():
    """History points keep compact trend values from a full snapshot."""
    snapshot = build_demo_observability_snapshot()

    point = build_history_point(snapshot)

    assert point["generated_at"] == snapshot["generated_at"]
    assert point["workers_online"] == snapshot["totals"]["workers_online"]
    assert point["active_executions"] == snapshot["totals"]["active_executions"]
    assert point["completed_executions"] == snapshot["status_counts"]["COMPLETED"]
    assert point["failed_executions"] == snapshot["status_counts"]["FAILED"]
    assert point["cancelled_executions"] == snapshot["status_counts"]["CANCELLED"]
    assert point["terminal_executions"] == (
        snapshot["status_counts"]["COMPLETED"]
        + snapshot["status_counts"]["FAILED"]
        + snapshot["status_counts"]["CANCELLED"]
    )
    assert point["queue_depth_total"] == 9
    assert point["consumer_pending_total"] == 3
    assert point["max_delivery_count"] == 2
    assert point["alert_count"] == len(snapshot["alerts"])
    assert point["latency_p95_ms"] == snapshot["latency"]["p95_ms"]
    assert point["queue_latency_p95_ms"] == snapshot["latency"]["queue"]["p95_ms"]
    assert point["total_latency_p95_ms"] == snapshot["latency"]["total"]["p95_ms"]
    assert point["success_ratio_ppm"] == snapshot["slo"]["success_ratio_ppm"]
    assert point["deadletter_count"] == snapshot["slo"]["deadletter_count"]


def test_alert_policy_customizes_thresholds():
    """Alert policy controls when derived health alerts fire."""
    snapshot = build_demo_observability_snapshot(
        alert_policy=AlertPolicy(
            failed_execution_threshold=5,
            delivery_pending_threshold=5,
            consumer_pending_threshold=5,
            queue_backlog_threshold=4,
        )
    )

    assert {
        "code": "QUEUE_BACKLOG",
        "severity": "warning",
        "message": "4 messages queued for agent type planner.",
        "value": 4,
        "threshold": 4,
    } in snapshot["alerts"]
    assert not any(alert["code"] == "FAILED_EXECUTIONS" for alert in snapshot["alerts"])
    assert not any(
        alert["code"] == "PENDING_DELIVERIES" for alert in snapshot["alerts"]
    )
    assert not any(alert["code"] == "CONSUMER_PENDING" for alert in snapshot["alerts"])
    assert snapshot["health"] == {
        "status": "warning",
        "score": 90,
        "critical_alerts": 0,
        "warning_alerts": 1,
        "summary": "1 warning alert active.",
    }


def test_slo_policy_adds_sli_context_to_alerts_and_health():
    """SLO alerts carry SLI, window, burn-rate, and runbook metadata."""
    snapshot = build_demo_observability_snapshot(
        alert_policy=AlertPolicy(
            failed_execution_threshold=99,
            delivery_pending_threshold=99,
            consumer_pending_threshold=99,
            queue_backlog_threshold=99,
            slo_policy=SLOPolicy(
                success_ratio_target=0.99,
                total_latency_p95_ms=1000,
                deadletter_threshold=0,
                freshness_max_age_ms=1000,
                window="5m",
            ),
        )
    )

    alerts_by_code = {alert["code"]: alert for alert in snapshot["alerts"]}
    assert alerts_by_code["SLO_SUCCESS_RATIO"]["sli"] == "execution_success_ratio"
    assert alerts_by_code["SLO_SUCCESS_RATIO"]["window"] == "5m"
    assert alerts_by_code["SLO_SUCCESS_RATIO"]["burn_rate"] > 1
    assert alerts_by_code["SLO_SUCCESS_RATIO"]["runbook_id"] == "slo-success-ratio"
    assert alerts_by_code["SLO_SUCCESS_RATIO"]["runbook"]["id"] == "slo-success-ratio"
    assert alerts_by_code["SLO_SUCCESS_RATIO"]["runbook"]["title"]
    assert alerts_by_code["SLO_SUCCESS_RATIO"]["runbook"]["actions"]
    assert alerts_by_code["SLO_LATENCY_P95"]["sli"] == "execution_total_latency_p95"
    assert snapshot["slo"]["success_ratio_ppm"] < 990000


def test_alert_policy_can_produce_healthy_demo_snapshot():
    """Health summary reports healthy when configured thresholds suppress alerts."""
    snapshot = build_demo_observability_snapshot(
        alert_policy=AlertPolicy(
            failed_execution_threshold=5,
            delivery_pending_threshold=5,
            consumer_pending_threshold=5,
            queue_backlog_threshold=10,
        )
    )

    assert snapshot["alerts"] == []
    assert snapshot["health"] == {
        "status": "healthy",
        "score": 100,
        "critical_alerts": 0,
        "warning_alerts": 0,
        "summary": "No active health alerts.",
    }


def test_build_demo_observability_history_has_ordered_points():
    """Demo history provides ordered points for trend charts."""
    history = build_demo_observability_history(samples=6)

    assert len(history) == 6
    assert [point["generated_at"] for point in history] == sorted(
        point["generated_at"] for point in history
    )
    assert all("queue_depth_total" in point for point in history)
    assert all("consumer_pending_total" in point for point in history)
    assert all("latency_p95_ms" in point for point in history)
    assert all("queue_latency_p95_ms" in point for point in history)
    assert all("total_latency_p95_ms" in point for point in history)
    assert all("completed_executions" in point for point in history)
    assert all("terminal_executions" in point for point in history)
    assert all("deadletter_count" in point for point in history)
    assert all("max_delivery_count" in point for point in history)


def test_build_demo_session_observability_snapshot_has_tree_and_events():
    """Demo session snapshot lets the frontend preview drilldown without Redis."""
    snapshot = build_demo_session_observability_snapshot()

    assert snapshot["session_id"] == "sess-demo"
    assert snapshot["execution_tree"]
    assert snapshot["timeline"]
    assert snapshot["recent_events"]


def test_build_demo_trace_observability_snapshot_has_spans_and_timeline():
    """Demo trace snapshot previews the trace waterfall contract."""
    snapshot = build_demo_trace_observability_snapshot()

    assert snapshot["trace_id"] == "trace-demo"
    assert snapshot["status"] == "RUNNING"
    assert snapshot["duration_ms"] > 0
    assert [span["operation"] for span in snapshot["spans"]] == [
        "client.dispatch",
        "queue.wait",
        "worker.execute",
        "agent.process",
        "agent.emit_chunk",
    ]
    assert snapshot["tree"][0]["operation"] == "client.dispatch"
    assert all("offset_ms" in item for item in snapshot["timeline"])
    assert all("duration_ms" in item for item in snapshot["timeline"])


@pytest.mark.asyncio
async def test_build_session_observability_snapshot_returns_tree_and_events():
    """Session snapshot reconstructs execution tree and data stream events."""
    redis = StreamAwareRedis()
    registry = WorkerRegistry(redis)
    await registry.initialize_execution(
        {
            "execution_id": "exec-root",
            "message_id": "msg-root",
            "session_id": "sess-tree",
            "trace_id": "trace-1",
            "target_agent_type": "planner",
            "status": "QUEUED",
        }
    )
    await registry.initialize_execution(
        {
            "execution_id": "exec-child",
            "message_id": "msg-child",
            "session_id": "sess-tree",
            "trace_id": "trace-1",
            "parent_message_id": "msg-root",
            "target_agent_type": "writer",
            "status": "QUEUED",
        }
    )
    await registry.mark_execution_finished("exec-child", "sess-tree", "COMPLETED")
    stream_name = RedisKeys.session_data_stream("sess-tree")
    redis.stream_entries[stream_name] = [
        (
            "1-0",
            DataMessage(
                trace_id="trace-1",
                session_id="sess-tree",
                event_type="ANSWER_DELTA",
                source_agent_type="planner",
                message_id="msg-root",
                data={"content": "hello"},
            ).to_redis_payload(),
        )
    ]

    snapshot = await build_session_observability_snapshot(redis, "sess-tree")

    assert snapshot["session_id"] == "sess-tree"
    assert snapshot["totals"]["executions"] == 2
    assert snapshot["status_counts"] == {"COMPLETED": 1, "QUEUED": 1}
    assert snapshot["execution_tree"][0]["message_id"] == "msg-root"
    assert snapshot["execution_tree"][0]["children"][0]["message_id"] == "msg-child"
    assert snapshot["recent_events"][0]["stream_id"] == "1-0"
    assert snapshot["recent_events"][0]["event_type"] == "ANSWER_DELTA"
    assert snapshot["timeline"]
    assert {item["kind"] for item in snapshot["timeline"]} == {
        "execution_status",
        "data_event",
    }


@pytest.mark.asyncio
async def test_build_trace_observability_snapshot_reconstructs_from_session_registry():
    """Trace snapshot can be reconstructed from existing execution and data events."""
    redis = StreamAwareRedis()
    registry = WorkerRegistry(redis)
    await registry.initialize_execution(
        {
            "execution_id": "exec-root",
            "message_id": "msg-root",
            "session_id": "sess-trace",
            "trace_id": "trace-session",
            "target_agent_type": "planner",
            "status": "QUEUED",
        }
    )
    await registry.update_execution_status("exec-root", "sess-trace", "RUNNING")
    await registry.mark_execution_finished("exec-root", "sess-trace", "COMPLETED")
    stream_name = RedisKeys.session_data_stream("sess-trace")
    redis.stream_entries[stream_name] = [
        (
            "1-0",
            DataMessage(
                trace_id="trace-session",
                session_id="sess-trace",
                event_type="ANSWER_DELTA",
                source_agent_type="planner",
                message_id="msg-root",
                data={"content": "hello"},
            ).to_redis_payload(),
        )
    ]

    snapshot = await build_trace_observability_snapshot(
        redis, "trace-session", session_id="sess-trace"
    )

    assert snapshot["trace_id"] == "trace-session"
    assert snapshot["session_id"] == "sess-trace"
    assert snapshot["status"] == "COMPLETED"
    assert snapshot["duration_ms"] >= 0
    assert [span["operation"] for span in snapshot["spans"]] == [
        "client.dispatch",
        "queue.wait",
        "worker.execute",
        "agent.emit_chunk",
    ]
    assert snapshot["spans"][0]["component"] == "client"
    assert snapshot["spans"][1]["component"] == "redis"
    assert snapshot["spans"][1]["parent_span_id"] == "msg-root:client.dispatch"
    assert snapshot["spans"][2]["execution_id"] == "exec-root"
    assert snapshot["spans"][2]["parent_span_id"] == "exec-root:queue.wait"
    assert snapshot["spans"][3]["event_type"] == "ANSWER_DELTA"
    assert snapshot["spans"][3]["parent_span_id"] == "exec-root:worker.execute"
    assert snapshot["tree"][0]["operation"] == "client.dispatch"
    assert snapshot["tree"]
    assert all("offset_ms" in item for item in snapshot["timeline"])


@pytest.mark.asyncio
async def test_span_recorder_persists_trace_spans_and_indexes():
    """SpanRecorder writes the dedicated Redis trace storage shape."""
    redis = StreamAwareRedis()
    recorder = SpanRecorder(redis)

    await recorder.record_span(
        TraceSpan(
            trace_id="trace-store",
            span_id="span-1",
            parent_span_id="",
            operation="client.dispatch",
            component="client",
            start_ts=100,
            end_ts=160,
            status="COMPLETED",
            session_id="sess-store",
            worker_id="worker-1",
            target_agent_type="planner",
        )
    )

    assert redis.data[RedisKeys.trace_spans("trace-store")]
    assert redis.data[RedisKeys.trace_index_session("sess-store")] == {
        "trace-store": 100
    }
    assert redis.data[RedisKeys.trace_index_worker("worker-1")] == {"trace-store": 100}
    assert redis.data[RedisKeys.trace_index_agent("planner")] == {"trace-store": 100}
    assert redis.data[RedisKeys.trace_meta("trace-store")]["session_id"] == "sess-store"


@pytest.mark.asyncio
async def test_build_trace_observability_snapshot_reads_stored_spans_without_session():
    """Trace lookup uses dedicated trace storage before session reconstruction."""
    redis = StreamAwareRedis()
    recorder = SpanRecorder(redis)
    await recorder.record_span(
        TraceSpan(
            trace_id="trace-store",
            span_id="span-worker",
            parent_span_id="",
            operation="worker.execute",
            component="worker",
            start_ts=200,
            end_ts=500,
            status="COMPLETED",
            session_id="sess-store",
            worker_id="worker-1",
            target_agent_type="planner",
        )
    )

    snapshot = await build_trace_observability_snapshot(redis, "trace-store")

    assert snapshot["trace_id"] == "trace-store"
    assert snapshot["session_id"] == "sess-store"
    assert snapshot["spans"][0]["operation"] == "worker.execute"
    assert snapshot["timeline"][0]["offset_ms"] == 0


def test_build_prometheus_metrics_exports_core_snapshot_values():
    """Prometheus export includes totals, state counts, workers, and queues."""
    snapshot = build_demo_observability_snapshot()

    metrics = build_prometheus_metrics(snapshot)

    assert "by_framework_workers_online 2" in metrics
    assert 'by_framework_execution_status_current{status="RUNNING"} 3' in metrics
    assert (
        'by_framework_queue_depth{queue_type="agent_type",name="planner",'
        'stream="byai_gateway:ctrl:agent_type:planner"} 4'
    ) in metrics
    assert "by_framework_worker_active_executions" not in metrics
    assert 'by_framework_alerts_current{severity="warning"} 3' in metrics
    assert "by_framework_execution_latency_avg_ms " in metrics
    assert "by_framework_execution_latency_p95_ms " in metrics
    assert "by_framework_execution_queue_latency_p95_ms " in metrics
    assert "by_framework_execution_total_latency_p95_ms " in metrics
    assert "by_framework_execution_total_duration_p95_seconds " in metrics
    assert "by_framework_execution_queue_duration_p95_seconds " in metrics
    assert "by_framework_execution_run_duration_p95_seconds " in metrics
    assert "# TYPE by_framework_execution_total_duration_seconds" not in metrics
    assert "by_framework_stream_depth" in metrics
    assert "by_framework_stream_oldest_pending_age_seconds" in metrics
    assert (
        'by_framework_stream_max_delivery_count{queue_type="agent_type",'
        'name="planner",group="agent_engines"} 2'
    ) in metrics
    assert "by_framework_slo_burn_rate" in metrics
    assert (
        'by_framework_stream_pending_messages{queue_type="agent_type",'
        'name="planner",group="agent_engines"} 1'
    ) in metrics
    assert (
        'by_framework_execution_recent_failures{error_type="RuntimeError"} 1' in metrics
    )
    assert 'by_framework_agent_queue_depth{agent_type="planner"} 4' in metrics
    assert 'by_framework_agent_workers{agent_type="planner"} 1' in metrics


def test_build_prometheus_metrics_uses_consistent_metric_contract():
    """Snapshot metrics keep stable names and Prometheus-compatible types."""
    metrics = build_prometheus_metrics(build_demo_observability_snapshot())
    metric_types = {}

    for line in metrics.splitlines():
        if not line.startswith("# TYPE "):
            continue
        _, _, name, metric_type = line.split(maxsplit=3)
        assert name not in metric_types
        metric_types[name] = metric_type

    assert metric_types["by_framework_execution_status_current"] == "gauge"
    assert metric_types["by_framework_tracked_executions"] == "gauge"
    assert metric_types["by_framework_execution_recent_failures"] == "gauge"
    assert metric_types["by_framework_alerts_current"] == "gauge"
    assert metric_types["by_framework_execution_total_duration_p95_seconds"] == "gauge"
    assert metric_types["by_framework_stream_depth"] == "gauge"
    assert metric_types["by_framework_stream_max_delivery_count"] == "gauge"
    assert "by_framework_execution_status_total" not in metric_types
    assert all(
        metric_type == "counter"
        for name, metric_type in metric_types.items()
        if name.endswith("_total")
    )
