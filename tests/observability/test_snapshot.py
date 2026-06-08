"""Tests for observability dashboard snapshots."""

import pytest

from by_framework import RedisKeys, WorkerRegistry
from by_framework.core.protocol.data_message import DataMessage
from by_framework.observability.snapshot import (
    AlertPolicy,
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
from by_framework.observability.span_recorder import SpanRecorder, TraceSpan


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

    async def execute(self):
        for command in self.commands:
            if command[0] == "hset":
                await self.redis.hset(command[1], {command[2]: command[3]})
            elif command[0] == "hdel":
                await self.redis.hdel(command[1], *command[2])
            elif command[0] == "zadd":
                await self.redis.zadd(command[1], command[2])
            elif command[0] == "zrem":
                await self.redis.zrem(command[1], *command[2])
            elif command[0] == "rpush":
                await self.redis.rpush(command[1], command[2])
            elif command[0] == "hincrby":
                await self.redis.hincrby(command[1], command[2], command[3])
            elif command[0] == "expire":
                await self.redis.expire(command[1], command[2])
        return []


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
        self.xinfo_consumers_calls = []

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

    assert snapshot["worker_scan"] == {
        "source": "known_workers_fallback",
        "known_workers": 2,
        "scanned_workers": 1,
        "truncated": True,
    }
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
    assert point["failed_executions"] == snapshot["status_counts"]["FAILED"]
    assert point["queue_depth_total"] == 9
    assert point["consumer_pending_total"] == 3
    assert point["alert_count"] == len(snapshot["alerts"])
    assert point["latency_p95_ms"] == snapshot["latency"]["p95_ms"]
    assert point["queue_latency_p95_ms"] == snapshot["latency"]["queue"]["p95_ms"]
    assert point["total_latency_p95_ms"] == snapshot["latency"]["total"]["p95_ms"]


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
    assert 'by_framework_execution_status_total{status="RUNNING"} 3' in metrics
    assert (
        'by_framework_queue_depth{queue_type="agent_type",name="planner",'
        'stream="byai_gateway:ctrl:agent_type:planner"} 4'
    ) in metrics
    assert (
        'by_framework_worker_active_executions{worker_id="worker-planner-1"} 3'
        in metrics
    )
    assert 'by_framework_alerts_total{severity="warning"} 3' in metrics
    assert "by_framework_execution_latency_avg_ms " in metrics
    assert "by_framework_execution_latency_p95_ms " in metrics
    assert "by_framework_execution_queue_latency_p95_ms " in metrics
    assert "by_framework_execution_total_latency_p95_ms " in metrics
    assert (
        'by_framework_stream_pending_messages{queue_type="agent_type",'
        'name="planner",group="agent_engines"} 1'
    ) in metrics
    assert (
        'by_framework_execution_failure_total{error_type="RuntimeError"} 1' in metrics
    )
    assert 'by_framework_agent_queue_depth{agent_type="planner"} 4' in metrics
    assert 'by_framework_agent_workers{agent_type="planner"} 1' in metrics
