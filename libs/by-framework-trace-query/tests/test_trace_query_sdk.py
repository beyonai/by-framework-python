"""Tests for the trace write schema and trace query SDK."""

import asyncio
import http.client
import json
import threading
import time
from http.server import ThreadingHTTPServer

import pytest
from by_framework import RedisKeys
from by_framework.metrics import MetricsReadResult, MetricsWindow
from by_framework.trace import (
    EventRecord,
    ExecutionRecord,
    SpanRecord,
    TraceRecord,
    TraceWriteClient,
)
from by_framework.trace.trace_schema import decode_redis_value
from by_framework_dashboard.adapters import trace_result_to_dashboard_trace
from by_framework_dashboard.dashboard import make_handler

from by_framework_trace_query import TraceReadClient
from by_framework_trace_query.merger import TraceMerger


class FailingRedis:
    """Redis fake that fails all operations so best-effort writes can be verified."""

    def pipeline(self):
        raise RuntimeError("redis down")

    async def hset(self, *args, **kwargs):
        raise RuntimeError("redis down")

    async def expire(self, *args, **kwargs):
        raise RuntimeError("redis down")

    async def xadd(self, *args, **kwargs):
        raise RuntimeError("redis down")


class FailingTraceSource:
    """Trace source fake that fails reads."""

    name = "failing"

    async def get_trace(self, trace_id, *, session_id=""):
        del trace_id, session_id
        raise RuntimeError("source unavailable")

    async def list_trace_ids(
        self, *, session_id="", worker_id="", agent_type="", limit=50
    ):
        del session_id, worker_id, agent_type, limit
        raise RuntimeError("source unavailable")


class SlowTraceSource:
    """Trace source fake used to verify list_traces reads details concurrently."""

    name = "slow"

    async def list_trace_ids(
        self, *, session_id="", worker_id="", agent_type="", limit=50
    ):
        del session_id, worker_id, agent_type, limit
        return ["trace-1", "trace-2", "trace-3"]

    async def get_trace(self, trace_id, *, session_id=""):
        del session_id
        await asyncio.sleep(0.1)
        return (
            TraceRecord(trace_id=trace_id, output={"ok": True}),
            [
                SpanRecord(
                    trace_id=trace_id,
                    span_id=f"{trace_id}-client",
                    operation="client.dispatch",
                    component="client",
                    start_ts=1,
                    end_ts=2,
                ),
                SpanRecord(
                    trace_id=trace_id,
                    span_id=f"{trace_id}-worker",
                    operation="worker.execute",
                    component="worker",
                    start_ts=2,
                    end_ts=3,
                ),
            ],
            [],
        )


class RecordingMetricsClient:
    """Metrics client fake that records requested correlation windows."""

    def __init__(self):
        self.calls = []

    async def explain_window(self, *, start_ts, end_ts, buffer_ms=5000, limit=120):
        self.calls.append(
            {
                "start_ts": start_ts,
                "end_ts": end_ts,
                "buffer_ms": buffer_ms,
                "limit": limit,
            }
        )
        return MetricsReadResult(
            window=MetricsWindow(
                start_ts=start_ts - buffer_ms,
                end_ts=end_ts + buffer_ms,
            ),
            samples=[
                {
                    "generated_at": start_ts,
                    "queue_depth_total": 3,
                    "consumer_pending_total": 0,
                }
            ],
            summary={"sample_count": 1, "queue_depth_total": {"max": 3}},
        )


class FailingMetricsClient:
    """Metrics client fake that fails reads."""

    async def explain_window(self, *, start_ts, end_ts, buffer_ms=5000, limit=120):
        del start_ts, end_ts, buffer_ms, limit
        raise RuntimeError("metrics unavailable")


class QueryPipeline:
    """Minimal Redis pipeline used by trace writer tests."""

    def __init__(self, redis):
        self.redis = redis
        self.commands = []

    def hset(self, name, key, value):
        self.commands.append(("hset", name, key, value))
        return self

    def rpush(self, name, value):
        self.commands.append(("rpush", name, value))
        return self

    def zadd(self, name, mapping):
        self.commands.append(("zadd", name, mapping))
        return self

    def expire(self, name, ttl):
        self.commands.append(("expire", name, ttl))
        return self

    async def execute(self):
        for command in self.commands:
            if command[0] == "hset":
                await self.redis.hset(command[1], {command[2]: command[3]})
            elif command[0] == "rpush":
                await self.redis.rpush(command[1], command[2])
            elif command[0] == "zadd":
                await self.redis.zadd(command[1], command[2])
            elif command[0] == "expire":
                await self.redis.expire(command[1], command[2])
        return []


class QueryRedis:
    """Small async Redis fake for trace query tests."""

    def __init__(self):
        self.data = {}
        self.expires = {}

    async def hset(self, name, mapping=None, key=None, value=None):
        self.data.setdefault(name, {})
        if mapping:
            self.data[name].update(mapping)
        else:
            self.data[name][key] = value

    async def hgetall(self, name):
        return self.data.get(name, {})

    async def hget(self, name, key):
        return self.data.get(name, {}).get(key)

    async def rpush(self, name, value):
        self.data.setdefault(name, []).append(value)

    async def lrange(self, name, start, end):
        values = self.data.get(name, [])
        if end == -1:
            end = len(values) - 1
        return values[start : end + 1]

    async def zadd(self, name, mapping):
        self.data.setdefault(name, {}).update(mapping)

    async def zrevrange(self, name, start, end):
        values = sorted(self.data.get(name, {}).items(), key=lambda item: item[1])
        values.reverse()
        return [item[0] for item in values[start : end + 1]]

    async def expire(self, name, ttl):
        self.expires[name] = ttl
        return 1

    async def xadd(self, name, fields):
        self.data.setdefault(name, []).append(("1-0", fields))
        return "1-0"

    async def xrevrange(self, name, max="+", min="-", count=None):  # pylint: disable=redefined-builtin
        del max, min
        values = list(reversed(self.data.get(name, [])))
        if count is not None:
            return values[:count]
        return values

    def pipeline(self):
        return QueryPipeline(self)


def test_trace_records_are_json_serializable_and_redact_metadata():
    """Shared trace models serialize cleanly and redact sensitive metadata."""
    trace = TraceRecord(
        trace_id="trace-1",
        name="client.dispatch:planner",
        session_id="session-1",
        input={"text": "hello"},
        output={"answer": "world"},
        metadata={"api_key": "secret", "safe": "visible"},
    )
    execution = ExecutionRecord(
        execution_id="exec-1",
        message_id="msg-1",
        trace_id="trace-1",
        session_id="session-1",
        target_agent_type="planner",
    )
    span = SpanRecord(
        trace_id="trace-1",
        span_id="span-1",
        name="client.dispatch:planner",
        operation="client.dispatch",
        component="client",
        start_ts=100,
        end_ts=120,
        input={"text": "hello"},
        tokens={"input": 1, "output": 2},
        cost={"total": 0.01},
    )
    event = EventRecord(
        event_id="event-1",
        trace_id="trace-1",
        session_id="session-1",
        message_id="msg-1",
        event_type="chunk",
        payload={"delta": "x"},
    )

    payload = {
        "trace": trace.to_dict(),
        "execution": execution.to_dict(),
        "span": span.to_dict(),
        "event": event.to_dict(),
    }

    encoded = json.dumps(payload, ensure_ascii=False)
    assert "trace-1" in encoded
    assert payload["trace"]["metadata"]["api_key"] == "[REDACTED]"


def test_trace_schema_decodes_json_numbers_and_preserves_false_values():
    """Redis decoding should round-trip JSON primitives without type drift."""
    assert decode_redis_value("1234") == 1234
    assert decode_redis_value("-12.5") == -12.5
    assert decode_redis_value("false") is False

    span = SpanRecord(
        trace_id="trace-1",
        span_id="span-1",
        operation="worker.execute",
        component="worker",
        retryable=False,
    )

    assert span.to_dict()["retryable"] is False


def test_span_record_converts_to_trace_span_without_manual_field_mapping():
    """SpanRecord owns the TraceSpan conversion used by write clients."""
    record = SpanRecord(
        trace_id="trace-1",
        span_id="span-1",
        parent_span_id="parent",
        name="worker.execute",
        operation="worker.execute",
        component="worker",
        kind="internal",
        input={"prompt": "hi"},
        output={"answer": "ok"},
        tokens={"input": 1},
        cost={"total": 0.01},
        retryable=False,
        metadata={"safe": "value"},
    )

    trace_span = record.to_trace_span()

    assert trace_span.trace_id == record.trace_id
    assert trace_span.parent_span_id == "parent"
    assert trace_span.input == {"prompt": "hi"}
    assert trace_span.retryable is False
    assert trace_span.to_payload()["retryable"] is False


@pytest.mark.asyncio
async def test_trace_write_client_records_trace_meta_and_indexes():
    """TraceWriteClient writes trace-level Redis metadata in a queryable shape."""
    redis = QueryRedis()
    writer = TraceWriteClient(redis)

    await writer.record_trace(
        TraceRecord(
            trace_id="trace-1",
            name="client.dispatch:planner",
            session_id="session-1",
            root_agent_type="planner",
            root_message_id="msg-1",
            input={"text": "hello"},
            output={"answer": "world"},
            status="COMPLETED",
            start_ts=100,
            end_ts=200,
        )
    )

    meta = await redis.hgetall(RedisKeys.trace_meta("trace-1"))
    assert meta["name"] == "client.dispatch:planner"
    assert json.loads(meta["input"]) == {"text": "hello"}
    assert json.loads(meta["output"]) == {"answer": "world"}
    assert meta["root_agent_type"] == "planner"
    assert await redis.zrevrange(RedisKeys.trace_index_session("session-1"), 0, 10) == [
        "trace-1"
    ]
    assert await redis.zrevrange(RedisKeys.trace_index_agent("planner"), 0, 10) == [
        "trace-1"
    ]


@pytest.mark.asyncio
async def test_trace_write_client_records_execution_event_and_is_best_effort():
    """TraceWriteClient stores execution/event fallback data and ignores failures."""
    redis = QueryRedis()
    writer = TraceWriteClient(redis)

    await writer.record_execution(
        ExecutionRecord(
            execution_id="exec-1",
            message_id="msg-1",
            trace_id="trace-1",
            session_id="session-1",
            status="COMPLETED",
        )
    )
    await writer.record_event(
        EventRecord(
            event_id="event-1",
            trace_id="trace-1",
            session_id="session-1",
            message_id="msg-1",
            event_type="TEXT_CHUNK",
            timestamp=150,
            payload={"delta": "hello"},
        )
    )

    registry = await redis.hgetall(RedisKeys.session_registry("session-1"))
    execution = json.loads(registry["exec:exec-1"])
    stream_entries = redis.data[RedisKeys.session_data_stream("session-1")]
    event_payload = json.loads(stream_entries[0][1]["data"])
    assert execution["trace_id"] == "trace-1"
    assert event_payload["trace_id"] == "trace-1"
    assert event_payload["data"] == {"delta": "hello"}

    failing_writer = TraceWriteClient(FailingRedis())
    await failing_writer.record_trace(TraceRecord(trace_id="trace-failed"))
    await failing_writer.record_execution(
        ExecutionRecord(
            execution_id="exec-failed",
            message_id="msg-failed",
            trace_id="trace-failed",
            session_id="session-failed",
        )
    )
    await failing_writer.record_event(
        EventRecord(
            event_id="event-failed",
            trace_id="trace-failed",
            session_id="session-failed",
            message_id="msg-failed",
            event_type="TEXT_CHUNK",
        )
    )


@pytest.mark.asyncio
async def test_trace_read_client_reads_stored_redis_spans_and_tree():
    """Redis trace source prefers stored spans and builds the dashboard tree."""
    redis = QueryRedis()
    writer = TraceWriteClient(redis)
    await writer.record_trace(
        TraceRecord(
            trace_id="trace-1",
            name="client.dispatch:planner",
            session_id="session-1",
            root_agent_type="planner",
            status="COMPLETED",
            output={"answer": "done"},
        )
    )
    await writer.record_span(
        SpanRecord(
            trace_id="trace-1",
            span_id="client",
            name="client.dispatch:planner",
            operation="client.dispatch",
            component="client",
            start_ts=100,
            end_ts=110,
            session_id="session-1",
            target_agent_type="planner",
        )
    )
    await writer.record_span(
        SpanRecord(
            trace_id="trace-1",
            span_id="worker",
            parent_span_id="client",
            name="worker.execute",
            operation="worker.execute",
            component="worker",
            start_ts=111,
            end_ts=180,
            session_id="session-1",
            worker_id="worker-1",
            target_agent_type="planner",
        )
    )

    result = await TraceReadClient(redis_client=redis).get_trace(
        "trace-1", session_id="session-1"
    )

    assert result.status == "ok"
    assert result.sources == ["redis"]
    assert result.trace.name == "client.dispatch:planner"
    assert [span.span_id for span in result.spans] == ["client", "worker"]
    assert result.tree[0].span.span_id == "client"
    assert result.tree[0].children[0].span.span_id == "worker"
    assert (
        trace_result_to_dashboard_trace(result)["tree"][0]["children"][0]["span_id"]
        == "worker"
    )


@pytest.mark.asyncio
async def test_trace_read_client_reconstructs_spans_from_session_fallback():
    """Trace detail falls back to session execution and event records."""
    redis = QueryRedis()
    writer = TraceWriteClient(redis)
    await redis.hset(
        RedisKeys.session_registry("session-fallback"),
        {
            "exec:exec-1": json.dumps(
                {
                    "execution_id": "exec-1",
                    "message_id": "msg-1",
                    "trace_id": "trace-fallback",
                    "session_id": "session-fallback",
                    "worker_id": "worker-1",
                    "target_agent_type": "planner",
                    "status": "COMPLETED",
                    "created_at": 100,
                    "started_at": 120,
                    "finished_at": 200,
                }
            )
        },
    )
    await writer.record_event(
        EventRecord(
            event_id="event-1",
            trace_id="trace-fallback",
            session_id="session-fallback",
            message_id="msg-1",
            event_type="TEXT_CHUNK",
            timestamp=160,
            payload={"delta": "hi"},
        )
    )

    result = await TraceReadClient(redis_client=redis).get_trace(
        "trace-fallback", session_id="session-fallback"
    )

    operations = [span.operation for span in result.spans]
    assert result.status == "partial"
    assert "source_partial" in result.diagnostic_codes()
    assert "client.dispatch" in operations
    assert "queue.wait" in operations
    assert "worker.execute" in operations
    assert "agent.emit_chunk" in operations


def test_trace_merger_reports_missing_parent_and_cycle():
    """TraceMerger diagnoses broken parent relationships without recursion loops."""
    merger = TraceMerger()

    missing = merger.merge(
        TraceRecord(trace_id="trace-missing"),
        [
            SpanRecord(
                trace_id="trace-missing",
                span_id="child",
                parent_span_id="missing-parent",
                operation="worker.execute",
                component="worker",
                start_ts=1,
                end_ts=2,
            )
        ],
    )
    assert "missing_client_dispatch" in missing.diagnostic_codes()
    assert "missing_parent" in missing.diagnostic_codes()

    cycle = merger.merge(
        TraceRecord(trace_id="trace-cycle", output={"ok": True}),
        [
            SpanRecord(
                trace_id="trace-cycle",
                span_id="a",
                parent_span_id="b",
                operation="client.dispatch",
                component="client",
                start_ts=1,
                end_ts=2,
            ),
            SpanRecord(
                trace_id="trace-cycle",
                span_id="b",
                parent_span_id="a",
                operation="worker.execute",
                component="worker",
                start_ts=2,
                end_ts=3,
            ),
        ],
    )
    assert "parent_cycle" in cycle.diagnostic_codes()
    assert trace_result_to_dashboard_trace(cycle)["span_count"] == 2


def test_trace_merger_dedupes_more_complete_span_and_caps_span_count():
    """Duplicate spans keep richer payloads and oversized traces produce diagnostics."""
    merger = TraceMerger(max_spans=2)

    result = merger.merge(
        TraceRecord(trace_id="trace-dedupe", output={"ok": True}),
        [
            SpanRecord(
                trace_id="trace-dedupe",
                span_id="same",
                operation="client.dispatch",
                component="client",
                start_ts=1,
                end_ts=2,
            ),
            SpanRecord(
                trace_id="trace-dedupe",
                span_id="same",
                operation="client.dispatch",
                component="client",
                start_ts=1,
                end_ts=2,
                input={"prompt": "hello"},
                output={"answer": "world"},
                tokens={"input": 3, "output": 4},
                cost={"total": 0.01},
            ),
            SpanRecord(
                trace_id="trace-dedupe",
                span_id="worker",
                operation="worker.execute",
                component="worker",
                start_ts=2,
                end_ts=3,
            ),
            SpanRecord(
                trace_id="trace-dedupe",
                span_id="extra",
                operation="agent.emit_event",
                component="agent_context",
                start_ts=3,
                end_ts=4,
            ),
        ],
    )

    assert result.spans[0].span_id == "same"
    assert result.spans[0].input == {"prompt": "hello"}
    assert result.spans[0].output == {"answer": "world"}
    assert "span_count_exceeded" in result.diagnostic_codes()
    assert len(result.spans) == 2


@pytest.mark.asyncio
async def test_trace_read_client_reports_partial_status_for_source_failure():
    """Source failures are diagnostics instead of hard read failures."""
    result = await TraceReadClient(sources=[FailingTraceSource()]).get_trace(
        "trace-source-failed",
        session_id="session-1",
    )

    assert result.status == "partial"
    assert "source_timeout" in result.diagnostic_codes()
    assert result.trace.trace_id == "trace-source-failed"


@pytest.mark.asyncio
async def test_trace_read_client_lists_traces_by_session_index():
    """TraceReadClient list_traces reads the session trace index."""
    redis = QueryRedis()
    writer = TraceWriteClient(redis)
    await writer.record_trace(
        TraceRecord(
            trace_id="trace-1",
            name="client.dispatch:planner",
            session_id="session-1",
            root_agent_type="planner",
            status="COMPLETED",
            start_ts=100,
            output={"answer": "one"},
        )
    )
    await writer.record_trace(
        TraceRecord(
            trace_id="trace-2",
            name="client.dispatch:writer",
            session_id="session-1",
            root_agent_type="writer",
            status="COMPLETED",
            start_ts=200,
            output={"answer": "two"},
        )
    )

    traces = await TraceReadClient(redis_client=redis).list_traces(
        session_id="session-1", limit=10
    )

    assert [result.trace.trace_id for result in traces] == ["trace-2", "trace-1"]


@pytest.mark.asyncio
async def test_trace_read_client_lists_trace_details_concurrently():
    """Trace list details should be fetched concurrently, not one by one."""
    client = TraceReadClient(sources=[SlowTraceSource()])

    started = time.perf_counter()
    traces = await client.list_traces(session_id="session-1", limit=3)
    elapsed = time.perf_counter() - started

    assert [result.trace.trace_id for result in traces] == [
        "trace-1",
        "trace-2",
        "trace-3",
    ]
    assert elapsed < 0.2


@pytest.mark.asyncio
async def test_trace_read_client_lists_traces_by_worker_and_agent_indexes():
    """TraceReadClient list_traces supports worker and agent trace indexes."""
    redis = QueryRedis()
    writer = TraceWriteClient(redis)
    await writer.record_span(
        SpanRecord(
            trace_id="trace-worker",
            span_id="worker",
            operation="worker.execute",
            component="worker",
            start_ts=100,
            end_ts=110,
            worker_id="worker-1",
            target_agent_type="planner",
        )
    )
    await writer.record_trace(
        TraceRecord(
            trace_id="trace-agent",
            name="client.dispatch:writer",
            root_agent_type="writer",
            status="COMPLETED",
            start_ts=200,
            output={"answer": "ok"},
        )
    )

    worker_results = await TraceReadClient(redis_client=redis).list_traces(
        worker_id="worker-1", limit=10
    )
    agent_results = await TraceReadClient(redis_client=redis).list_traces(
        agent_type="writer", limit=10
    )

    assert [result.trace.trace_id for result in worker_results] == ["trace-worker"]
    assert [result.trace.trace_id for result in agent_results] == ["trace-agent"]


@pytest.mark.asyncio
async def test_trace_read_client_lists_traces_from_session_registry_fallback():
    """Trace listing falls back to session executions when trace index is absent."""
    redis = QueryRedis()
    writer = TraceWriteClient(redis)
    await writer.record_execution(
        ExecutionRecord(
            execution_id="exec-1",
            message_id="msg-1",
            trace_id="trace-fallback",
            session_id="session-fallback",
            status="COMPLETED",
            timing={"created_at": 100},
        )
    )
    await writer.record_span(
        SpanRecord(
            trace_id="trace-fallback",
            span_id="worker",
            operation="worker.execute",
            component="worker",
            start_ts=100,
            end_ts=200,
            session_id="session-fallback",
        )
    )

    traces = await TraceReadClient(redis_client=redis).list_traces(
        session_id="session-fallback", limit=10
    )

    assert [result.trace.trace_id for result in traces] == ["trace-fallback"]
    assert traces[0].spans[0].span_id == "worker"


@pytest.mark.asyncio
async def test_trace_explain_correlates_metrics_by_trace_time_window():
    """Trace explanation includes metrics for the trace start/end window."""
    redis = QueryRedis()
    metrics_client = RecordingMetricsClient()
    writer = TraceWriteClient(redis)
    await writer.record_trace(
        TraceRecord(
            trace_id="trace-explain",
            session_id="session-explain",
            start_ts=100,
            end_ts=200,
            status="COMPLETED",
        )
    )
    await writer.record_span(
        SpanRecord(
            trace_id="trace-explain",
            span_id="worker",
            operation="worker.execute",
            component="worker",
            start_ts=120,
            end_ts=180,
        )
    )

    explanation = await TraceReadClient(
        redis_client=redis,
        metrics_client=metrics_client,
    ).explain_trace(
        "trace-explain",
        session_id="session-explain",
        metrics_buffer_ms=10,
    )

    assert explanation["time_window"] == {
        "start_ts": 100,
        "end_ts": 200,
        "duration_ms": 100,
    }
    assert metrics_client.calls == [
        {"start_ts": 100, "end_ts": 200, "buffer_ms": 10, "limit": 120}
    ]
    assert explanation["related_metrics"]["summary"]["queue_depth_total"]["max"] == 3


@pytest.mark.asyncio
async def test_trace_explain_keeps_trace_result_when_metrics_read_fails():
    """Metrics correlation is best-effort and does not fail trace explain."""
    redis = QueryRedis()
    writer = TraceWriteClient(redis)
    await writer.record_span(
        SpanRecord(
            trace_id="trace-metrics-fail",
            span_id="client",
            operation="client.dispatch",
            component="client",
            start_ts=100,
            end_ts=150,
        )
    )

    explanation = await TraceReadClient(
        redis_client=redis,
        metrics_client=FailingMetricsClient(),
    ).explain_trace("trace-metrics-fail")

    assert explanation["span_count"] == 1
    assert explanation["related_metrics"]["status"] == "partial"
    assert explanation["related_metrics"]["diagnostics"][0]["code"] == (
        "metrics_source_failed"
    )


@pytest.mark.asyncio
async def test_dashboard_trace_endpoints_use_trace_read_sdk_shape():
    """Dashboard trace routes keep the legacy response shape through the SDK."""
    redis = QueryRedis()
    writer = TraceWriteClient(redis)
    await writer.record_trace(
        TraceRecord(
            trace_id="trace-dashboard",
            name="client.dispatch:planner",
            session_id="session-dashboard",
            root_agent_type="planner",
            status="COMPLETED",
            start_ts=100,
            end_ts=200,
            output={"answer": "ok"},
        )
    )
    await writer.record_span(
        SpanRecord(
            trace_id="trace-dashboard",
            span_id="client",
            operation="client.dispatch",
            component="client",
            start_ts=100,
            end_ts=110,
            session_id="session-dashboard",
            target_agent_type="planner",
        )
    )

    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(redis_client=redis))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.server_address[1]
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        connection.request(
            "GET",
            "/api/trace/trace-dashboard?session_id=session-dashboard",
        )
        trace_response = connection.getresponse()
        trace_payload = json.loads(trace_response.read().decode("utf-8"))

        connection.request("GET", "/api/traces?session_id=session-dashboard")
        traces_response = connection.getresponse()
        traces_payload = json.loads(traces_response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert trace_response.status == 200
    assert trace_payload["trace_id"] == "trace-dashboard"
    assert trace_payload["spans"]
    assert "diagnostics" in trace_payload
    assert traces_response.status == 200
    assert traces_payload["traces"][0]["trace_id"] == "trace-dashboard"
    assert traces_payload["traces"][0]["span_count"] == trace_payload["span_count"]


@pytest.mark.asyncio
async def test_trace_write_client_records_trace_root_start():
    """TraceWriteClient persists start_ts and QUEUED status for trace root."""
    redis = QueryRedis()
    writer = TraceWriteClient(redis)
    await writer.record_trace(
        TraceRecord(
            trace_id="trace-root-start",
            name="planner",
            session_id="session-x",
            root_message_id="msg-1",
            root_agent_type="planner",
            input={"text": "hello"},
            status="QUEUED",
            start_ts=1000,
        )
    )

    meta = {
        k.decode() if isinstance(k, bytes) else k: decode_redis_value(v)
        for k, v in (
            await redis.hgetall(RedisKeys.trace_meta("trace-root-start"))
        ).items()
    }
    assert meta["trace_id"] == "trace-root-start"
    assert meta["status"] == "QUEUED"
    assert int(meta["start_ts"]) == 1000
    assert meta["root_agent_type"] == "planner"
    assert meta["root_message_id"] == "msg-1"
    assert meta.get("input") == {"text": "hello"}


@pytest.mark.asyncio
async def test_trace_write_client_records_trace_root_end():
    """TraceWriteClient persists end_ts, status and output when root finishes."""
    redis = QueryRedis()
    writer = TraceWriteClient(redis)
    # Simulate the start record written by the client
    await writer.record_trace(
        TraceRecord(
            trace_id="trace-root-end",
            name="planner",
            root_agent_type="planner",
            status="QUEUED",
            start_ts=1000,
        )
    )
    # Simulate the end record written by the worker
    await writer.record_trace(
        TraceRecord(
            trace_id="trace-root-end",
            status="COMPLETED",
            end_ts=5000,
            output={"answer": "42"},
        )
    )

    meta = {
        k.decode() if isinstance(k, bytes) else k: decode_redis_value(v)
        for k, v in (
            await redis.hgetall(RedisKeys.trace_meta("trace-root-end"))
        ).items()
    }
    assert meta["status"] == "COMPLETED"
    assert int(meta["end_ts"]) == 5000
    assert meta.get("output") == {"answer": "42"}


@pytest.mark.asyncio
async def test_trace_read_client_returns_start_ts_from_trace_record():
    """TraceReadClient propagates start_ts written by the client into the result."""
    redis = QueryRedis()
    writer = TraceWriteClient(redis)
    await writer.record_trace(
        TraceRecord(
            trace_id="trace-ts-check",
            name="scheduler",
            session_id="sess-ts",
            root_agent_type="scheduler",
            status="QUEUED",
            start_ts=2000,
        )
    )
    from by_framework_trace_query.client import TraceReadClient as TRC

    client = TRC(redis_client=redis)
    result = await client.get_trace("trace-ts-check", session_id="sess-ts")
    assert result.trace.start_ts == 2000
    assert result.trace.root_agent_type == "scheduler"
