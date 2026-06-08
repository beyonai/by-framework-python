"""Tests for observability dashboard static serving helpers."""

import asyncio
import http.client
import json
import threading
from http.server import ThreadingHTTPServer

import pytest

from by_framework.observability.dashboard import (
    DashboardAsyncRunner,
    DashboardRuntimeState,
    build_dashboard_runtime_metrics,
    make_handler,
    read_static_asset,
    record_history_snapshot,
    serialize_json,
    serialize_text,
)
from by_framework.observability.snapshot import build_demo_observability_snapshot


def test_read_static_asset_loads_dashboard_index():
    """Dashboard static helper returns packaged HTML and content type."""
    body, content_type = read_static_asset("index.html")

    assert content_type == "text/html; charset=utf-8"
    assert b"by-framework observability" in body


def test_serialize_json_uses_utf8_response_contract():
    """JSON helper returns encoded API payloads with dashboard content type."""
    body, content_type = serialize_json({"status": "ok"})

    assert content_type == "application/json; charset=utf-8"
    assert json.loads(body.decode("utf-8")) == {"status": "ok"}


def test_serialize_text_uses_plain_text_response_contract():
    """Text helper returns UTF-8 bytes for metrics-style payloads."""
    body, content_type = serialize_text("metric 1\n")

    assert content_type == "text/plain; version=0.0.4; charset=utf-8"
    assert body == b"metric 1\n"


def test_record_history_snapshot_caps_points():
    """Dashboard history cache stores compact points with a fixed limit."""
    history = []
    snapshot = build_demo_observability_snapshot()

    record_history_snapshot(history, snapshot, limit=2)
    second = {**snapshot, "generated_at": snapshot["generated_at"] + 1}
    third = {**snapshot, "generated_at": snapshot["generated_at"] + 2}
    record_history_snapshot(history, second, limit=2)
    record_history_snapshot(history, third, limit=2)

    assert len(history) == 2
    assert history[0]["generated_at"] == second["generated_at"]
    assert history[1]["generated_at"] == third["generated_at"]
    assert "queue_depth_total" in history[0]


def test_dashboard_async_runner_reuses_event_loop(monkeypatch):
    """Live dashboard requests share one loop so Redis clients are not loop-stale."""

    async def noop_close_redis():
        return None

    monkeypatch.setattr(
        "by_framework.observability.dashboard.close_redis", noop_close_redis
    )
    runner = DashboardAsyncRunner()

    async def current_loop_id():
        return id(asyncio.get_running_loop())

    try:
        first_loop_id = runner.run(current_loop_id())
        second_loop_id = runner.run(current_loop_id())
    finally:
        runner.close()

    assert first_loop_id == second_loop_id


def test_dashboard_runtime_state_tracks_success_and_errors():
    """Runtime state exposes dashboard API health and recent failure context."""
    state = DashboardRuntimeState(started_at_ms=1000)

    state.record_success(route="/api/health", duration_ms=5, now_ms=2000)
    state.record_error(
        "/api/workers", RuntimeError("redis unavailable"), duration_ms=42, now_ms=3000
    )

    assert state.to_payload(now_ms=4000) == {
        "status": "degraded",
        "started_at": 1000,
        "uptime_ms": 3000,
        "api_success_count": 1,
        "api_error_count": 1,
        "last_success_at": 2000,
        "last_error_at": 3000,
        "last_error_route": "/api/workers",
        "last_error_type": "RuntimeError",
        "last_error_message": "redis unavailable",
        "routes": [
            {
                "route": "/api/health",
                "request_count": 1,
                "error_count": 0,
                "last_duration_ms": 5,
                "max_duration_ms": 5,
                "last_error_type": "",
            },
            {
                "route": "/api/workers",
                "request_count": 1,
                "error_count": 1,
                "last_duration_ms": 42,
                "max_duration_ms": 42,
                "last_error_type": "RuntimeError",
            },
        ],
    }


def test_build_dashboard_runtime_metrics_exports_self_observability():
    """Dashboard runtime health is available to Prometheus scrapers."""
    state = DashboardRuntimeState(started_at_ms=1000)
    state.record_success(route="/api/health", duration_ms=5, now_ms=2000)
    state.record_error(
        "/api/workers", RuntimeError("redis unavailable"), duration_ms=42, now_ms=3000
    )

    metrics = build_dashboard_runtime_metrics(state, now_ms=4000)

    assert "by_framework_dashboard_uptime_ms 3000" in metrics
    assert "by_framework_dashboard_api_success_total 1" in metrics
    assert "by_framework_dashboard_api_errors_total 1" in metrics
    assert 'by_framework_dashboard_runtime_status{status="degraded"} 1' in metrics
    assert (
        'by_framework_dashboard_last_error_info{route="/api/workers",'
        'error_type="RuntimeError"} 1'
    ) in metrics
    assert (
        'by_framework_dashboard_route_requests_total{route="/api/workers"} 1' in metrics
    )
    assert (
        'by_framework_dashboard_route_errors_total{route="/api/workers"} 1' in metrics
    )
    assert (
        'by_framework_dashboard_route_last_duration_ms{route="/api/workers"} 42'
        in metrics
    )
    assert (
        'by_framework_dashboard_route_max_duration_ms{route="/api/workers"} 42'
        in metrics
    )


def test_flow_endpoint_returns_backend_data_flow_model():
    """Flow API exposes backend-computed data flow visualization data."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        connection.request("GET", "/api/flow?demo=1")
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert response.status == 200
    assert payload["data_flow"]["summary"]["queue_depth_total"] == 9
    assert [node["id"] for node in payload["data_flow"]["nodes"]] == [
        "client",
        "control_queues",
        "workers",
        "data_stream",
        "websocket_backend",
        "control_plane",
    ]
    assert payload["data_flow"]["edges"][0]["source"] == "client"


def test_dashboard_auth_token_protects_api_routes():
    """Dashboard API routes require the configured bearer token."""
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), make_handler(auth_token="secret-token")
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.server_address[1]
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        connection.request("GET", "/api/health")
        denied = connection.getresponse()
        denied_payload = json.loads(denied.read().decode("utf-8"))

        connection.request(
            "GET",
            "/api/health",
            headers={"Authorization": "Bearer secret-token"},
        )
        allowed = connection.getresponse()
        allowed_payload = json.loads(allowed.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert denied.status == 401
    assert denied_payload["error"] == "unauthorized"
    assert allowed.status == 200
    assert allowed_payload["status"] == "ok"


def test_trace_endpoints_return_demo_trace_data():
    """Trace APIs expose trace detail, timeline, and trace summaries."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        port = server.server_address[1]
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        connection.request("GET", "/api/trace/trace-demo?demo=1")
        trace_response = connection.getresponse()
        trace_payload = json.loads(trace_response.read().decode("utf-8"))

        connection.request("GET", "/api/trace/trace-demo/timeline?demo=1")
        timeline_response = connection.getresponse()
        timeline_payload = json.loads(timeline_response.read().decode("utf-8"))

        connection.request("GET", "/api/traces?demo=1")
        traces_response = connection.getresponse()
        traces_payload = json.loads(traces_response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert trace_response.status == 200
    assert trace_payload["trace_id"] == "trace-demo"
    assert trace_payload["spans"]
    assert trace_payload["tree"]
    assert timeline_response.status == 200
    assert timeline_payload["trace_id"] == "trace-demo"
    assert all("offset_ms" in item for item in timeline_payload["timeline"])
    assert traces_response.status == 200
    assert traces_payload["traces"][0]["trace_id"] == "trace-demo"
    assert traces_payload["traces"][0]["span_count"] == trace_payload["span_count"]


def test_live_trace_endpoint_allows_trace_id_without_session_id(monkeypatch):
    """Live trace detail can read dedicated trace storage by trace_id alone."""

    async def fake_build_trace(
        redis_client, trace_id, *, session_id="", event_limit=100
    ):
        return {
            "generated_at": 1234,
            "trace_id": trace_id,
            "session_id": session_id,
            "status": "COMPLETED",
            "duration_ms": 10,
            "span_count": 1,
            "spans": [
                {
                    "trace_id": trace_id,
                    "span_id": "span-1",
                    "parent_span_id": "",
                    "operation": "worker.execute",
                    "component": "worker",
                    "start_ts": 100,
                    "end_ts": 110,
                    "duration_ms": 10,
                    "status": "COMPLETED",
                }
            ],
            "tree": [],
            "timeline": [],
        }

    monkeypatch.setattr(
        "by_framework.observability.dashboard.build_trace_observability_snapshot",
        fake_build_trace,
    )
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        connection.request("GET", "/api/trace/trace-live")
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert response.status == 200
    assert payload["trace_id"] == "trace-live"
    assert payload["session_id"] == ""
    assert payload["spans"][0]["operation"] == "worker.execute"


@pytest.mark.asyncio
async def test_trace_fallback_routing(monkeypatch):
    """Fallback Trace retrieval maps external Jaeger JSON format successfully."""
    from unittest.mock import MagicMock

    from by_framework.observability.dashboard import _fetch_trace_from_fallback

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "data": [
            {
                "traceID": "trace-fallback-123",
                "spans": [
                    {
                        "spanID": "span-ext-1",
                        "operationName": "agent.process",
                        "startTime": 1000000,
                        "duration": 500000,
                        "tags": [
                            {"key": "component", "value": "agent_runner"},
                            {"key": "session_id", "value": "sess-fallback"},
                        ],
                        "references": [],
                    }
                ],
            }
        ]
    }

    # Mock httpx.AsyncClient
    class FakeAsyncClient:

        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

        async def get(self, url):
            return mock_response

    import by_framework.observability.dashboard as dashboard

    monkeypatch.setattr(dashboard, "_fallback_http_client_class", FakeAsyncClient)

    trace = await _fetch_trace_from_fallback(
        "trace-fallback-123", "http://jaeger/{trace_id}"
    )
    assert trace is not None
    assert trace["trace_id"] == "trace-fallback-123"
    assert trace["session_id"] == "sess-fallback"
    assert trace["spans"][0]["operation"] == "agent.process"
    assert trace["spans"][0]["component"] == "agent_runner"
    assert trace["spans"][0]["duration_ms"] == 500


def test_trace_fallback_api_routing(monkeypatch):
    """API routing for external fallback trace returns trace snap successfully."""
    mock_trace = {
        "generated_at": 1234,
        "trace_id": "trace-fallback-api-123",
        "session_id": "sess-fallback-api",
        "status": "COMPLETED",
        "duration_ms": 500,
        "span_count": 1,
        "spans": [
            {
                "trace_id": "trace-fallback-api-123",
                "span_id": "span-ext-1",
                "parent_span_id": "",
                "operation": "agent.process",
                "component": "agent_runner",
                "start_ts": 1000,
                "end_ts": 1500,
                "duration_ms": 500,
                "status": "COMPLETED",
            }
        ],
        "tree": [],
        "timeline": [],
    }

    async def fake_fetch(trace_id, fallback_url):
        del trace_id, fallback_url
        return mock_trace

    async def fake_build(redis_client, trace_id, *, session_id="", event_limit=100):
        del redis_client, trace_id, session_id, event_limit
        return {"status": "UNKNOWN", "spans": []}

    import by_framework.observability.dashboard as dashboard

    monkeypatch.setattr(dashboard, "_fetch_trace_from_fallback", fake_fetch)
    monkeypatch.setattr(dashboard, "build_trace_observability_snapshot", fake_build)
    monkeypatch.setenv(
        "BYAI_TRACE_FALLBACK_URL", "http://jaeger:16686/api/traces/{trace_id}"
    )

    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        connection.request("GET", "/api/trace/trace-fallback-api-123")
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert response.status == 200
    assert payload["trace_id"] == "trace-fallback-api-123"
    assert payload["session_id"] == "sess-fallback-api"
    assert payload["spans"][0]["operation"] == "agent.process"
