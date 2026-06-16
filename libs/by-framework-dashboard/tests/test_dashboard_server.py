"""Tests for observability dashboard static serving helpers."""

# pylint: disable=line-too-long

import asyncio
import http.client
import json
import threading
from http.server import ThreadingHTTPServer

import pytest
from by_framework.metrics.snapshot import build_demo_observability_snapshot

from by_framework_dashboard.dashboard import (
    DashboardAsyncRunner,
    DashboardRuntimeState,
    build_dashboard_runtime_metrics,
    make_handler,
    read_static_asset,
    record_history_snapshot,
    serialize_json,
    serialize_text,
)


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
        "by_framework_dashboard.dashboard.close_redis", noop_close_redis
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


def test_alerts_endpoint_returns_actionable_demo_alerts():
    """Alert center API returns normalized alerts with remediation guidance."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        connection.request("GET", "/api/alerts?demo=1")
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert response.status == 200
    assert payload["summary"]["total"] >= 1
    assert payload["alerts"][0]["status"] == "open"
    assert payload["alerts"][0]["recommendations"]
    assert payload["recommendations"]


def test_actions_endpoint_returns_demo_action_center():
    """Action center API returns backend-derived operational tasks."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        connection.request("GET", "/api/actions?demo=1")
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert response.status == 200
    assert payload["summary"]["total"] >= 1
    assert payload["actions"][0]["target_view"]
    assert payload["actions"][0]["recommendations"]


def test_action_detail_endpoint_returns_demo_action_context():
    """Action detail API backs risk and to-do drill-downs."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        connection.request("GET", "/api/actions?demo=1")
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
        action_id = payload["actions"][0]["id"]

        connection.request("GET", f"/api/actions/{action_id}?demo=1")
        detail_response = connection.getresponse()
        detail = json.loads(detail_response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert response.status == 200
    assert detail_response.status == 200
    assert detail["action"]["id"] == action_id
    assert detail["recommendations"]
    assert "related" in detail


def test_action_detail_endpoint_404_for_unknown_action():
    """Unknown action inspect requests fail explicitly."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        connection.request("GET", "/api/actions/not-real?demo=1")
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert response.status == 404
    assert payload["error"] == "action not found"


def test_worker_detail_endpoint_returns_demo_worker_context():
    """Worker detail API backs worker row drill-down."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        connection.request("GET", "/api/workers/worker-planner-1?demo=1")
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert response.status == 200
    assert payload["worker"]["worker_id"] == "worker-planner-1"
    assert payload["executions"]
    assert payload["recommendations"]


def test_metrics_catalog_endpoint_returns_metric_metadata():
    """Metrics catalog API exposes normalized metric meaning and debug split."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        connection.request("GET", "/api/metrics/catalog")
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert response.status == 200
    assert payload["total"] == len(payload["metrics"])
    assert payload["core_count"] > 0
    assert payload["debug_count"] > 0
    total_duration = payload["metrics"]["by_framework_execution_total_duration_seconds"]
    assert total_duration["kind"] == "histogram"
    assert total_duration["unit"] == "seconds"
    assert total_duration["labels"] == ["status", "agent_type"]
    assert total_duration["description"]
    assert total_duration["interpretation"]
    legacy_latency = payload["metrics"]["by_framework_execution_latency_ms"]
    assert legacy_latency["debug_only"] is True
    assert "worker_id" in legacy_latency["labels"]


def test_execution_detail_includes_agent_config_audit_projection(monkeypatch):
    """Execution drill-down exposes the config projection captured for that run."""
    base_snapshot = build_demo_observability_snapshot()
    execution = base_snapshot["recent_executions"][0]
    execution["execution_id"] = "exec-with-agent-config"
    execution["target_agent_type"] = "weather-agent"
    execution["agent_config_audit"] = {
        "version": 3,
        "target_agent_type": "weather-agent",
        "snapshot_hash": "sha256:test",
        "target_agent_config": {
            "agent_id": "weather-agent",
            "name": "Weather Agent",
            "tools": {"weather_api": {"config_hash": "sha256:tool"}},
        },
    }

    def fake_snapshot(*_args, **_kwargs):
        return base_snapshot

    monkeypatch.setattr(
        "by_framework_dashboard.dashboard.build_demo_observability_snapshot",
        fake_snapshot,
    )
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        connection.request("GET", "/api/executions/exec-with-agent-config?demo=1")
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert response.status == 200
    assert payload["agent_config"]["target_agent_type"] == "weather-agent"
    assert payload["agent_config"]["target_agent_config"]["agent_id"] == (
        "weather-agent"
    )


def test_worker_detail_endpoint_404_for_unknown_worker():
    """Unknown worker drill-down requests fail explicitly."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        connection.request("GET", "/api/workers/not-a-worker?demo=1")
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert response.status == 404
    assert payload["error"] == "worker not found"


def test_execution_detail_endpoint_returns_demo_execution_context():
    """Execution detail API backs execution row drill-down."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        connection.request("GET", "/api/executions/exec-demo-failed?demo=1")
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert response.status == 200
    assert payload["execution"]["execution_id"] == "exec-demo-failed"
    assert payload["failures"]
    assert payload["recommendations"]


def test_execution_detail_endpoint_404_for_unknown_execution():
    """Unknown execution drill-down requests fail explicitly."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        connection.request("GET", "/api/executions/not-an-exec?demo=1")
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert response.status == 404
    assert payload["error"] == "execution not found"


def test_queue_detail_endpoint_returns_demo_queue_guidance():
    """Queue detail API backs the row-level queue inspect action."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        connection.request("GET", "/api/queues/planner?demo=1")
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert response.status == 200
    assert payload["queue"]["name"] == "planner"
    assert payload["queue"]["pending_total"] == 1
    assert payload["queue"]["status"] == "warning"
    assert payload["recommendations"]


def test_queue_detail_endpoint_404_for_unknown_queue():
    """Unknown queue inspect requests fail explicitly."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        connection.request("GET", "/api/queues/not-a-real-queue?demo=1")
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert response.status == 404
    assert payload["error"] == "queue not found"


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


def test_config_endpoint_reports_dashboard_capabilities():
    """Config API backs the dashboard configuration page."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        connection.request("GET", "/api/config?demo=1")
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert response.status == 200
    assert payload["dashboard"]["demo_mode"] is True
    assert payload["observability"]["history_limit"] > 0
    assert any(cap["id"] == "exports" for cap in payload["capabilities"])


def test_export_endpoint_returns_scoped_demo_payload():
    """Export API returns a downloadable JSON envelope for selected scope."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        connection.request("GET", "/api/export?demo=1&scope=alerts")
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert response.status == 200
    assert payload["scope"] == "alerts"
    assert payload["format"] == "json"
    assert payload["payload"]["summary"]["total"] >= 1


def test_export_endpoint_rejects_unknown_scope():
    """Export API rejects unsupported scopes with a clear error."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        connection.request("GET", "/api/export?demo=1&scope=bogus")
        response = connection.getresponse()
        payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert response.status == 400
    assert "unsupported export scope" in payload["error"]


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
    assert trace_payload["metrics_window"]["summary"]["sample_count"] >= 1
    assert "slo_window" in trace_payload["metrics_window"]["summary"]
    assert {
        item["category"]
        for item in trace_payload["metrics_window"]["summary"]["signal_explain"]
    } == {"queue", "worker", "errors"}
    queue_signal = next(
        item
        for item in trace_payload["metrics_window"]["summary"]["signal_explain"]
        if item["category"] == "queue"
    )
    assert "max_delivery_count" in queue_signal["metrics"]
    assert trace_payload["metrics_window"]["diagnostics"]
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
        "by_framework_dashboard.dashboard.build_trace_observability_snapshot",
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

    from by_framework_dashboard.dashboard import _fetch_trace_from_fallback

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

    import by_framework_dashboard.dashboard as dashboard

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

    import by_framework_dashboard.dashboard as dashboard

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


# --- Admin route tests ---


def _make_admin_server(monkeypatch):
    """Spin up a dashboard server with WorkerManager fully mocked."""
    from unittest.mock import AsyncMock, MagicMock

    async def fake_build_workers(*args, **kwargs):
        del args, kwargs
        return {
            "generated_at": 9000,
            "totals": {"workers_online": 1, "agent_types": 1},
            "status_counts": {},
            "workers": [
                {
                    "worker_id": "w1",
                    "online": True,
                    "agent_types": ["chat"],
                    "last_seen": 8000,
                    "ip_address": "192.168.1.1",
                    "lifecycle": "active",
                    "lifecycle_reason": "",
                    "active_count": 0,
                    "total_tracked": 0,
                    "counts": {},
                    "status_counts": {},
                }
            ],
            "agent_types": ["chat"],
            "worker_scan": {},
            "alerts": [],
            "health": {"status": "ok"},
        }

    monkeypatch.setattr(
        "by_framework_dashboard.dashboard.build_worker_observability_snapshot",
        fake_build_workers,
    )

    mgr_mock = MagicMock()
    mgr_mock.suspend_worker = AsyncMock()
    mgr_mock.resume_worker = AsyncMock()
    mgr_mock.evict_worker = AsyncMock()
    mgr_mock.allow_worker_rejoin = AsyncMock()
    mgr_mock.deny_worker_for_type = AsyncMock()
    mgr_mock.allow_worker_for_type = AsyncMock()
    mgr_mock.get_type_denylist = AsyncMock(return_value=["w2"])

    monkeypatch.setattr(
        "by_framework_dashboard.dashboard.WorkerManager",
        lambda *a, **kw: mgr_mock,
    )

    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, mgr_mock


def _post(connection, path, body=None):
    payload = json.dumps(body or {}).encode("utf-8")
    connection.request(
        "POST",
        path,
        body=payload,
        headers={
            "Content-Type": "application/json",
            "Content-Length": str(len(payload)),
        },
    )
    resp = connection.getresponse()
    return resp, json.loads(resp.read().decode("utf-8"))


def test_api_workers_includes_lifecycle_and_ip(monkeypatch):
    """/api/workers response includes lifecycle, lifecycle_reason, ip_address per worker."""
    server, thread, _ = _make_admin_server(monkeypatch)
    try:
        conn = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        conn.request("GET", "/api/workers")
        resp = conn.getresponse()
        payload = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert resp.status == 200
    w = payload["workers"][0]
    assert w["lifecycle"] == "active"
    assert "lifecycle_reason" in w
    assert w["ip_address"] == "192.168.1.1"


def test_admin_denylist_get_returns_denied_workers(monkeypatch):
    """GET /api/admin/type/{t}/denylist returns denied worker IDs."""
    server, thread, mgr_mock = _make_admin_server(monkeypatch)
    try:
        conn = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        conn.request("GET", "/api/admin/type/chat/denylist")
        resp = conn.getresponse()
        payload = json.loads(resp.read().decode("utf-8"))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert resp.status == 200
    assert payload["agent_type"] == "chat"
    assert payload["denied"] == ["w2"]
    mgr_mock.get_type_denylist.assert_awaited_once_with("chat")


def test_admin_suspend_calls_worker_manager(monkeypatch):
    """POST /api/admin/worker/{id}/suspend invokes WorkerManager.suspend_worker."""
    server, thread, mgr_mock = _make_admin_server(monkeypatch)
    try:
        conn = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        resp, payload = _post(conn, "/api/admin/worker/w1/suspend", {"reason": "maint"})
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert resp.status == 200
    assert payload["ok"] is True
    assert payload["action"] == "suspend"
    mgr_mock.suspend_worker.assert_awaited_once_with("w1", reason="maint")


def test_admin_resume_calls_worker_manager(monkeypatch):
    """POST /api/admin/worker/{id}/resume invokes WorkerManager.resume_worker."""
    server, thread, mgr_mock = _make_admin_server(monkeypatch)
    try:
        conn = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        resp, payload = _post(conn, "/api/admin/worker/w1/resume")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert resp.status == 200
    assert payload["ok"] is True
    assert payload["action"] == "resume"
    mgr_mock.resume_worker.assert_awaited_once_with("w1")


def test_admin_evict_with_force_calls_worker_manager(monkeypatch):
    """POST /api/admin/worker/{id}/evict with force=true invokes evict_worker(force=True)."""
    server, thread, mgr_mock = _make_admin_server(monkeypatch)
    try:
        conn = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        resp, payload = _post(
            conn, "/api/admin/worker/w1/evict", {"force": True, "reason": "bye"}
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert resp.status == 200
    assert payload["ok"] is True
    assert payload["action"] == "evict"
    mgr_mock.evict_worker.assert_awaited_once_with("w1", force=True, reason="bye")


def test_admin_allow_rejoin_calls_worker_manager(monkeypatch):
    """POST /api/admin/worker/{id}/allow-rejoin clears the admin lifecycle lock."""
    server, thread, mgr_mock = _make_admin_server(monkeypatch)
    try:
        conn = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        resp, payload = _post(conn, "/api/admin/worker/w1/allow-rejoin")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert resp.status == 200
    assert payload["ok"] is True
    assert payload["action"] == "allow-rejoin"
    mgr_mock.allow_worker_rejoin.assert_awaited_once_with("w1")


def test_admin_deny_calls_worker_manager(monkeypatch):
    """POST /api/admin/type/{t}/deny invokes WorkerManager.deny_worker_for_type."""
    server, thread, mgr_mock = _make_admin_server(monkeypatch)
    try:
        conn = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        resp, payload = _post(conn, "/api/admin/type/chat/deny", {"worker_id": "w1"})
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert resp.status == 200
    assert payload["ok"] is True
    assert payload["action"] == "deny"
    mgr_mock.deny_worker_for_type.assert_awaited_once_with("chat", "w1")


def test_admin_allow_calls_worker_manager(monkeypatch):
    """POST /api/admin/type/{t}/allow invokes WorkerManager.allow_worker_for_type."""
    server, thread, mgr_mock = _make_admin_server(monkeypatch)
    try:
        conn = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        resp, payload = _post(conn, "/api/admin/type/chat/allow", {"worker_id": "w1"})
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert resp.status == 200
    assert payload["ok"] is True
    assert payload["action"] == "allow"
    mgr_mock.allow_worker_for_type.assert_awaited_once_with("chat", "w1")


def test_admin_post_requires_auth_token(monkeypatch):
    """Unauthenticated admin POST returns 401 when a token is configured."""
    from unittest.mock import AsyncMock, MagicMock

    mgr_mock = MagicMock()
    mgr_mock.suspend_worker = AsyncMock()
    monkeypatch.setattr(
        "by_framework_dashboard.dashboard.WorkerManager",
        lambda *a, **kw: mgr_mock,
    )

    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(auth_token="secret"))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        conn = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        resp, payload = _post(conn, "/api/admin/worker/w1/suspend", {"reason": "x"})
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert resp.status == 401
    assert payload["error"] == "unauthorized"
    mgr_mock.suspend_worker.assert_not_awaited()


def test_admin_post_unknown_path_returns_404(monkeypatch):
    """POST to an unrecognised /api/admin/... path returns 404."""
    from unittest.mock import MagicMock

    mgr_mock = MagicMock()
    monkeypatch.setattr(
        "by_framework_dashboard.dashboard.WorkerManager",
        lambda *a, **kw: mgr_mock,
    )

    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        conn = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=5
        )
        resp, payload = _post(conn, "/api/admin/unknown/path")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert resp.status == 404
    assert payload["error"] == "not found"
