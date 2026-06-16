"""Serve the built-in observability dashboard."""

# pylint: disable=line-too-long,inconsistent-quotes,invalid-name,too-many-lines

from __future__ import annotations

import argparse
import asyncio
import hmac
import json
import os
import threading
import time
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.resources import files
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from by_framework.admin import WorkerManager
from by_framework.common.config import RedisConfig
from by_framework.common.logger import logger, observability_log_extra
from by_framework.common.redis_client import close_redis, init_redis
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
    load_history_from_redis,
    save_history_point_to_redis,
)

from by_framework_dashboard.adapters import (
    trace_result_to_dashboard_summary,
    trace_result_to_dashboard_trace,
)

METRICS_CACHE_TTL = 15  # Cache /metrics briefly to avoid full Redis scans per scrape.

# A hook for custom HTTP clients (e.g. for testing fallback tracing retrieval)
_fallback_http_client_class = None

STATIC_PACKAGE = "by_framework_dashboard.static"
CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".svg": "image/svg+xml",
}
HISTORY_LIMIT = 120


class DashboardAsyncRunner:
    """Run dashboard async Redis operations on a reusable event loop."""

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._lock = threading.Lock()

    def run(self, coroutine: Any) -> Any:
        """Run an async operation without closing the Redis-bound event loop."""
        with self._lock:
            return self._loop.run_until_complete(coroutine)

    def close(self) -> None:
        """Close Redis resources and the reusable event loop."""
        with self._lock:
            if self._loop.is_closed():
                return
            self._loop.run_until_complete(close_redis())
            self._loop.close()


@dataclass
class DashboardRuntimeState:
    """Track dashboard HTTP/API runtime health."""

    started_at_ms: int
    api_success_count: int = 0
    api_error_count: int = 0
    last_success_at: int = 0
    last_error_at: int = 0
    last_error_route: str = ""
    last_error_type: str = ""
    last_error_message: str = ""
    routes: dict[str, dict[str, Any]] | None = None

    def record_success(
        self,
        *,
        route: str = "",
        duration_ms: int = 0,
        now_ms: int | None = None,
    ) -> None:
        """Record a successful API operation."""
        self.api_success_count += 1
        self.last_success_at = now_ms or _now_ms()
        if route:
            self._record_route(route, duration_ms=duration_ms)

    def record_error(
        self,
        route: str,
        error: Exception,
        *,
        duration_ms: int = 0,
        now_ms: int | None = None,
    ) -> None:
        """Record a failed API operation."""
        self.api_error_count += 1
        self.last_error_at = now_ms or _now_ms()
        self.last_error_route = route
        self.last_error_type = type(error).__name__
        self.last_error_message = str(error)
        self._record_route(
            route,
            duration_ms=duration_ms,
            error_type=self.last_error_type,
        )

    def to_payload(self, *, now_ms: int | None = None) -> dict[str, Any]:
        """Return a JSON-serializable runtime health payload."""
        current_ms = now_ms or _now_ms()
        return {
            "status": "degraded" if self.api_error_count else "ok",
            "started_at": self.started_at_ms,
            "uptime_ms": max(0, current_ms - self.started_at_ms),
            "api_success_count": self.api_success_count,
            "api_error_count": self.api_error_count,
            "last_success_at": self.last_success_at,
            "last_error_at": self.last_error_at,
            "last_error_route": self.last_error_route,
            "last_error_type": self.last_error_type,
            "last_error_message": self.last_error_message,
            "routes": self._routes_payload(),
        }

    def _record_route(
        self,
        route: str,
        *,
        duration_ms: int,
        error_type: str = "",
    ) -> None:
        if self.routes is None:
            self.routes = {}
        current = self.routes.setdefault(
            route,
            {
                "route": route,
                "request_count": 0,
                "error_count": 0,
                "last_duration_ms": 0,
                "max_duration_ms": 0,
                "last_error_type": "",
            },
        )
        current["request_count"] = int(current.get("request_count", 0)) + 1
        current["last_duration_ms"] = max(0, int(duration_ms or 0))
        current["max_duration_ms"] = max(
            int(current.get("max_duration_ms", 0)),
            int(current["last_duration_ms"]),
        )
        if error_type:
            current["error_count"] = int(current.get("error_count", 0)) + 1
            current["last_error_type"] = error_type

    def _routes_payload(self) -> list[dict[str, Any]]:
        if not self.routes:
            return []
        return [self.routes[route] for route in sorted(self.routes)]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _elapsed_ms(started_at_ms: int) -> int:
    return max(0, _now_ms() - started_at_ms)


def serialize_json(payload: dict[str, Any], status: int = 200) -> tuple[bytes, str]:
    """Serialize a JSON response body for dashboard APIs."""
    del status
    return (
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        "application/json; charset=utf-8",
    )


def serialize_text(payload: str) -> tuple[bytes, str]:
    """Serialize a plain-text response body for metrics APIs."""
    return (
        payload.encode("utf-8"),
        "text/plain; version=0.0.4; charset=utf-8",
    )


def build_dashboard_runtime_metrics(
    state: DashboardRuntimeState,
    *,
    now_ms: int | None = None,
) -> str:
    """Render dashboard process self-observability as Prometheus metrics."""
    payload = state.to_payload(now_ms=now_ms)
    status = _escape_metric_label(str(payload["status"]))
    route = _escape_metric_label(str(payload["last_error_route"]))
    error_type = _escape_metric_label(str(payload["last_error_type"]))
    lines = [
        "# HELP by_framework_dashboard_uptime_ms Dashboard process uptime in milliseconds.",
        "# TYPE by_framework_dashboard_uptime_ms gauge",
        f"by_framework_dashboard_uptime_ms {int(payload['uptime_ms'])}",
        "# HELP by_framework_dashboard_api_success_total Dashboard API successes.",
        "# TYPE by_framework_dashboard_api_success_total counter",
        f"by_framework_dashboard_api_success_total {int(payload['api_success_count'])}",
        "# HELP by_framework_dashboard_api_errors_total Dashboard API errors.",
        "# TYPE by_framework_dashboard_api_errors_total counter",
        f"by_framework_dashboard_api_errors_total {int(payload['api_error_count'])}",
        "# HELP by_framework_dashboard_runtime_status Dashboard runtime status.",
        "# TYPE by_framework_dashboard_runtime_status gauge",
        f'by_framework_dashboard_runtime_status{{status="{status}"}} 1',
    ]
    if payload["last_error_route"] or payload["last_error_type"]:
        lines.extend(
            [
                "# HELP by_framework_dashboard_last_error_info Latest dashboard API error labels.",
                "# TYPE by_framework_dashboard_last_error_info gauge",
                (
                    "by_framework_dashboard_last_error_info"
                    f'{{route="{route}",error_type="{error_type}"}} 1'
                ),
            ]
        )
    routes = payload.get("routes", [])
    if routes:
        lines.extend(
            [
                "# HELP by_framework_dashboard_route_requests_total Dashboard API requests by route.",
                "# TYPE by_framework_dashboard_route_requests_total counter",
                "# HELP by_framework_dashboard_route_errors_total Dashboard API errors by route.",
                "# TYPE by_framework_dashboard_route_errors_total counter",
                "# HELP by_framework_dashboard_route_last_duration_ms Latest dashboard API route duration.",
                "# TYPE by_framework_dashboard_route_last_duration_ms gauge",
                "# HELP by_framework_dashboard_route_max_duration_ms Max dashboard API route duration.",
                "# TYPE by_framework_dashboard_route_max_duration_ms gauge",
            ]
        )
    for route_stats in routes:
        route = _escape_metric_label(str(route_stats.get("route", "")))
        lines.append(
            "by_framework_dashboard_route_requests_total"
            f'{{route="{route}"}} {int(route_stats.get("request_count", 0))}'
        )
        lines.append(
            "by_framework_dashboard_route_errors_total"
            f'{{route="{route}"}} {int(route_stats.get("error_count", 0))}'
        )
        lines.append(
            "by_framework_dashboard_route_last_duration_ms"
            f'{{route="{route}"}} {int(route_stats.get("last_duration_ms", 0))}'
        )
        lines.append(
            "by_framework_dashboard_route_max_duration_ms"
            f'{{route="{route}"}} {int(route_stats.get("max_duration_ms", 0))}'
        )
    return "\n".join(lines) + "\n"


def read_static_asset(asset_name: str) -> tuple[bytes, str]:
    """Read a packaged dashboard asset by name."""
    normalized = asset_name.strip("/") or "index.html"
    if "/" in normalized or normalized.startswith("."):
        raise FileNotFoundError(normalized)

    resource = files(STATIC_PACKAGE).joinpath(normalized)
    if not resource.is_file():
        raise FileNotFoundError(normalized)

    suffix = "." + normalized.rsplit(".", 1)[-1] if "." in normalized else ""
    content_type = CONTENT_TYPES.get(suffix, "application/octet-stream")
    return resource.read_bytes(), content_type


def record_history_snapshot(
    history: list[dict[str, int]],
    snapshot: dict[str, Any],
    *,
    limit: int = HISTORY_LIMIT,
    async_runner: DashboardAsyncRunner | None = None,
    redis_client: Any = None,
) -> None:
    """Append a trend point in memory and optionally persist it to Redis."""
    point = build_history_point(snapshot)
    history.append(point)
    del history[: max(0, len(history) - limit)]
    if async_runner is not None and redis_client is not None:
        try:
            async_runner.run(save_history_point_to_redis(redis_client, point))
        except Exception:  # pylint: disable=broad-exception-caught
            pass


async def _fetch_trace_from_fallback(
    trace_id: str, fallback_url_template: str
) -> dict[str, Any] | None:
    """Fetch trace spans from an external APM system when Redis cache expires."""
    client_cls = _fallback_http_client_class
    if client_cls is None:
        import httpx

        client_cls = httpx.AsyncClient

    url = fallback_url_template.replace("{trace_id}", trace_id)
    try:
        async with client_cls(timeout=5.0) as client:
            response = await client.get(url)
            if response.status_code == 200:
                external_data = response.json()
                return _parse_external_trace(trace_id, external_data)
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.warning(
            "Failed to fetch trace %s from fallback %s: %s",
            trace_id,
            url,
            e,
            **observability_log_extra(trace_id=trace_id),
        )
    return None


def _parse_external_trace(trace_id: str, data: dict[str, Any]) -> dict[str, Any] | None:
    """Map external Jaeger trace JSON to our dashboard trace snapshot."""
    try:
        traces = data.get("data", [])
        if not traces:
            return None
        jaeger_trace = traces[0]
        spans = []
        for j_span in jaeger_trace.get("spans", []):
            start_us = int(j_span.get("startTime", 0))
            duration_us = int(j_span.get("duration", 0))
            start_ts = start_us // 1000
            end_ts = (start_us + duration_us) // 1000

            metadata = {}
            for tag in j_span.get("tags", []):
                metadata[tag.get("key")] = tag.get("value")

            parent_span_id = ""
            for ref in j_span.get("references", []):
                if ref.get("refType") == "CHILD_OF":
                    parent_span_id = ref.get("spanID", "")
                    break

            spans.append(
                {
                    "trace_id": trace_id,
                    "span_id": j_span.get("spanID", ""),
                    "parent_span_id": parent_span_id,
                    "operation": j_span.get("operationName", ""),
                    "component": metadata.get("component", "unknown"),
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "duration_ms": duration_us // 1000,
                    "status": (
                        "FAILED"
                        if any(
                            tag.get("key") == "error" and tag.get("value") is True
                            for tag in j_span.get("tags", [])
                        )
                        else "COMPLETED"
                    ),
                    "metadata": metadata,
                }
            )

        from by_framework.metrics.snapshot import _build_trace_snapshot

        session_id = spans[0].get("metadata", {}).get("session_id", "") if spans else ""
        return _build_trace_snapshot(trace_id, session_id, spans)
    except Exception as err:  # pylint: disable=broad-exception-caught
        logger.warning("Error parsing external Jaeger trace: %s", err)
        return None


def make_handler(
    runner: DashboardAsyncRunner | None = None,
    redis_client: Any = None,
    redis_config: RedisConfig | None = None,
    queue_backlog_threshold: int = 100,
    alert_policy: AlertPolicy | None = None,
    runtime_state: DashboardRuntimeState | None = None,
    auth_token: str = "",
) -> type[BaseHTTPRequestHandler]:
    """Build a request handler bound to the dashboard routes."""

    history_points: list[dict[str, int]] = []
    async_runner = runner or DashboardAsyncRunner()
    policy = alert_policy or AlertPolicy(
        queue_backlog_threshold=queue_backlog_threshold
    )
    state = runtime_state or DashboardRuntimeState(started_at_ms=_now_ms())
    required_token = auth_token or os.environ.get("BY_FRAMEWORK_DASHBOARD_TOKEN", "")
    # Cache /metrics snapshots to avoid a full Redis scan on every Prometheus scrape.
    _metrics_cache: dict[str, Any] = {"snapshot": None, "time": 0.0}

    class DashboardHandler(BaseHTTPRequestHandler):
        """HTTP handler for static dashboard assets and JSON snapshots."""

        server_version = "ByFrameworkObservability/0.1"

        def do_GET(self) -> None:  # pylint: disable=invalid-name
            parsed = urlparse(self.path)
            path = parsed.path
            request_started_ms = _now_ms()
            if self._requires_auth(path) and not self._is_authorized():
                self._send_json(
                    {"error": "unauthorized", "status": "error"},
                    status=HTTPStatus.UNAUTHORIZED,
                )
                return
            if path == "/api/health":
                self._send_json(state.to_payload())
                return
            if path == "/api/config":
                params = parse_qs(parsed.query)
                server_host, server_port = self.server.server_address[:2]
                payload = _config_payload(
                    host=str(server_host),
                    port=int(server_port),
                    redis_config=redis_config,
                    auth_enabled=bool(required_token),
                    demo=_truthy_param(params, "demo"),
                    queue_backlog_threshold=queue_backlog_threshold,
                )
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send_json(payload)
                return
            if path == "/api/export":
                params = parse_qs(parsed.query)
                scope = _first_param(params, "scope") or "snapshot"
                try:
                    if _truthy_param(params, "demo"):
                        snapshot = build_demo_observability_snapshot()
                    else:
                        snapshot = async_runner.run(
                            build_observability_snapshot(
                                active_limit=_int_param(params, "active_limit", 25),
                                history_limit=_int_param(params, "history_limit", 20),
                                include_consumer_details=True,
                                worker_scan_limit=_int_param(
                                    params, "worker_scan_limit", 300
                                ),
                                alert_policy=policy,
                            )
                        )
                    payload = _export_payload(snapshot, scope=scope)
                except ValueError as err:
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path,
                        err,
                        duration_ms=_elapsed_ms(request_started_ms),
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send_json(payload)
                return
            if path == "/api/snapshot":
                params = parse_qs(parsed.query)
                if _truthy_param(params, "demo"):
                    self._send_json(build_demo_observability_snapshot())
                    return
                try:
                    snapshot = async_runner.run(
                        build_observability_snapshot(
                            active_limit=_int_param(params, "active_limit", 25),
                            history_limit=_int_param(params, "history_limit", 20),
                            include_consumer_details=_truthy_param(
                                params, "consumer_details"
                            ),
                            worker_scan_limit=_int_param(
                                params, "worker_scan_limit", 300
                            ),
                            alert_policy=policy,
                        )
                    )
                    record_history_snapshot(
                        history_points,
                        snapshot,
                        async_runner=async_runner,
                        redis_client=redis_client,
                    )
                    _metrics_cache["snapshot"] = snapshot
                    _metrics_cache["time"] = time.time()
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path,
                        err,
                        duration_ms=_elapsed_ms(request_started_ms),
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send_json(snapshot)
                return
            if path == "/api/workers":
                params = parse_qs(parsed.query)
                if _truthy_param(params, "demo"):
                    snapshot = build_demo_observability_snapshot()
                    state.record_success(
                        route=path, duration_ms=_elapsed_ms(request_started_ms)
                    )
                    self._send_json(
                        _pick_keys(
                            snapshot,
                            [
                                "generated_at",
                                "totals",
                                "status_counts",
                                "workers",
                                "alerts",
                                "health",
                                "agent_types",
                            ],
                        )
                    )
                    return
                try:
                    snapshot = async_runner.run(
                        build_worker_observability_snapshot(
                            worker_scan_limit=_int_param(
                                params, "worker_scan_limit", 300
                            ),
                            alert_policy=policy,
                        )
                    )
                    record_history_snapshot(
                        history_points,
                        snapshot,
                        async_runner=async_runner,
                        redis_client=redis_client,
                    )
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path,
                        err,
                        duration_ms=_elapsed_ms(request_started_ms),
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send_json(snapshot)
                return
            if path.startswith("/api/workers/"):
                params = parse_qs(parsed.query)
                worker_id = unquote(path.removeprefix("/api/workers/").strip("/"))
                if not worker_id:
                    self._send_json(
                        {"error": "worker_id is required", "status": "error"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                try:
                    if _truthy_param(params, "demo"):
                        snapshot = build_demo_observability_snapshot()
                    else:
                        snapshot = async_runner.run(
                            build_worker_observability_snapshot(
                                worker_scan_limit=_int_param(
                                    params, "worker_scan_limit", 300
                                ),
                                alert_policy=policy,
                            )
                        )
                    detail = _worker_detail_payload(snapshot, worker_id)
                except KeyError:
                    self._send_json(
                        {"error": "worker not found", "status": "error"},
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path,
                        err,
                        duration_ms=_elapsed_ms(request_started_ms),
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route="/api/workers/{worker_id}",
                    duration_ms=_elapsed_ms(request_started_ms),
                )
                self._send_json(detail)
                return
            if path == "/api/flow":
                params = parse_qs(parsed.query)
                if _truthy_param(params, "demo"):
                    snapshot = build_demo_observability_snapshot()
                    state.record_success(
                        route=path, duration_ms=_elapsed_ms(request_started_ms)
                    )
                    self._send_json(
                        _pick_keys(
                            snapshot,
                            ["generated_at", "health", "alerts", "data_flow"],
                        )
                    )
                    return
                try:
                    snapshot = async_runner.run(
                        build_observability_snapshot(
                            active_limit=_int_param(params, "active_limit", 0),
                            history_limit=_int_param(params, "history_limit", 20),
                            include_consumer_details=_truthy_param(
                                params, "consumer_details"
                            ),
                            worker_scan_limit=_int_param(
                                params, "worker_scan_limit", 300
                            ),
                            alert_policy=policy,
                        )
                    )
                    record_history_snapshot(
                        history_points,
                        snapshot,
                        async_runner=async_runner,
                        redis_client=redis_client,
                    )
                    _metrics_cache["snapshot"] = snapshot
                    _metrics_cache["time"] = time.time()
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path,
                        err,
                        duration_ms=_elapsed_ms(request_started_ms),
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send_json(
                    _pick_keys(
                        snapshot,
                        ["generated_at", "health", "alerts", "data_flow"],
                    )
                )
                return
            if path == "/api/executions":
                params = parse_qs(parsed.query)
                if _truthy_param(params, "demo"):
                    snapshot = build_demo_observability_snapshot()
                    state.record_success(
                        route=path, duration_ms=_elapsed_ms(request_started_ms)
                    )
                    self._send_json(
                        _pick_keys(
                            snapshot,
                            [
                                "generated_at",
                                "totals",
                                "status_counts",
                                "recent_executions",
                                "latency",
                                "failures",
                                "alerts",
                                "health",
                                "agent_health",
                                "agent_types",
                            ],
                        )
                    )
                    return
                try:
                    snapshot = async_runner.run(
                        build_execution_observability_snapshot(
                            history_limit=_int_param(params, "history_limit", 20),
                            worker_scan_limit=_int_param(
                                params, "worker_scan_limit", 300
                            ),
                            alert_policy=policy,
                        )
                    )
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path,
                        err,
                        duration_ms=_elapsed_ms(request_started_ms),
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send_json(snapshot)
                return
            if path.startswith("/api/executions/"):
                params = parse_qs(parsed.query)
                execution_id = unquote(path.removeprefix("/api/executions/").strip("/"))
                if not execution_id:
                    self._send_json(
                        {"error": "execution_id is required", "status": "error"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                try:
                    if _truthy_param(params, "demo"):
                        snapshot = build_demo_observability_snapshot()
                    else:
                        snapshot = async_runner.run(
                            build_execution_observability_snapshot(
                                history_limit=_int_param(params, "history_limit", 20),
                                worker_scan_limit=_int_param(
                                    params, "worker_scan_limit", 300
                                ),
                                alert_policy=policy,
                            )
                        )
                    detail = _execution_detail_payload(snapshot, execution_id)
                except KeyError:
                    self._send_json(
                        {"error": "execution not found", "status": "error"},
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path,
                        err,
                        duration_ms=_elapsed_ms(request_started_ms),
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route="/api/executions/{execution_id}",
                    duration_ms=_elapsed_ms(request_started_ms),
                )
                self._send_json(detail)
                return
            if path == "/api/queues":
                params = parse_qs(parsed.query)
                if _truthy_param(params, "demo"):
                    snapshot = build_demo_observability_snapshot()
                    state.record_success(
                        route=path, duration_ms=_elapsed_ms(request_started_ms)
                    )
                    self._send_json(
                        _pick_keys(
                            snapshot, ["generated_at", "queues", "alerts", "health"]
                        )
                    )
                    return
                try:
                    agent_types = _list_param(params, "agent_type")
                    snapshot = async_runner.run(
                        build_queue_observability_snapshot(
                            agent_types=agent_types or None,
                            include_consumer_details=_truthy_param(
                                params, "consumer_details"
                            ),
                            worker_scan_limit=_int_param(
                                params, "worker_scan_limit", 300
                            ),
                            alert_policy=policy,
                        )
                    )
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path,
                        err,
                        duration_ms=_elapsed_ms(request_started_ms),
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send_json(snapshot)
                return
            if path.startswith("/api/queues/"):
                params = parse_qs(parsed.query)
                queue_name = unquote(path.removeprefix("/api/queues/").strip("/"))
                if not queue_name:
                    self._send_json(
                        {"error": "queue name is required", "status": "error"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                try:
                    if _truthy_param(params, "demo"):
                        snapshot = build_demo_observability_snapshot()
                    else:
                        agent_types = _list_param(params, "agent_type")
                        snapshot = async_runner.run(
                            build_queue_observability_snapshot(
                                agent_types=agent_types or None,
                                include_consumer_details=True,
                                worker_scan_limit=_int_param(
                                    params, "worker_scan_limit", 300
                                ),
                                alert_policy=policy,
                            )
                        )
                    detail = _queue_detail_payload(snapshot, queue_name)
                except KeyError:
                    self._send_json(
                        {"error": "queue not found", "status": "error"},
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path,
                        err,
                        duration_ms=_elapsed_ms(request_started_ms),
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route="/api/queues/{name}",
                    duration_ms=_elapsed_ms(request_started_ms),
                )
                self._send_json(detail)
                return
            if path == "/api/alerts":
                params = parse_qs(parsed.query)
                try:
                    if _truthy_param(params, "demo"):
                        snapshot = build_demo_observability_snapshot()
                    else:
                        snapshot = async_runner.run(
                            build_observability_snapshot(
                                active_limit=_int_param(params, "active_limit", 25),
                                history_limit=_int_param(params, "history_limit", 20),
                                include_consumer_details=True,
                                worker_scan_limit=_int_param(
                                    params, "worker_scan_limit", 300
                                ),
                                alert_policy=policy,
                            )
                        )
                        record_history_snapshot(
                            history_points,
                            snapshot,
                            async_runner=async_runner,
                            redis_client=redis_client,
                        )
                    payload = _alerts_payload(snapshot)
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path,
                        err,
                        duration_ms=_elapsed_ms(request_started_ms),
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send_json(payload)
                return
            if path == "/api/actions":
                params = parse_qs(parsed.query)
                try:
                    if _truthy_param(params, "demo"):
                        snapshot = build_demo_observability_snapshot()
                    else:
                        snapshot = async_runner.run(
                            build_observability_snapshot(
                                active_limit=_int_param(params, "active_limit", 25),
                                history_limit=_int_param(params, "history_limit", 20),
                                include_consumer_details=True,
                                worker_scan_limit=_int_param(
                                    params, "worker_scan_limit", 300
                                ),
                                alert_policy=policy,
                            )
                        )
                        record_history_snapshot(
                            history_points,
                            snapshot,
                            async_runner=async_runner,
                            redis_client=redis_client,
                        )
                    payload = _actions_payload(snapshot)
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path,
                        err,
                        duration_ms=_elapsed_ms(request_started_ms),
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send_json(payload)
                return
            if path.startswith("/api/actions/"):
                params = parse_qs(parsed.query)
                action_id = unquote(path.removeprefix("/api/actions/").strip("/"))
                try:
                    if _truthy_param(params, "demo"):
                        snapshot = build_demo_observability_snapshot()
                    else:
                        snapshot = async_runner.run(
                            build_observability_snapshot(
                                active_limit=_int_param(params, "active_limit", 25),
                                history_limit=_int_param(params, "history_limit", 20),
                                include_consumer_details=True,
                                worker_scan_limit=_int_param(
                                    params, "worker_scan_limit", 300
                                ),
                                alert_policy=policy,
                            )
                        )
                    payload = _action_detail_payload(snapshot, action_id)
                except KeyError:
                    state.record_error(
                        "/api/actions/{action_id}",
                        KeyError(action_id),
                        duration_ms=_elapsed_ms(request_started_ms),
                    )
                    self._send_json(
                        {"error": "action not found", "action_id": action_id},
                        status=HTTPStatus.NOT_FOUND,
                    )
                    return
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        "/api/actions/{action_id}",
                        err,
                        duration_ms=_elapsed_ms(request_started_ms),
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route="/api/actions/{action_id}",
                    duration_ms=_elapsed_ms(request_started_ms),
                )
                self._send_json(payload)
                return
            if path == "/api/history":
                params = parse_qs(parsed.query)
                if _truthy_param(params, "demo"):
                    state.record_success(
                        route=path, duration_ms=_elapsed_ms(request_started_ms)
                    )
                    self._send_json(
                        {
                            "generated_at": int(
                                build_demo_observability_snapshot()["generated_at"]
                            ),
                            "points": build_demo_observability_history(),
                        }
                    )
                    return
                # Load Redis history when process memory is empty after a restart.
                active_points = history_points
                if not active_points and redis_client is not None:
                    try:
                        active_points = async_runner.run(
                            load_history_from_redis(redis_client)
                        )
                        history_points.extend(active_points)
                    except Exception:  # pylint: disable=broad-exception-caught
                        pass
                self._send_json(
                    {
                        "generated_at": (
                            active_points[-1]["generated_at"] if active_points else 0
                        ),
                        "points": active_points,
                    }
                )
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                return
            if path == "/api/session":
                params = parse_qs(parsed.query)
                session_id = _first_param(params, "session_id")
                if not session_id:
                    self._send_json(
                        {"error": "session_id is required", "status": "error"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                if _truthy_param(params, "demo"):
                    state.record_success(
                        route=path, duration_ms=_elapsed_ms(request_started_ms)
                    )
                    self._send_json(build_demo_session_observability_snapshot())
                    return
                try:
                    snapshot = async_runner.run(
                        build_session_observability_snapshot(
                            None,
                            session_id,
                            trace_id=_first_param(params, "trace_id"),
                        )
                    )
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path,
                        err,
                        duration_ms=_elapsed_ms(request_started_ms),
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send_json(snapshot)
                return
            if path == "/api/traces":
                params = parse_qs(parsed.query)
                if _truthy_param(params, "demo"):
                    trace = build_demo_trace_observability_snapshot()
                    state.record_success(
                        route=path, duration_ms=_elapsed_ms(request_started_ms)
                    )
                    self._send_json(
                        {
                            "generated_at": _now_ms(),
                            "traces": [_trace_summary(trace)],
                        }
                    )
                    return
                session_id = _first_param(params, "session_id")
                worker_id = _first_param(params, "worker_id")
                agent_type = _first_param(params, "agent_type")
                if not session_id and not worker_id and not agent_type:
                    self._send_json(
                        {
                            "error": (
                                "session_id, worker_id, or agent_type is required "
                                "for live trace listing"
                            ),
                            "status": "error",
                        },
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                try:
                    traces = async_runner.run(
                        _list_trace_summaries_via_read_sdk(
                            redis_client,
                            session_id=session_id,
                            worker_id=worker_id,
                            agent_type=agent_type,
                            limit=_int_param(params, "limit", 50) or 50,
                        )
                    )
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path,
                        err,
                        duration_ms=_elapsed_ms(request_started_ms),
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send_json({"generated_at": _now_ms(), "traces": traces})
                return
            if path.startswith("/api/trace/"):
                params = parse_qs(parsed.query)
                trace_path = path.removeprefix("/api/trace/").strip("/")
                timeline_only = trace_path.endswith("/timeline")
                trace_id = (
                    trace_path.removesuffix("/timeline").strip("/")
                    if timeline_only
                    else trace_path
                )
                if not trace_id:
                    self._send_json(
                        {"error": "trace_id is required", "status": "error"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                if _truthy_param(params, "demo"):
                    trace = build_demo_trace_observability_snapshot()
                    if not timeline_only:
                        trace = {
                            **trace,
                            "metrics_window": _demo_trace_metrics_window(trace),
                        }
                    state.record_success(
                        route=path, duration_ms=_elapsed_ms(request_started_ms)
                    )
                    self._send_json(
                        _trace_timeline_payload(trace) if timeline_only else trace
                    )
                    return
                session_id = _first_param(params, "session_id")
                try:

                    async def _get_trace_with_fallback() -> dict[str, Any]:
                        trace_snap = await _get_trace_snapshot_via_read_sdk(
                            redis_client,
                            trace_id,
                            session_id=session_id,
                        )
                        if (
                            not trace_snap or not trace_snap.get("spans")
                        ) and os.environ.get("BYAI_TRACE_FALLBACK_URL"):
                            fallback_url = os.environ["BYAI_TRACE_FALLBACK_URL"]
                            ext_trace = await _fetch_trace_from_fallback(
                                trace_id, fallback_url
                            )
                            if ext_trace:
                                return ext_trace
                        return trace_snap

                    trace = async_runner.run(_get_trace_with_fallback())
                    if not timeline_only:
                        trace = async_runner.run(
                            _attach_metrics_window(redis_client, trace)
                        )
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path,
                        err,
                        duration_ms=_elapsed_ms(request_started_ms),
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send_json(
                    _trace_timeline_payload(trace) if timeline_only else trace
                )
                return
            if path == "/api/metrics/catalog":
                from by_framework.metrics import get_metric_catalog_payload

                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send_json(get_metric_catalog_payload())
                return
            if path == "/metrics":
                params = parse_qs(parsed.query)
                try:
                    if _truthy_param(params, "demo"):
                        snapshot = build_demo_observability_snapshot()
                    else:
                        # Reuse cached snapshots to avoid a full Redis scan per scrape.
                        cached = _metrics_cache.get("snapshot")
                        age = time.time() - float(_metrics_cache.get("time", 0))
                        if cached is not None and age < METRICS_CACHE_TTL:
                            snapshot = cached
                        else:
                            snapshot = async_runner.run(
                                build_observability_snapshot(
                                    active_limit=_int_param(params, "active_limit", 25),
                                    history_limit=_int_param(
                                        params, "history_limit", 20
                                    ),
                                    include_consumer_details=_truthy_param(
                                        params, "consumer_details"
                                    ),
                                    worker_scan_limit=_int_param(
                                        params, "worker_scan_limit", 300
                                    ),
                                    alert_policy=policy,
                                )
                            )
                            _metrics_cache["snapshot"] = snapshot
                            _metrics_cache["time"] = time.time()
                    from by_framework.metrics import (
                        build_observability_diagnostics_metrics,
                        generate_latest_metrics,
                    )
                    from by_framework.trace.span_recorder import get_observability_diagnostics

                    body, content_type = serialize_text(
                        build_prometheus_metrics(snapshot)
                        + build_dashboard_runtime_metrics(state)
                        + build_observability_diagnostics_metrics(
                            get_observability_diagnostics()
                        )
                        + generate_latest_metrics()
                    )
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path,
                        err,
                        duration_ms=_elapsed_ms(request_started_ms),
                    )
                    body, content_type = serialize_text(f"# error {err}\n")
                    self._send(
                        body,
                        content_type,
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send(body, content_type)
                return

            if path.startswith("/api/admin/type/") and path.endswith("/denylist"):
                agent_type = (
                    path.removeprefix("/api/admin/type/")
                    .removesuffix("/denylist")
                    .strip("/")
                )
                if not agent_type:
                    self._send_json(
                        {"error": "agent_type required", "status": "error"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                try:
                    denied = async_runner.run(
                        WorkerManager(redis_client).get_type_denylist(agent_type)
                    )
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path, err, duration_ms=_elapsed_ms(request_started_ms)
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send_json({"agent_type": agent_type, "denied": denied})
                return

            asset_name = "index.html" if path in ("", "/") else path.removeprefix("/")
            try:
                body, content_type = read_static_asset(asset_name)
            except FileNotFoundError:
                self._send_json(
                    {"error": "not found", "status": "error"},
                    status=HTTPStatus.NOT_FOUND,
                )
                return
            self._send(body, content_type)

        def log_message(self, format: str, *args: Any) -> None:
            """Keep dashboard request logging quiet by default."""

        def _requires_auth(self, path: str) -> bool:
            return bool(required_token) and (
                path.startswith("/api/") or path == "/metrics"
            )

        def _is_authorized(self) -> bool:
            if not required_token:
                return True
            header = self.headers.get("Authorization", "")
            prefix = "Bearer "
            if not header.startswith(prefix):
                return False
            return hmac.compare_digest(header[len(prefix) :].strip(), required_token)

        def _send_json(
            self,
            payload: dict[str, Any],
            status: HTTPStatus = HTTPStatus.OK,
        ) -> None:
            body, content_type = serialize_json(payload, status=int(status))
            self._send(body, content_type, status=status)

        def _send(
            self,
            body: bytes,
            content_type: str,
            status: HTTPStatus = HTTPStatus.OK,
        ) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _read_body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length", 0))
            if length == 0:
                return {}
            try:
                return json.loads(self.rfile.read(length))
            except (json.JSONDecodeError, ValueError):
                return {}

        def do_POST(self) -> None:  # pylint: disable=invalid-name
            parsed = urlparse(self.path)
            path = parsed.path
            request_started_ms = _now_ms()
            if self._requires_auth(path) and not self._is_authorized():
                self._send_json(
                    {"error": "unauthorized", "status": "error"},
                    status=HTTPStatus.UNAUTHORIZED,
                )
                return

            body = self._read_body()
            mgr = WorkerManager(redis_client)

            if path.startswith("/api/admin/worker/") and path.endswith("/suspend"):
                worker_id = (
                    path.removeprefix("/api/admin/worker/")
                    .removesuffix("/suspend")
                    .strip("/")
                )
                try:
                    async_runner.run(
                        mgr.suspend_worker(worker_id, reason=body.get("reason", ""))
                    )
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path, err, duration_ms=_elapsed_ms(request_started_ms)
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send_json(
                    {"ok": True, "worker_id": worker_id, "action": "suspend"}
                )
                return

            if path.startswith("/api/admin/worker/") and path.endswith("/resume"):
                worker_id = (
                    path.removeprefix("/api/admin/worker/")
                    .removesuffix("/resume")
                    .strip("/")
                )
                try:
                    async_runner.run(mgr.resume_worker(worker_id))
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path, err, duration_ms=_elapsed_ms(request_started_ms)
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send_json(
                    {"ok": True, "worker_id": worker_id, "action": "resume"}
                )
                return

            if path.startswith("/api/admin/worker/") and path.endswith("/evict"):
                worker_id = (
                    path.removeprefix("/api/admin/worker/")
                    .removesuffix("/evict")
                    .strip("/")
                )
                try:
                    async_runner.run(
                        mgr.evict_worker(
                            worker_id,
                            force=bool(body.get("force", False)),
                            reason=body.get("reason", ""),
                        )
                    )
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path, err, duration_ms=_elapsed_ms(request_started_ms)
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send_json({"ok": True, "worker_id": worker_id, "action": "evict"})
                return

            if path.startswith("/api/admin/worker/") and path.endswith("/allow-rejoin"):
                worker_id = (
                    path.removeprefix("/api/admin/worker/")
                    .removesuffix("/allow-rejoin")
                    .strip("/")
                )
                try:
                    async_runner.run(mgr.allow_worker_rejoin(worker_id))
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path, err, duration_ms=_elapsed_ms(request_started_ms)
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send_json(
                    {"ok": True, "worker_id": worker_id, "action": "allow-rejoin"}
                )
                return

            if path.startswith("/api/admin/type/") and path.endswith("/deny"):
                agent_type = (
                    path.removeprefix("/api/admin/type/")
                    .removesuffix("/deny")
                    .strip("/")
                )
                worker_id = body.get("worker_id", "")
                if not agent_type or not worker_id:
                    self._send_json(
                        {
                            "error": "agent_type and worker_id required",
                            "status": "error",
                        },
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                try:
                    async_runner.run(mgr.deny_worker_for_type(agent_type, worker_id))
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path, err, duration_ms=_elapsed_ms(request_started_ms)
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send_json(
                    {
                        "ok": True,
                        "agent_type": agent_type,
                        "worker_id": worker_id,
                        "action": "deny",
                    }
                )
                return

            if path.startswith("/api/admin/type/") and path.endswith("/allow"):
                agent_type = (
                    path.removeprefix("/api/admin/type/")
                    .removesuffix("/allow")
                    .strip("/")
                )
                worker_id = body.get("worker_id", "")
                if not agent_type or not worker_id:
                    self._send_json(
                        {
                            "error": "agent_type and worker_id required",
                            "status": "error",
                        },
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                try:
                    async_runner.run(mgr.allow_worker_for_type(agent_type, worker_id))
                except Exception as err:  # pylint: disable=broad-exception-caught
                    state.record_error(
                        path, err, duration_ms=_elapsed_ms(request_started_ms)
                    )
                    self._send_json(
                        {"error": str(err), "status": "error"},
                        status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                state.record_success(
                    route=path, duration_ms=_elapsed_ms(request_started_ms)
                )
                self._send_json(
                    {
                        "ok": True,
                        "agent_type": agent_type,
                        "worker_id": worker_id,
                        "action": "allow",
                    }
                )
                return

            self._send_json(
                {"error": "not found", "status": "error"}, status=HTTPStatus.NOT_FOUND
            )

    return DashboardHandler


def _first_param(params: dict[str, list[str]], key: str) -> str:
    values = params.get(key, [])
    return values[0] if values else ""


def _truthy_param(params: dict[str, list[str]], key: str) -> bool:
    return _first_param(params, key).lower() in ("1", "true", "yes", "on")


def _list_param(params: dict[str, list[str]], key: str) -> list[str]:
    values = []
    for raw in params.get(key, []):
        values.extend(item.strip() for item in raw.split(",") if item.strip())
    return values


def _int_param(params: dict[str, list[str]], key: str, default: int) -> int:
    raw = _first_param(params, key)
    if not raw:
        return default
    try:
        return max(0, int(raw))
    except ValueError:
        return default


def _pick_keys(payload: dict[str, Any], keys: list[str]) -> dict[str, Any]:
    return {key: payload[key] for key in keys if key in payload}


def _config_payload(
    *,
    host: str,
    port: int,
    redis_config: RedisConfig | None,
    auth_enabled: bool,
    demo: bool,
    queue_backlog_threshold: int,
) -> dict[str, Any]:
    redis_payload = {
        "host": getattr(redis_config, "host", ""),
        "port": getattr(redis_config, "port", 0),
        "db": getattr(redis_config, "db", 0),
        "username_configured": bool(getattr(redis_config, "username", "")),
        "password_configured": bool(getattr(redis_config, "password", "")),
        "max_connections": getattr(redis_config, "max_connections", 0),
    }
    return {
        "generated_at": _now_ms(),
        "dashboard": {
            "host": host,
            "port": port,
            "demo_mode": demo,
            "auth_enabled": auth_enabled,
            "static_package": STATIC_PACKAGE,
        },
        "redis": redis_payload,
        "observability": {
            "history_limit": HISTORY_LIMIT,
            "metrics_cache_ttl_seconds": METRICS_CACHE_TTL,
            "queue_backlog_threshold": queue_backlog_threshold,
            "trace_fallback_enabled": bool(os.environ.get("BYAI_TRACE_FALLBACK_URL")),
        },
        "capabilities": [
            {"id": "workers", "label": "Worker lifecycle management", "enabled": True},
            {"id": "queues", "label": "Redis Streams inspection", "enabled": True},
            {"id": "alerts", "label": "Alert center", "enabled": True},
            {"id": "traces", "label": "Trace/session drill-down", "enabled": True},
            {"id": "exports", "label": "JSON exports", "enabled": True},
            {"id": "metrics", "label": "Prometheus metrics", "enabled": True},
        ],
    }


def _export_payload(snapshot: dict[str, Any], *, scope: str) -> dict[str, Any]:
    scopes = {
        "snapshot": snapshot,
        "workers": _pick_keys(
            snapshot,
            ["generated_at", "totals", "status_counts", "workers", "alerts", "health"],
        ),
        "queues": _pick_keys(snapshot, ["generated_at", "queues", "alerts", "health"]),
        "executions": _pick_keys(
            snapshot,
            [
                "generated_at",
                "totals",
                "status_counts",
                "recent_executions",
                "latency",
                "failures",
                "agent_health",
                "alerts",
                "health",
            ],
        ),
        "alerts": _alerts_payload(snapshot),
    }
    if scope not in scopes:
        raise ValueError(
            "unsupported export scope: "
            f"{scope}; expected one of {', '.join(sorted(scopes))}"
        )
    return {
        "generated_at": int(snapshot.get("generated_at", 0) or _now_ms()),
        "scope": scope,
        "format": "json",
        "payload": scopes[scope],
    }


def _alerts_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    alerts = [
        {
            "id": f"alert-{index}-{alert.get('code', 'unknown')}",
            "code": str(alert.get("code", "")),
            "severity": str(alert.get("severity", "info")),
            "message": str(alert.get("message", "")),
            "value": alert.get("value"),
            "threshold": alert.get("threshold"),
            "component": _alert_component(alert),
            "status": "open",
            "started_at": int(snapshot.get("generated_at", 0) or _now_ms()),
            "owner": _alert_owner(alert),
            "recommendations": _alert_recommendations(alert),
        }
        for index, alert in enumerate(snapshot.get("alerts", []), start=1)
    ]
    failures = [
        {
            "id": f"failure-{failure.get('execution_id', index)}",
            "code": str(failure.get("error_type", "EXECUTION_FAILURE")),
            "severity": "critical",
            "message": str(failure.get("error_message", "Execution failed")),
            "component": str(
                failure.get("target_agent_type")
                or failure.get("worker_id")
                or "execution"
            ),
            "status": "open",
            "started_at": int(
                failure.get("updated_at", 0)
                or snapshot.get("generated_at", 0)
                or _now_ms()
            ),
            "owner": "后端团队",
            "execution_id": failure.get("execution_id", ""),
            "recommendations": [
                "查看执行详情和 route_policy，确认是否为路由或 Worker 处理异常。",
                "按 execution_id 查询关联 trace/session，定位失败阶段。",
                "如果同类错误持续出现，先隔离对应 Worker 或 Agent 类型。",
            ],
        }
        for index, failure in enumerate(
            snapshot.get("failures", {}).get("recent", []),
            start=1,
        )
    ]
    items = alerts + failures
    return {
        "generated_at": int(snapshot.get("generated_at", 0) or _now_ms()),
        "summary": {
            "total": len(items),
            "critical": sum(1 for item in items if item["severity"] == "critical"),
            "warning": sum(1 for item in items if item["severity"] == "warning"),
            "open": sum(1 for item in items if item["status"] == "open"),
        },
        "alerts": items,
        "recommendations": _global_recommendations(snapshot),
    }


def _actions_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    actions = _build_action_items(snapshot)
    return {
        "generated_at": int(snapshot.get("generated_at", 0) or _now_ms()),
        "summary": {
            "total": len(actions),
            "critical": sum(
                1 for action in actions if action["severity"] == "critical"
            ),
            "warning": sum(1 for action in actions if action["severity"] == "warning"),
            "info": sum(1 for action in actions if action["severity"] == "info"),
        },
        "actions": actions,
        "recommendations": _global_recommendations(snapshot),
    }


def _action_detail_payload(snapshot: dict[str, Any], action_id: str) -> dict[str, Any]:
    actions = _build_action_items(snapshot)
    for action in actions:
        if action["id"] == action_id:
            return {
                "generated_at": int(snapshot.get("generated_at", 0) or _now_ms()),
                "action": action,
                "related": _action_related_payload(snapshot, action),
                "recommendations": action["recommendations"],
            }
    raise KeyError(action_id)


def _build_action_items(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    generated_at = int(snapshot.get("generated_at", 0) or _now_ms())
    actions: list[dict[str, Any]] = []
    for index, alert in enumerate(snapshot.get("alerts", []), start=1):
        severity = str(alert.get("severity", "warning"))
        code = str(alert.get("code", "UNKNOWN"))
        actions.append(
            {
                "id": f"alert:{index}:{code}",
                "kind": "alert",
                "severity": severity,
                "tone": _action_tone(severity),
                "title": str(alert.get("message", code)),
                "description": code,
                "component": _alert_component(alert),
                "source": code,
                "target_view": _alert_target_view(alert),
                "target_ref": code,
                "started_at": generated_at,
                "score": 92 if severity == "critical" else 72,
                "recommendations": _alert_recommendations(alert),
            }
        )
    for queue in _iter_dashboard_queues(snapshot):
        pending = sum(
            int(group.get("pending", 0) or 0)
            for group in queue.get("consumer_groups", [])
        )
        length = int(queue.get("length", 0) or 0)
        name = str(
            queue.get("name") or queue.get("agent_type") or queue.get("stream") or ""
        )
        if not name or (pending == 0 and length == 0):
            continue
        severity = "critical" if name == "deadletter" and length else "warning"
        actions.append(
            {
                "id": f"queue:{name}",
                "kind": "queue",
                "severity": severity,
                "tone": _action_tone(severity),
                "title": f"处理 {name} 队列积压",
                "description": f"{pending} 条 Pending，队列长度 {length}",
                "component": "Redis Streams",
                "source": name,
                "target_view": "queues",
                "target_ref": name,
                "started_at": generated_at,
                "score": 88
                if severity == "critical"
                else min(85, 55 + pending + length),
                "recommendations": _queue_recommendations(queue, pending=pending),
            }
        )
    for failure in snapshot.get("failures", {}).get("recent", [])[:5]:
        execution_id = str(failure.get("execution_id", ""))
        actions.append(
            {
                "id": f"execution:{execution_id}",
                "kind": "execution",
                "severity": "critical",
                "tone": "red",
                "title": str(failure.get("error_type") or "执行失败"),
                "description": str(failure.get("error_message") or execution_id),
                "component": str(
                    failure.get("target_agent_type")
                    or failure.get("worker_id")
                    or "execution"
                ),
                "source": execution_id,
                "target_view": "executions",
                "target_ref": execution_id,
                "started_at": int(failure.get("updated_at", 0) or generated_at),
                "score": 92,
                "recommendations": [
                    "查看执行详情和 route_policy，确认是否为路由或 Worker 处理异常。",
                    "按 execution_id 查询关联 trace/session，定位失败阶段。",
                    "如果同类错误持续出现，先隔离对应 Worker 或 Agent 类型。",
                ],
            }
        )
    return actions[:20]


def _action_related_payload(
    snapshot: dict[str, Any], action: dict[str, Any]
) -> dict[str, Any]:
    kind = str(action.get("kind", ""))
    target_ref = str(action.get("target_ref", ""))
    if kind == "queue" and target_ref:
        try:
            return {"queue": _queue_detail_payload(snapshot, target_ref)}
        except KeyError:
            return {}
    if kind == "execution" and target_ref:
        try:
            return {"execution": _execution_detail_payload(snapshot, target_ref)}
        except KeyError:
            return {}
    if kind == "alert":
        return {"alerts": _alerts_payload(snapshot)}
    return {}


def _action_tone(severity: str) -> str:
    if severity == "critical":
        return "red"
    if severity == "warning":
        return "amber"
    return "blue"


def _alert_target_view(alert: dict[str, Any]) -> str:
    code = str(alert.get("code", ""))
    if "WORKER" in code:
        return "workers"
    if (
        "PENDING" in code
        or "QUEUE" in code
        or "CONSUMER" in code
        or "DEADLETTER" in code
    ):
        return "queues"
    return "executions"


def _worker_detail_payload(snapshot: dict[str, Any], worker_id: str) -> dict[str, Any]:
    worker = _find_worker(snapshot, worker_id)
    worker_executions = [
        execution
        for execution in _iter_dashboard_executions(snapshot)
        if str(execution.get("worker_id", "")) == worker_id
    ]
    worker_alerts = [
        alert
        for alert in snapshot.get("alerts", [])
        if "WORKER" in str(alert.get("code", ""))
        or worker_id in str(alert.get("message", ""))
    ]
    return {
        "generated_at": int(snapshot.get("generated_at", 0) or _now_ms()),
        "worker": worker,
        "executions": worker_executions,
        "alerts": worker_alerts,
        "recommendations": _worker_recommendations(worker, worker_alerts),
    }


def _execution_detail_payload(
    snapshot: dict[str, Any], execution_id: str
) -> dict[str, Any]:
    execution = _find_execution(snapshot, execution_id)
    related_failures = [
        failure
        for failure in snapshot.get("failures", {}).get("recent", [])
        if str(failure.get("execution_id", "")) == execution_id
    ]
    return {
        "generated_at": int(snapshot.get("generated_at", 0) or _now_ms()),
        "execution": execution,
        "agent_config": execution.get("agent_config_audit") or None,
        "failures": related_failures,
        "trace": {
            "trace_id": execution.get("trace_id", ""),
            "session_id": execution.get("session_id", ""),
        },
        "recommendations": _execution_recommendations(execution, related_failures),
    }


def _queue_detail_payload(snapshot: dict[str, Any], queue_name: str) -> dict[str, Any]:
    queue = _find_queue(snapshot, queue_name)
    pending = sum(
        int(group.get("pending", 0) or 0) for group in queue.get("consumer_groups", [])
    )
    queue_type = str(queue.get("queue_type", "agent_type"))
    status = "warning" if pending or int(queue.get("length", 0) or 0) else "healthy"
    if queue_name == "deadletter" and int(queue.get("length", 0) or 0):
        status = "critical"
    return {
        "generated_at": int(snapshot.get("generated_at", 0) or _now_ms()),
        "queue": {
            **queue,
            "name": str(queue.get("name", queue_name)),
            "queue_type": queue_type,
            "pending_total": pending,
            "status": status,
        },
        "recommendations": _queue_recommendations(queue, pending=pending),
    }


def _find_queue(snapshot: dict[str, Any], queue_name: str) -> dict[str, Any]:
    target = queue_name.lower()
    for queue in _iter_dashboard_queues(snapshot):
        names = {
            str(queue.get("name", "")),
            str(queue.get("agent_type", "")),
            str(queue.get("stream", "")),
        }
        if target in {name.lower() for name in names if name}:
            return queue
    raise KeyError(queue_name)


def _iter_dashboard_queues(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    queues = snapshot.get("queues", {})
    rows = [
        {
            **queue,
            "name": str(queue.get("agent_type", "")),
            "queue_type": "agent_type",
        }
        for queue in queues.get("agent_type_streams", [])
    ]
    rows.extend(
        {
            **queue,
            "name": str(name),
            "queue_type": "control_plane",
        }
        for name, queue in queues.get("control_plane", {}).items()
    )
    return rows


def _find_worker(snapshot: dict[str, Any], worker_id: str) -> dict[str, Any]:
    for worker in snapshot.get("workers", []):
        if str(worker.get("worker_id", "")) == worker_id:
            return worker
    raise KeyError(worker_id)


def _find_execution(snapshot: dict[str, Any], execution_id: str) -> dict[str, Any]:
    for execution in _iter_dashboard_executions(snapshot):
        ids = {
            str(execution.get("execution_id", "")),
            str(execution.get("message_id", "")),
        }
        if execution_id in ids:
            return execution
    raise KeyError(execution_id)


def _iter_dashboard_executions(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    seen = set()
    rows = []
    for execution in snapshot.get("recent_executions", []):
        key = str(execution.get("execution_id", ""))
        if key and key not in seen:
            rows.append(execution)
            seen.add(key)
    for worker in snapshot.get("workers", []):
        for field in ("active_executions", "recent_executions"):
            for execution in worker.get(field, []):
                key = str(execution.get("execution_id", ""))
                if key and key not in seen:
                    rows.append(execution)
                    seen.add(key)
    return rows


def _alert_component(alert: dict[str, Any]) -> str:
    code = str(alert.get("code", ""))
    if "WORKER" in code:
        return "Worker Pool"
    if "PENDING" in code or "QUEUE" in code or "CONSUMER" in code:
        return "Redis Streams"
    if "DEADLETTER" in code:
        return "Control Plane"
    return "Dashboard Policy"


def _alert_owner(alert: dict[str, Any]) -> str:
    code = str(alert.get("code", ""))
    if "WORKER" in code:
        return "运维团队"
    if "PENDING" in code or "QUEUE" in code or "CONSUMER" in code:
        return "后端团队"
    return "平台团队"


def _alert_recommendations(alert: dict[str, Any]) -> list[str]:
    code = str(alert.get("code", ""))
    if "WORKER" in code:
        return [
            "打开 Workers 页面确认是否存在 suspended/evicted 或离线实例。",
            "必要时恢复 Worker，或驱逐异常实例后允许新实例重加入。",
            "检查 Worker 所在机器的 Redis 连接与心跳日志。",
        ]
    if "PENDING" in code or "CONSUMER" in code:
        return [
            "打开队列详情确认具体 Consumer Group 的 pending 与 lag。",
            "检查对应 Worker 是否仍在线且未被 denylist 排除。",
            "如果 pending 持续增长，临时扩容 Worker 或降低入口流量。",
        ]
    if "FAILED" in code:
        return [
            "进入告警与分析页面查看最近失败执行。",
            "按 execution_id 或 session_id 查询 Trace 瀑布。",
            "若错误集中在某个 Agent 类型，先隔离对应 Worker。",
        ]
    return [
        "查看相关页面详情并确认指标是否持续异常。",
        "结合 Redis Stream 和 Worker 日志定位根因。",
    ]


def _queue_recommendations(queue: dict[str, Any], *, pending: int) -> list[str]:
    name = str(queue.get("name") or queue.get("agent_type") or "")
    length = int(queue.get("length", 0) or 0)
    if name == "deadletter" and length:
        return [
            "优先查看 deadletter 消息内容，确认失败原因是否可重放。",
            "处理完成后从业务侧重新投递，不要直接丢弃未知消息。",
        ]
    if pending:
        return [
            "检查 Consumer Group 中 pending 最高的消费者是否仍在线。",
            "确认对应 Worker 未被暂停、驱逐或加入 Agent-type denylist。",
            "必要时扩容同 Agent 类型 Worker 并观察 pending 是否下降。",
        ]
    if length:
        return [
            "队列仍有积压但暂无 pending，确认是否有消费者组可用。",
            "检查 Worker membership 与 Redis Stream consumer group 配置。",
        ]
    return ["当前队列无明显积压，保持观察即可。"]


def _worker_recommendations(
    worker: dict[str, Any], alerts: list[dict[str, Any]]
) -> list[str]:
    lifecycle = str(worker.get("lifecycle", "active"))
    online = bool(worker.get("online", True))
    if lifecycle == "suspended":
        return [
            "确认暂停原因和维护窗口，恢复前检查该 Worker 当前任务是否已清空。",
            "恢复后观察对应 Agent 类型队列 pending 是否下降。",
        ]
    if lifecycle == "evicted":
        return [
            "确认驱逐原因已处理，再允许该 worker_id 重加入。",
            "如果机器已替换，确保新实例使用新的 worker_id 或清理旧 admin 状态。",
        ]
    if not online:
        return [
            "检查 Worker 进程、机器网络和 Redis 连接。",
            "确认 online lease 是否过期，必要时从集群中清理该实例。",
        ]
    if alerts:
        return [
            "该 Worker 命中告警策略，优先查看最近失败执行和心跳时间。",
            "如错误集中在同一 Agent 类型，可临时 deny 该 Worker 消费该类型。",
        ]
    return ["Worker 当前可用，继续观察活跃任务、失败计数和最后心跳。"]


def _execution_recommendations(
    execution: dict[str, Any], failures: list[dict[str, Any]]
) -> list[str]:
    status = str(execution.get("status", ""))
    if status == "FAILED" or failures:
        return [
            "查看 failure error_type/error_message，确认失败阶段。",
            "使用 session_id 或 trace_id 打开会话页面检查调用瀑布。",
            "如果同类失败重复出现，先隔离对应 Worker 或 Agent 类型。",
        ]
    if status in {"RUNNING", "QUEUED", "CANCELLING"}:
        return [
            "观察 queue_latency_ms 与 run_latency_ms 是否持续增长。",
            "检查目标 Worker 是否在线，以及队列 Consumer Group 是否有 pending。",
        ]
    return ["执行已进入终态，可结合 Trace 和 Session 事件做事后分析。"]


def _global_recommendations(snapshot: dict[str, Any]) -> list[str]:
    if snapshot.get("alerts"):
        return [
            "先处理 critical，再处理 warning，避免控制面和数据面同时积压。",
            "对同一组件的重复告警合并排查，优先定位共同上游。",
        ]
    return ["当前无活跃告警，可继续观察趋势与容量余量。"]


def _trace_summary(trace: dict[str, Any]) -> dict[str, Any]:
    return {
        "trace_id": trace.get("trace_id", ""),
        "session_id": trace.get("session_id", ""),
        "status": trace.get("status", ""),
        "start_ts": trace.get("start_ts", 0),
        "end_ts": trace.get("end_ts", 0),
        "duration_ms": trace.get("duration_ms", 0),
        "span_count": trace.get("span_count", 0),
    }


def _demo_trace_metrics_window(trace: dict[str, Any]) -> dict[str, Any]:
    start_ts = int(trace.get("start_ts", 0) or 0)
    end_ts = int(trace.get("end_ts", 0) or 0)
    samples = [
        point
        for point in build_demo_observability_history()
        if start_ts <= int(point.get("generated_at", 0) or 0) <= end_ts
    ]
    if not samples:
        samples = build_demo_observability_history(samples=1)
    return {
        "window": {"start_ts": start_ts, "end_ts": end_ts, "buffer_ms": 0},
        "samples": samples,
        "summary": _metrics_window_summary(samples),
        "diagnostics": [
            {
                "code": "metrics_trace_window",
                "message": "Metrics history overlaps this trace window.",
                "severity": "info",
            }
        ],
        "status": "ok",
    }


async def _attach_metrics_window(
    redis_client: Any, trace: dict[str, Any]
) -> dict[str, Any]:
    start_ts = int(trace.get("start_ts", 0) or 0)
    end_ts = int(trace.get("end_ts", 0) or 0)
    if not start_ts or not end_ts:
        return {
            **trace,
            "metrics_window": {
                "window": {"start_ts": start_ts, "end_ts": end_ts, "buffer_ms": 0},
                "samples": [],
                "summary": {"sample_count": 0},
                "diagnostics": [
                    {
                        "code": "metrics_trace_window_missing",
                        "message": "Trace does not contain a usable time window.",
                        "severity": "warning",
                    }
                ],
                "status": "partial",
            },
        }
    try:
        from by_framework.metrics import MetricsReadClient

        result = await MetricsReadClient(redis_client).explain_window(
            start_ts=start_ts,
            end_ts=end_ts,
            buffer_ms=5_000,
            slo_policy=SLOPolicy(),
        )
        metrics_window = result.to_dict()
    except Exception as err:  # pylint: disable=broad-exception-caught
        metrics_window = {
            "window": {"start_ts": start_ts, "end_ts": end_ts, "buffer_ms": 5_000},
            "samples": [],
            "summary": {"sample_count": 0},
            "diagnostics": [
                {
                    "code": "metrics_trace_window_failed",
                    "message": f"Metrics window lookup failed: {err}",
                    "severity": "warning",
                }
            ],
            "status": "partial",
        }
    return {**trace, "metrics_window": metrics_window}


def _metrics_window_summary(samples: list[dict[str, int]]) -> dict[str, Any]:
    if not samples:
        return {"sample_count": 0}
    fields = (
        "workers_online",
        "active_executions",
        "failed_executions",
        "queue_depth_total",
        "consumer_pending_total",
        "deadletter_count",
        "oldest_pending_age_seconds",
        "max_delivery_count",
        "alert_count",
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
    summary["slo_window"] = _metrics_window_slo_summary(samples, SLOPolicy())
    summary["signal_explain"] = _metrics_window_signal_explain(summary)
    return summary


def _summary_max(summary: dict[str, Any], field_name: str) -> int:
    value = summary.get(field_name, {})
    return int(value.get("max", 0) or 0) if isinstance(value, dict) else 0


def _summary_last(summary: dict[str, Any], field_name: str) -> int:
    value = summary.get(field_name, {})
    return int(value.get("last", 0) or 0) if isinstance(value, dict) else 0


def _metrics_window_signal_explain(
    summary: dict[str, Any],
) -> list[dict[str, Any]]:
    queue_depth = _summary_max(summary, "queue_depth_total")
    pending = _summary_max(summary, "consumer_pending_total")
    oldest_pending_age = _summary_max(summary, "oldest_pending_age_seconds")
    max_delivery_count = _summary_max(summary, "max_delivery_count")
    workers_online = _summary_last(summary, "workers_online")
    active_executions = _summary_max(summary, "active_executions")
    failed_executions = _summary_max(summary, "failed_executions")
    deadletter_count = _summary_max(summary, "deadletter_count")
    alert_count = _summary_max(summary, "alert_count")
    return [
        {
            "category": "queue",
            "severity": (
                "warning"
                if (
                    queue_depth > 0
                    or pending > 0
                    or oldest_pending_age > 0
                    or max_delivery_count > 1
                )
                else "ok"
            ),
            "message": (
                "Queue backlog or pending delivery overlapped this trace window."
                if (
                    queue_depth > 0
                    or pending > 0
                    or oldest_pending_age > 0
                    or max_delivery_count > 1
                )
                else "No queue backlog was observed in this trace window."
            ),
            "metrics": {
                "queue_depth_total": queue_depth,
                "consumer_pending_total": pending,
                "oldest_pending_age_seconds": oldest_pending_age,
                "max_delivery_count": max_delivery_count,
            },
        },
        {
            "category": "worker",
            "severity": (
                "warning"
                if workers_online <= 0 or active_executions > workers_online
                else "ok"
            ),
            "message": (
                "Worker capacity was absent or saturated during this trace window."
                if workers_online <= 0 or active_executions > workers_online
                else "Worker capacity was available during this trace window."
            ),
            "metrics": {
                "workers_online": workers_online,
                "active_executions": active_executions,
            },
        },
        {
            "category": "errors",
            "severity": (
                "warning"
                if failed_executions > 0 or deadletter_count > 0 or alert_count > 0
                else "ok"
            ),
            "message": (
                "Failures, deadletters, or alerts overlapped this trace window."
                if failed_executions > 0 or deadletter_count > 0 or alert_count > 0
                else "No failure signal was observed in this trace window."
            ),
            "metrics": {
                "failed_executions": failed_executions,
                "deadletter_count": deadletter_count,
                "alert_count": alert_count,
            },
        },
    ]


def _metrics_window_slo_summary(
    samples: list[dict[str, int]], slo_policy: SLOPolicy
) -> dict[str, Any]:
    first = samples[0]
    last = samples[-1]
    successful_executions = max(
        0,
        int(last.get("completed_executions", 0) or 0)
        - int(first.get("completed_executions", 0) or 0),
    )
    terminal_executions = max(
        0,
        int(last.get("terminal_executions", 0) or 0)
        - int(first.get("terminal_executions", 0) or 0),
    )
    success_ratio = (
        successful_executions / terminal_executions if terminal_executions > 0 else 1.0
    )
    target = min(1.0, max(0.0, float(slo_policy.success_ratio_target)))
    burn_rate = max(0.0, (target - success_ratio) / max(1.0 - target, 0.000001))
    total_latency_p95_ms = max(
        int(sample.get("total_latency_p95_ms", 0) or 0) for sample in samples
    )
    deadletter_count = max(
        int(sample.get("deadletter_count", 0) or 0) for sample in samples
    )
    freshness_age_ms = max(
        int(sample.get("freshness_age_ms", 0) or 0) for sample in samples
    )
    return {
        "window": slo_policy.window,
        "successful_executions": successful_executions,
        "terminal_executions": terminal_executions,
        "success_ratio_ppm": int(success_ratio * 1_000_000),
        "success_ratio_target_ppm": int(target * 1_000_000),
        "success_ratio_objective_met": success_ratio >= target,
        "burn_rate": round(burn_rate, 3),
        "total_latency_p95_ms": total_latency_p95_ms,
        "latency_objective_met": (
            total_latency_p95_ms <= int(slo_policy.total_latency_p95_ms)
        ),
        "deadletter_count": deadletter_count,
        "deadletter_objective_met": (
            deadletter_count <= int(slo_policy.deadletter_threshold)
        ),
        "freshness_age_ms": freshness_age_ms,
        "freshness_objective_met": (
            freshness_age_ms <= int(slo_policy.freshness_max_age_ms)
        ),
    }


def _trace_read_client(redis_client: Any = None) -> Any:
    from by_framework_trace_query import TraceReadClient

    return TraceReadClient(redis_client=redis_client)


async def _list_trace_summaries_via_read_sdk(
    redis_client: Any,
    *,
    session_id: str = "",
    worker_id: str = "",
    agent_type: str = "",
    limit: int = 50,
) -> list[dict[str, Any]]:
    try:
        trace_results = await asyncio.wait_for(
            _trace_read_client(redis_client).list_traces(
                session_id=session_id,
                worker_id=worker_id,
                agent_type=agent_type,
                limit=limit,
            ),
            timeout=2.0,
        )
        return [trace_result_to_dashboard_summary(result) for result in trace_results]
    except Exception as err:  # pylint: disable=broad-exception-caught
        logger.warning(
            "TraceReadClient list fallback: %s",
            err,
            **observability_log_extra(
                session_id=session_id,
                worker_id=worker_id,
                agent_type=agent_type,
            ),
        )
        if not session_id:
            raise
        session_snapshot = await build_session_observability_snapshot(
            redis_client, session_id
        )
        traces = []
        for trace_id in sorted(
            {
                str(execution.get("trace_id", ""))
                for execution in session_snapshot.get("executions", [])
                if execution.get("trace_id")
            }
        )[:limit]:
            trace = await build_trace_observability_snapshot(
                redis_client,
                trace_id,
                session_id=session_id,
            )
            traces.append(_trace_summary(trace))
        return traces


async def _get_trace_snapshot_via_read_sdk(
    redis_client: Any,
    trace_id: str,
    *,
    session_id: str = "",
) -> dict[str, Any]:
    try:
        trace_result = await asyncio.wait_for(
            _trace_read_client(redis_client).get_trace(trace_id, session_id=session_id),
            timeout=2.0,
        )
        trace = trace_result_to_dashboard_trace(trace_result)
        if trace.get("spans"):
            return trace
        return await build_trace_observability_snapshot(
            redis_client,
            trace_id,
            session_id=session_id,
        )
    except Exception as err:  # pylint: disable=broad-exception-caught
        logger.warning(
            "TraceReadClient fallback for trace %s: %s",
            trace_id,
            err,
            **observability_log_extra(trace_id=trace_id, session_id=session_id),
        )
        return await build_trace_observability_snapshot(
            redis_client,
            trace_id,
            session_id=session_id,
        )


def _trace_timeline_payload(trace: dict[str, Any]) -> dict[str, Any]:
    return {
        "generated_at": trace.get("generated_at", 0),
        "trace_id": trace.get("trace_id", ""),
        "session_id": trace.get("session_id", ""),
        "status": trace.get("status", ""),
        "duration_ms": trace.get("duration_ms", 0),
        "timeline": trace.get("timeline", []),
    }


def _escape_metric_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def serve(
    host: str = "127.0.0.1",
    port: int = 8765,
    redis_client: Any = None,
    redis_config: RedisConfig | None = None,
    queue_backlog_threshold: int = 100,
    alert_policy: AlertPolicy | None = None,
    auth_token: str = "",
) -> None:
    """Start the observability dashboard HTTP server."""
    if host not in ("127.0.0.1", "localhost", "::1") and not auth_token:
        logger.warning(
            "Observability dashboard is bound to %s without an auth token.", host
        )
    runner = DashboardAsyncRunner()
    server = ThreadingHTTPServer(
        (host, port),
        make_handler(
            runner,
            redis_client=redis_client,
            redis_config=redis_config,
            queue_backlog_threshold=queue_backlog_threshold,
            alert_policy=alert_policy,
            auth_token=auth_token,
        ),
    )
    print(f"by-framework observability dashboard: http://{host}:{port}")
    try:
        server.serve_forever()
    finally:
        server.server_close()
        runner.close()


def parse_args() -> argparse.Namespace:
    """Parse dashboard CLI arguments."""
    config = RedisConfig.from_env()
    parser = argparse.ArgumentParser(description="Serve by-framework observability UI")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--redis-host", default=config.host)
    parser.add_argument("--redis-port", type=int, default=config.port)
    parser.add_argument("--redis-db", type=int, default=config.db)
    parser.add_argument("--redis-username", default=config.username)
    parser.add_argument("--redis-password", default=config.password)
    parser.add_argument(
        "--redis-max-connections", type=int, default=config.max_connections
    )
    parser.add_argument(
        "--queue-backlog-threshold",
        type=int,
        default=100,
        help="Queue backlog alert threshold in messages. Default: 100.",
    )
    parser.add_argument(
        "--delivery-pending-threshold",
        type=int,
        default=0,
        help="Control-plane pending delivery alert threshold. Default: 0.",
    )
    parser.add_argument(
        "--consumer-pending-threshold",
        type=int,
        default=0,
        help="Consumer group pending message alert threshold. Default: 0.",
    )
    parser.add_argument(
        "--failed-execution-threshold",
        type=int,
        default=0,
        help="Failed execution alert threshold. Default: 0.",
    )
    parser.add_argument(
        "--auth-token",
        default=os.environ.get("BY_FRAMEWORK_DASHBOARD_TOKEN", ""),
        help="Bearer token required for dashboard API and metrics routes.",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entry point for the dashboard server."""
    args = parse_args()
    redis = init_redis(
        host=args.redis_host,
        port=args.redis_port,
        db=args.redis_db,
        username=args.redis_username,
        password=args.redis_password,
        max_connections=args.redis_max_connections,
    )
    redis_config = RedisConfig(
        host=args.redis_host,
        port=args.redis_port,
        db=args.redis_db,
        username=args.redis_username,
        password=args.redis_password,
        max_connections=args.redis_max_connections,
    )
    serve(
        host=args.host,
        port=args.port,
        redis_client=redis,
        redis_config=redis_config,
        alert_policy=AlertPolicy(
            queue_backlog_threshold=args.queue_backlog_threshold,
            delivery_pending_threshold=args.delivery_pending_threshold,
            consumer_pending_threshold=args.consumer_pending_threshold,
            failed_execution_threshold=args.failed_execution_threshold,
        ),
        auth_token=args.auth_token,
    )


if __name__ == "__main__":
    main()
