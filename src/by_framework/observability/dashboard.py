"""Serve the built-in observability dashboard."""

# pylint: disable=line-too-long,inconsistent-quotes,invalid-name

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
from urllib.parse import parse_qs, urlparse

from by_framework.common.config import RedisConfig
from by_framework.common.logger import logger
from by_framework.common.redis_client import close_redis, init_redis
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
    load_history_from_redis,
    save_history_point_to_redis,
)

METRICS_CACHE_TTL = 15  # Cache /metrics briefly to avoid full Redis scans per scrape.

# A hook for custom HTTP clients (e.g. for testing fallback tracing retrieval)
_fallback_http_client_class = None

STATIC_PACKAGE = "by_framework.observability.static"
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
            "Failed to fetch trace %s from fallback %s: %s", trace_id, url, e
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

        from by_framework.observability.snapshot import _build_trace_snapshot

        session_id = spans[0].get("metadata", {}).get("session_id", "") if spans else ""
        return _build_trace_snapshot(trace_id, session_id, spans)
    except Exception as err:  # pylint: disable=broad-exception-caught
        logger.warning("Error parsing external Jaeger trace: %s", err)
        return None


def make_handler(
    runner: DashboardAsyncRunner | None = None,
    redis_client: Any = None,
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
                if not session_id:
                    self._send_json(
                        {
                            "error": "session_id is required for live trace listing",
                            "status": "error",
                        },
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                try:
                    session_snapshot = async_runner.run(
                        build_session_observability_snapshot(None, session_id)
                    )
                    traces = []
                    for trace_id in sorted(
                        {
                            str(execution.get("trace_id", ""))
                            for execution in session_snapshot.get("executions", [])
                            if execution.get("trace_id")
                        }
                    ):
                        trace = async_runner.run(
                            build_trace_observability_snapshot(
                                None,
                                trace_id,
                                session_id=session_id,
                            )
                        )
                        traces.append(_trace_summary(trace))
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
                        trace_snap = await build_trace_observability_snapshot(
                            None,
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
                    from by_framework.observability.metrics import (
                        build_observability_diagnostics_metrics,
                        generate_latest_metrics,
                    )
                    from by_framework.observability.span_recorder import get_observability_diagnostics

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
    serve(
        host=args.host,
        port=args.port,
        redis_client=redis,
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
