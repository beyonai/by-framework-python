"""Contract tests for the Worker readiness HTTP endpoint (/readyz).

Mirrors libs/by-framework-dashboard/tests/test_dashboard_server.py's
pattern: bind a real server to an OS-assigned ephemeral port, exercise it
with a real HTTP client, assert on the real response - never reach into
private state.
"""

import http.client
import json

import pytest

from by_framework.worker.health_server import WorkerHealthServer


def _start(**overrides):
    defaults = {
        "worker_id": "worker-1",
        "port": 0,
        "has_started": lambda: False,
        "is_draining": lambda: False,
        "admin_lifecycle": lambda: "active",
        "consumer_healthy": lambda: True,
    }
    defaults.update(overrides)
    server = WorkerHealthServer(**defaults)
    server.start()
    return server


def _get(server, path="/readyz"):
    connection = http.client.HTTPConnection("127.0.0.1", server.port, timeout=5)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        raw = response.read()
        body = json.loads(raw.decode("utf-8")) if raw else None
        return response.status, body
    finally:
        connection.close()


def test_readyz_reports_starting_before_consumer_has_ticked():
    server = _start(has_started=lambda: False)
    try:
        status, body = _get(server)
    finally:
        server.stop()

    assert status == 503
    assert body["ready"] is False
    assert body["reason"] == "starting"


def test_readyz_reports_serving_once_consumer_has_ticked():
    server = _start(has_started=lambda: True)
    try:
        status, body = _get(server)
    finally:
        server.stop()

    assert status == 200
    assert body["ready"] is True
    assert body["reason"] == "serving"


def test_readyz_reports_draining_once_shutdown_signal_received():
    server = _start(has_started=lambda: True, is_draining=lambda: True)
    try:
        status, body = _get(server)
    finally:
        server.stop()

    assert status == 503
    assert body["ready"] is False
    assert body["reason"] == "draining"


def test_draining_outranks_serving_even_if_consumer_still_healthy():
    # A Worker mid-shutdown must never be reported "serving", regardless of
    # whether its consume loop happens to still be healthy in the meantime.
    server = _start(
        has_started=lambda: True,
        is_draining=lambda: True,
        consumer_healthy=lambda: True,
    )
    try:
        _, body = _get(server)
    finally:
        server.stop()

    assert body["reason"] == "draining"


def test_starting_outranks_draining():
    # Priority order per docs/architecture/worker-readiness-endpoint.md:
    # starting > draining. A Worker that receives a shutdown signal before
    # it ever finished starting reports "starting", not "draining".
    server = _start(has_started=lambda: False, is_draining=lambda: True)
    try:
        _, body = _get(server)
    finally:
        server.stop()

    assert body["reason"] == "starting"


def test_readyz_reports_suspended_from_admin_lifecycle():
    server = _start(has_started=lambda: True, admin_lifecycle=lambda: "suspended")
    try:
        status, body = _get(server)
    finally:
        server.stop()

    assert status == 503
    assert body["ready"] is False
    assert body["reason"] == "suspended"
    assert body["admin_lifecycle"] == "suspended"


def test_readyz_reports_evicted_from_admin_lifecycle():
    server = _start(has_started=lambda: True, admin_lifecycle=lambda: "evicted")
    try:
        status, body = _get(server)
    finally:
        server.stop()

    assert status == 503
    assert body["ready"] is False
    assert body["reason"] == "evicted"
    assert body["admin_lifecycle"] == "evicted"


def test_draining_outranks_suspended_and_evicted():
    for lifecycle in ("suspended", "evicted"):
        server = _start(
            has_started=lambda: True,
            is_draining=lambda: True,
            admin_lifecycle=lambda lc=lifecycle: lc,
        )
        try:
            _, body = _get(server)
        finally:
            server.stop()

        assert body["reason"] == "draining"


def test_readyz_reports_consumer_stalled_once_started_but_gone_stale():
    server = _start(has_started=lambda: True, consumer_healthy=lambda: False)
    try:
        status, body = _get(server)
    finally:
        server.stop()

    assert status == 503
    assert body["ready"] is False
    assert body["reason"] == "consumer_stalled"
    assert body["consumer_healthy"] is False


def test_consumer_stalled_is_lowest_priority():
    # suspended/evicted/draining must all outrank consumer_stalled - an
    # administratively-paused or shutting-down Worker is never reported as
    # merely stalled, even if its consume loop also happens to be stale.
    for override in (
        {"is_draining": lambda: True},
        {"admin_lifecycle": lambda: "suspended"},
        {"admin_lifecycle": lambda: "evicted"},
    ):
        server = _start(
            has_started=lambda: True, consumer_healthy=lambda: False, **override
        )
        try:
            _, body = _get(server)
        finally:
            server.stop()

        assert body["reason"] != "consumer_stalled"


def test_readyz_body_includes_diagnostic_fields():
    server = _start(
        worker_id="worker-42",
        has_started=lambda: True,
        admin_lifecycle=lambda: "active",
        consumer_healthy=lambda: True,
    )
    try:
        _, body = _get(server)
    finally:
        server.stop()

    assert body["worker_id"] == "worker-42"
    assert body["admin_lifecycle"] == "active"
    assert body["consumer_healthy"] is True
    assert isinstance(body["uptime_ms"], int)
    assert body["uptime_ms"] >= 0


def test_readyz_body_never_leaks_secrets():
    server = _start()
    try:
        _, body = _get(server)
    finally:
        server.stop()

    serialized = json.dumps(body).lower()
    for forbidden in ("password", "redis://", "token", "secret"):
        assert forbidden not in serialized


def test_unknown_path_returns_404():
    server = _start()
    try:
        status, _ = _get(server, path="/nope")
    finally:
        server.stop()

    assert status == 404


def test_start_is_idempotent_and_stop_frees_the_port():
    server = _start(has_started=lambda: True)
    server.start()  # calling start() twice must not raise or rebind

    status, _ = _get(server)
    assert status == 200

    server.stop()
    with pytest.raises(ConnectionRefusedError):
        _get(server)


def test_is_running_reflects_actual_bind_state():
    server = WorkerHealthServer(
        worker_id="worker-1",
        port=0,
        has_started=lambda: True,
        is_draining=lambda: False,
        admin_lifecycle=lambda: "active",
        consumer_healthy=lambda: True,
    )
    assert server.is_running is False

    server.start()
    assert server.is_running is True
    assert server.port != 0  # sanity: an ephemeral port was actually assigned

    server.stop()
    assert server.is_running is False

    # Stopping an already-stopped server must be a safe no-op.
    server.stop()
    assert server.is_running is False
