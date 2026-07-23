"""Worker readiness HTTP endpoint for Docker/Kubernetes health checks.

See docs/architecture/worker-readiness-endpoint.md for the full design
record (why readiness-only, why a dedicated thread, why /readyz, the
reason-priority order, and the hard rule against ever wiring this to a
liveness probe).
"""

import json
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Optional
from urllib.parse import urlparse

from by_framework.common.logger import logger

READYZ_PATH = "/readyz"


def _make_readyz_handler(
    compute_state: Callable[[], dict],
) -> type[BaseHTTPRequestHandler]:
    """Build a request handler bound to the given state computation."""

    class ReadyzHandler(BaseHTTPRequestHandler):
        """Serves /readyz; every other path is 404."""

        server_version = "ByFrameworkWorkerReadiness/0.1"

        def do_GET(self) -> None:  # pylint: disable=invalid-name
            if urlparse(self.path).path != READYZ_PATH:
                self.send_response(HTTPStatus.NOT_FOUND)
                self.end_headers()
                return

            state = compute_state()
            status = HTTPStatus.OK if state["ready"] else HTTPStatus.SERVICE_UNAVAILABLE
            body = json.dumps(state).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:  # pylint: disable=redefined-builtin
            # The stdlib default logs every request to stderr - a probe
            # hitting this every few seconds would spam Worker logs.
            pass

    return ReadyzHandler


class WorkerHealthServer:
    """Runs /readyz on a dedicated thread, never the Worker's main event loop.

    Mirrors WorkerHeartbeat's "dedicated thread" pattern for the same
    reason: a busy consume loop must not make readiness checks
    unreachable. Reads Worker state via plain callables passed in by the
    caller (WorkerRunner) - this class has no knowledge of WorkerRunner
    itself, so it can be started standalone against fake state in tests.
    """

    def __init__(
        self,
        worker_id: str,
        port: int,
        has_started: Callable[[], bool],
        is_draining: Callable[[], bool],
        admin_lifecycle: Callable[[], str],
        consumer_healthy: Callable[[], bool],
        host: str = "0.0.0.0",
    ):
        self.worker_id = worker_id
        self.host = host
        self.port = port
        self._has_started = has_started
        self._is_draining = is_draining
        self._admin_lifecycle = admin_lifecycle
        self._consumer_healthy = consumer_healthy
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._started_at_monotonic = 0.0

    @staticmethod
    def _compute_reason(
        has_started: bool,
        is_draining: bool,
        admin_lifecycle: str,
        consumer_healthy: bool,
    ) -> str:
        # Priority order, first match wins - see
        # docs/architecture/worker-readiness-endpoint.md.
        if not has_started:
            return "starting"
        if is_draining:
            return "draining"
        if admin_lifecycle == "evicted":
            return "evicted"
        if admin_lifecycle == "suspended":
            return "suspended"
        if not consumer_healthy:
            return "consumer_stalled"
        return "serving"

    def _compute_state(self) -> dict:
        # Read each accessor once - _compute_reason() takes the values
        # rather than re-deriving them, so a request never calls into
        # WorkerRunner's state twice for the same field.
        admin_lifecycle = self._admin_lifecycle()
        consumer_healthy = self._consumer_healthy()
        reason = self._compute_reason(
            has_started=self._has_started(),
            is_draining=self._is_draining(),
            admin_lifecycle=admin_lifecycle,
            consumer_healthy=consumer_healthy,
        )
        uptime_ms = 0
        if self._started_at_monotonic:
            uptime_ms = int((time.monotonic() - self._started_at_monotonic) * 1000)
        return {
            "ready": reason == "serving",
            "reason": reason,
            "worker_id": self.worker_id,
            "admin_lifecycle": admin_lifecycle,
            "consumer_healthy": consumer_healthy,
            "uptime_ms": uptime_ms,
        }

    @property
    def is_running(self) -> bool:
        """Whether the server has actually bound and is serving - distinct
        from the WorkerHealthServer object merely having been constructed."""
        return self._server is not None

    def start(self) -> None:
        """Bind and start serving /readyz on a dedicated thread. No-op if
        already started."""
        if self._server is not None:
            return
        self._started_at_monotonic = time.monotonic()
        handler = _make_readyz_handler(self._compute_state)
        self._server = ThreadingHTTPServer((self.host, self.port), handler)
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            daemon=True,
            name=f"health-server-{self.worker_id}",
        )
        self._thread.start()
        logger.info(
            "[%s] Readiness endpoint listening on %s:%d%s",
            self.worker_id,
            self.host,
            self.port,
            READYZ_PATH,
        )

    def stop(self) -> None:
        """Stop serving and release the port. Safe to call more than once."""
        if self._server is None:
            return
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._server = None
        self._thread = None
