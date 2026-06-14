"""Worker heartbeat component for maintaining worker presence in cluster."""

import asyncio
import threading
from typing import Callable, List, Optional

from by_framework.common.constants import RedisKeys
from by_framework.common.logger import logger
from by_framework.common.redis_client import Redis, get_redis
from by_framework.core.registry import WorkerRegistry


class WorkerHeartbeat:
    """
    Standalone heartbeat component to maintain worker presence in the cluster.

    Runs in a dedicated background thread with its own asyncio event loop so that
    heartbeat renewal is never starved by long-running tasks in the main event loop
    (e.g. LLM/LangGraph calls that hold the loop for tens of seconds).
    """

    def __init__(
        self,
        worker_id: str,
        agent_types: List[str],
        redis_client: Optional[Redis] = None,
        registry: Optional[WorkerRegistry] = None,
        interval: int = RedisKeys.WORKER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        lease_ttl_seconds: int = RedisKeys.WORKER_DEFAULT_LEASE_TTL_SECONDS,
        health_check: Optional[Callable[[], bool]] = None,
        lifecycle_callback: Optional[Callable[[str], None]] = None,
        denylist_refresh: Optional[Callable[[frozenset], None]] = None,
    ):
        self.worker_id = worker_id
        self.agent_types = agent_types
        self.redis = redis_client or get_redis()
        self.registry = registry or WorkerRegistry(self.redis)
        self.interval = interval
        self.lease_ttl_seconds = lease_ttl_seconds
        self.health_check = health_check
        # Called after each successful heartbeat with the latest admin lifecycle value.
        self.lifecycle_callback = lifecycle_callback
        # Called with the current denied agent_type set after each successful heartbeat.
        self.denylist_refresh = denylist_refresh
        self._failure_deadline_seconds = max(
            float(self.interval),
            float(self.lease_ttl_seconds) - float(self.interval),
        )

        # asyncio task that the runner monitors for heartbeat health.
        self._task: Optional[asyncio.Task] = None
        # The asyncio event loop that owns _task.
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None
        # Set by the heartbeat thread when it detects the lock was stolen;
        # triggers the watcher task in the main loop.
        self._lock_stolen_event: Optional[asyncio.Event] = None
        self._failure_reason: str = ""

        self._thread: Optional[threading.Thread] = None
        self._thread_stop = threading.Event()

    @property
    def task(self) -> Optional[asyncio.Task]:
        """Return the watcher asyncio task (monitored by WorkerRunner)."""
        return self._task

    async def start(self):
        """Start the heartbeat background thread."""
        if self._task:
            return

        # Read the admin-controlled lifecycle BEFORE registering membership.
        # A worker that restarts while suspended must not re-join the
        # agent_type:members sets or start consuming until explicitly resumed.
        startup_lifecycle = "active"
        if hasattr(self.registry, "get_worker_admin_state"):
            admin_state = await self.registry.get_worker_admin_state(self.worker_id)
            startup_lifecycle = admin_state.get("lifecycle", "active") or "active"

        if startup_lifecycle == "active":
            await self.registry.register_worker_membership(
                self.worker_id, self.agent_types
            )
        else:
            logger.warning(
                "[%s] Startup admin lifecycle is '%s'; skipping member registration "
                "— worker will not consume until resumed",
                self.worker_id,
                startup_lifecycle,
            )

        ok = await self.registry.heartbeat_worker(
            self.worker_id, self.lease_ttl_seconds
        )
        if not ok:
            raise RuntimeError(
                f"Worker ID '{self.worker_id}' heartbeat was rejected; "
                "another instance may own the lease"
            )

        # Propagate the startup lifecycle to the runner immediately, so that
        # _admin_lifecycle reflects the persisted state before the consume loop
        # opens (runner calls reader_start.set() right after start_heartbeat()).
        # Without this, the runner defaults to "active" and consumes for up to
        # one full heartbeat interval before the thread callback corrects it.
        if self.lifecycle_callback is not None and startup_lifecycle != "active":
            self.lifecycle_callback(startup_lifecycle)

        lock_tokens = getattr(self.registry, "_lock_tokens", {})
        token = lock_tokens.get(self.worker_id, "") if lock_tokens else ""

        self._main_loop = asyncio.get_running_loop()
        self._lock_stolen_event = asyncio.Event()
        self._thread_stop.clear()

        self._thread = threading.Thread(
            target=self._run_in_thread,
            args=(token,),
            daemon=True,
            name=f"heartbeat-{self.worker_id}",
        )
        self._thread.start()

        # Thin asyncio task: just waits for the stolen-event and raises so the
        # runner's existing _heartbeat_task.done() check works unchanged.
        self._task = asyncio.create_task(self._watcher())
        logger.info("[%s] Heartbeat started (dedicated thread)", self.worker_id)

    async def _watcher(self):
        """Watcher coroutine: unblocks only when the thread signals lock-stolen."""
        await self._lock_stolen_event.wait()
        reason = self._failure_reason or (
            f"Worker ID '{self.worker_id}' lock was stolen by another instance; "
            "this process must exit"
        )
        raise RuntimeError(reason)

    # ------------------------------------------------------------------
    # Thread-side implementation
    # ------------------------------------------------------------------

    def _run_in_thread(self, token: str) -> None:
        """Entry point for the dedicated heartbeat thread."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        clean_stop = False
        try:
            loop.run_until_complete(self._async_heartbeat_loop(token))
            clean_stop = self._thread_stop.is_set()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error(
                "[%s] Heartbeat thread exited with error: %s", self.worker_id, exc
            )
        finally:
            if not clean_stop:
                self._signal_lock_stolen()
            loop.close()
            asyncio.set_event_loop(None)

    async def _async_heartbeat_loop(self, token: str) -> None:
        """Heartbeat loop running inside the thread's own event loop."""
        dedicated_redis = None
        if isinstance(self.registry, WorkerRegistry):
            dedicated_redis = await self._create_isolated_redis()
            if dedicated_redis is None:
                logger.error(
                    "[%s] Could not create isolated Redis connection; "
                    "heartbeat thread stopping",
                    self.worker_id,
                )
                self._signal_lock_stolen()
                return

            heartbeat_registry = WorkerRegistry(dedicated_redis)
            if token:
                heartbeat_registry._lock_tokens[self.worker_id] = token
        else:
            heartbeat_registry = self.registry

        loop = asyncio.get_running_loop()
        last_success = loop.time()
        try:
            while not self._thread_stop.is_set():
                if await self._sleep_or_stop(self.interval):
                    break
                if self.health_check is not None and not self.health_check():
                    logger.critical(
                        "[%s] Heartbeat stopping because reader is unhealthy",
                        self.worker_id,
                    )
                    self._signal_lock_stolen(
                        f"Worker ID '{self.worker_id}' reader is unhealthy; "
                        "this process must exit"
                    )
                    return
                try:
                    ok = await heartbeat_registry.heartbeat_worker(
                        self.worker_id, self.lease_ttl_seconds
                    )
                    if not ok:
                        logger.critical(
                            "[%s] Heartbeat lock stolen by another instance — "
                            "triggering shutdown to prevent duplicate worker",
                            self.worker_id,
                        )
                        self._signal_lock_stolen()
                        return
                    last_success = loop.time()
                    # Read lifecycle before deciding whether to re-register: a
                    # suspended or evicted worker must not re-add itself to the
                    # agent_type:members sets on every heartbeat cycle.
                    current_lifecycle = "active"
                    if hasattr(heartbeat_registry, "get_worker_admin_state"):
                        admin_state = await heartbeat_registry.get_worker_admin_state(
                            self.worker_id
                        )
                        current_lifecycle = (
                            admin_state.get("lifecycle", "active") or "active"
                        )
                        if self.lifecycle_callback is not None:
                            self.lifecycle_callback(current_lifecycle)
                    if (
                        hasattr(heartbeat_registry, "register_worker_membership")
                        and current_lifecycle == "active"
                    ):
                        await heartbeat_registry.register_worker_membership(
                            self.worker_id, self.agent_types
                        )
                    if self.denylist_refresh is not None and hasattr(
                        heartbeat_registry, "is_worker_denied_for_type"
                    ):
                        denied: set[str] = set()
                        for agent_type in self.agent_types:
                            if await heartbeat_registry.is_worker_denied_for_type(
                                agent_type, self.worker_id
                            ):
                                denied.add(agent_type)
                        self.denylist_refresh(frozenset(denied))
                    logger.debug("[%s] Heartbeat sent", self.worker_id)
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    logger.error("[%s] Heartbeat failed: %s", self.worker_id, exc)
                    elapsed = loop.time() - last_success
                    if elapsed >= self._failure_deadline_seconds:
                        logger.critical(
                            "[%s] Heartbeat has not succeeded for %.1fs "
                            "(deadline %.1fs); triggering shutdown to avoid "
                            "running without a valid lease",
                            self.worker_id,
                            elapsed,
                            self._failure_deadline_seconds,
                        )
                        self._signal_lock_stolen()
                        return
        finally:
            if dedicated_redis is not None:
                try:
                    await dedicated_redis.aclose()
                except Exception:  # pylint: disable=broad-exception-caught
                    pass

    async def _sleep_or_stop(self, delay: float) -> bool:
        """Sleep in short slices so stop() can join the thread promptly."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + delay
        while not self._thread_stop.is_set():
            remaining = deadline - loop.time()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(remaining, 0.1))
        return True

    async def _create_isolated_redis(self) -> Optional[Redis]:
        """Create a fresh Redis connection for the heartbeat thread."""
        try:
            if not hasattr(self.redis, "connection_pool"):
                return None
            pool = self.redis.connection_pool
            conn_kwargs = dict(pool.connection_kwargs)
            # Strip keys that belong to the pool or client layer, not the connection.
            for key in (
                "retry",
                "retry_on_timeout",
                "retry_on_error",
                "response_callbacks",
            ):
                conn_kwargs.pop(key, None)

            from redis.asyncio import Redis as AsyncRedis  # pylint: disable=import-outside-toplevel

            return AsyncRedis(**conn_kwargs)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error(
                "[%s] Failed to create isolated Redis for heartbeat thread: %s",
                self.worker_id,
                exc,
            )
            return None

    def _signal_lock_stolen(self, reason: str = "") -> None:
        """Notify the main event loop that the lock has been stolen."""
        if reason:
            self._failure_reason = reason
        if (
            self._main_loop
            and not self._main_loop.is_closed()
            and self._lock_stolen_event is not None
        ):
            self._main_loop.call_soon_threadsafe(self._lock_stolen_event.set)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def stop(self):
        """Stop the heartbeat thread and watcher task."""
        self._thread_stop.set()

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self.interval + 2)
        self._thread = None

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # pylint: disable=broad-exception-caught
                pass
            self._task = None

        logger.info("[%s] Heartbeat stopped", self.worker_id)
