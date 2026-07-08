"""Worker application entry point and initialization utilities."""

import asyncio
import inspect
import pkgutil
from dataclasses import replace
from importlib import import_module
from types import ModuleType
from typing import (Awaitable, Callable, List, Literal, Optional, Tuple, Type, Union)

from by_framework.common.config import RedisConfig
from by_framework.common.emitter import DataLayoutBuilder
from by_framework.common.logger import logger
from by_framework.common.redis_client import close_redis, init_redis
from by_framework.core.extensions import (Plugin, PluginRegistry, TraceProviderFactory)
from by_framework.core.registry import WorkerRegistry
from by_framework.core.runtime.filestore import FileStorage
from by_framework.core.runtime.history import (BaseHistoryBackend, HistoryManager)
from by_framework.core.workspace import WorkspaceManager
from by_framework.worker.runner import WorkerRunner
from by_framework.worker.worker import GatewayWorker

TRACE_PROVIDER_MODULE_PREFIX = "by_framework_trace_"


def _discover_trace_provider_module_names() -> list[str]:
    """List installed Python modules that follow the trace provider naming rule."""
    return sorted(
        {
            module.name
            for module in pkgutil.iter_modules()
            if module.name.startswith(TRACE_PROVIDER_MODULE_PREFIX)
        }
    )


def _load_trace_provider_factories_from_module(
    module: ModuleType,
) -> list[TraceProviderFactory]:
    """Instantiate concrete trace provider factories exported by a module."""
    factories: list[TraceProviderFactory] = []
    for _, member in inspect.getmembers(module, inspect.isclass):
        if not issubclass(member, TraceProviderFactory):
            continue
        if member is TraceProviderFactory or inspect.isabstract(member):
            continue
        factories.append(member())
    return factories


def _discover_trace_provider_factories() -> list[TraceProviderFactory]:
    """Import installed trace provider modules and collect their factory classes."""
    factories: list[TraceProviderFactory] = []
    for module_name in _discover_trace_provider_module_names():
        try:
            module = import_module(module_name)
        except ImportError:
            logger.debug("Skipping unavailable trace provider module: %s", module_name)
            continue
        factories.extend(_load_trace_provider_factories_from_module(module))
    return factories


def _build_auto_trace_plugin() -> Plugin | None:
    """Build the single configured trace plugin from installed provider factories."""
    active_plugins: list[tuple[str, Plugin]] = []
    for factory in _discover_trace_provider_factories():
        plugin = factory.build_plugin_from_env()
        if plugin is None:
            continue
        active_plugins.append((factory.provider_name, plugin))

    if not active_plugins:
        return None

    if len(active_plugins) > 1:
        provider_names = ", ".join(provider_name for provider_name, _ in active_plugins)
        raise RuntimeError(
            "Multiple trace providers are enabled at the same time: "
            f"{provider_names}. Please keep only one configured."
        )

    return active_plugins[0][1]


async def _run_worker_async(
    worker_class: Type[GatewayWorker],
    worker_id: str,
    redis_host: Optional[str],
    redis_port: Optional[int],
    redis_db: Optional[int],
    redis_password: Optional[str],
    redis_username: Optional[str],
    workspace_dir: str,
    consumer_group: str,
    max_concurrency: int = 50,
    fetch_count: int = 10,
    redis_max_connections: Optional[int] = None,
    plugin_list: Optional[List[Plugin]] = None,
    plugin_configurator: Optional[
        Callable[[PluginRegistry], Union[None, Awaitable[None]]]
    ] = None,
    plugin_hook_timeout_seconds: Optional[float] = None,
    plugin_log_hook_stats_on_shutdown: bool = True,
    history_backend: Optional[BaseHistoryBackend] = None,
    plugin_dir: Optional[str] = None,
    storage: Optional[FileStorage] = None,
    layout_builder: Optional[DataLayoutBuilder] = None,
    redis_mode: Optional[Literal["standalone", "cluster"]] = None,
    redis_cluster_nodes: Optional[List[Tuple[str, int]]] = None,
    **worker_kwargs,
):
    """Async worker runner initialization."""
    logger.info("Initializing By-Framework for worker: %s", worker_class.__name__)

    # 1. Auto-link or read environment variable to configure Redis connection pool
    actual_redis_max_conns = redis_max_connections
    if actual_redis_max_conns is None:
        import os

        env_val = os.environ.get("BYAI_REDIS_MAX_CONNECTIONS")
        if env_val:
            actual_redis_max_conns = int(env_val)
        else:
            # Default auto-link: concurrency + 10 management connections
            actual_redis_max_conns = max_concurrency + 10

    # 2. Establish Redis connection (must be inside event loop). Explicit
    # args always take precedence over env vars, for every field, in both
    # standalone and cluster mode - one unified resolution path so third
    # parties can configure either mode purely programmatically or purely
    # via env vars, without the two modes behaving inconsistently.
    env_config = RedisConfig.from_env()
    if redis_mode is not None:
        effective_mode = redis_mode
    elif redis_cluster_nodes:
        # Mirrors REDIS_CLUSTER_HOST's env-var precedence: passing
        # redis_cluster_nodes alone is enough to imply cluster mode, no
        # separate redis_mode="cluster" required. Without this, passing
        # redis_cluster_nodes alone would silently stay in standalone mode
        # and the nodes list would be dropped on the floor.
        effective_mode = "cluster"
    else:
        effective_mode = env_config.mode
    effective_config = replace(
        env_config,
        host=redis_host if redis_host is not None else env_config.host,
        port=redis_port if redis_port is not None else env_config.port,
        db=redis_db if redis_db is not None else env_config.db,
        password=(
            redis_password if redis_password is not None else env_config.password
        ),
        username=(
            redis_username if redis_username is not None else env_config.username
        ),
        mode=effective_mode,
        cluster_nodes=(
            redis_cluster_nodes
            if redis_cluster_nodes is not None
            else env_config.cluster_nodes
        ),
    )
    redis_client = init_redis(
        config=effective_config, max_connections=actual_redis_max_conns
    )
    if effective_config.mode == "cluster":
        logger.info(
            "Worker Redis initialized in Cluster mode (nodes=%s)",
            effective_config.cluster_nodes,
        )

    # 2.1 Configure history message storage backend
    # The storage backend itself decides which operations are supported
    # (e.g., if save_message is not implemented, it cannot save)
    if history_backend is None:
        import os

        pg_dsn = os.environ.get("BYAI_HISTORY_PG_DSN")
        if pg_dsn:
            try:
                from by_framework_history_postgres import PostgresHistoryBackend

                logger.info("Auto-initializing PostgresHistoryBackend from environment")
                history_backend = PostgresHistoryBackend(dsn=pg_dsn)
            except ImportError:
                logger.warning(
                    "BYAI_HISTORY_PG_DSN is set but "
                    "by-framework-history-postgres is not installed"
                )

    if history_backend is not None:
        HistoryManager.set_default_backend(history_backend)

    # 3. Create and configure PluginRegistry
    plugin_registry = PluginRegistry()

    # 3.1 If plugin directory is specified, perform scan and load first
    if plugin_dir:
        plugin_registry.load_plugins_from_dir(plugin_dir)

    trace_plugin = _build_auto_trace_plugin()
    if trace_plugin is not None:
        plugin_registry.register_bundle(trace_plugin)

    if plugin_list:
        for plugin in plugin_list:
            plugin_registry.register_bundle(plugin)

    if plugin_configurator:
        result = plugin_configurator(plugin_registry)
        if inspect.isawaitable(result):
            await result

    if plugin_hook_timeout_seconds is not None:
        plugin_registry.apply_default_hook_timeout(plugin_hook_timeout_seconds)
    plugin_registry.log_hook_stats_on_shutdown = plugin_log_hook_stats_on_shutdown

    # Note: Plugin auto-discovery (discover_plugins) and initialization
    # (initialize_plugins) are now integrated into PluginRegistry.on_worker_startup,
    # which will automatically execute when worker starts and triggers heartbeat.

    # 4. Initialize core components
    registry = WorkerRegistry(redis_client=redis_client)
    workspace_manager = WorkspaceManager(workspace_dir)

    # 5. Instantiate specific business Worker
    worker = worker_class(
        worker_id=worker_id,
        registry=registry,
        workspace_manager=workspace_manager,
        redis_client=redis_client,
        plugin_registry=plugin_registry,
        storage=storage,
        **worker_kwargs,
    )
    if layout_builder is not None:
        worker.layout_builder = layout_builder

    # 6. Create runner and block-start
    runner = WorkerRunner(
        worker=worker,
        redis_client=redis_client,
        group_name=consumer_group,
        max_concurrency=max_concurrency,
        fetch_count=fetch_count,
    )

    try:
        await runner.start()
    except asyncio.CancelledError:
        pass
    finally:
        await close_redis()


def run_worker(
    worker_class: Type[GatewayWorker],
    worker_id: str = "worker-1",
    redis_host: Optional[str] = None,
    redis_port: Optional[int] = None,
    redis_db: Optional[int] = None,
    redis_password: Optional[str] = None,
    redis_username: Optional[str] = None,
    workspace_dir: str = "/tmp/gateway-workspace",
    consumer_group: str = "agent_engines",
    max_concurrency: Optional[int] = None,
    fetch_count: Optional[int] = None,
    redis_max_connections: Optional[int] = None,
    plugin_list: Optional[List[Plugin]] = None,
    plugin_configurator: Optional[
        Callable[[PluginRegistry], Union[None, Awaitable[None]]]
    ] = None,
    plugin_hook_timeout_seconds: Optional[float] = None,
    plugin_log_hook_stats_on_shutdown: bool = True,
    history_backend: Optional[BaseHistoryBackend] = None,
    plugin_dir: Optional[str] = None,
    storage: Optional[FileStorage] = None,
    layout_builder: Optional[DataLayoutBuilder] = None,
    redis_mode: Optional[Literal["standalone", "cluster"]] = None,
    redis_cluster_nodes: Optional[List[Tuple[str, int]]] = None,
    **worker_kwargs,
):
    """
    Quick entry point for starting By-Framework Worker, for direct use by
    third-party businesses.

    Redis configuration: every redis_* argument defaults to None, meaning
    "not specified" - when not passed, the value is read from the
    corresponding REDIS_* environment variable (REDIS_HOST, REDIS_PORT,
    REDIS_DATABASE, REDIS_PASSWORD, REDIS_USERNAME, REDIS_MODE,
    REDIS_CLUSTER_HOST, REDIS_CLUSTER_NODES), falling back to
    "localhost"/6379/standalone if none is set. REDIS_DATABASE replaces
    REDIS_DB (still works as a deprecated fallback, logging a warning).
    Setting REDIS_CLUSTER_HOST (comma-separated "host:port" list) alone is
    enough to switch to cluster mode - no separate REDIS_MODE=cluster
    required, though an explicit REDIS_MODE still wins if set; it also
    implies REDIS_KEY_SCHEMA_VERSION=v2 unless that's set explicitly. An
    explicitly-passed argument always takes precedence over its env var,
    for every field, in both standalone and cluster mode. The same
    single-argument convenience carries over to the programmatic path:
    passing just ``redis_cluster_nodes=[("h1", 6379)]`` (no ``redis_mode``)
    is enough to switch to cluster mode, mirroring REDIS_CLUSTER_HOST -
    ``redis_mode`` only needs to be passed to explicitly force a mode
    (e.g. pin a script to standalone regardless of REDIS_CLUSTER_HOST).
    So either mode can be configured purely programmatically or purely via
    env vars, without the two paths behaving inconsistently.

    Plugin support:
    - **Auto mode**: System automatically discovers and loads all subclasses
      inheriting from `Plugin`.
    - **Directory scan mode**: Specify directory via `plugin_dir`, automatically
      scans and loads Python scripts in it.
    - **Explicit mode**: Manually pass plugin instances via `plugin_list`, or
      customize via `plugin_configurator`.

    History message storage:
    - ``history_backend=BaseHistoryBackend``: Uses the specified custom storage backend.
      The storage backend itself decides which operations are supported.
    """
    import os

    if max_concurrency is None:
        max_concurrency = int(os.environ.get("BYAI_WORKER_CONCURRENCY", 50))
    if fetch_count is None:
        fetch_count = int(os.environ.get("BYAI_WORKER_FETCH_COUNT", 10))

    try:
        asyncio.run(
            _run_worker_async(
                worker_class,
                worker_id,
                redis_host,
                redis_port,
                redis_db,
                redis_password,
                redis_username,
                workspace_dir,
                consumer_group,
                max_concurrency=max_concurrency,
                fetch_count=fetch_count,
                redis_max_connections=redis_max_connections,
                plugin_list=plugin_list,
                plugin_configurator=plugin_configurator,
                plugin_hook_timeout_seconds=plugin_hook_timeout_seconds,
                plugin_log_hook_stats_on_shutdown=plugin_log_hook_stats_on_shutdown,
                history_backend=history_backend,
                plugin_dir=plugin_dir,
                storage=storage,
                layout_builder=layout_builder,
                redis_mode=redis_mode,
                redis_cluster_nodes=redis_cluster_nodes,
                **worker_kwargs,
            )
        )
    except KeyboardInterrupt:
        logger.info("Worker runner stopped by user")
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("Error running worker: %s", str(e))
        import traceback

        traceback.print_exc()
