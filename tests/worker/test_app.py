import asyncio
import os
import signal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from by_framework_trace_langfuse import LangfuseConfig

from by_framework.core.extensions import (Plugin, PluginManifest, TraceProviderFactory)
from by_framework.worker.app import (
    _build_auto_trace_plugin,
    _load_trace_provider_factories_from_module,
    _run_with_graceful_shutdown,
    _run_worker_async,
    run_worker,
)
from by_framework.worker.worker import GatewayWorker


class MyTestWorker(GatewayWorker):

    def get_agent_types(self):
        return ["test-agent-type"]


class CustomLayoutBuilder:

    def build(self, content, role, content_type, source_agent_type, **kwargs):
        return {"content": content, "agent": source_agent_type}


class FakeTracePlugin(Plugin):
    """Minimal plugin used to verify auto-registered trace providers."""

    def __init__(self):
        super().__init__(PluginManifest(plugin_id="fake-trace"))

    async def register_agent_configs(self, build_context):
        del build_context
        return None


class FakeTraceProviderFactory(TraceProviderFactory):
    """Test double for provider discovery and conflict handling."""

    def __init__(self, provider_name: str = "fake", plugin: Plugin | None = None):
        self._provider_name = provider_name
        self._plugin = plugin

    @property
    def provider_name(self) -> str:
        return self._provider_name

    def build_plugin_from_env(self) -> Plugin | None:
        return self._plugin


async def _noop_coro():
    return None


@pytest.mark.asyncio
async def test_run_with_graceful_shutdown_cancels_task_on_sigterm():
    """SIGTERM (what `docker stop`/Kubernetes pod termination actually send,
    not SIGINT) must cancel the wrapped task so its own try/finally drain
    sequence (WorkerRunner._shutdown) runs, instead of the process dying
    immediately under Python's default SIGTERM disposition. Sends a real
    SIGTERM to this test process - safe here because
    _run_with_graceful_shutdown has already installed an asyncio handler
    that overrides the default disposition before the signal is sent."""
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def long_running():
        started.set()
        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    run_task = asyncio.ensure_future(_run_with_graceful_shutdown(long_running()))
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(run_task, timeout=2)
    finally:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.remove_signal_handler(sig)
            except (NotImplementedError, ValueError):
                pass

    assert cancelled.is_set()
    assert run_task.exception() is None


@pytest.mark.asyncio
async def test_run_with_graceful_shutdown_falls_back_when_signal_handler_unsupported():
    """On platforms without add_signal_handler support (e.g. Windows'
    default event loop), registration must log and continue rather than
    raise - SIGINT still gets graceful handling there via run_worker()'s
    outer `except KeyboardInterrupt`."""
    loop = asyncio.get_running_loop()
    original_add_signal_handler = loop.add_signal_handler
    loop.add_signal_handler = MagicMock(side_effect=NotImplementedError)
    try:
        result = await _run_with_graceful_shutdown(_noop_coro())
    finally:
        loop.add_signal_handler = original_add_signal_handler

    assert result is None


@pytest.mark.asyncio
async def test_run_worker_async_flow():
    """Verify the complete startup flow of _run_worker_async."""

    # Mock all external dependencies
    with (
        patch("by_framework.worker.app.init_redis") as mock_init_redis,
        patch(
            "by_framework.worker.app.close_redis", new_callable=AsyncMock
        ) as mock_close_redis,
        patch("by_framework.worker.app.WorkerRegistry") as mock_worker_registry,
        patch("by_framework.worker.app.WorkspaceManager") as mock_workspace_manager,
        patch("by_framework.worker.app.WorkerRunner") as mock_runner,
    ):

        # Configure Mock Behavior
        mock_init_redis.return_value = MagicMock()  # Mock Redis Client
        mock_runner_instance = mock_runner.return_value
        mock_runner_instance.start = AsyncMock()

        # Execute test function
        await _run_worker_async(
            worker_class=MyTestWorker,
            worker_id="test-w1",
            redis_host="localhost",
            redis_port=6379,
            redis_db=0,
            redis_password=None,
            redis_username=None,
            workspace_dir="/tmp/test-ws",
            consumer_group="test-group",
            max_concurrency=10,
            fetch_count=5,
        )

        # 1. Verify Redis initialization - routes through the unified
        # config= path (same for standalone and cluster mode)
        mock_init_redis.assert_called_once()
        _, init_redis_kwargs = mock_init_redis.call_args
        assert init_redis_kwargs["config"].host == "localhost"
        assert init_redis_kwargs["config"].port == 6379
        assert init_redis_kwargs["config"].db == 0
        assert init_redis_kwargs["config"].password == ""
        assert init_redis_kwargs["config"].username is None
        assert init_redis_kwargs["config"].mode == "standalone"
        assert init_redis_kwargs["max_connections"] == 20  # max_concurrency (10) + 10

        # 2. Verify core component instantiation
        mock_worker_registry.assert_called_once()
        mock_workspace_manager.assert_called_once_with("/tmp/test-ws")

        # 3. Verify Runner start
        mock_runner_instance.start.assert_awaited_once()

        # 4. Verify resource cleanup
        mock_close_redis.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_worker_async_standalone_falls_back_to_env_when_arg_not_passed():
    """Standalone mode must respect REDIS_HOST/REDIS_PASSWORD env vars when
    the caller doesn't pass the corresponding explicit arg — today it
    doesn't, since the standalone branch never consults
    RedisConfig.from_env() at all."""
    with (
        patch.dict(
            "os.environ",
            {"REDIS_HOST": "env-host", "REDIS_PASSWORD": "env-secret"},
            clear=False,
        ),
        patch("by_framework.worker.app.init_redis") as mock_init_redis,
        patch("by_framework.worker.app.close_redis", new_callable=AsyncMock),
        patch("by_framework.worker.app.WorkerRegistry"),
        patch("by_framework.worker.app.WorkspaceManager"),
        patch("by_framework.worker.app.WorkerRunner") as mock_runner,
    ):
        mock_init_redis.return_value = MagicMock()
        mock_runner.return_value.start = AsyncMock()

        await _run_worker_async(
            worker_class=MyTestWorker,
            worker_id="test-w1",
            redis_host=None,
            redis_port=None,
            redis_db=None,
            redis_password=None,
            redis_username=None,
            workspace_dir="/tmp/test-ws",
            consumer_group="test-group",
            max_concurrency=10,
            fetch_count=5,
        )

        mock_init_redis.assert_called_once()
        _, kwargs = mock_init_redis.call_args
        assert kwargs["config"].host == "env-host"
        assert kwargs["config"].password == "env-secret"
        assert kwargs["config"].mode == "standalone"


@pytest.mark.asyncio
async def test_run_worker_async_explicit_arg_overrides_env_in_standalone_mode():
    """An explicit arg must still win over an env var set for the same field."""
    with (
        patch.dict(
            "os.environ",
            {"REDIS_HOST": "env-host", "REDIS_PASSWORD": "env-secret"},
            clear=False,
        ),
        patch("by_framework.worker.app.init_redis") as mock_init_redis,
        patch("by_framework.worker.app.close_redis", new_callable=AsyncMock),
        patch("by_framework.worker.app.WorkerRegistry"),
        patch("by_framework.worker.app.WorkspaceManager"),
        patch("by_framework.worker.app.WorkerRunner") as mock_runner,
    ):
        mock_init_redis.return_value = MagicMock()
        mock_runner.return_value.start = AsyncMock()

        await _run_worker_async(
            worker_class=MyTestWorker,
            worker_id="test-w1",
            redis_host="explicit-host",
            redis_port=None,
            redis_db=None,
            redis_password="explicit-secret",
            redis_username=None,
            workspace_dir="/tmp/test-ws",
            consumer_group="test-group",
            max_concurrency=10,
            fetch_count=5,
        )

        _, kwargs = mock_init_redis.call_args
        assert kwargs["config"].host == "explicit-host"
        assert kwargs["config"].password == "explicit-secret"


@pytest.mark.asyncio
async def test_run_worker_async_supports_fully_programmatic_cluster_config():
    """Cluster mode must be fully configurable via explicit args alone, with
    no env vars at all - true parity with standalone's configurability,
    which is the whole point of unifying the two resolution paths."""
    with (
        patch("by_framework.worker.app.init_redis") as mock_init_redis,
        patch("by_framework.worker.app.close_redis", new_callable=AsyncMock),
        patch("by_framework.worker.app.WorkerRegistry"),
        patch("by_framework.worker.app.WorkspaceManager"),
        patch("by_framework.worker.app.WorkerRunner") as mock_runner,
    ):
        mock_init_redis.return_value = MagicMock()
        mock_runner.return_value.start = AsyncMock()

        await _run_worker_async(
            worker_class=MyTestWorker,
            worker_id="test-w1",
            redis_host=None,
            redis_port=None,
            redis_db=None,
            redis_password="cluster-secret",
            redis_username=None,
            workspace_dir="/tmp/test-ws",
            consumer_group="test-group",
            max_concurrency=10,
            fetch_count=5,
            redis_mode="cluster",
            redis_cluster_nodes=[("h1", 6379), ("h2", 6380)],
        )

        _, kwargs = mock_init_redis.call_args
        assert kwargs["config"].mode == "cluster"
        assert kwargs["config"].cluster_nodes == [("h1", 6379), ("h2", 6380)]
        assert kwargs["config"].password == "cluster-secret"


@pytest.mark.asyncio
async def test_run_worker_async_cluster_nodes_alone_implies_cluster_mode():
    """Passing redis_cluster_nodes without redis_mode must imply cluster
    mode, mirroring REDIS_CLUSTER_HOST's env-var precedence - otherwise the
    nodes list would be silently dropped in standalone mode."""
    with (
        patch("by_framework.worker.app.init_redis") as mock_init_redis,
        patch("by_framework.worker.app.close_redis", new_callable=AsyncMock),
        patch("by_framework.worker.app.WorkerRegistry"),
        patch("by_framework.worker.app.WorkspaceManager"),
        patch("by_framework.worker.app.WorkerRunner") as mock_runner,
    ):
        mock_init_redis.return_value = MagicMock()
        mock_runner.return_value.start = AsyncMock()

        await _run_worker_async(
            worker_class=MyTestWorker,
            worker_id="test-w1",
            redis_host=None,
            redis_port=None,
            redis_db=None,
            redis_password=None,
            redis_username=None,
            workspace_dir="/tmp/test-ws",
            consumer_group="test-group",
            max_concurrency=10,
            fetch_count=5,
            redis_cluster_nodes=[("h1", 6379), ("h2", 6380)],
        )

        _, kwargs = mock_init_redis.call_args
        assert kwargs["config"].mode == "cluster"
        assert kwargs["config"].cluster_nodes == [("h1", 6379), ("h2", 6380)]


@pytest.mark.asyncio
async def test_run_worker_async_explicit_standalone_mode_overrides_cluster_nodes():
    """An explicitly-passed redis_mode still wins over the redis_cluster_nodes
    inference, for callers that need to force standalone regardless."""
    with (
        patch("by_framework.worker.app.init_redis") as mock_init_redis,
        patch("by_framework.worker.app.close_redis", new_callable=AsyncMock),
        patch("by_framework.worker.app.WorkerRegistry"),
        patch("by_framework.worker.app.WorkspaceManager"),
        patch("by_framework.worker.app.WorkerRunner") as mock_runner,
    ):
        mock_init_redis.return_value = MagicMock()
        mock_runner.return_value.start = AsyncMock()

        await _run_worker_async(
            worker_class=MyTestWorker,
            worker_id="test-w1",
            redis_host=None,
            redis_port=None,
            redis_db=None,
            redis_password=None,
            redis_username=None,
            workspace_dir="/tmp/test-ws",
            consumer_group="test-group",
            max_concurrency=10,
            fetch_count=5,
            redis_mode="standalone",
            redis_cluster_nodes=[("h1", 6379)],
        )

        _, kwargs = mock_init_redis.call_args
        assert kwargs["config"].mode == "standalone"


@pytest.mark.asyncio
async def test_run_worker_async_initializes_cluster_client_when_mode_is_cluster():
    """REDIS_MODE=cluster must route init_redis through config=, not the
    individual host/port args, so REDIS_CLUSTER_NODES is actually honored."""
    with (
        patch.dict(
            "os.environ",
            {"REDIS_MODE": "cluster", "REDIS_CLUSTER_NODES": "h1:6379,h2:6380"},
            clear=False,
        ),
        patch("by_framework.worker.app.init_redis") as mock_init_redis,
        patch("by_framework.worker.app.close_redis", new_callable=AsyncMock),
        patch("by_framework.worker.app.WorkerRegistry"),
        patch("by_framework.worker.app.WorkspaceManager"),
        patch("by_framework.worker.app.WorkerRunner") as mock_runner,
    ):
        mock_init_redis.return_value = MagicMock()
        mock_runner.return_value.start = AsyncMock()

        await _run_worker_async(
            worker_class=MyTestWorker,
            worker_id="test-w1",
            redis_host="localhost",
            redis_port=6379,
            redis_db=0,
            redis_password=None,
            redis_username=None,
            workspace_dir="/tmp/test-ws",
            consumer_group="test-group",
            max_concurrency=10,
            fetch_count=5,
        )

        mock_init_redis.assert_called_once()
        _, kwargs = mock_init_redis.call_args
        assert "config" in kwargs
        assert kwargs["config"].mode == "cluster"
        assert kwargs["config"].cluster_nodes == [("h1", 6379), ("h2", 6380)]


@pytest.mark.asyncio
async def test_run_worker_async_attaches_layout_builder():
    """Verify _run_worker_async wires layout_builder onto the worker."""
    layout_builder = CustomLayoutBuilder()
    with (
        patch("by_framework.worker.app.init_redis") as mock_init_redis,
        patch(
            "by_framework.worker.app.close_redis", new_callable=AsyncMock
        ) as mock_close_redis,
        patch("by_framework.worker.app.WorkerRegistry"),
        patch("by_framework.worker.app.WorkspaceManager"),
        patch("by_framework.worker.app.WorkerRunner") as mock_runner,
    ):
        mock_init_redis.return_value = MagicMock()
        mock_runner.return_value.start = AsyncMock()

        await _run_worker_async(
            worker_class=MyTestWorker,
            worker_id="test-layout-worker",
            redis_host="localhost",
            redis_port=6379,
            redis_db=0,
            redis_password=None,
            redis_username=None,
            workspace_dir="/tmp/test-ws",
            consumer_group="test-group",
            layout_builder=layout_builder,
        )

        _, kwargs = mock_runner.call_args
        worker = kwargs["worker"]
        assert worker.get_data_layout_builder() is layout_builder

        # 4. Verify resource cleanup
        mock_close_redis.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_worker_with_plugins():
    """Verify plugin configuration logic."""
    mock_plugin = MagicMock()
    mock_configurator = MagicMock()

    with (
        patch("by_framework.worker.app.init_redis"),
        patch("by_framework.worker.app.close_redis", new_callable=AsyncMock),
        patch("by_framework.worker.app.WorkerRegistry"),
        patch("by_framework.worker.app.WorkspaceManager"),
        patch("by_framework.worker.app.PluginRegistry") as mock_plugin_registry_cls,
        patch("by_framework.worker.app.WorkerRunner") as mock_runner,
    ):

        mock_plugin_registry = mock_plugin_registry_cls.return_value
        mock_runner_instance = mock_runner.return_value
        mock_runner_instance.start = AsyncMock()

        await _run_worker_async(
            worker_class=MyTestWorker,
            worker_id="test-w2",
            redis_host="localhost",
            redis_port=6379,
            redis_db=0,
            redis_password=None,
            redis_username=None,
            workspace_dir="/tmp/test-ws",
            consumer_group="test-group",
            plugin_list=[mock_plugin],
            plugin_configurator=mock_configurator,
        )

        # Verify plugin registration
        mock_plugin_registry.register_bundle.assert_called_once_with(mock_plugin)
        # Verify configuration callback
        mock_configurator.assert_called_once_with(mock_plugin_registry)


def test_load_trace_provider_factories_from_module_discovers_concrete_subclasses():
    """Verify subclass-based module discovery only loads concrete factories."""

    class ConcreteFactory(TraceProviderFactory):

        @property
        def provider_name(self) -> str:
            return "concrete"

        def build_plugin_from_env(self) -> Plugin | None:
            return FakeTracePlugin()

    class _AbstractFactory(TraceProviderFactory):

        @property
        def provider_name(self) -> str:
            return "abstract"

    fake_module = type(
        "FakeModule",
        (),
        {
            "ConcreteFactory": ConcreteFactory,
            "_AbstractFactory": _AbstractFactory,
            "Irrelevant": object,
        },
    )()

    factories = _load_trace_provider_factories_from_module(fake_module)

    assert len(factories) == 1
    assert factories[0].provider_name == "concrete"


def test_build_auto_trace_plugin_returns_single_active_plugin():
    """Verify provider discovery returns the single configured trace plugin."""
    plugin = FakeTracePlugin()
    with patch(
        "by_framework.worker.app._discover_trace_provider_factories"
    ) as mock_discover:
        mock_discover.return_value = [
            FakeTraceProviderFactory(provider_name="langfuse", plugin=plugin)
        ]

        discovered = _build_auto_trace_plugin()

    assert discovered is plugin


def test_build_auto_trace_plugin_returns_none_when_all_providers_disabled():
    """Verify provider factories can opt out cleanly when not configured."""
    with patch(
        "by_framework.worker.app._discover_trace_provider_factories"
    ) as mock_discover:
        mock_discover.return_value = [
            FakeTraceProviderFactory(provider_name="langfuse", plugin=None)
        ]

        discovered = _build_auto_trace_plugin()

    assert discovered is None


def test_build_auto_trace_plugin_raises_when_multiple_providers_are_enabled():
    """Verify only one trace provider can be active at a time."""
    with patch(
        "by_framework.worker.app._discover_trace_provider_factories"
    ) as mock_discover:
        mock_discover.return_value = [
            FakeTraceProviderFactory(
                provider_name="langfuse", plugin=FakeTracePlugin()
            ),
            FakeTraceProviderFactory(
                provider_name="langsmith", plugin=FakeTracePlugin()
            ),
        ]

        with pytest.raises(RuntimeError, match="Multiple trace providers"):
            _build_auto_trace_plugin()


@pytest.mark.asyncio
async def test_run_worker_auto_registers_discovered_trace_plugin():
    """Verify a discovered trace provider plugin is auto-registered."""
    with (
        patch("by_framework.worker.app.init_redis"),
        patch("by_framework.worker.app.close_redis", new_callable=AsyncMock),
        patch("by_framework.worker.app.WorkerRegistry"),
        patch("by_framework.worker.app.WorkspaceManager"),
        patch("by_framework.worker.app.PluginRegistry") as mock_plugin_registry_cls,
        patch("by_framework.worker.app.WorkerRunner") as mock_runner,
        patch("by_framework.worker.app._build_auto_trace_plugin") as mock_builder,
    ):
        mock_plugin_registry = mock_plugin_registry_cls.return_value
        mock_runner.return_value.start = AsyncMock()
        mock_trace_plugin = FakeTracePlugin()
        mock_builder.return_value = mock_trace_plugin

        await _run_worker_async(
            worker_class=MyTestWorker,
            worker_id="test-w-trace",
            redis_host="localhost",
            redis_port=6379,
            redis_db=0,
            redis_password=None,
            redis_username=None,
            workspace_dir="/tmp/test-ws",
            consumer_group="test-group",
        )

        mock_plugin_registry.register_bundle.assert_any_call(mock_trace_plugin)


@pytest.mark.asyncio
async def test_run_worker_skips_trace_plugin_when_no_provider_is_active():
    """Verify worker startup does nothing when no trace provider is enabled."""
    with (
        patch("by_framework.worker.app.init_redis"),
        patch("by_framework.worker.app.close_redis", new_callable=AsyncMock),
        patch("by_framework.worker.app.WorkerRegistry"),
        patch("by_framework.worker.app.WorkspaceManager"),
        patch("by_framework.worker.app.PluginRegistry") as mock_plugin_registry_cls,
        patch("by_framework.worker.app.WorkerRunner") as mock_runner,
        patch("by_framework.worker.app._build_auto_trace_plugin") as mock_builder,
    ):
        mock_runner.return_value.start = AsyncMock()
        mock_plugin_registry = mock_plugin_registry_cls.return_value
        mock_builder.return_value = None

        await _run_worker_async(
            worker_class=MyTestWorker,
            worker_id="test-w-no-trace",
            redis_host="localhost",
            redis_port=6379,
            redis_db=0,
            redis_password=None,
            redis_username=None,
            workspace_dir="/tmp/test-ws",
            consumer_group="test-group",
        )

        mock_plugin_registry.register_bundle.assert_not_called()


def test_langfuse_config_from_env_strips_quotes_and_validates_required_fields():
    """Verify env loading accepts quoted values and returns normalized config."""
    with patch.dict(
        "os.environ",
        {
            "LANGFUSE_SECRET_KEY": '"sk-test"',
            "LANGFUSE_PUBLIC_KEY": "'pk-test'",
            "LANGFUSE_BASE_URL": "“http://localhost:3000”",
        },
        clear=False,
    ):
        config = LangfuseConfig.from_env()

    assert config is not None
    assert config.secret_key == "sk-test"
    assert config.public_key == "pk-test"
    assert config.base_url == "http://localhost:3000"


def test_langfuse_config_returns_none_when_explicitly_disabled():
    """Verify BYAI_LANGFUSE_ENABLED=false disables Langfuse even with keys present."""
    with patch.dict(
        "os.environ",
        {
            "LANGFUSE_SECRET_KEY": "sk-test",
            "LANGFUSE_PUBLIC_KEY": "pk-test",
            "LANGFUSE_BASE_URL": "http://localhost:3000",
            "BYAI_LANGFUSE_ENABLED": "false",
        },
        clear=False,
    ):
        config = LangfuseConfig.from_env()

    assert config is None


@pytest.mark.asyncio
async def test_run_worker_with_history():
    """Verify history backend configuration logic."""
    mock_history = MagicMock()

    with (
        patch("by_framework.worker.app.init_redis"),
        patch("by_framework.worker.app.close_redis", new_callable=AsyncMock),
        patch("by_framework.worker.app.WorkerRegistry"),
        patch("by_framework.worker.app.WorkspaceManager"),
        patch(
            "by_framework.worker.app.HistoryManager.set_default_backend"
        ) as mock_set_backend,
        patch("by_framework.worker.app.WorkerRunner") as mock_runner,
    ):

        mock_runner_instance = mock_runner.return_value
        mock_runner_instance.start = AsyncMock()

        await _run_worker_async(
            worker_class=MyTestWorker,
            worker_id="test-w-history",
            redis_host="localhost",
            redis_port=6379,
            redis_db=0,
            redis_password=None,
            redis_username=None,
            workspace_dir="/tmp/test-ws",
            consumer_group="test-group",
            history_backend=mock_history,
        )

        # Verify history backend was set
        mock_set_backend.assert_called_once_with(mock_history)


def test_run_worker_sync_entry():
    """Verify that the sync entry run_worker correctly starts the event loop."""
    with (
        patch("by_framework.worker.app.asyncio.run") as mock_asyncio_run,
        patch(
            "by_framework.worker.app._run_worker_async", new_callable=MagicMock
        ) as mock_run_async,
    ):

        run_worker(worker_class=MyTestWorker, worker_id="sync-w1")

        # Verify asyncio.run was called
        mock_asyncio_run.assert_called_once()

        # Verify _run_worker_async was called, and parameters were passed correctly
        mock_run_async.assert_called_once()
        args, _ = mock_run_async.call_args
        # positional: worker_class, worker_id, redis_host, ...
        assert args[1] == "sync-w1"
        # redis_host defaults to None (not "localhost") - "not specified",
        # falls back to REDIS_HOST env var / RedisConfig's own default
        # inside _run_worker_async, not here.
        assert args[2] is None
