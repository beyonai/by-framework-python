from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from by_framework_trace_langfuse import LangfuseConfig

from by_framework.core.extensions import (Plugin, PluginManifest, TraceProviderFactory)
from by_framework.worker.app import (
    _build_auto_trace_plugin,
    _load_trace_provider_factories_from_module,
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

        # 1. Verify Redis initialization
        mock_init_redis.assert_called_once_with(
            host="localhost",
            port=6379,
            db=0,
            password=None,
            username=None,
            max_connections=20,  # max_concurrency (10) + 10
        )

        # 2. Verify core component instantiation
        mock_worker_registry.assert_called_once()
        mock_workspace_manager.assert_called_once_with("/tmp/test-ws")

        # 3. Verify Runner start
        mock_runner_instance.start.assert_awaited_once()

        # 4. Verify resource cleanup
        mock_close_redis.assert_awaited_once()


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
        assert args[2] == "localhost"
