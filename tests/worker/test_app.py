import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from by_framework.worker.app import _run_worker_async, run_worker
from by_framework.worker.worker import GatewayWorker


class MyTestWorker(GatewayWorker):
    def get_capabilities(self):
        return ["test-capability"]


@pytest.mark.asyncio
async def test_run_worker_async_flow():
    """验证 _run_worker_async 的完整启动流程"""
    
    # Mock 所有外部依赖
    with patch("by_framework.worker.app.init_redis") as mock_init_redis, \
         patch("by_framework.worker.app.close_redis", new_callable=AsyncMock) as mock_close_redis, \
         patch("by_framework.worker.app.WorkerRegistry") as mock_worker_registry, \
         patch("by_framework.worker.app.WorkspaceManager") as mock_workspace_manager, \
         patch("by_framework.worker.app.WorkerRunner") as mock_runner:
        
        # 配置 Mock Behavior
        mock_init_redis.return_value = MagicMock()  # Mock Redis Client
        mock_runner_instance = mock_runner.return_value
        mock_runner_instance.start = AsyncMock()
        
        # 执行测试函数
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
            fetch_count=5
        )
        
        # 1. 验证 Redis 初始化
        mock_init_redis.assert_called_once_with(
            host="localhost",
            port=6379,
            db=0,
            password=None,
            username=None,
            max_connections=20  # max_concurrency (10) + 10
        )
        
        # 2. 验证核心组件实例化
        mock_worker_registry.assert_called_once()
        mock_workspace_manager.assert_called_once_with("/tmp/test-ws")
        
        # 3. 验证 Runner 启动
        mock_runner_instance.start.assert_awaited_once()
        
        # 4. 验证资源清理
        mock_close_redis.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_worker_with_plugins():
    """验证插件配置逻辑"""
    mock_plugin = MagicMock()
    mock_configurator = MagicMock()
    
    with patch("by_framework.worker.app.init_redis"), \
         patch("by_framework.worker.app.close_redis", new_callable=AsyncMock), \
         patch("by_framework.worker.app.WorkerRegistry"), \
         patch("by_framework.worker.app.WorkspaceManager"), \
         patch("by_framework.worker.app.PluginRegistry") as mock_plugin_registry_cls, \
         patch("by_framework.worker.app.WorkerRunner") as mock_runner:
        
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
            plugin_configurator=mock_configurator
        )
        
        # 验证插件注册
        mock_plugin_registry.register_bundle.assert_called_once_with(mock_plugin)
        # 验证配置回调
        mock_configurator.assert_called_once_with(mock_plugin_registry)


@pytest.mark.asyncio
async def test_run_worker_with_history():
    """验证 history 后端配置逻辑"""
    mock_history = MagicMock()
    
    with patch("by_framework.worker.app.init_redis"), \
         patch("by_framework.worker.app.close_redis", new_callable=AsyncMock), \
         patch("by_framework.worker.app.WorkerRegistry"), \
         patch("by_framework.worker.app.WorkspaceManager"), \
         patch("by_framework.worker.app.HistoryManager.set_default_backend") as mock_set_backend, \
         patch("by_framework.worker.app.WorkerRunner") as mock_runner:
        
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
            history_backend=mock_history
        )
        
        # 验证历史记录后端被设置
        mock_set_backend.assert_called_once_with(mock_history)


def test_run_worker_sync_entry():
    """验证同步入口 run_worker 是否正确启动事件循环"""
    with patch("by_framework.worker.app.asyncio.run") as mock_asyncio_run, \
         patch("by_framework.worker.app._run_worker_async", new_callable=MagicMock) as mock_run_async:
        
        run_worker(
            worker_class=MyTestWorker,
            worker_id="sync-w1"
        )
        
        # 验证 asyncio.run 被调用
        mock_asyncio_run.assert_called_once()
        
        # 验证 _run_worker_async 被调用，且参数传递正确
        mock_run_async.assert_called_once()
        args, _ = mock_run_async.call_args
        # positional: worker_class, worker_id, redis_host, ...
        assert args[1] == "sync-w1"
        assert args[2] == "localhost"
