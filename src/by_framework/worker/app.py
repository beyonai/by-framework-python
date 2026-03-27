import asyncio
import inspect
from typing import Awaitable, Callable, List, Optional, Type, Union

from by_framework.common.logger import logger
from by_framework.common.redis_client import close_redis, init_redis
from by_framework.core.extensions import Plugin, PluginRegistry
from by_framework.core.runtime.history import BaseHistoryStorage, HistoryManager
from by_framework.core.registry import WorkerRegistry
from by_framework.core.workspace import WorkspaceManager
from by_framework.worker.runner import WorkerRunner
from by_framework.worker.worker import GatewayWorker


async def _run_worker_async(
    worker_class: Type[GatewayWorker],
    worker_id: str,
    redis_host: str,
    redis_port: int,
    redis_db: int,
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
    history: Optional[BaseHistoryStorage] = None,
    plugin_dir: Optional[str] = None,
    **worker_kwargs,
):
    logger.info("Initializing Gateway SDK for worker: %s", worker_class.__name__)

    # 1. 自动联动或读取环境变量配置 Redis 连接池
    actual_redis_max_conns = redis_max_connections
    if actual_redis_max_conns is None:
        import os

        env_val = os.environ.get("BYAI_REDIS_MAX_CONNECTIONS")
        if env_val:
            actual_redis_max_conns = int(env_val)
        else:
            # 默认联动：并发数 + 10个管理用连接
            actual_redis_max_conns = max_concurrency + 10

    # 2. 建立 Redis 连接 (必须在事件循环内部)
    redis_client = init_redis(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        password=redis_password,
        username=redis_username,
        max_connections=actual_redis_max_conns,
    )

    # 2.1 配置历史消息存储后端
    # - history=None: 使用默认 in-memory 存储
    # - history=BaseHistoryStorage: 使用指定存储
    # 存储后端自身决定支持哪些操作（如 save_message 未实现则无法保存）
    if history is not None:
        HistoryManager.set_default_storage(history)

    # 3. 创建并配置 PluginRegistry
    plugin_registry = PluginRegistry()

    # 3.1 如果指定了插件目录，先执行扫描加载
    if plugin_dir:
        plugin_registry.load_plugins_from_dir(plugin_dir)

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

    # 注意：插件的自动发现（discover_plugins）和初始化（initialize_plugins）
    # 现在已集成在 PluginRegistry.on_worker_startup 中，
    # 会在下文 worker 启动并触发心跳时自动执行。

    # 4. 初始化核心组件
    registry = WorkerRegistry(redis_client=redis_client)
    workspace_manager = WorkspaceManager(workspace_dir)

    # 5. 实例化具体的业务 Worker
    worker = worker_class(
        worker_id=worker_id,
        registry=registry,
        workspace_manager=workspace_manager,
        redis_client=redis_client,
        plugin_registry=plugin_registry,
        **worker_kwargs,
    )

    # 6. 创建运行器并阻塞启动
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
    redis_host: str = "localhost",
    redis_port: int = 6379,
    redis_db: int = 0,
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
    history: Optional[BaseHistoryStorage] = None,
    plugin_dir: Optional[str] = None,
    **worker_kwargs,
):
    """
    启动 Gateway Worker 的快捷入口，供第三方业务方直接调用。

    插件支持：
    - **自动模式**：系统会自动发现并加载所有继承 `Plugin` 的子类。
    - **目录扫描模式**：通过 `plugin_dir` 指定目录，自动扫描并加载其中的 Python 脚本。
    - **显式模式**：可以通过 `plugin_list` 手动传入插件实例，或通过 `plugin_configurator` 进行自定义配置。

    历史消息存储：
    - ``history=None``: 使用默认 in-memory 存储。
    - ``history=BaseHistoryStorage``: 使用指定的自定义存储后端。
      存储后端自身决定支持哪些操作（如未实现 ``save_message`` 则无法保存消息）。
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
                history=history,
                plugin_dir=plugin_dir,
                **worker_kwargs,
            )
        )
    except KeyboardInterrupt:
        logger.info("Worker runner stopped by user")
    except Exception as e:
        logger.error("Error running worker: %s", str(e))
        import traceback

        traceback.print_exc()
