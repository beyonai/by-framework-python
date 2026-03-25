"""
Plugin system core definitions.

This module provides the Plugin abstract base class and supporting types
for the extensible plugin architecture of the Gateway SDK.
"""

from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from string import Formatter
from typing import TYPE_CHECKING, Any, List, Type

from .agent_config import AgentConfig

if TYPE_CHECKING:
    from byclaw_gateway_sdk.core.protocol.commands import CancelTaskCommand
    from byclaw_gateway_sdk.worker.context import AgentContext
    from byclaw_gateway_sdk.worker.worker import GatewayWorker


@dataclass
class PromptTemplate:
    """提示词模板工具类型，支持变量占位符，可放入 AgentConfig.prompts。

    Attributes:
        content: 模板内容字符串，支持 {variable} 格式的变量占位符
        variables: 自动提取的变量名列表
    """

    content: str
    variables: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.variables:
            self.variables = self._extract_variables(self.content)

    @staticmethod
    def _extract_variables(content: str) -> List[str]:
        """从模板内容中提取所有变量名。"""
        field_names: List[str] = []
        for _, field_name, _, _ in Formatter().parse(content):
            if field_name:
                field_names.append(field_name)
        return field_names

    def render(self, **kwargs: Any) -> str:
        """使用提供的变量值渲染模板。

        Args:
            **kwargs: 变量名到值的映射

        Returns:
            渲染后的字符串

        Raises:
            KeyError: 如果提供的变量不完整
        """
        missing = [var for var in self.variables if var not in kwargs]
        if missing:
            raise KeyError(
                f"Prompt missing variables: {missing}; provided keys: {sorted(kwargs.keys())}"
            )
        return self.content.format(**kwargs)


@dataclass
class PluginManifest:
    """插件清单信息。

    Attributes:
        plugin_id: 插件唯一标识符
        version: 插件版本号
        priority: 插件优先级，数值越大优先级越高
        enabled: 插件是否启用
    """

    plugin_id: str
    version: str = "1.0.0"
    priority: int = 0
    enabled: bool = True


@dataclass
class PluginBuildContext:
    """插件注册阶段使用的构建上下文（非运行时 AgentContext）。

    在插件注册过程中提供对 AgentConfig 的只读访问和写入能力。
    """

    agent_configs: list[AgentConfig] = field(default_factory=list)
    _prev_agent_configs: tuple[AgentConfig, ...] = ()

    def set_agent_configs(self, new_configs: list[AgentConfig]) -> None:
        """设置新的 AgentConfig 列表。"""
        self.agent_configs = list(new_configs)

    def list_agent_configs(self) -> list[AgentConfig]:
        """返回当前 AgentConfig 列表的副本。"""
        return list(self.agent_configs)

    def freeze_prev_agent_configs(self) -> None:
        """冻结当前的 AgentConfigs 作为只读快照。"""
        self._prev_agent_configs = tuple(self.agent_configs)

    def get_prev_agent_configs(self) -> tuple[AgentConfig, ...]:
        """获取上一版本的 AgentConfigs 只读快照。"""
        return self._prev_agent_configs


class Plugin(ABC):
    """插件抽象基类。

    插件负责注册 AgentConfig，并可选地提供生命周期钩子。
    通过继承此类并实现 register_agent_configs 方法来创建插件。
    """

    _registered_plugins: List[Type["Plugin"]] = []

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not inspect.isabstract(cls):
            Plugin._registered_plugins.append(cls)

    @classmethod
    def get_registered_plugins(cls) -> List[Type["Plugin"]]:
        """获取所有已注册的插件类。"""
        return cls._registered_plugins

    def __init__(
        self, manifest: PluginManifest, hook_timeout_seconds: float | None = None
    ):
        self.manifest = manifest
        self.name = manifest.plugin_id
        self.plugin_id = manifest.plugin_id
        self.version = manifest.version
        self.hook_timeout_seconds = hook_timeout_seconds

    @abstractmethod
    async def register_agent_configs(
        self, build_context: PluginBuildContext
    ) -> list[AgentConfig] | None:
        """插件注册入口方法。

        插件可读取 build_context 的只读快照，并返回新的 agent_configs 列表。

        Args:
            build_context: 插件构建上下文

        Returns:
            新的 AgentConfig 列表，或 None
        """
        raise NotImplementedError

    async def on_worker_startup(self, worker: "GatewayWorker") -> None:
        """Worker 启动时调用的钩子。

        Args:
            worker: GatewayWorker 实例
        """
        pass

    async def on_worker_shutdown(self, worker: "GatewayWorker") -> None:
        """Worker 关闭时调用的钩子。

        Args:
            worker: GatewayWorker 实例
        """
        pass

    async def on_task_start(self, context: "AgentContext") -> None:
        """任务开始时调用的钩子。

        Args:
            context: AgentContext 实例
        """
        pass

    async def on_task_complete(self, context: "AgentContext", result: Any) -> None:
        """任务完成时调用的钩子。

        Args:
            context: AgentContext 实例
            result: 任务执行结果
        """
        pass

    async def on_task_error(self, context: "AgentContext", error: Exception) -> None:
        """任务出错时调用的钩子。

        Args:
            context: AgentContext 实例
            error: 异常对象
        """
        pass

    async def on_task_cancel(
        self, context: "AgentContext", command: "CancelTaskCommand"
    ) -> None:
        """任务取消时调用的钩子。

        Args:
            context: AgentContext 实例
            command: 取消任务命令
        """
        pass
