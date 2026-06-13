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
    from by_framework.core.protocol.commands import (
        AskAgentCommand,
        CancelTaskCommand,
        ResumeCommand,
    )
    from by_framework.worker.context import AgentContext
    from by_framework.worker.worker import GatewayWorker


@dataclass
class PromptTemplate:
    """Prompt template utility type that supports variable placeholders.

    Can be placed in AgentConfig.prompts.

    Attributes:
        content: Template content string, supports {variable} format placeholders
        variables: List of automatically extracted variable names
    """

    content: str
    variables: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.variables:
            self.variables = self._extract_variables(self.content)

    @staticmethod
    def _extract_variables(content: str) -> List[str]:
        """Extract all variable names from template content."""
        field_names: List[str] = []
        for _, field_name, _, _ in Formatter().parse(content):
            if field_name:
                field_names.append(field_name)
        return field_names

    def render(self, **kwargs: Any) -> str:
        """Render the template using the provided variable values.

        Args:
            **kwargs: Mapping of variable names to values

        Returns:
            Rendered string

        Raises:
            KeyError: If provided variables are incomplete
        """
        missing = [var for var in self.variables if var not in kwargs]
        if missing:
            raise KeyError(
                f"Prompt missing variables: {missing}; "
                f"provided keys: {sorted(kwargs.keys())}"
            )
        return self.content.format(**kwargs)


@dataclass
class PluginManifest:
    """Plugin manifest information.

    Attributes:
        plugin_id: Plugin unique identifier
        version: Plugin version number
        priority: Plugin priority, higher number runs earlier
        enabled: Whether the plugin is enabled
    """

    plugin_id: str
    version: str = "1.0.0"
    priority: int = 0
    enabled: bool = True


@dataclass
class PluginBuildContext:
    """Build context used during plugin registration phase (not runtime).

    Provides read-only access and write capability to AgentConfig during
    plugin registration.
    """

    agent_configs: list[AgentConfig] = field(default_factory=list)
    _prev_agent_configs: tuple[AgentConfig, ...] = ()

    def set_agent_configs(self, new_configs: list[AgentConfig]) -> None:
        """Set new AgentConfig list."""
        self.agent_configs = list(new_configs)

    def list_agent_configs(self) -> list[AgentConfig]:
        """Return a copy of the current AgentConfig list."""
        return list(self.agent_configs)

    def freeze_prev_agent_configs(self) -> None:
        """Freeze current AgentConfigs as a read-only snapshot."""
        self._prev_agent_configs = tuple(self.agent_configs)

    def get_prev_agent_configs(self) -> tuple[AgentConfig, ...]:
        """Get the read-only snapshot of the previous version of AgentConfigs."""
        return self._prev_agent_configs


@dataclass(frozen=True)
class AgentConfigsSnapshot:
    """Immutable view of a stable AgentConfig collection version."""

    version: int
    configs: tuple[AgentConfig, ...]


@dataclass(frozen=True)
class PluginReloadContext:
    """Context passed to plugin reload hooks.

    The current_agent_configs field represents the working config list for the
    current reload stage. Plugins can transform that list and return the next
    full config version.
    """

    plugin_id: str
    reload_id: str
    reason: str
    current_agent_configs: tuple[AgentConfig, ...]
    previous_stable_agent_configs: tuple[AgentConfig, ...]
    current_version: int


@dataclass(frozen=True)
class PluginReloadResult:
    """Optional structured result for reload workflows."""

    agent_configs: tuple[AgentConfig, ...]


class Plugin(ABC):
    """Plugin abstract base class.

    Plugins are responsible for registering AgentConfig and optionally providing
    lifecycle hooks. Create a plugin by inheriting from this class and implementing
    the register_agent_configs method.
    """

    _registered_plugins: List[Type["Plugin"]] = []

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not inspect.isabstract(cls):
            Plugin._registered_plugins.append(cls)

    @classmethod
    def get_registered_plugins(cls) -> List[Type["Plugin"]]:
        """Get all registered plugin classes."""
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
        """Plugin registration entry method.

        Plugin can read the read-only snapshot of build_context and return a new
        agent_configs list.

        Args:
            build_context: Plugin build context

        Returns:
            New AgentConfig list, or None
        """
        raise NotImplementedError

    async def reload(
        self,
        context: PluginReloadContext,
    ) -> list[AgentConfig] | PluginReloadResult | None:
        """Transform the current stable config chain into the next version.

        Default behavior is a no-op so existing plugins remain compatible.
        Plugins that support hot reload can override this method to return the
        next full AgentConfig list for the current reload stage.
        """
        return list(context.current_agent_configs)

    async def on_worker_startup(self, worker: "GatewayWorker") -> None:
        """Hook called when Worker starts.

        Args:
            worker: GatewayWorker instance
        """
        pass

    async def on_worker_shutdown(self, worker: "GatewayWorker") -> None:
        """Hook called when Worker shuts down.

        Args:
            worker: GatewayWorker instance
        """
        pass

    async def on_task_start(self, context: "AgentContext") -> None:
        """Hook called when task starts.

        Args:
            context: AgentContext instance
        """
        pass

    async def on_task_complete(self, context: "AgentContext", result: Any) -> None:
        """Hook called when task completes.

        Args:
            context: AgentContext instance
            result: Task execution result
        """
        pass

    async def on_task_error(self, context: "AgentContext", error: Exception) -> None:
        """Hook called when task encounters an error.

        Args:
            context: AgentContext instance
            error: Exception object
        """
        pass

    async def on_task_cancel(
        self, context: "AgentContext", command: "CancelTaskCommand"
    ) -> None:
        """Hook called when task is cancelled.

        Args:
            context: AgentContext instance
            command: Cancel task command
        """
        pass

    async def on_call_agent_start(
        self, context: "AgentContext", command: "AskAgentCommand"
    ) -> None:
        """Hook triggered before calling another Agent."""
        pass

    async def on_call_agent_complete(
        self,
        context: "AgentContext",
        command: "AskAgentCommand",
        result: Any,
    ) -> None:
        """Hook triggered after successfully enqueuing a call to another Agent."""
        pass

    async def on_call_agent_error(
        self,
        context: "AgentContext",
        command: "AskAgentCommand",
        error: Exception,
    ) -> None:
        """Hook triggered when calling another Agent fails."""
        pass

    async def on_agent_return_start(
        self,
        context: "AgentContext",
        command: "AskAgentCommand",
        callback_command: "ResumeCommand",
    ) -> None:
        """Hook triggered before enqueueing a ResumeCommand to the caller."""
        pass

    async def on_agent_return_complete(
        self,
        context: "AgentContext",
        command: "AskAgentCommand",
        callback_command: "ResumeCommand",
    ) -> None:
        """Hook triggered after successfully enqueueing a ResumeCommand."""
        pass

    async def on_agent_return_error(
        self,
        context: "AgentContext",
        command: "AskAgentCommand",
        callback_command: "ResumeCommand",
        error: Exception,
    ) -> None:
        """Hook triggered when enqueueing a ResumeCommand fails."""
        pass
