"""
Plugin registry module.

Provides the PluginRegistry class for plugin discovery, registration,
and lifecycle management in Gateway Workers.
"""

from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
import time
from typing import TYPE_CHECKING, Any, Dict, List, Tuple

import dill

from by_framework.common.logger import logger

from .agent_config import AgentConfig
from .plugin import (AgentConfigsSnapshot, Plugin, PluginBuildContext,
                     PluginReloadContext, PluginReloadResult)

if TYPE_CHECKING:
    from by_framework.core.protocol.commands import (AskAgentCommand,
                                                     CancelTaskCommand)
    from by_framework.worker.context import AgentContext
    from by_framework.worker.worker import GatewayWorker


class PluginRegistry:
    """Plugin registry for discovery, registration, and lifecycle management.

    Plugins are the extension mechanism for Gateway Worker. Each plugin
    registers AgentConfig and can implement lifecycle hooks.
    """

    def __init__(self):
        self.plugins: List[Plugin] = []
        self.log_hook_stats_on_shutdown: bool = True
        self._agent_configs: List[AgentConfig] = []
        self._agent_configs_version: int = 0
        self._initialized_plugins: set[int] = set()
        self.hook_stats: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._processed_reload_ids: set[str] = set()
        self._reload_status: Dict[str, Dict[str, Any]] = {}

    @property
    def agent_configs(self) -> List[AgentConfig]:
        return self._agent_configs

    @property
    def agent_configs_version(self) -> int:
        return self._agent_configs_version

    def get_agent_configs_snapshot(self) -> AgentConfigsSnapshot:
        """Return an immutable snapshot of the current stable config version."""
        return AgentConfigsSnapshot(
            version=self._agent_configs_version,
            configs=tuple(self._agent_configs),
        )

    @staticmethod
    def serialize_agent_configs_snapshot(snapshot: AgentConfigsSnapshot) -> bytes:
        """Serialize a snapshot for durable restart recovery."""
        return dill.dumps(snapshot)

    @staticmethod
    def deserialize_agent_configs_snapshot(payload: bytes) -> AgentConfigsSnapshot:
        """Restore a serialized snapshot payload."""
        restored = dill.loads(payload)
        if not isinstance(restored, AgentConfigsSnapshot):
            raise TypeError(
                "Persisted agent configs snapshot payload did not contain "
                "an AgentConfigsSnapshot"
            )
        return restored

    def get_reload_status(self, reload_id: str) -> Dict[str, Any] | None:
        """Return the recorded status for a reload id, if present."""
        status = self._reload_status.get(reload_id)
        return dict(status) if status is not None else None

    def agent_config(self, agent_id: str) -> AgentConfig | None:
        return next(
            filter(lambda config: config.agent_id == agent_id, self._agent_configs),
            None,
        )

    def register_bundle(self, plugin: Plugin) -> None:
        if plugin not in self.plugins:
            if any(existing.name == plugin.name for existing in self.plugins):
                logger.warning("Duplicate plugin name detected: %s", plugin.name)
            self.plugins.append(plugin)

    def register_bundles(self, plugins: List[Plugin]) -> None:
        for plugin in plugins:
            self.register_bundle(plugin)

    def get_active_plugins(self) -> List[Plugin]:
        active_plugins = [
            plugin
            for plugin in self.plugins
            if getattr(plugin.manifest, "enabled", True)
        ]
        return sorted(
            active_plugins,
            key=lambda plugin: (-getattr(plugin.manifest, "priority", 0), plugin.name),
        )

    def get_plugin(self, plugin_id: str) -> Plugin | None:
        return next(filter(lambda p: p.plugin_id == plugin_id, self.plugins), None)

    @staticmethod
    def _extract_context_ids(context: Any) -> Tuple[str, str]:
        session_id = getattr(context, "session_id", "") if context is not None else ""
        trace_id = getattr(context, "trace_id", "") if context is not None else ""
        return session_id, trace_id

    def _ensure_hook_stats(self, plugin_name: str, hook_name: str) -> Dict[str, Any]:
        plugin_stats = self.hook_stats.setdefault(plugin_name, {})
        return plugin_stats.setdefault(
            hook_name,
            {
                "success": 0,
                "failure": 0,
                "timeout": 0,
                "total_ms": 0.0,
                "last_error": "",
            },
        )

    async def _execute_hook(
        self,
        plugin: Plugin,
        hook_name: str,
        coro: Any,
        session_id: str = "",
        trace_id: str = "",
        worker_id: str = "",
    ) -> None:
        """Execute plugin hooks with timeout and error handling."""

        stat = self._ensure_hook_stats(plugin.name, hook_name)
        started_at = time.perf_counter()
        timeout_seconds = getattr(plugin, "hook_timeout_seconds", None)
        timeout_seconds = (
            timeout_seconds if (timeout_seconds and timeout_seconds > 0) else None
        )

        logger.info(
            "Executing plugin hook: plugin=%s hook=%s timeout=%s "
            "worker_id=%s session_id=%s trace_id=%s",
            plugin.name,
            hook_name,
            timeout_seconds,
            worker_id,
            session_id,
            trace_id,
        )

        try:
            if timeout_seconds:
                await asyncio.wait_for(coro, timeout=timeout_seconds)
            else:
                await coro
            elapsed_ms = (time.perf_counter() - started_at) * 1000
            stat["success"] += 1
            stat["total_ms"] += elapsed_ms
            logger.info(
                "Plugin hook completed: plugin=%s hook=%s elapsed_ms=%.2f",
                plugin.name,
                hook_name,
                elapsed_ms,
            )
        except asyncio.TimeoutError as e:
            stat["failure"] += 1
            stat["timeout"] += 1
            stat["last_error"] = str(e)
            logger.exception(
                "Plugin %s %s timed out (timeout=%ss, worker_id=%s, "
                "session_id=%s, trace_id=%s)",
                plugin.name,
                hook_name,
                timeout_seconds,
                worker_id,
                session_id,
                trace_id,
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            stat["failure"] += 1
            stat["last_error"] = str(e)
            logger.exception(
                "Plugin %s %s failed: %s (worker_id=%s, session_id=%s, trace_id=%s)",
                plugin.name,
                hook_name,
                e,
                worker_id,
                session_id,
                trace_id,
            )
            stat["total_ms"] += (time.perf_counter() - started_at) * 1000

    def get_hook_stats(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Get execution statistics for all plugin hooks.

        Returns:
            Dictionary containing statistics for each plugin's each hook
        """
        snapshot: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for plugin_name, plugin_stats in self.hook_stats.items():
            snapshot[plugin_name] = {}
            for hook_name, stat in plugin_stats.items():
                total_runs = stat["success"] + stat["failure"]
                avg_ms = stat["total_ms"] / total_runs if total_runs > 0 else 0.0
                snapshot[plugin_name][hook_name] = {
                    **stat,
                    "avg_ms": avg_ms,
                    "total_runs": total_runs,
                }
        return snapshot

    def log_hook_stats(self) -> None:
        stats = self.get_hook_stats()
        if not stats:
            logger.info("Plugin hook stats: no data")
            return

        for plugin_name, plugin_stats in stats.items():
            for hook_name, stat in plugin_stats.items():
                logger.info(
                    "Plugin hook stats: plugin=%s hook=%s total_runs=%s "
                    "success=%s failure=%s timeout=%s avg_ms=%.2f last_error=%s",
                    plugin_name,
                    hook_name,
                    stat.get("total_runs", 0),
                    stat.get("success", 0),
                    stat.get("failure", 0),
                    stat.get("timeout", 0),
                    stat.get("avg_ms", 0.0),
                    stat.get("last_error", ""),
                )

    def reset_hook_stats(self, plugin_name: str = "", hook_name: str = "") -> None:
        """Reset hook statistics for a plugin or specific hook."""
        if not plugin_name:
            self.hook_stats.clear()
            return

        plugin_stats = self.hook_stats.get(plugin_name)
        if not plugin_stats:
            return

        if not hook_name:
            self.hook_stats.pop(plugin_name, None)
            return

        plugin_stats.pop(hook_name, None)
        if not plugin_stats:
            self.hook_stats.pop(plugin_name, None)

    def _validate_agent_config(self, config: AgentConfig) -> None:
        if not config.agent_id:
            raise ValueError("AgentConfig.agent_id must not be empty")

    def _validate_agent_config_list(self, configs: List[AgentConfig]) -> None:
        for config in configs:
            self._validate_agent_config(config)

    def _set_agent_configs(self, configs: List[AgentConfig]) -> None:
        self._validate_agent_config_list(configs)
        self._agent_configs = list(configs)
        self._agent_configs_version += 1

    @staticmethod
    def _extract_reload_configs(
        result: List[AgentConfig] | PluginReloadResult | None,
        fallback: List[AgentConfig],
    ) -> List[AgentConfig]:
        if result is None:
            return list(fallback)
        if isinstance(result, PluginReloadResult):
            return list(result.agent_configs)
        return list(result)

    async def _replay_reload_chain(
        self,
        *,
        base_snapshot: AgentConfigsSnapshot,
        reload_id: str,
        reason: str,
    ) -> List[AgentConfig]:
        """Replay plugin reload hooks in order to build the next config version."""
        stable_configs = base_snapshot.configs
        stable_version = base_snapshot.version
        working_configs = list(stable_configs)

        for plugin in self.get_active_plugins():
            reload_context = PluginReloadContext(
                plugin_id=plugin.plugin_id,
                reload_id=reload_id,
                reason=reason,
                current_agent_configs=tuple(working_configs),
                previous_stable_agent_configs=stable_configs,
                current_version=stable_version,
            )
            result = await plugin.reload(reload_context)
            working_configs = self._extract_reload_configs(result, working_configs)
            self._validate_agent_config_list(working_configs)

        return working_configs

    def _record_reload_success(
        self,
        *,
        reload_id: str,
        reason: str,
        version_before: int,
        version_after: int,
    ) -> None:
        self._processed_reload_ids.add(reload_id)
        self._reload_status[reload_id] = {
            "status": "success",
            "reason": reason,
            "version_before": version_before,
            "version_after": version_after,
            "error": "",
        }

    def _record_reload_failure(
        self,
        *,
        reload_id: str,
        reason: str,
        version_before: int,
        version_after: int,
        error: Exception,
    ) -> None:
        self._processed_reload_ids.add(reload_id)
        self._reload_status[reload_id] = {
            "status": "failure",
            "reason": reason,
            "version_before": version_before,
            "version_after": version_after,
            "error": str(error),
        }

    async def _register_plugin_agent_configs(
        self, plugin: Plugin, build_context: PluginBuildContext
    ) -> None:
        """Register agent configs from a plugin into the registry."""
        # Provide the plugin with a read-only snapshot of the previous version
        build_context.freeze_prev_agent_configs()
        new_configs = await plugin.register_agent_configs(build_context)
        if new_configs is not None:
            build_context.set_agent_configs(new_configs)

        configs = build_context.list_agent_configs()
        if not configs:
            return

        staged_configs = list(self._agent_configs)
        for config in configs:
            self._validate_agent_config(config)
            agent_id = config.agent_id
            existing = next(
                filter(
                    lambda item, target=agent_id: item.agent_id == target,
                    staged_configs,
                ),
                None,
            )
            if existing:
                if existing is config:
                    continue
                if config.on_conflict == "error":
                    raise ValueError(f"agent_config '{agent_id}' is already registered")
                if config.on_conflict == "skip":
                    logger.warning(
                        "Skip duplicate agent_config registration: %s", agent_id
                    )
                    continue
                logger.warning(
                    "Overwrite duplicate agent_config registration: %s", agent_id
                )
                staged_configs.remove(existing)

            staged_configs.append(config)

        self._agent_configs = staged_configs

    async def discover_plugins(self) -> None:
        """Auto-discover registered plugin classes and instantiate for registration."""
        for cls in Plugin.get_registered_plugins():
            if any(isinstance(p, cls) for p in self.plugins):
                continue

            try:
                plugin = cls()
                self.register_bundle(plugin)
                logger.info(
                    "Auto-discovered and registered plugin: %s", plugin.plugin_id
                )
            except TypeError as e:
                logger.debug(
                    "Skip auto-instantiation for plugin class %s: %s", cls.__name__, e
                )
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error(
                    "Failed to auto-instantiate plugin class %s: %s", cls.__name__, e
                )

    def load_plugins_from_dir(self, directory: str) -> None:
        """Dynamically load plugin modules from the specified directory.

        Args:
            directory: Directory path containing plugin Python files
        """
        if not os.path.isdir(directory):
            logger.warning(
                "Plugin directory not found or not a directory: %s", directory
            )
            return

        abs_dir = os.path.abspath(directory)
        if abs_dir not in sys.path:
            sys.path.insert(0, abs_dir)

        logger.info("Scanning directory for plugins: %s", abs_dir)

        for filename in os.listdir(abs_dir):
            if filename.endswith(".py") and not filename.startswith("__"):
                module_name = filename[:-3]
                file_path = os.path.join(abs_dir, filename)

                try:
                    spec = importlib.util.spec_from_file_location(
                        module_name, file_path
                    )
                    if spec and spec.loader:
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                        logger.info("Dynamically loaded plugin module: %s", module_name)
                except Exception as e:  # pylint: disable=broad-exception-caught
                    logger.error(
                        "Failed to load plugin module from %s: %s", file_path, e
                    )

    async def initialize_plugins(
        self, build_context: PluginBuildContext | None = None
    ) -> None:
        """Initialize all discovered plugins.

        Args:
            build_context: Optional build context
        """
        previous_configs = list(self._agent_configs)
        if build_context is None:
            build_context = PluginBuildContext(agent_configs=list(self._agent_configs))

        for plugin in self.get_active_plugins():
            plugin_id = id(plugin)
            if plugin_id in self._initialized_plugins:
                continue

            before_success = self._ensure_hook_stats(
                plugin.name, "register_agent_configs"
            )["success"]
            await self._execute_hook(
                plugin,
                "register_agent_configs",
                self._register_plugin_agent_configs(
                    plugin, build_context=build_context
                ),
            )
            after_success = self._ensure_hook_stats(
                plugin.name, "register_agent_configs"
            )["success"]
            if after_success > before_success:
                self._initialized_plugins.add(plugin_id)

        if self._agent_configs != previous_configs:
            self._agent_configs_version += 1

    async def reload_plugins(
        self,
        reload_id: str,
        reason: str = "",
    ) -> AgentConfigsSnapshot:
        """Sequentially replay plugin reload hooks over the current stable version.

        Each plugin receives the current working AgentConfig list and may return
        the next full version. The stable version is only replaced after the
        whole pipeline succeeds.
        """
        if reload_id in self._processed_reload_ids:
            return self.get_agent_configs_snapshot()

        base_snapshot = self.get_agent_configs_snapshot()

        try:
            next_configs = await self._replay_reload_chain(
                base_snapshot=base_snapshot,
                reload_id=reload_id,
                reason=reason,
            )
            self._set_agent_configs(next_configs)
            next_snapshot = self.get_agent_configs_snapshot()
            self._record_reload_success(
                reload_id=reload_id,
                reason=reason,
                version_before=base_snapshot.version,
                version_after=next_snapshot.version,
            )
            return next_snapshot
        except Exception as error:
            self._record_reload_failure(
                reload_id=reload_id,
                reason=reason,
                version_before=base_snapshot.version,
                version_after=self._agent_configs_version,
                error=error,
            )
            raise

    def apply_default_hook_timeout(self, timeout_seconds: float) -> None:
        """Set default hook timeout for all plugins that don't have timeout set.

        Args:
            timeout_seconds: Default timeout in seconds
        """
        if timeout_seconds <= 0:
            return
        for plugin in self.plugins:
            if getattr(plugin, "hook_timeout_seconds", None) is None:
                plugin.hook_timeout_seconds = timeout_seconds

    async def on_worker_startup(self, worker: "GatewayWorker"):
        await self.discover_plugins()
        await self.initialize_plugins()

        for plugin in self.get_active_plugins():
            await self._execute_hook(
                plugin,
                "on_worker_startup",
                plugin.on_worker_startup(worker),
                worker_id=getattr(worker, "worker_id", ""),
            )

    async def on_worker_shutdown(self, worker: "GatewayWorker"):
        for plugin in self.get_active_plugins():
            await self._execute_hook(
                plugin,
                "on_worker_shutdown",
                plugin.on_worker_shutdown(worker),
                worker_id=getattr(worker, "worker_id", ""),
            )

    async def on_task_start(self, context: "AgentContext"):
        session_id, trace_id = self._extract_context_ids(context)
        for plugin in self.get_active_plugins():
            await self._execute_hook(
                plugin,
                "on_task_start",
                plugin.on_task_start(context),
                session_id=session_id,
                trace_id=trace_id,
            )

    async def on_task_complete(self, context: "AgentContext", result: Any):
        session_id, trace_id = self._extract_context_ids(context)
        for plugin in self.get_active_plugins():
            await self._execute_hook(
                plugin,
                "on_task_complete",
                plugin.on_task_complete(context, result),
                session_id=session_id,
                trace_id=trace_id,
            )

    async def on_task_error(self, context: "AgentContext", error: Exception):
        session_id, trace_id = self._extract_context_ids(context)
        for plugin in self.get_active_plugins():
            await self._execute_hook(
                plugin,
                "on_task_error",
                plugin.on_task_error(context, error),
                session_id=session_id,
                trace_id=trace_id,
            )

    async def on_task_cancel(
        self, context: "AgentContext", command: "CancelTaskCommand"
    ):
        session_id, trace_id = self._extract_context_ids(context)
        for plugin in self.get_active_plugins():
            await self._execute_hook(
                plugin,
                "on_task_cancel",
                plugin.on_task_cancel(context, command),
                session_id=session_id,
                trace_id=trace_id,
            )

    async def on_call_agent_start(
        self, context: "AgentContext", command: "AskAgentCommand"
    ) -> None:
        session_id, trace_id = self._extract_context_ids(context)
        for plugin in self.get_active_plugins():
            await self._execute_hook(
                plugin,
                "on_call_agent_start",
                plugin.on_call_agent_start(context, command),
                session_id=session_id,
                trace_id=trace_id,
            )

    async def on_call_agent_complete(
        self,
        context: "AgentContext",
        command: "AskAgentCommand",
        result: Any,
    ) -> None:
        session_id, trace_id = self._extract_context_ids(context)
        for plugin in self.get_active_plugins():
            await self._execute_hook(
                plugin,
                "on_call_agent_complete",
                plugin.on_call_agent_complete(context, command, result),
                session_id=session_id,
                trace_id=trace_id,
            )

    async def on_call_agent_error(
        self,
        context: "AgentContext",
        command: "AskAgentCommand",
        error: Exception,
    ) -> None:
        session_id, trace_id = self._extract_context_ids(context)
        for plugin in self.get_active_plugins():
            await self._execute_hook(
                plugin,
                "on_call_agent_error",
                plugin.on_call_agent_error(context, command, error),
                session_id=session_id,
                trace_id=trace_id,
            )
