import asyncio

import pytest

from byclaw_gateway_sdk import (
    AgentConfig,
    AgentContext,
    Plugin,
    PluginManifest,
    PluginRegistry,
)


class AutoRegisteredPlugin(Plugin):
    """一个用于测试自动注册的插件"""

    def __init__(self):
        super().__init__(
            manifest=PluginManifest(plugin_id="auto_plugin", version="1.0.0")
        )
        self.hook_called = False

    async def register_agent_configs(self, agent_context=None) -> list[AgentConfig]:
        return [
            AgentConfig(
                agent_id="auto_agent",
                tools={"auto_tool": lambda x: x},
            )
        ]

    async def on_task_start(self, context: AgentContext) -> None:
        self.hook_called = True


@pytest.mark.asyncio
async def test_plugin_discovery_and_context():
    """Test that plugins are discovered on worker startup and agent configs are accessible via context."""
    registry = PluginRegistry()

    class MockWorker:

        def __init__(self):
            self.worker_id = "test_worker"

    await registry.on_worker_startup(MockWorker())

    plugin = registry.get_plugin("auto_plugin")
    assert plugin is not None
    assert isinstance(plugin, AutoRegisteredPlugin)

    config = registry.agent_config("auto_agent")
    assert config is not None
    assert callable(config.tools.get("auto_tool"))

    context = AgentContext(session_id="test_session", trace_id="test_trace")
    context.set_agent_configs(registry.agent_configs)
    context_config = context.get_agent_config("auto_agent")
    assert context_config is config


if __name__ == "__main__":
    asyncio.run(test_plugin_discovery_and_context())
