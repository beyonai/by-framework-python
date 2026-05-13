#!/usr/bin/env python3
"""Simple plugin system test script (agent-config only)."""
import asyncio

import pytest

from by_framework import (
    AgentConfig,
    AgentContext,
    GatewayWorker,
    Plugin,
    PluginManifest,
    PluginRegistry,
    PromptTemplate,
)


class MockBundle(Plugin):

    def __init__(self, plugin_id: str, priority: int = 0):
        super().__init__(PluginManifest(plugin_id=plugin_id, priority=priority))

    async def register_agent_configs(
        self, agent_context=None
    ) -> list[AgentConfig] | None:
        return None


async def mock_tool(query: str):
    return f"Result for {query}"


class MockCalculatorSkill:

    async def add(self, a: int, b: int):
        return a + b


class ResourcesBundle(Plugin):

    def __init__(self):
        super().__init__(PluginManifest(plugin_id="resources_bundle"))

    async def register_agent_configs(self, agent_context=None) -> list[AgentConfig]:
        return [
            AgentConfig(
                agent_id="resources_agent",
                tools={"test_tool": mock_tool},
                prompts={
                    "test_prompt": PromptTemplate(
                        content="Hello {name}, welcome to the system!",
                        variables=["name"],
                    )
                },
                skills={"calculator": MockCalculatorSkill()},
            )
        ]


class MockWorker(GatewayWorker):

    def get_agent_types(self):
        return ["test_worker"]

    async def process_command(self, command, context: AgentContext):
        config = context.get_agent_config("resources_agent")
        assert config is not None

        prompt = config.prompts.get("test_prompt")
        if prompt:
            assert prompt.render(name="User") == "Hello User, welcome to the system!"

        result = await context.call_tool("test_tool", query="Hello World")
        assert result == "Result for Hello World"

        calculator = config.skills.get("calculator")
        if calculator:
            sum_result = await calculator.add(2, 3)
            assert sum_result == 5

        return {"status": "success"}


@pytest.mark.asyncio
async def test_plugins():
    """Test that multiple plugins can register bundles and agent configs
    are accessible."""
    registry = PluginRegistry()
    registry.register_bundle(MockBundle(plugin_id="test_bundle_1"))
    registry.register_bundle(MockBundle(plugin_id="test_bundle_2"))
    registry.register_bundle(ResourcesBundle())

    await registry.initialize_plugins()

    assert len(registry.plugins) == 3
    config = registry.agent_config("resources_agent")
    assert config is not None
    assert config.tools.get("test_tool") is not None
    assert config.prompts.get("test_prompt") is not None
    assert config.skills.get("calculator") is not None


if __name__ == "__main__":
    asyncio.run(test_plugins())
