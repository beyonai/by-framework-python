import asyncio

import pytest

from byclaw_gateway_sdk import (
    AgentConfig,
    Plugin,
    PluginManifest,
    PluginRegistry,
    PromptTemplate,
)


class BasicBundle(Plugin):

    def __init__(self):
        super().__init__(PluginManifest(plugin_id="basic_bundle"))

    async def register_agent_configs(self, agent_context=None) -> list[AgentConfig]:
        async def test_func(query):
            return f"Result for {query}"

        class Calculator:

            async def add(self, a, b):
                return a + b

        return [
            AgentConfig(
                agent_id="basic_agent",
                tools={"test_tool": test_func},
                prompts={
                    "test_prompt": PromptTemplate(
                        content="Hello {name}", variables=["name"]
                    )
                },
                skills={"calculator": Calculator()},
            )
        ]


@pytest.mark.asyncio
async def test_plugin_registration():
    """Test that a plugin can register agent configs with tools, prompts, and skills."""
    registry = PluginRegistry()
    registry.register_bundle(BasicBundle())

    assert len(registry.plugins) == 1

    await registry.initialize_plugins()
    config = registry.agent_config("basic_agent")
    assert config is not None

    assert config.tools.get("test_tool") is not None
    assert config.prompts.get("test_prompt") is not None
    assert config.skills.get("calculator") is not None
    assert config.prompts["test_prompt"].render(name="User") == "Hello User"


if __name__ == "__main__":
    asyncio.run(test_plugin_registration())
