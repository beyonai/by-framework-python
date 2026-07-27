"""Example 1: the smallest possible NativeAgentWorker.

No tools, no sub_agents — just an AgentConfig and a model turn. Run:

    uv run python examples/01_basic_chat.py
"""

# pylint: disable=invalid-name  # numbered filename for tutorial ordering

import asyncio

from _infra import Dispatcher, InMemoryRedis
from by_framework.core.extensions.agent_config import AgentConfig
from by_framework.core.extensions.registry import PluginRegistry
from by_framework.core.protocol.commands import AskAgentCommand
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.core.workspace import WorkspaceManager

from by_framework_agent import NativeAgentWorker
from by_framework_agent.model_client import ModelChunk
from by_framework_agent.testing import StubModelClient


class AssistantWorker(NativeAgentWorker):

    def get_agent_types(self) -> list[str]:
        return ["assistant"]


def build_registry() -> PluginRegistry:
    registry = PluginRegistry()
    registry._set_agent_configs(  # pylint: disable=protected-access
        [
            AgentConfig(
                agent_id="assistant",
                name="Assistant",
                prompts={"system": "You are a concise, friendly assistant."},
                # NativeAgentWorker reads the model from AgentConfig.extra["model"] —
                # any litellm-supported model string works with the real
                # LiteLLMModelClient. This example scripts the model call instead.
                extra={"model": "gpt-4o-mini"},
            )
        ]
    )
    return registry


async def main() -> None:
    redis = InMemoryRedis()
    dispatcher = Dispatcher(redis=redis)
    dispatcher.register(
        "assistant",
        lambda: AssistantWorker(
            worker_id="assistant-worker",
            redis_client=redis,
            workspace_manager=WorkspaceManager(),
            plugin_registry=build_registry(),
            # A real deployment omits model_client entirely and gets the
            # default LiteLLMModelClient, which calls a real provider. This
            # example scripts the reply so it runs with no API key.
            model_client=StubModelClient(
                turns=[
                    [
                        ModelChunk(content="Hi there"),
                        ModelChunk(content="! How can I help today?"),
                        ModelChunk(
                            is_final=True,
                            finish_reason="stop",
                            usage={"prompt_tokens": 18, "completion_tokens": 9},
                            model="gpt-4o-mini",
                        ),
                    ]
                ]
            ),
        ),
    )

    command = AskAgentCommand(
        header=MessageHeader(
            message_id="msg-1",
            session_id="session-1",
            trace_id="trace-1",
            user_code="user-1",
            user_name="Alice",
            target_agent_type="assistant",
        ),
        content="Hello!",
    )

    result = await dispatcher.dispatch_root(command)
    print(f"status:  {result.status}")
    print(f"content: {result.content}")


if __name__ == "__main__":
    asyncio.run(main())
