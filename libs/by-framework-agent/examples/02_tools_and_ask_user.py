"""Example 2: a local tool, plus the built-in ask_user tool.

Shows the harness's suspend/resume cycle: the model calls ask_user, the
worker returns WAITING_USER, and — on a *separate* worker instance,
demonstrating this doesn't depend on process affinity — a ResumeCommand with
the user's reply continues the same loop to completion. Run:

    uv run python examples/02_tools_and_ask_user.py
"""

# pylint: disable=invalid-name  # numbered filename for tutorial ordering

import asyncio
import datetime
import json

from _infra import Dispatcher, InMemoryRedis
from by_framework.core.extensions.agent_config import AgentConfig
from by_framework.core.extensions.registry import PluginRegistry
from by_framework.core.protocol.commands import AskAgentCommand, ResumeCommand
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.core.workspace import WorkspaceManager

from by_framework_agent import NativeAgentWorker, ToolSpec
from by_framework_agent.model_client import ModelChunk
from by_framework_agent.testing import StubModelClient


async def get_current_time(context, arguments) -> str:
    del context, arguments
    # A real tool would do real work; this one's deterministic for the demo.
    return datetime.datetime(2026, 1, 1, 9, 30).isoformat()


def build_registry() -> PluginRegistry:
    registry = PluginRegistry()
    registry._set_agent_configs(  # pylint: disable=protected-access
        [
            AgentConfig(
                agent_id="assistant",
                name="Assistant",
                prompts={"system": "You are a friendly scheduling assistant."},
                tools={
                    "get_current_time": ToolSpec(
                        name="get_current_time",
                        handler=get_current_time,
                        description="Return the current date and time.",
                    )
                },
                extra={"model": "gpt-4o-mini"},
            )
        ]
    )
    return registry


class AssistantWorker(NativeAgentWorker):

    def get_agent_types(self) -> list[str]:
        return ["assistant"]


async def main() -> None:
    redis = InMemoryRedis()
    dispatcher = Dispatcher(redis=redis)

    # Turn 1: call the local tool. Turn 2: ask the user a question — this
    # suspends the loop instead of producing a final answer. Turn 3 (scripted
    # on a *different* worker instance below): the final answer, once the
    # user's reply is available as a tool result.
    first_worker_model_client = StubModelClient(
        turns=[
            [
                ModelChunk(
                    tool_call_deltas=[
                        {
                            "index": 0,
                            "id": "call_time",
                            "function": {"name": "get_current_time", "arguments": "{}"},
                        }
                    ],
                    is_final=True,
                    finish_reason="tool_calls",
                )
            ],
            [
                ModelChunk(
                    tool_call_deltas=[
                        {
                            "index": 0,
                            "id": "call_ask",
                            "function": {
                                "name": "ask_user",
                                "arguments": json.dumps(
                                    {"prompt": "What's your name?"}
                                ),
                            },
                        }
                    ],
                    is_final=True,
                    finish_reason="tool_calls",
                )
            ],
        ]
    )
    dispatcher.register(
        "assistant",
        lambda: AssistantWorker(
            worker_id="assistant-worker-a",
            redis_client=redis,
            workspace_manager=WorkspaceManager(),
            plugin_registry=build_registry(),
            model_client=first_worker_model_client,
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
        content="What time is it, and can you greet me by name?",
    )

    execution_id = "exec-demo-1"
    suspend_result = await dispatcher.dispatch_root(command, execution_id=execution_id)
    print(f"after ask_user:  status={suspend_result.status}")
    assert suspend_result.status == "WAITING_USER"

    # A fresh worker instance — same execution_id, no shared Python state —
    # picks up the user's reply and finishes the conversation.
    dispatcher.register(
        "assistant",
        lambda: AssistantWorker(
            worker_id="assistant-worker-b",
            redis_client=redis,
            workspace_manager=WorkspaceManager(),
            plugin_registry=build_registry(),
            model_client=StubModelClient(
                turns=[
                    [
                        ModelChunk(content="Nice to meet you, Bob"),
                        ModelChunk(content="! It's 09:30 on 2026-01-01."),
                        ModelChunk(is_final=True, finish_reason="stop"),
                    ]
                ]
            ),
        ),
    )
    reply = ResumeCommand(
        header=MessageHeader(
            message_id="msg-2",
            session_id="session-1",
            trace_id="trace-1",
            parent_message_id="msg-1",
            user_code="user-1",
            user_name="Alice",
            target_agent_type="assistant",
        ),
        content="Bob",
    )
    final_result = await dispatcher.dispatch_root(reply, execution_id=execution_id)
    print(f"after resume:    status={final_result.status}")
    print(f"final content:   {final_result.content}")


if __name__ == "__main__":
    asyncio.run(main())
