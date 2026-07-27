"""Example 3: agent-as-tool — delegating to one sub-agent via `call_agent`.

`AgentConfig.sub_agents` entries are auto-registered as callable tools, with
no separate AgentConfig.tools entry needed. When the model calls one, the
harness dispatches a *real* AskAgentCommand — here, `Dispatcher.drain()`
plays the role a real Redis Streams cluster would: it picks up the dispatched
command, hands it to the sub-agent's own worker, and relays that worker's
reply back as a ResumeCommand, which the orchestrator's suspended loop then
resumes from. Run:

    uv run python examples/03_agent_delegation.py
"""

# pylint: disable=invalid-name  # numbered filename for tutorial ordering

import asyncio
import json

from _infra import Dispatcher, InMemoryRedis
from by_framework.core.extensions.agent_config import AgentConfig
from by_framework.core.extensions.registry import PluginRegistry
from by_framework.core.protocol.commands import AskAgentCommand
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.core.workspace import WorkspaceManager

from by_framework_agent import NativeAgentWorker
from by_framework_agent.model_client import ModelChunk
from by_framework_agent.testing import StubModelClient


class OrchestratorWorker(NativeAgentWorker):

    def get_agent_types(self) -> list[str]:
        return ["orchestrator"]


class ResearcherWorker(NativeAgentWorker):

    def get_agent_types(self) -> list[str]:
        return ["researcher"]


def orchestrator_registry() -> PluginRegistry:
    registry = PluginRegistry()
    registry._set_agent_configs(  # pylint: disable=protected-access
        [
            AgentConfig(
                agent_id="orchestrator",
                name="Orchestrator",
                description="Delegates research questions to a specialist.",
                prompts={"system": "You delegate research questions to 'researcher'."},
                sub_agents=["researcher"],
                extra={"model": "gpt-4o-mini"},
            ),
            # The orchestrator's config manager needs to resolve "researcher"
            # too, so it can read its description for the auto-generated
            # tool schema — see HarnessLoop._sub_agent_tool_schemas.
            AgentConfig(
                agent_id="researcher",
                name="Researcher",
                description="Answers focused research questions.",
                extra={"model": "gpt-4o-mini"},
            ),
        ]
    )
    return registry


def researcher_registry() -> PluginRegistry:
    registry = PluginRegistry()
    registry._set_agent_configs(  # pylint: disable=protected-access
        [
            AgentConfig(
                agent_id="researcher",
                name="Researcher",
                prompts={"system": "You answer focused research questions concisely."},
                extra={"model": "gpt-4o-mini"},
            )
        ]
    )
    return registry


async def main() -> None:
    redis = InMemoryRedis()
    dispatcher = Dispatcher(redis=redis)
    redis.mark_agent_type_online("researcher", worker_id="researcher-worker")

    orchestrator_model_client = StubModelClient(
        turns=[
            [
                ModelChunk(
                    tool_call_deltas=[
                        {
                            "index": 0,
                            "id": "call_research",
                            "function": {
                                "name": "researcher",
                                "arguments": json.dumps(
                                    {"content": "What year was Redis released?"}
                                ),
                            },
                        }
                    ],
                    is_final=True,
                    finish_reason="tool_calls",
                )
            ],
            [
                ModelChunk(content="Redis was first released in 2009."),
                ModelChunk(is_final=True, finish_reason="stop"),
            ],
        ]
    )
    dispatcher.register(
        "orchestrator",
        lambda: OrchestratorWorker(
            worker_id="orchestrator-worker",
            redis_client=redis,
            workspace_manager=WorkspaceManager(),
            plugin_registry=orchestrator_registry(),
            model_client=orchestrator_model_client,
        ),
    )
    dispatcher.register(
        "researcher",
        lambda: ResearcherWorker(
            worker_id="researcher-worker",
            redis_client=redis,
            workspace_manager=WorkspaceManager(),
            plugin_registry=researcher_registry(),
            model_client=StubModelClient(
                turns=[
                    [
                        ModelChunk(content="Redis was released in 2009."),
                        ModelChunk(is_final=True, finish_reason="stop"),
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
            target_agent_type="orchestrator",
        ),
        content="What year was Redis released?",
    )

    suspend_result = await dispatcher.dispatch_root(command)
    print(f"after delegation: status={suspend_result.status}")
    assert suspend_result.status == "QUEUED"

    # Drain routes the dispatched call_agent command to the researcher
    # worker, then relays its reply back to the orchestrator — the same
    # Group Join / ResumeCommand machinery a real Redis deployment uses.
    processed = await dispatcher.drain()
    for agent_type, result in processed:
        print(f"[{agent_type}] status={result.status} content={result.content!r}")


if __name__ == "__main__":
    asyncio.run(main())
