"""Example 4: agent-as-tool with *two* sub-agents in one turn — Task Group.

Per ADR-0001, a turn that delegates to more than one distinct sub-agent
fans out as a single `call_agents` (Task Group) dispatch instead of
sequential `call_agent` hops. Group Join then resumes the orchestrator's
loop exactly once, only after *both* siblings have replied, with each
reply correlated back to its own tool_call_id. Run:

    uv run python examples/04_task_group.py
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


class SpecialistWorker(NativeAgentWorker):
    """Reused for both `pricing_specialist` and `availability_specialist` —
    only the AgentConfig/model_client passed in differs per instance."""

    def get_agent_types(self) -> list[str]:
        return [self._agent_type]

    def __init__(self, *args, agent_type: str, **kwargs) -> None:
        self._agent_type = agent_type
        super().__init__(*args, **kwargs)


def orchestrator_registry() -> PluginRegistry:
    registry = PluginRegistry()
    registry._set_agent_configs(  # pylint: disable=protected-access
        [
            AgentConfig(
                agent_id="orchestrator",
                name="Orchestrator",
                prompts={
                    "system": (
                        "You plan a trip by asking pricing_specialist and "
                        "availability_specialist, then combine their answers."
                    )
                },
                sub_agents=["pricing_specialist", "availability_specialist"],
                extra={"model": "gpt-4o-mini"},
            ),
            # Needed so the orchestrator's auto-generated tool schemas can
            # read each sub-agent's description (see _sub_agent_tool_schemas).
            AgentConfig(
                agent_id="pricing_specialist",
                name="Pricing Specialist",
                description="Quotes prices for a destination.",
                extra={"model": "gpt-4o-mini"},
            ),
            AgentConfig(
                agent_id="availability_specialist",
                name="Availability Specialist",
                description="Checks date availability for a destination.",
                extra={"model": "gpt-4o-mini"},
            ),
        ]
    )
    return registry


def specialist_registry(agent_id: str, system_prompt: str) -> PluginRegistry:
    registry = PluginRegistry()
    registry._set_agent_configs(  # pylint: disable=protected-access
        [
            AgentConfig(
                agent_id=agent_id,
                prompts={"system": system_prompt},
                extra={"model": "gpt-4o-mini"},
            )
        ]
    )
    return registry


async def main() -> None:
    redis = InMemoryRedis()
    dispatcher = Dispatcher(redis=redis)
    redis.mark_agent_type_online("pricing_specialist", worker_id="pricing-worker")
    redis.mark_agent_type_online(
        "availability_specialist", worker_id="availability-worker"
    )

    # One turn, two distinct sub-agent tool calls — this is what triggers
    # Task Group batching (HarnessLoop._dispatch_sub_agent_task_group)
    # instead of the single-target call_agent path.
    orchestrator_model_client = StubModelClient(
        turns=[
            [
                ModelChunk(
                    tool_call_deltas=[
                        {
                            "index": 0,
                            "id": "call_pricing",
                            "function": {
                                "name": "pricing_specialist",
                                "arguments": json.dumps(
                                    {"content": "Quote a price for Tokyo."}
                                ),
                            },
                        },
                        {
                            "index": 1,
                            "id": "call_availability",
                            "function": {
                                "name": "availability_specialist",
                                "arguments": json.dumps(
                                    {"content": "Check availability for Tokyo."}
                                ),
                            },
                        },
                    ],
                    is_final=True,
                    finish_reason="tool_calls",
                )
            ],
            [
                ModelChunk(
                    content=(
                        "Tokyo: $1,200 round trip, and dates in March are " "available."
                    )
                ),
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
        "pricing_specialist",
        lambda: SpecialistWorker(
            worker_id="pricing-worker",
            agent_type="pricing_specialist",
            redis_client=redis,
            workspace_manager=WorkspaceManager(),
            plugin_registry=specialist_registry(
                "pricing_specialist", "You quote a round-trip price in USD."
            ),
            model_client=StubModelClient(
                turns=[
                    [
                        ModelChunk(content="$1,200 round trip to Tokyo."),
                        ModelChunk(is_final=True, finish_reason="stop"),
                    ]
                ]
            ),
        ),
    )
    dispatcher.register(
        "availability_specialist",
        lambda: SpecialistWorker(
            worker_id="availability-worker",
            agent_type="availability_specialist",
            redis_client=redis,
            workspace_manager=WorkspaceManager(),
            plugin_registry=specialist_registry(
                "availability_specialist", "You check date availability."
            ),
            model_client=StubModelClient(
                turns=[
                    [
                        ModelChunk(content="Tokyo is available in March."),
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
        content="Plan a trip to Tokyo — price and availability, please.",
    )

    suspend_result = await dispatcher.dispatch_root(command)
    print(f"after fan-out:   status={suspend_result.status}")
    assert suspend_result.status == "QUEUED"

    # Exactly one Task Group dispatch went out for both specialists — drain
    # routes each to its own worker and relays both replies back, and Group
    # Join only resumes the orchestrator once BOTH have answered.
    processed = await dispatcher.drain()
    for agent_type, result in processed:
        print(f"[{agent_type}] status={result.status} content={result.content!r}")


if __name__ == "__main__":
    asyncio.run(main())
