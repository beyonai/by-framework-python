"""Example 5: a real model doing ReAct (reason, call a tool, observe, repeat).

Every other example scripts the model's replies with StubModelClient so they
run offline. This one calls a real provider through litellm's
LiteLLMModelClient — the harness's *default* ModelClient, so this is really
just examples/02's tool-calling loop with `model_client=...` removed.

Setup:

    export OPENAI_API_KEY=sk-...        # or ANTHROPIC_API_KEY, etc.
    export MODEL=gpt-4o-mini            # any litellm model string; see below
    cd libs/by-framework-agent
    uv run python examples/05_real_model_react.py "What is 12 * (7 + 5)?"

Switching providers is just the `MODEL` string plus the matching API key env
var — litellm reads it automatically:

    MODEL=anthropic/claude-3-5-sonnet-20241022   ANTHROPIC_API_KEY=...
    MODEL=gemini/gemini-2.0-flash                GEMINI_API_KEY=...
    MODEL=azure/<deployment-name>                AZURE_API_KEY=..., AZURE_API_BASE=...

Redis is still faked (InMemoryRedis, see _infra.py) — only the model call is
real. A production deployment additionally runs this behind WorkerRunner
against a real Redis Streams cluster instead of Dispatcher.
"""

# pylint: disable=invalid-name  # numbered filename for tutorial ordering

import asyncio
import os
import sys

from _infra import Dispatcher, InMemoryRedis
from by_framework.core.extensions.agent_config import AgentConfig
from by_framework.core.extensions.registry import PluginRegistry
from by_framework.core.protocol.commands import AskAgentCommand
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.core.workspace import WorkspaceManager

from by_framework_agent import NativeAgentWorker, ToolSpec

# Restricted to a handful of safe operators — a real calculator tool would
# use a proper expression parser instead of eval().
_ALLOWED_CALC_CHARS = set("0123456789+-*/(). ")


async def calculate(context, arguments) -> str:
    del context
    expression = str(arguments.get("expression", ""))
    if not expression or not set(expression) <= _ALLOWED_CALC_CHARS:
        return "error: expression must be a simple arithmetic expression"
    try:
        # Safe here specifically because _ALLOWED_CALC_CHARS above already
        # rejected anything but digits/operators/parens/spaces — no
        # identifier characters means no attribute-chain sandbox escape.
        return str(
            eval(expression, {"__builtins__": {}}, {})  # pylint: disable=eval-used
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return f"error: {exc}"


def build_registry() -> PluginRegistry:
    registry = PluginRegistry()
    registry._set_agent_configs(  # pylint: disable=protected-access
        [
            AgentConfig(
                agent_id="assistant",
                name="Assistant",
                prompts={
                    "system": (
                        "You are a careful assistant. For any arithmetic, "
                        "call the calculate tool instead of computing it "
                        "yourself, then answer using its result."
                    )
                },
                tools={
                    "calculate": ToolSpec(
                        name="calculate",
                        handler=calculate,
                        description="Evaluate a simple arithmetic expression.",
                        parameters={
                            "type": "object",
                            "properties": {
                                "expression": {
                                    "type": "string",
                                    "description": "e.g. '12 * (7 + 5)'",
                                }
                            },
                            "required": ["expression"],
                        },
                    )
                },
                # This is the whole configuration surface for a real model:
                # - extra["model"]: any litellm model string
                # - extra["model_params"]: forwarded to every ModelClient.
                #   complete() call (temperature, max_tokens, api_base, ...)
                extra={
                    "model": os.environ.get("MODEL", "gpt-4o-mini"),
                    "model_params": {"temperature": 0.2},
                },
            )
        ]
    )
    return registry


class AssistantWorker(NativeAgentWorker):

    def get_agent_types(self) -> list[str]:
        return ["assistant"]

    # No model_client override here — omitting it entirely (as this worker
    # does) is what makes NativeAgentWorker default to LiteLLMModelClient.


async def main() -> None:
    question = " ".join(sys.argv[1:]) or "What is 12 * (7 + 5)?"

    redis = InMemoryRedis()
    dispatcher = Dispatcher(redis=redis)
    dispatcher.register(
        "assistant",
        lambda: AssistantWorker(
            worker_id="assistant-worker",
            redis_client=redis,
            workspace_manager=WorkspaceManager(),
            plugin_registry=build_registry(),
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
        content=question,
    )

    print(f"question: {question}")
    result = await dispatcher.dispatch_root(command)
    print(f"status:   {result.status}")
    print(f"answer:   {result.content}")


if __name__ == "__main__":
    asyncio.run(main())
