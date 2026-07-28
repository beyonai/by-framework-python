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

Every AgentConfig.extra["model_params"] key litellm accepts is configurable
via its own env var, so you don't have to edit this file to try one:

    MODEL_TEMPERATURE=0.2   # float, forwarded as model_params["temperature"]
    MODEL_MAX_TOKENS=500    # int,   forwarded as model_params["max_tokens"]
    MODEL_API_BASE=...      # str,   for a self-hosted/proxy/OpenAI-compatible
                             #        endpoint (e.g. vLLM) instead of the
                             #        provider's default
    MODEL_API_KEY=...       # str,   overrides the provider's usual env var
                             #        just for this agent (e.g. a self-hosted
                             #        model's own key)
    MODEL_API_VERSION=...   # str,   e.g. for Azure OpenAI deployments
    MODEL_PARAMS_JSON='{"seed": 7, "top_p": 0.9}'
                             # any other litellm kwarg not covered above —
                             # merged in last, so it wins on overlapping keys

Example — point at a local vLLM/OpenAI-compatible server instead of a real
provider:

    MODEL=openai/local-llama \
    MODEL_API_BASE=http://localhost:8000/v1 \
    MODEL_API_KEY=sk-local-anything \
    uv run python examples/05_real_model_react.py "What is 9 * 6?"

Redis is still faked (InMemoryRedis, see _infra.py) — only the model call is
real. A production deployment additionally runs this behind WorkerRunner
against a real Redis Streams cluster instead of Dispatcher.
"""

# pylint: disable=invalid-name  # numbered filename for tutorial ordering

import asyncio
import json
import os
import sys
from typing import Any

from _infra import Dispatcher, InMemoryRedis
from by_framework.core.extensions.agent_config import AgentConfig, CallbackType
from by_framework.core.extensions.registry import PluginRegistry
from by_framework.core.protocol.commands import AskAgentCommand
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.core.workspace import WorkspaceManager

from by_framework_agent import NativeAgentWorker, ToolSpec

# Restricted to a handful of safe operators — a real calculator tool would
# use a proper expression parser instead of eval().
_ALLOWED_CALC_CHARS = set("0123456789+-*/(). ")


def _model_params_from_env() -> dict[str, Any]:
    """Build AgentConfig.extra["model_params"] from env vars — see the
    module docstring for the full list. Every value here is forwarded
    as-is to litellm.acompletion(), so names match litellm's own kwargs
    (temperature, max_tokens, api_base, api_key, api_version, ...)."""
    params: dict[str, Any] = {}

    if "MODEL_TEMPERATURE" in os.environ:
        params["temperature"] = float(os.environ["MODEL_TEMPERATURE"])
    if "MODEL_MAX_TOKENS" in os.environ:
        params["max_tokens"] = int(os.environ["MODEL_MAX_TOKENS"])
    if "MODEL_API_BASE" in os.environ:
        params["api_base"] = os.environ["MODEL_API_BASE"]
    if "MODEL_API_KEY" in os.environ:
        params["api_key"] = os.environ["MODEL_API_KEY"]
    if "MODEL_API_VERSION" in os.environ:
        params["api_version"] = os.environ["MODEL_API_VERSION"]

    raw_extra = os.environ.get("MODEL_PARAMS_JSON")
    if raw_extra:
        extra = json.loads(raw_extra)
        if not isinstance(extra, dict):
            raise ValueError(
                f"MODEL_PARAMS_JSON must decode to a JSON object, got {extra!r}"
            )
        params.update(extra)  # wins on any key also set via a named var above

    return params


class ReactTelemetry:
    """Makes it possible to tell "the model computed this itself" apart from
    "the calculate tool computed this" — the two look identical in the final
    answer text alone. Wired in as both the tool handler (so a tool
    invocation is directly observable) and before/after_model_callback (so
    a second model turn — only reachable after a tool result — is too).
    """

    def __init__(self) -> None:
        self.model_turn = 0
        self.tool_call_count = 0

    async def before_model(self, context, payload) -> None:
        del context
        self.model_turn += 1
        num_messages = len(payload["messages"])
        print(f"[MODEL TURN {self.model_turn}] sending {num_messages} messages")

    async def after_model(self, context, payload) -> None:
        del context
        preview = payload["content"] or "(no text — likely requesting a tool call)"
        print(f"[MODEL TURN {self.model_turn}] replied: {preview!r}")

    async def calculate(self, context, arguments) -> str:
        del context
        expression = str(arguments.get("expression", ""))
        if not expression or not set(expression) <= _ALLOWED_CALC_CHARS:
            return "error: expression must be a simple arithmetic expression"
        try:
            # Safe here specifically because _ALLOWED_CALC_CHARS above
            # already rejected anything but digits/operators/parens/spaces —
            # no identifier characters means no attribute-chain sandbox
            # escape.
            result = str(
                eval(expression, {"__builtins__": {}}, {})  # pylint: disable=eval-used
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            return f"error: {exc}"
        self.tool_call_count += 1
        print(f"[TOOL CALLED] calculate({expression!r}) -> {result}")
        return result


def build_registry(telemetry: ReactTelemetry) -> PluginRegistry:
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
                        handler=telemetry.calculate,
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
                callbacks={
                    CallbackType.before_model_callback: [telemetry.before_model],
                    CallbackType.after_model_callback: [telemetry.after_model],
                },
                # This is the whole configuration surface for a real model:
                # - extra["model"]: any litellm model string
                # - extra["model_params"]: forwarded to every ModelClient.
                #   complete() call (temperature, max_tokens, api_base, ...)
                # Both are sourced from env vars here — see the module
                # docstring for the full list — so trying a different
                # model/provider/endpoint never requires editing this file.
                extra={
                    "model": os.environ.get("MODEL", "gpt-4o-mini"),
                    "model_params": _model_params_from_env(),
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

    model = os.environ.get("MODEL", "gpt-4o-mini")
    model_params = _model_params_from_env()
    params_label = model_params or "(none set)"
    print(f"model:    {model}")
    print(f"params:   {params_label}")

    telemetry = ReactTelemetry()
    redis = InMemoryRedis()
    dispatcher = Dispatcher(redis=redis)
    dispatcher.register(
        "assistant",
        lambda: AssistantWorker(
            worker_id="assistant-worker",
            redis_client=redis,
            workspace_manager=WorkspaceManager(),
            plugin_registry=build_registry(telemetry),
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
    print()
    print(f"model turns taken:    {telemetry.model_turn}")
    print(f"calculate tool calls: {telemetry.tool_call_count}")
    if telemetry.tool_call_count:
        print(
            "-> confirmed: the numeric result above came from the calculate "
            "tool (see the [TOOL CALLED] line above), not the model's own "
            "arithmetic."
        )
    else:
        print(
            "-> the calculate tool was never invoked — the model answered "
            "directly. If the answer is numeric, it computed it itself "
            "(and may be wrong); try a stricter system prompt or a model "
            "with better tool-use adherence."
        )


if __name__ == "__main__":
    asyncio.run(main())
