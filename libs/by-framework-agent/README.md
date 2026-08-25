# by-framework-agent

Native agent harness for `by-framework`: a `litellm`-backed reasoning loop
(`NativeAgentWorker`) that ships as a first-party alternative to hand-written
`process_command()` implementations and to the `by-framework-langgraph` /
`by-framework-adk` external-framework adapters.

See [beyonai/by-framework-python#98](https://github.com/beyonai/by-framework-python/issues/98)
for the design PRD and #99-#104 for the vertical-slice breakdown.

## Quick Start

```python
from by_framework_agent import NativeAgentWorker, ToolSpec
from by_framework.core.extensions.agent_config import AgentConfig
from by_framework.core.extensions.registry import PluginRegistry


async def get_weather(context, arguments) -> str:
    return f"It's sunny in {arguments['city']}."


class AssistantWorker(NativeAgentWorker):
    def get_agent_types(self) -> list[str]:
        return ["assistant"]


registry = PluginRegistry()
registry._set_agent_configs([
    AgentConfig(
        agent_id="assistant",
        prompts={"system": "You are a helpful assistant."},
        tools={"get_weather": ToolSpec(name="get_weather", handler=get_weather)},
        sub_agents=["researcher"],  # auto-exposed as a callable tool too
        extra={"model": "gpt-4o-mini"},  # any litellm-supported model string
    ),
])

worker = AssistantWorker(
    worker_id="assistant-1",
    plugin_registry=registry,
    # model_client defaults to LiteLLMModelClient — omit it to call a real
    # provider; pass a by_framework_agent.testing.StubModelClient in tests.
)
```

No hand-written `process_command()` needed — declaring the `AgentConfig` is
enough to get a working tool-calling, delegating, suspend/resume-capable
agent. See [`examples/`](examples/) for five runnable, progressively more
complete demos (tools, `ask_user`, single-agent delegation, multi-agent
`call_agents` Task Group fan-out, and a real-model ReAct loop), and
[`tests/`](tests/) for the full behavioral spec.

## Connecting a real model (ReAct)

Every example above except one uses `by_framework_agent.testing.StubModelClient`
so it runs offline. To call a real provider instead, there is nothing extra
to wire up — just **don't pass `model_client=...`** when constructing your
worker. `NativeAgentWorker` defaults to `LiteLLMModelClient`, which calls
whatever provider `AgentConfig.extra["model"]` names via `litellm` — the
same tool-calling loop then behaves as ReAct (reason → call a tool →
observe the result → repeat) against a real model, with no code changes.

```python
AgentConfig(
    agent_id="assistant",
    tools={"calculate": ...},
    extra={
        "model": "gpt-4o-mini",              # any litellm model string
        "model_params": {"temperature": 0.2},  # forwarded to every model call
    },
)
```

- **`extra["model"]`** — any [litellm-supported](https://docs.litellm.ai/docs/providers)
  model string: `"gpt-4o-mini"`, `"anthropic/claude-3-5-sonnet-20241022"`,
  `"gemini/gemini-2.0-flash"`, `"azure/<deployment-name>"`, etc.
- **The matching API key** — set as an env var the way litellm expects
  (`OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GEMINI_API_KEY`, ...). litellm
  reads it automatically; nothing to pass explicitly.
- **`extra["model_params"]`** (optional) — a dict forwarded as-is to every
  `ModelClient.complete()` call, i.e. straight into `litellm.acompletion(...)`.
  Anything litellm's `completion`/`acompletion` accepts as a kwarg works:
  `temperature`, `max_tokens`, `api_key` (overrides the env var for just
  this agent), `api_base` (point at a self-hosted/proxy/OpenAI-compatible
  endpoint, e.g. vLLM or a local gateway), `api_version`, etc.

`extra` lives on `AgentConfig`, not the worker — so **different agents can
each use a completely different provider, model, and credentials** in the
same process. An orchestrator calling OpenAI and a `sub_agents` delegate
pointed at a self-hosted model behind a different key both "just work":

```python
AgentConfig(
    agent_id="orchestrator",
    sub_agents=["local_specialist"],
    extra={"model": "gpt-4o-mini", "model_params": {"api_key": "sk-..."}},
)
AgentConfig(
    agent_id="local_specialist",
    extra={
        "model": "openai/local-llama",  # litellm's OpenAI-compatible provider
        "model_params": {
            "api_key": "sk-local-anything",
            "api_base": "http://localhost:8000/v1",
        },
    },
)
```

See [`examples/05_real_model_react.py`](examples/05_real_model_react.py) for
a complete runnable version of the snippet above, with a `calculate` tool to
demonstrate the reasoning loop:

```bash
export OPENAI_API_KEY=sk-...
cd libs/by-framework-agent
uv run python examples/05_real_model_react.py "What is 12 * (7 + 5)?"
```
