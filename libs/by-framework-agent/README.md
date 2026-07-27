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
agent. See [`examples/`](examples/) for four runnable, progressively more
complete demos (tools, `ask_user`, single-agent delegation, and multi-agent
`call_agents` Task Group fan-out), and [`tests/`](tests/) for the full
behavioral spec.
