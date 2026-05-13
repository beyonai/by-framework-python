# by-framework-langgraph

LangGraph integration for by-framework. Provides two integration modes:

1. **Adapter Mode** — Plug existing LangGraph graphs into by-framework with one line
2. **Native Mode** — Build LangGraph workers with native `call_agent` / `ask_user` / `resume` support

## Installation

```bash
uv add by-framework-langgraph
```

## Quick Start

### Adapter Mode — Plug in existing graphs

```python
from by_framework.worker import ByaiWorker
from by_framework_langgraph import LangGraphAdapter

class MyWorker(ByaiWorker):
    def get_agent_types(self):
        return ["my-agent"]

    async def process_command(self, command, context):
        graph = build_my_existing_graph()  # your existing LangGraph
        adapter = LangGraphAdapter(graph, context)
        return await adapter.run(command)
```

### Native Mode — Framework-native LangGraph workers

```python
from by_framework_langgraph import LangGraphWorker, make_remote_agent_tool, make_ask_user_tool

class OrchestratorWorker(LangGraphWorker):
    def get_agent_types(self):
        return ["orchestrator"]

    def build_graph(self, context, command):
        poet = make_remote_agent_tool(context, "invoke_poet", "poet-agent", "调度诗人创作")
        ask = make_ask_user_tool(context)
        llm = ChatOpenAI(model="gpt-4o").bind_tools([poet, ask])
        # ... build and return compiled graph
```
