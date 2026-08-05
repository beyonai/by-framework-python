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

## Common Pitfalls

Both of these fail **silently** — no exception, just wrong or empty results — so they're easy to lose time to.

### `add_messages` must be imported as a function, not written as a string

```python
from langgraph.graph.message import add_messages

# Correct — the reducer actually runs; messages accumulate across nodes.
messages: Annotated[list, add_messages]

# Wrong — looks plausible (some older LangGraph docs used this form), but
# the reducer silently does nothing: each node only sees its own output,
# not the accumulated history. Your agent node ends up calling the model
# with just the latest ToolMessage and no prior context, and typically
# returns an empty or confused reply.
messages: Annotated[list, "add_messages"]
```

### A routing function must return the `END` constant, not the string `"end"`

```python
from langgraph.graph import END

# Correct — the graph terminates properly; the agent node's final
# AIMessage is saved to state before the graph stops.
def should_continue(state):
    ...
    return END

# Wrong — "end" looks like it should work as a node name, but LangGraph
# only recognizes the END constant (whose actual value is "__end__").
# LangGraph logs "wrote to unknown channel branch:to:end, ignoring it."
# and the graph doesn't route there — the agent's last turn is dropped,
# and `state["messages"][-1]` after execution can end up being something
# other than the model's real final answer (e.g. a stale ToolMessage from
# an earlier step), not the reply you expected.
def should_continue(state):
    ...
    return "end"
```
