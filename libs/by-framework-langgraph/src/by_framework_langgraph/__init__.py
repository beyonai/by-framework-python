"""LangGraph integration for by-framework.

Provides two integration modes:

1. **Adapter Mode** — Plug existing LangGraph graphs into by-framework::

       from by_framework_langgraph import LangGraphAdapter

       adapter = LangGraphAdapter(my_graph, context)
       return await adapter.run(command)

2. **Native Mode** — Build LangGraph workers with framework-native tools::

       from by_framework_langgraph import (
           LangGraphWorker,
           make_remote_agent_tool,
           make_ask_user_tool,
       )

       class MyWorker(LangGraphWorker):
           def build_graph(self, context, command):
               poet = make_remote_agent_tool(context, ...)
               ask = make_ask_user_tool(context)
               # ... build and return compiled graph
"""

from .adapter import LangGraphAdapter
from .tools import make_ask_user_tool, make_remote_agent_tool
from .worker import LangGraphWorker

__all__ = [
    "LangGraphAdapter",
    "LangGraphWorker",
    "make_ask_user_tool",
    "make_remote_agent_tool",
]
