"""LangGraph worker base class for by-framework.

Provides a ready-to-use Worker base that handles the full command lifecycle
(initial invoke, resume, suspend detection, checkpoint management),
so subclasses only need to implement ``build_graph()``.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from by_framework.common.logger import logger
from by_framework.worker.byai_worker import ByaiWorker
from langgraph.checkpoint.memory import MemorySaver

from .adapter import LangGraphAdapter

if TYPE_CHECKING:
    from by_framework.core.protocol.commands import GatewayCommand
    from by_framework.worker.context import AgentContext
    from langgraph.checkpoint.base import BaseCheckpointSaver
    from langgraph.graph.state import CompiledStateGraph


class LangGraphWorker(ByaiWorker):
    """Base Worker class for LangGraph-powered agents.

    Subclasses only need to implement:
    - ``get_agent_types()`` → list of agent type strings
    - ``build_graph(context, command)`` → compiled LangGraph StateGraph

    The base class automatically handles:
    - AskAgentCommand → ``graph.ainvoke(initial_state)``
    - ResumeCommand → ``graph.ainvoke(Command(resume=data))``
    - Graph suspend detection via ``get_state().next``
    - Checkpoint lifecycle management
    - Streaming output to frontend

    Example::

        class MyWorker(LangGraphWorker):
            def get_agent_types(self):
                return ["my-agent"]

            def build_graph(self, context, command):
                tools = [make_remote_agent_tool(context, ...)]
                llm = ChatOpenAI(model="gpt-4o").bind_tools(tools)
                workflow = StateGraph(AgentState)
                # ... build graph ...
                return workflow.compile(checkpointer=self.get_checkpointer())
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the worker with a lazy checkpointer placeholder."""
        super().__init__(*args, **kwargs)
        self._checkpointer: BaseCheckpointSaver | None = None

    @abstractmethod
    def build_graph(
        self,
        context: AgentContext,
        command: GatewayCommand,
    ) -> CompiledStateGraph:
        """Build and return a compiled LangGraph StateGraph.

        Called on every ``process_command`` invocation. The graph should
        include a checkpointer (use ``self.get_checkpointer()``) to enable
        interrupt/resume across multiple ``process_command`` calls.

        Args:
            context: Current AgentContext with session info and
                framework primitives.
            command: The incoming command (AskAgentCommand or ResumeCommand).

        Returns:
            A compiled LangGraph StateGraph ready for invocation.
        """

    def get_checkpointer(self) -> BaseCheckpointSaver:
        """Get the checkpoint saver for this worker.

        Default implementation uses MemorySaver (in-memory, not persistent).
        Override this method to use a persistent checkpointer for production
        (e.g., ``langgraph-checkpoint-postgres``).

        Returns:
            A LangGraph BaseCheckpointSaver instance.
        """
        if self._checkpointer is None:
            self._checkpointer = MemorySaver()
        return self._checkpointer

    def get_thread_id(self, context: AgentContext) -> str:
        """Determine the thread ID for checkpoint isolation.

        Default uses ``context.session_id`` so that the same session
        shares checkpoint state across multiple commands.

        Override to customize thread isolation strategy (e.g., per-message).

        Args:
            context: Current AgentContext.

        Returns:
            A string used as the LangGraph thread_id.
        """
        return context.session_id

    def get_stream_enabled(self) -> bool:
        """Whether to use streaming mode for graph execution.

        Default is True. Override to disable streaming.

        Returns:
            True to stream, False for batch invocation.
        """
        return True

    def get_langgraph_run_name(
        self,
        context: AgentContext,
        command: GatewayCommand,
    ) -> str:
        """Return the LangGraph run name used by tracing callbacks."""
        del command
        agent_id = context.current_agent_id or "langgraph"
        return f"{agent_id}:langgraph"

    def get_langgraph_metadata(
        self,
        context: AgentContext,
        command: GatewayCommand,
    ) -> dict[str, Any]:
        """Return extra metadata merged into the LangGraph runnable config."""
        del context, command
        return {}

    def get_langgraph_callbacks(
        self,
        context: AgentContext,
        command: GatewayCommand,
    ) -> list[Any]:
        """Return extra LangChain-compatible callbacks for graph execution."""
        del context, command
        return []

    async def process_command(
        self,
        command: GatewayCommand,
        context: AgentContext,
    ) -> Any:
        """Framework entry point — delegates to LangGraphAdapter.

        Calls ``build_graph()`` to get the compiled graph, wraps it
        in a ``LangGraphAdapter``, and runs the command.

        Args:
            command: Incoming GatewayCommand (AskAgentCommand or
                ResumeCommand).
            context: Current AgentContext.

        Returns:
            Graph result or status dict if suspended.
        """
        logger.info(
            "[LangGraphWorker] Processing command, type=%s, session=%s",
            type(command).__name__,
            context.session_id,
        )

        graph = self.build_graph(context, command)
        adapter = LangGraphAdapter(
            graph,
            context,
            thread_id=self.get_thread_id(context),
            run_name=self.get_langgraph_run_name(context, command),
            metadata=self.get_langgraph_metadata(context, command),
            callbacks=self.get_langgraph_callbacks(context, command),
            stream=self.get_stream_enabled(),
        )
        return await adapter.run(command)
