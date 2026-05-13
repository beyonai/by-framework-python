"""LangGraph adapter for by-framework.

Bridges any compiled LangGraph StateGraph with by-framework's command lifecycle,
allowing users to plug in existing LangGraph graphs without rewriting them.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from importlib import import_module
from typing import TYPE_CHECKING, Any, Callable, Iterator

from by_framework.common.logger import logger
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.protocol.commands import ResumeCommand
from by_framework.core.protocol.events import StreamChunkEvent
from langchain_core.messages import HumanMessage
from langgraph.types import Command

from ._utils import (
    extract_content_text,
    extract_resume_data,
    str_to_uint64,
    str_to_uint128,
)

if TYPE_CHECKING:
    from by_framework.core.protocol.commands import GatewayCommand
    from by_framework.worker.context import AgentContext
    from langgraph.graph.state import CompiledStateGraph


LANGFUSE_OBSERVATION_ATTR = "_langfuse_observation"


@dataclass(frozen=True)
class _AdapterTracingConfig:
    """Tracing-related adapter config kept separate from core graph handles."""

    run_name: str
    metadata: dict[str, Any] = field(default_factory=dict)
    callbacks: list[Any] = field(default_factory=list)
    stream: bool = False


# pylint: disable=too-few-public-methods
class LangGraphAdapter:
    """Adapter that runs a compiled LangGraph inside by-framework's lifecycle.

    Supports two execution paths:
    - **Initial**: AskAgentCommand → ``graph.ainvoke(initial_state)``
    - **Resume**: ResumeCommand → ``graph.ainvoke(Command(resume=data))``

    After execution, automatically detects whether the graph is suspended
    (via ``get_state().next``) and returns the appropriate by-framework status.

    Usage::

        class MyWorker(ByaiWorker):
            async def process_command(self, command, context):
                graph = build_my_graph()
                adapter = LangGraphAdapter(graph, context)
                return await adapter.run(command)

    Args:
        graph: A compiled LangGraph StateGraph.
        context: The AgentContext from the current process_command call.
        thread_id: Thread ID for checkpoint isolation. Defaults to session_id.
        input_mapper: Custom function to convert command content into
            LangGraph input state. Defaults to wrapping in HumanMessage.
        output_handler: Custom async function to handle graph output.
            Receives ``(context, final_answer_str)`` and should handle
            emitting to frontend. Defaults to ``context.emit_chunk()``.
        stream: If True, uses ``astream_events`` for streaming output.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        graph: CompiledStateGraph,
        context: AgentContext,
        *,
        thread_id: str | None = None,
        input_mapper: Callable[[Any], dict] | None = None,
        output_handler: Callable[..., Any] | None = None,
        run_name: str | None = None,
        metadata: dict[str, Any] | None = None,
        callbacks: list[Any] | None = None,
        stream: bool = False,
    ) -> None:
        self._graph = graph
        self._context = context
        self._thread_id = thread_id or context.session_id
        self._state_config = {"configurable": {"thread_id": self._thread_id}}
        self._input_mapper = input_mapper or self._default_input_mapper
        self._output_handler = output_handler
        self._tracing = _AdapterTracingConfig(
            run_name=run_name or self._default_run_name(),
            metadata=dict(metadata or {}),
            callbacks=list(callbacks or []),
            stream=stream,
        )

    async def run(self, command: GatewayCommand) -> Any:
        """Execute the graph based on command type.

        - ``AskAgentCommand`` or other initial commands → invoke with input
        - ``ResumeCommand`` → resume from checkpoint with data

        Returns:
            The graph result or a status dict if the graph is suspended.
        """
        if isinstance(command, ResumeCommand):
            return await self._handle_resume(command)
        return await self._handle_initial(command)

    async def _handle_initial(self, command: GatewayCommand) -> Any:
        """Handle first-time graph invocation."""
        content = extract_content_text(getattr(command, "content", ""))
        input_state = self._input_mapper(content)

        logger.info(
            "[LangGraphAdapter] Initial invoke, thread_id=%s, content_len=%d",
            self._thread_id,
            len(content),
        )

        if self._tracing.stream:
            return await self._stream_invoke(input_state)
        return await self._batch_invoke(input_state)

    async def _handle_resume(self, command: ResumeCommand) -> Any:
        """Handle resumption from suspended graph."""
        resume_data = extract_resume_data(command)

        logger.info(
            "[LangGraphAdapter] Resume invoke, thread_id=%s, data_len=%d",
            self._thread_id,
            len(resume_data),
        )

        if self._tracing.stream:
            return await self._stream_invoke(Command(resume=resume_data))
        return await self._batch_invoke(Command(resume=resume_data))

    async def _batch_invoke(self, input_data: Any) -> Any:
        """Invoke the graph in batch mode (no streaming)."""
        with self._tracing_scope() as scoped_callbacks:
            result = await self._graph.ainvoke(
                input_data,
                config=self._build_config(extra_callbacks=scoped_callbacks),
            )
            return await self._process_result(result)

    async def _stream_invoke(self, input_data: Any) -> Any:
        """Invoke the graph in streaming mode via astream_events."""
        full_response = ""

        with self._tracing_scope() as scoped_callbacks:
            async for event in self._graph.astream_events(
                input_data,
                version="v2",
                config=self._build_config(extra_callbacks=scoped_callbacks),
            ):
                kind = event["event"]
                if kind == "on_chat_model_stream":
                    chunk = event["data"]["chunk"]
                    if chunk.content:
                        full_response += chunk.content
                        await self._context.emit_chunk(
                            chunk.content, content_type="text"
                        )
                elif kind == "on_tool_start":
                    tool_name = event["name"]
                    tool_input = event["data"].get("input")
                    # DEBUG: Dump full event structure to see what's available
                    logger.debug(
                        "[LangGraphAdapter] TOOL_START Event: %s",
                        json.dumps(
                            {
                                "run_id": event.get("run_id"),
                                "metadata": event.get("metadata"),
                                "data": event.get("data"),
                            },
                            default=str,
                            ensure_ascii=False,
                        ),
                    )

                    # Use a stable logical ID from metadata if available, fallback to run_id
                    stable_id = (
                        event.get("metadata", {}).get("tool_call_id")
                        or event.get("metadata", {}).get("checkpoint_ns")
                        or event.get("metadata", {}).get("langgraph_checkpoint_ns")
                        or event.get("run_id", "tool_call")
                    )
                    chunk_event = StreamChunkEvent(
                        tool_calls=[
                            {
                                "id": stable_id,
                                "type": "function",
                                "function": {
                                    "name": tool_name,
                                    "arguments": json.dumps(
                                        tool_input, ensure_ascii=False
                                    )
                                    if isinstance(tool_input, dict)
                                    else str(tool_input or ""),
                                },
                            }
                        ]
                    )
                    await self._context.emit_chunk(chunk_event)
                elif kind == "on_tool_end":
                    tool_name = event["name"]
                    tool_output = event["data"].get("output")
                    stable_id = (
                        event.get("metadata", {}).get("tool_call_id")
                        or event.get("metadata", {}).get("checkpoint_ns")
                        or event.get("metadata", {}).get("langgraph_checkpoint_ns")
                        or event.get("run_id", "tool_call")
                    )
                    chunk_event = StreamChunkEvent(
                        role="tool",
                        tool_responses=[
                            {
                                "tool_call_id": stable_id,
                                "content": str(tool_output),
                            }
                        ],
                        metadata={"tool_name": tool_name},
                    )
                    await self._context.emit_chunk(chunk_event)

        # After streaming completes, check if graph is suspended
        if self._is_graph_suspended():
            logger.info(
                "[LangGraphAdapter] Graph suspended after streaming, thread_id=%s",
                self._thread_id,
            )
            return {"status": AgentState.QUEUED.value}

        # Emit final answer if using custom output handler
        if self._output_handler and full_response:
            await self._output_handler(self._context, full_response)

        return full_response

    async def _process_result(self, result: dict) -> Any:
        """Analyze graph result and determine suspended vs completed."""
        if self._is_graph_suspended():
            logger.info(
                "[LangGraphAdapter] Graph suspended, thread_id=%s",
                self._thread_id,
            )
            return {"status": AgentState.QUEUED.value}

        # Extract final answer from last message
        messages = result.get("messages", [])
        if not messages:
            return result

        last_msg = messages[-1]
        answer = last_msg.content if hasattr(last_msg, "content") else str(last_msg)

        # Emit output
        if self._output_handler:
            await self._output_handler(self._context, answer)
        elif answer:
            await self._context.emit_chunk(answer, content_type="text")

        return answer

    def _is_graph_suspended(self) -> bool:
        """Check whether the graph is suspended at an interrupt point.

        Uses ``graph.get_state(config).next`` which is the authoritative
        LangGraph mechanism — if ``.next`` is non-empty, the graph has
        pending nodes blocked by an interrupt.
        """
        try:
            snapshot = self._graph.get_state(self._state_config)
            return bool(snapshot.next)
        except Exception:  # pylint: disable=broad-exception-caught
            return False

    def _build_config(self, extra_callbacks: list[Any] | None = None) -> dict[str, Any]:
        """Build the runnable config passed to LangGraph invocations."""
        config: dict[str, Any] = {
            **self._state_config,
            "run_name": self._tracing.run_name,
        }
        metadata = {
            **self._default_metadata(),
            **self._tracing.metadata,
        }
        if metadata:
            config["metadata"] = metadata

        callbacks = [*self._tracing.callbacks, *(extra_callbacks or [])]
        if callbacks:
            config["callbacks"] = callbacks
        return config

    def _default_run_name(self) -> str:
        """Build the default LangGraph run name for tracing UIs."""
        agent_id = getattr(self._context, "current_agent_id", "") or "langgraph"
        return f"{agent_id}:langgraph"

    def _default_metadata(self) -> dict[str, Any]:
        """Build default metadata for LangGraph/Langfuse tracing."""
        command = getattr(self._context, "current_command", None)
        header = getattr(command, "header", None)
        metadata = {
            "langfuse_session_id": getattr(self._context, "session_id", ""),
            "langfuse_user_id": getattr(header, "user_code", ""),
            "by_framework_trace_id": getattr(self._context, "trace_id", ""),
            "by_framework_message_id": getattr(self._context, "message_id", ""),
            "by_framework_parent_message_id": getattr(
                self._context, "parent_message_id", ""
            ),
            "by_framework_agent_id": getattr(self._context, "current_agent_id", ""),
            "langgraph_thread_id": self._thread_id,
        }
        return {
            key: value for key, value in metadata.items() if value not in ("", None)
        }

    @contextmanager
    def _tracing_scope(self) -> Iterator[list[Any]]:
        """Unified tracing scope for Langfuse and Phoenix."""
        callbacks: list[Any] = []

        with (
            self._phoenix_context_manager(),
            self._langfuse_callback_manager(callbacks),
        ):
            yield callbacks

    @contextmanager
    def _phoenix_context_manager(self) -> Iterator[None]:
        """Prepare OpenTelemetry context for Phoenix tracing."""
        # pylint: disable=import-outside-toplevel
        try:
            from opentelemetry import context, trace
            from opentelemetry.trace import (
                SpanContext,
                TraceFlags,
                set_span_in_context,
            )
        except ImportError:
            yield
            return

        # If tracing is disabled or no tracer is available, just yield
        tracer = trace.get_tracer("by-framework")
        if not tracer:
            yield
            return

        trace_id = getattr(self._context, "trace_id", "")
        message_id = getattr(self._context, "message_id", "")
        if not trace_id or not message_id:
            yield
            return

        # Reconstruct the OTEL context from framework IDs
        # Parent for LangGraph is the current framework message
        span_context = SpanContext(
            trace_id=str_to_uint128(trace_id),
            span_id=str_to_uint64(message_id),
            is_remote=True,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
        )
        ctx = set_span_in_context(trace.NonRecordingSpan(span_context))
        token = context.attach(ctx)
        try:
            yield
        finally:
            context.detach(token)

    @contextmanager
    def _langfuse_callback_manager(self, callbacks: list[Any]) -> Iterator[None]:
        """Prepare Langfuse callback and observation for LangChain."""
        # pylint: disable=import-outside-toplevel
        try:
            langfuse_config = import_module(
                "by_framework_trace_langfuse"
            ).LangfuseConfig
            if langfuse_config.from_env() is None:
                raise ImportError("Langfuse not configured")

            callback_handler = import_module("langfuse.langchain").CallbackHandler
            get_client = import_module("langfuse").get_client
        except (ImportError, AttributeError):
            yield
            return

        callbacks.append(callback_handler())

        framework_observation = getattr(self._context, LANGFUSE_OBSERVATION_ATTR, None)
        if framework_observation is None:
            yield
            return

        langfuse = get_client()
        with langfuse.start_as_current_observation(
            as_type="span",
            name=self._tracing.run_name,
            trace_context={
                "trace_id": getattr(self._context, "trace_id", ""),
                "parent_span_id": framework_observation.id,
            },
            metadata=self._default_metadata(),
        ):
            yield

    @staticmethod
    def _default_input_mapper(content: str) -> dict:
        """Default input mapper: wrap content as a HumanMessage."""
        return {"messages": [HumanMessage(content=content)]}
