"""Remote tool factories for bridging by-framework and LangGraph.

Provides factory functions to create LangGraph-compatible tools that
bridge by-framework's call_agent/ask_user with LangGraph's interrupt/resume
mechanism.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

from langchain_core.tools import BaseTool, InjectedToolCallId, tool
from langgraph.types import interrupt

if TYPE_CHECKING:
    from by_framework.worker.context import AgentContext


def make_remote_agent_tool(
    context: AgentContext,
    tool_name: str,
    target_agent_type: str,
    description: str,
    *,
    idempotency_ttl: int = 86400,
) -> BaseTool:
    """Create a LangGraph tool that dispatches work to a remote by-framework agent.

    The generated tool performs:
    1. Redis idempotency check (prevents duplicate dispatch on checkpoint restore)
    2. ``context.call_agent()`` to send AskAgentCommand to the target agent
    3. ``interrupt()`` to suspend the graph until the remote agent replies

    When the remote agent completes, the framework sends a ResumeCommand back.
    The caller then invokes ``graph.ainvoke(Command(resume=reply_data))``
    which causes ``interrupt()`` to return the reply data.

    Args:
        context: The current AgentContext from process_command.
        tool_name: Name for the generated tool (used by LLM for tool selection).
        target_agent_type: The agent_type of the remote agent to call.
        description: Tool description for the LLM.
        idempotency_ttl: TTL in seconds for the Redis idempotency key.

    Returns:
        A LangChain BaseTool instance ready to bind to an LLM.
    """

    @tool(tool_name, description=description)
    async def remote_agent_tool(
        topic: str,
        tool_call_id: Annotated[str, InjectedToolCallId],
    ) -> str:
        # Idempotency guard: checkpoint restore replays tool execution,
        # but we must not re-dispatch the command.
        redis_key = f"dispatched_task:{context.session_id}:{tool_call_id}"
        is_dispatched = await context.redis.exists(redis_key)

        if not is_dispatched:
            await context.call_agent(
                target_agent_type=target_agent_type,
                content=topic,
            )
            await context.redis.set(redis_key, "1", ex=idempotency_ttl)

        # Suspend the graph. When resumed, interrupt() returns the reply data.
        result = interrupt(f"Waiting for {target_agent_type} to finish.")
        return str(result)

    return remote_agent_tool  # type: ignore[return-value]


def make_ask_user_tool(
    context: AgentContext,
    *,
    tool_name: str = "ask_user",
    description: str = "向用户提问并等待回复。参数 prompt 是向用户展示的提示信息。",
    idempotency_ttl: int = 86400,
) -> BaseTool:
    """Create a LangGraph tool that asks the user for input.

    The generated tool performs:
    1. Redis idempotency check (prevents duplicate ask on checkpoint restore)
    2. ``context.ask_user()`` to send an AskUserEvent form to the frontend
    3. ``interrupt()`` to suspend the graph until the user replies

    When the user submits a response, the frontend/client sends a ResumeCommand.
    The caller then invokes ``graph.ainvoke(Command(resume=user_reply))``
    which causes ``interrupt()`` to return the user's reply.

    Args:
        context: The current AgentContext from process_command.
        tool_name: Name for the generated tool.
        description: Tool description for the LLM.
        idempotency_ttl: TTL in seconds for the Redis idempotency key.

    Returns:
        A LangChain BaseTool instance ready to bind to an LLM.
    """

    @tool(tool_name, description=description)
    async def ask_user_tool(
        prompt: str,
        tool_call_id: Annotated[str, InjectedToolCallId],
    ) -> str:
        # Idempotency guard
        redis_key = f"asked_user:{context.session_id}:{tool_call_id}"
        is_asked = await context.redis.exists(redis_key)

        if not is_asked:
            await context.ask_user(prompt)
            await context.redis.set(redis_key, "1", ex=idempotency_ttl)

        # Suspend the graph. When resumed, interrupt() returns the user reply.
        user_reply = interrupt(f"ask_user:{prompt}")
        return str(user_reply)

    return ask_user_tool  # type: ignore[return-value]
