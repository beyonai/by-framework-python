"""ADK adapter for by-framework.

Bridges any ADK Agent with by-framework's command lifecycle,
allowing users to run ADK agents smoothly within the framework.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from by_framework.common.logger import logger
from by_framework.core.protocol.commands import ResumeCommand
from by_framework.core.protocol.content_type import SseMessageType
from by_framework.core.protocol.event_type import EventType
from by_framework.core.protocol.events import StreamChunkEvent
from google.adk.agents.run_config import RunConfig, StreamingMode
from google.genai import types

from ._utils import extract_content_text, extract_resume_data

if TYPE_CHECKING:
    from by_framework.core.protocol.commands import GatewayCommand
    from by_framework.worker.context import AgentContext
    from google.adk.agents import LlmAgent
    from google.adk.runners import Runner


class AdkAdapter:
    """Adapter that runs a ADK Agent inside by-framework's lifecycle.

    Args:
        agent: An ADK LlmAgent.
        context: The AgentContext from the current process_command call.
        runner: The ADK Runner instance to use.
    """

    def __init__(
        self,
        agent: LlmAgent,
        context: AgentContext,
        runner: Runner,
    ) -> None:
        self._agent = agent
        self._context = context
        self._runner = runner
        # Text streamed via non-final events during this run — the source of
        # truth for the final answer. ADK's final-response event's own
        # `content.parts[0].text` is not reliable once streaming has already
        # happened (observed truncated to the first streamed fragment), so
        # the final answer must be reconstructed from what was actually
        # streamed rather than re-read from the final event. One adapter
        # instance is constructed per run() call (see AdkWorker), so this is
        # safe as instance state.
        self._accumulated_text = ""

    @property
    def agent(self) -> LlmAgent:
        """Get the underlying agent."""
        return self._agent

    async def run(self, command: GatewayCommand) -> Any:
        """Execute the agent based on command type."""
        if isinstance(command, ResumeCommand):
            content_str = extract_resume_data(command)
        else:
            content_str = extract_content_text(getattr(command, "content", ""))

        content = types.Content(role="user", parts=[types.Part(text=content_str)])

        user_id = (
            getattr(command.header, "user_code", "default_user")
            if hasattr(command, "header")
            else "default_user"
        )
        session_id = self._context.session_id

        logger.info(
            "[AdkAdapter] Running ADK agent %s, session_id=%s, user_id=%s",
            self._agent.name,
            session_id,
            user_id,
        )

        full_response = ""

        async for event in self._runner.run_async(
            user_id=user_id,
            session_id=session_id,
            new_message=content,
            run_config=RunConfig(streaming_mode=StreamingMode.SSE),
        ):
            final_text = await self._process_event(event)
            if final_text is not None:
                full_response = final_text

        return full_response

    async def _process_event(self, event: Any) -> str | None:
        """Process a single event from the ADK runner and emit chunks.

        Returns:
            The final text response if this is the final event, otherwise None.
        """
        has_specific_part = False
        if event.content and event.content.parts:
            for part in event.content.parts:
                part_processed = await self._process_part(part, event)
                has_specific_part = has_specific_part or part_processed

        # Check for final response
        if not has_specific_part and event.is_final_response():
            # Prefer what was actually streamed; only fall back to the final
            # event's own text when nothing was streamed beforehand (e.g. a
            # single-shot, non-streaming-style final event) — see __init__'s
            # docstring for why the final event's own text can't be trusted
            # once streaming has already happened.
            final_text = self._accumulated_text.strip() or _event_text(event)
            if final_text:
                await self._context.emit_chunk(
                    final_text,
                    content_type=SseMessageType.text.value,
                    event_type=EventType.FINAL_ANSWER.value,
                )
            return final_text
        return None

    async def _process_part(self, part: Any, event: Any) -> bool:
        """Process a single part of an event."""
        if getattr(part, "executable_code", None):
            await self._context.emit_chunk(
                f"\\n```python\\n{part.executable_code.code}\\n```\\n",
                content_type=SseMessageType.text.value,
            )
            return True
        if getattr(part, "code_execution_result", None):
            await self._context.emit_chunk(
                f"\\nExecution Result: {part.code_execution_result.outcome}\\n"
                f"{part.code_execution_result.output}\\n",
                content_type=SseMessageType.text.value,
            )
            return True
        if getattr(part, "function_call", None):
            func_call = part.function_call
            args = dict(func_call.args) if func_call.args else {}
            call_id = getattr(func_call, "id", None) or func_call.name
            chunk_event = StreamChunkEvent(
                tool_calls=[
                    {
                        "id": call_id,
                        "type": "function",
                        "function": {
                            "name": func_call.name,
                            "arguments": json.dumps(args, ensure_ascii=False),
                        },
                    }
                ]
            )
            await self._context.emit_chunk(chunk_event)
            return True
        if getattr(part, "function_response", None):
            func_resp = part.function_response
            resp_id = getattr(func_resp, "id", None) or func_resp.name
            chunk_event = StreamChunkEvent(
                role="tool",
                tool_responses=[
                    {
                        "tool_call_id": resp_id,
                        "content": str(func_resp.response),
                    }
                ],
                metadata={"tool_name": func_resp.name},
            )
            await self._context.emit_chunk(chunk_event)
            return True
        if getattr(part, "text", None) and not part.text.isspace():
            if not event.is_final_response():
                self._accumulated_text += part.text
                await self._context.emit_chunk(
                    part.text, content_type=SseMessageType.text.value
                )
        return False


def _event_text(event: Any) -> str:
    """Join every text part of an event — used only as a fallback when
    nothing was streamed via prior non-final events (see `_process_event`)."""
    if not (event.content and event.content.parts):
        return ""
    return "".join(
        part.text for part in event.content.parts if getattr(part, "text", None)
    ).strip()
