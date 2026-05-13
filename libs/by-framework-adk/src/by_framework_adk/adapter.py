"""ADK adapter for by-framework.

Bridges any ADK Agent with by-framework's command lifecycle,
allowing users to run ADK agents smoothly within the framework.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from by_framework.common.logger import logger
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.protocol.commands import ResumeCommand
from by_framework.core.protocol.events import StreamChunkEvent
from by_framework.core.protocol.content_type import SseMessageType
from by_framework.core.protocol.event_type import EventType
from google.genai import types

from ._utils import extract_content_text, extract_resume_data

from google.adk.agents.run_config import RunConfig, StreamingMode

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

        try:
            async for event in self._runner.run_async(
                user_id=user_id,
                session_id=session_id,
                new_message=content,
                run_config=RunConfig(streaming_mode=StreamingMode.SSE),
            ):
                has_specific_part = False
                if event.content and event.content.parts:
                    for part in event.content.parts:
                        if getattr(part, "executable_code", None):
                            # Streaming generated code
                            await self._context.emit_chunk(
                                f"\n```python\n{part.executable_code.code}\n```\n",
                                content_type=SseMessageType.text.value,
                            )
                            has_specific_part = True
                        elif getattr(part, "code_execution_result", None):
                            # Streaming code execution result
                            await self._context.emit_chunk(
                                f"\nExecution Result: {part.code_execution_result.outcome}\n{part.code_execution_result.output}\n",
                                content_type=SseMessageType.text.value,
                            )
                            has_specific_part = True
                        elif getattr(part, "function_call", None):
                            # Tool call
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
                                            "arguments": json.dumps(
                                                args, ensure_ascii=False
                                            ),
                                        },
                                    }
                                ]
                            )
                            await self._context.emit_chunk(chunk_event)
                            has_specific_part = True
                        elif getattr(part, "function_response", None):
                            # Tool response
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
                            has_specific_part = True
                        elif getattr(part, "text", None) and not part.text.isspace():
                            # Regular text part (could be intermediate thoughts or final)
                            if not event.is_final_response():
                                await self._context.emit_chunk(
                                    part.text, content_type=SseMessageType.text.value
                                )

                # Check for final response
                if not has_specific_part and event.is_final_response():
                    if (
                        event.content
                        and event.content.parts
                        and event.content.parts[0].text
                    ):
                        final_text = event.content.parts[0].text.strip()
                        full_response = final_text
                        await self._context.emit_chunk(final_text, content_type=SseMessageType.text.value, event_type=EventType.FINAL_ANSWER.value)

        except Exception as e:
            logger.error("[AdkAdapter] Error during agent run: %s", e)
            return {"status": AgentState.FAILED.value, "error": str(e)}

        return full_response
