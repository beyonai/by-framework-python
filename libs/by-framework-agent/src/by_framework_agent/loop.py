"""The harness's turn-execution loop.

Slice 1 scope: a single model turn (no tool calling yet — that's Slice 2).
Assembles messages from the existing history backend and ``AgentConfig``
prompts, streams the model's reply through the existing ``emit_chunk``/
``StreamChunkEvent`` path, and records token usage/cost through the existing
``record_token_usage`` accumulator.
"""

from __future__ import annotations

import inspect
from typing import Any

from by_framework.core.extensions.agent_config import AgentConfig, CallbackType
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.protocol.events import StreamChunkEvent
from by_framework.core.protocol.results import AgentTaskResult
from by_framework.worker.context import AgentContext

from .model_client import ModelClient

_HISTORY_LIMIT = 50
_MESSAGE_ROLES = {"user", "assistant", "system"}


class HarnessLoop:
    """Runs one native-agent turn for a given ``AgentConfig``."""

    def __init__(self, model_client: ModelClient, agent_config: AgentConfig):
        self._model_client = model_client
        self._agent_config = agent_config

    async def run(self, context: AgentContext) -> AgentTaskResult:
        messages = await self._build_messages(context)
        model = self._resolve_model()

        await self._fire_callbacks(
            CallbackType.before_model_callback, context, {"messages": messages}
        )

        content_parts: list[str] = []
        usage: dict[str, int] | None = None
        cost = 0.0
        response_model = model

        async for chunk in self._model_client.complete(messages, model=model):
            if chunk.content:
                content_parts.append(chunk.content)
                await context.emit_chunk(StreamChunkEvent(content=chunk.content))
            if chunk.is_final:
                usage = chunk.usage or usage
                response_model = chunk.model or response_model
                cost = chunk.cost or cost

        full_content = "".join(content_parts)

        if usage or cost:
            context.record_token_usage(
                prompt_tokens=(usage or {}).get("prompt_tokens", 0),
                completion_tokens=(usage or {}).get("completion_tokens", 0),
                model=response_model,
                cost=cost or None,
            )

        await self._fire_callbacks(
            CallbackType.after_model_callback,
            context,
            {"content": full_content, "usage": usage, "cost": cost},
        )

        return AgentTaskResult(status=AgentState.COMPLETED.value, content=full_content)

    async def _build_messages(self, context: AgentContext) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []
        system_prompt = self._render_system_prompt()
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})

        history = await context.agent_runtime_state.session_manager.history.get_history(
            limit=_HISTORY_LIMIT
        )
        for entry in history:
            role = entry.get("role", "user")
            if role not in _MESSAGE_ROLES:
                continue
            content = entry.get("content", "")
            if not isinstance(content, str):
                content = str(content)
            messages.append({"role": role, "content": content})

        return messages

    def _render_system_prompt(self) -> str:
        system_prompt = self._agent_config.prompts.get("system")
        if system_prompt is None:
            return ""
        if hasattr(system_prompt, "render"):
            return system_prompt.render()
        return str(system_prompt)

    def _resolve_model(self) -> str:
        model = self._agent_config.extra.get("model")
        if not model:
            raise ValueError(
                f"AgentConfig '{self._agent_config.agent_id}' has no 'model' set "
                "in extra['model']"
            )
        return str(model)

    async def _fire_callbacks(
        self,
        callback_type: CallbackType,
        context: AgentContext,
        payload: dict[str, Any],
    ) -> None:
        for callback in self._agent_config.callbacks.get(callback_type, []):
            result = callback(context, payload)
            if inspect.isawaitable(result):
                await result
