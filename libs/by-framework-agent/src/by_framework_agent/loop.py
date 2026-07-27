"""The harness's turn-execution loop.

Runs a full tool-calling loop for a given ``AgentConfig``: assemble
messages from the existing history backend and prompts, stream the model's
reply through the existing ``emit_chunk``/``StreamChunkEvent`` path, execute
any local tools the model requests (concurrently, within one turn), append
their results, and call the model again — repeating until a turn returns no
tool calls.
"""

from __future__ import annotations

import asyncio
import inspect
import json
from dataclasses import dataclass
from typing import Any

from by_framework.common.constants import HARNESS_STATE_TTL_SECONDS, RedisKeys
from by_framework.core.extensions.agent_config import AgentConfig, CallbackType
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.protocol.commands import ResumeCommand
from by_framework.core.protocol.events import StreamChunkEvent
from by_framework.core.protocol.results import AgentTaskResult
from by_framework.worker.context import AgentContext

from .model_client import ModelClient
from .tool_spec import ToolSpec

_HISTORY_LIMIT = 50
_MESSAGE_ROLES = {"user", "assistant", "system"}

_ASK_USER_TOOL_NAME = "ask_user"
_ASK_USER_TOOL_SCHEMA = {
    "type": "function",
    "function": {
        "name": _ASK_USER_TOOL_NAME,
        "description": "Ask the human user a question and wait for their reply.",
        "parameters": {
            "type": "object",
            "properties": {
                "prompt": {
                    "type": "string",
                    "description": "The question to ask the user.",
                }
            },
            "required": ["prompt"],
        },
    },
}


@dataclass
class _ResolvedToolCall:
    id: str
    name: str
    arguments: dict[str, Any]
    raw_arguments: str
    parse_error: str = ""


@dataclass
class _ModelTurn:
    content: str
    tool_calls: list[_ResolvedToolCall]
    usage: dict[str, int] | None
    cost: float
    model: str


class HarnessLoop:
    """Runs one native-agent execution (a tool-calling loop) for an ``AgentConfig``."""

    def __init__(self, model_client: ModelClient, agent_config: AgentConfig):
        self._model_client = model_client
        self._agent_config = agent_config

    async def run(self, context: AgentContext) -> AgentTaskResult:
        tool_specs = self._resolve_tools()
        tool_schemas = [spec.to_openai_schema() for spec in tool_specs.values()]
        tool_schemas.append(_ASK_USER_TOOL_SCHEMA)
        model = self._resolve_model()

        is_resume = isinstance(context.current_command, ResumeCommand)
        if is_resume:
            messages = await self._rehydrate_messages(context)
        else:
            messages = await self._build_messages(context)

        try:
            while True:
                turn = await self._run_model_turn(
                    context, messages, model, tool_schemas
                )

                if turn.usage or turn.cost:
                    context.record_token_usage(
                        prompt_tokens=(turn.usage or {}).get("prompt_tokens", 0),
                        completion_tokens=(turn.usage or {}).get(
                            "completion_tokens", 0
                        ),
                        model=turn.model,
                        cost=turn.cost or None,
                    )

                ask_user_call = next(
                    (tc for tc in turn.tool_calls if tc.name == _ASK_USER_TOOL_NAME),
                    None,
                )
                if ask_user_call is not None:
                    await self._suspend_for_ask_user(context, messages, ask_user_call)
                    return AgentTaskResult(status=AgentState.WAITING_USER.value)

                if not turn.tool_calls:
                    return AgentTaskResult(
                        status=AgentState.COMPLETED.value, content=turn.content
                    )

                messages.append(
                    self._assistant_tool_call_message(turn.content, turn.tool_calls)
                )
                await self._persist_assistant_tool_call_turn(
                    context, turn.content, turn.tool_calls
                )

                tool_results = await self._execute_tool_calls(
                    context, turn.tool_calls, tool_specs
                )
                messages.extend(tool_results)
                await self._persist_tool_result_turns(context, tool_results)
        except Exception:
            if context.execution_id:
                await self._clear_harness_state(context)
            raise

    async def _suspend_for_ask_user(
        self,
        context: AgentContext,
        messages: list[dict[str, Any]],
        ask_user_call: _ResolvedToolCall,
    ) -> None:
        if not context.execution_id:
            raise RuntimeError(
                "NativeAgentWorker requires a tracked execution_id to suspend a "
                "loop on ask_user — ensure the worker is run via WorkerRunner"
            )

        await self._persist_assistant_tool_call_turn(context, "", [ask_user_call])
        suspended_messages = messages + [
            self._assistant_tool_call_message("", [ask_user_call])
        ]
        await self._save_harness_state(
            context,
            messages=suspended_messages,
            pending_tool_call_id=ask_user_call.id,
            pending_tool_name=ask_user_call.name,
        )

        prompt = ""
        if isinstance(ask_user_call.arguments, dict):
            prompt = str(ask_user_call.arguments.get("prompt", ""))
        await context.ask_user(prompt)

    async def _rehydrate_messages(self, context: AgentContext) -> list[dict[str, Any]]:
        if not context.execution_id:
            raise RuntimeError(
                "NativeAgentWorker requires a tracked execution_id to resume a "
                "suspended loop — ensure the worker is run via WorkerRunner"
            )

        state = await self._load_harness_state(context)
        if state is None:
            raise RuntimeError(
                "Received a resume for execution_id="
                f"{context.execution_id!r} but no harness loop state was found "
                "(stray or expired resume)"
            )
        await self._clear_harness_state(context)

        messages = state["messages"]
        tool_result = {
            "role": "tool",
            "tool_call_id": state["pending_tool_call_id"],
            "name": state["pending_tool_name"],
            "content": self._extract_resume_reply(context.current_command),
        }
        messages.append(tool_result)
        await self._persist_tool_result_turns(context, [tool_result])
        return messages

    @staticmethod
    def _extract_resume_reply(command: Any) -> str:
        content = getattr(command, "content", "") or ""
        if content:
            return (
                content
                if isinstance(content, str)
                else json.dumps(content, ensure_ascii=False)
            )
        reply_data = getattr(command, "reply_data", None)
        if reply_data is not None:
            return (
                reply_data
                if isinstance(reply_data, str)
                else json.dumps(reply_data, ensure_ascii=False)
            )
        return ""

    async def _save_harness_state(
        self,
        context: AgentContext,
        *,
        messages: list[dict[str, Any]],
        pending_tool_call_id: str,
        pending_tool_name: str,
    ) -> None:
        key = RedisKeys.harness_state(context.execution_id)
        payload = json.dumps(
            {
                "messages": messages,
                "pending_tool_call_id": pending_tool_call_id,
                "pending_tool_name": pending_tool_name,
            },
            ensure_ascii=False,
        )
        await context.redis.set(key, payload, ex=HARNESS_STATE_TTL_SECONDS)

    async def _load_harness_state(self, context: AgentContext) -> dict[str, Any] | None:
        key = RedisKeys.harness_state(context.execution_id)
        raw = await context.redis.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    async def _clear_harness_state(self, context: AgentContext) -> None:
        key = RedisKeys.harness_state(context.execution_id)
        await context.redis.delete(key)

    async def _run_model_turn(
        self,
        context: AgentContext,
        messages: list[dict[str, Any]],
        model: str,
        tool_schemas: list[dict[str, Any]] | None,
    ) -> _ModelTurn:
        await self._fire_callbacks(
            CallbackType.before_model_callback, context, {"messages": messages}
        )

        content_parts: list[str] = []
        tool_call_accumulator: dict[int, dict[str, str]] = {}
        usage: dict[str, int] | None = None
        cost = 0.0
        response_model = model

        async for chunk in self._model_client.complete(
            messages, model=model, tools=tool_schemas
        ):
            if chunk.content:
                content_parts.append(chunk.content)
                await context.emit_chunk(StreamChunkEvent(content=chunk.content))
            if chunk.tool_call_deltas:
                _merge_tool_call_deltas(tool_call_accumulator, chunk.tool_call_deltas)
            if chunk.is_final:
                usage = chunk.usage or usage
                response_model = chunk.model or response_model
                cost = chunk.cost or cost

        full_content = "".join(content_parts)

        await self._fire_callbacks(
            CallbackType.after_model_callback,
            context,
            {"content": full_content, "usage": usage, "cost": cost},
        )

        tool_calls = (
            _finalize_tool_calls(tool_call_accumulator) if tool_call_accumulator else []
        )
        return _ModelTurn(
            content=full_content,
            tool_calls=tool_calls,
            usage=usage,
            cost=cost,
            model=response_model,
        )

    async def _execute_tool_calls(
        self,
        context: AgentContext,
        tool_calls: list[_ResolvedToolCall],
        tool_specs: dict[str, ToolSpec],
    ) -> list[dict[str, Any]]:
        return await asyncio.gather(
            *(
                self._execute_one_tool_call(context, call, tool_specs)
                for call in tool_calls
            )
        )

    async def _execute_one_tool_call(
        self,
        context: AgentContext,
        call: _ResolvedToolCall,
        tool_specs: dict[str, ToolSpec],
    ) -> dict[str, Any]:
        if call.parse_error:
            return _tool_error_message(
                call, f"Invalid JSON arguments: {call.parse_error}"
            )

        spec = tool_specs.get(call.name)
        if spec is None:
            return _tool_error_message(call, f"Unknown tool: {call.name!r}")

        try:
            await self._fire_callbacks(
                CallbackType.before_tool_callback, context, {"tool_call": call}
            )
            result = await spec.handler(context, call.arguments)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            return _tool_error_message(call, str(exc))

        content = (
            result
            if isinstance(result, str)
            else json.dumps(result, ensure_ascii=False)
        )
        await self._fire_callbacks(
            CallbackType.after_tool_callback,
            context,
            {"tool_call": call, "result": content},
        )
        return {
            "role": "tool",
            "tool_call_id": call.id,
            "name": call.name,
            "content": content,
        }

    async def _persist_assistant_tool_call_turn(
        self,
        context: AgentContext,
        content: str,
        tool_calls: list[_ResolvedToolCall],
    ) -> None:
        history = context.agent_runtime_state.session_manager.history
        payload_content = content or json.dumps(
            {
                "tool_calls": [
                    {"name": tc.name, "arguments": tc.arguments} for tc in tool_calls
                ]
            },
            ensure_ascii=False,
        )
        await history.save_message(
            role="assistant",
            content=payload_content,
            metadata={
                "tool_calls": [
                    {"id": tc.id, "name": tc.name, "arguments": tc.arguments}
                    for tc in tool_calls
                ]
            },
        )

    async def _persist_tool_result_turns(
        self, context: AgentContext, tool_results: list[dict[str, Any]]
    ) -> None:
        history = context.agent_runtime_state.session_manager.history
        for result in tool_results:
            await history.save_message(
                role="tool",
                content=result["content"],
                metadata={
                    "tool_call_id": result["tool_call_id"],
                    "name": result["name"],
                },
            )

    def _resolve_tools(self) -> dict[str, ToolSpec]:
        resolved: dict[str, ToolSpec] = {}
        for key, spec in self._agent_config.tools.items():
            if not isinstance(spec, ToolSpec):
                raise TypeError(
                    f"AgentConfig.tools[{key!r}] must be a ToolSpec, "
                    f"got {type(spec).__name__}"
                )
            resolved[spec.name] = spec
        return resolved

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

    @staticmethod
    def _assistant_tool_call_message(
        content: str, tool_calls: list[_ResolvedToolCall]
    ) -> dict[str, Any]:
        return {
            "role": "assistant",
            "content": content or None,
            "tool_calls": [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {"name": tc.name, "arguments": tc.raw_arguments},
                }
                for tc in tool_calls
            ],
        }


def _merge_tool_call_deltas(
    accumulator: dict[int, dict[str, str]], deltas: list[dict[str, Any]]
) -> None:
    """Fold streamed tool-call fragments into per-index accumulators.

    Mirrors OpenAI/litellm's incremental tool-call delta shape: each delta
    carries an ``index`` plus whichever of ``id``/``function.name``/
    ``function.arguments`` arrived in this fragment.
    """
    for delta in deltas:
        index = delta.get("index", 0)
        entry = accumulator.setdefault(index, {"id": "", "name": "", "arguments": ""})
        if delta.get("id"):
            entry["id"] = delta["id"]
        function = delta.get("function") or {}
        if function.get("name"):
            entry["name"] += function["name"]
        if function.get("arguments"):
            entry["arguments"] += function["arguments"]


def _finalize_tool_calls(
    accumulator: dict[int, dict[str, str]],
) -> list[_ResolvedToolCall]:
    resolved: list[_ResolvedToolCall] = []
    for index in sorted(accumulator):
        entry = accumulator[index]
        raw_arguments = entry["arguments"] or "{}"
        try:
            arguments = json.loads(raw_arguments) if raw_arguments.strip() else {}
            parse_error = ""
        except json.JSONDecodeError as exc:
            arguments = {}
            parse_error = str(exc)
        resolved.append(
            _ResolvedToolCall(
                id=entry["id"] or f"call_{index}",
                name=entry["name"],
                arguments=arguments,
                raw_arguments=raw_arguments,
                parse_error=parse_error,
            )
        )
    return resolved


def _tool_error_message(call: _ResolvedToolCall, error: str) -> dict[str, Any]:
    return {
        "role": "tool",
        "tool_call_id": call.id,
        "name": call.name,
        "content": json.dumps({"error": error}, ensure_ascii=False),
    }
