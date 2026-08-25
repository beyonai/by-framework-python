"""Default ``ModelClient`` implementation, backed by litellm."""

from __future__ import annotations

from typing import Any, AsyncIterator, cast

import litellm
from litellm import CustomStreamWrapper

from .model_client import ModelChunk


class LiteLLMModelClient:
    """Wraps ``litellm.acompletion`` behind the harness's ``ModelClient`` seam.

    Any provider litellm supports is usable without new harness code — model
    selection is entirely a matter of the ``model`` string passed in.
    """

    async def complete(
        self,
        messages: list[dict[str, Any]],
        *,
        model: str,
        tools: list[dict[str, Any]] | None = None,
        **params: Any,
    ) -> AsyncIterator[ModelChunk]:
        # litellm's overloads don't discriminate on a runtime `stream=True`
        # bool, so the return type widens to include the non-streaming
        # ModelResponse branch; stream=True is passed literally above, so
        # this is always actually a CustomStreamWrapper at runtime.
        stream = cast(
            CustomStreamWrapper,
            await litellm.acompletion(
                model=model,
                messages=messages,
                tools=tools or None,
                stream=True,
                stream_options={"include_usage": True},
                **params,
            ),
        )

        collected_raw_chunks = []
        async for raw_chunk in stream:
            collected_raw_chunks.append(raw_chunk)
            choice = raw_chunk.choices[0] if raw_chunk.choices else None
            delta = getattr(choice, "delta", None) if choice else None
            content_delta = getattr(delta, "content", None) if delta else None
            tool_call_deltas = getattr(delta, "tool_calls", None) if delta else None
            finish_reason = getattr(choice, "finish_reason", None) if choice else None
            usage = getattr(raw_chunk, "usage", None)

            yield ModelChunk(
                content=content_delta or "",
                tool_call_deltas=[
                    tool_call.model_dump()
                    if hasattr(tool_call, "model_dump")
                    else dict(tool_call)
                    for tool_call in (tool_call_deltas or [])
                ],
                finish_reason=finish_reason or "",
                is_final=usage is not None,
                usage={
                    "prompt_tokens": usage.prompt_tokens,
                    "completion_tokens": usage.completion_tokens,
                }
                if usage
                else None,
                model=model,
            )

        if not collected_raw_chunks:
            return

        cost = 0.0
        try:
            full_response = litellm.stream_chunk_builder(
                collected_raw_chunks, messages=messages
            )
            cost = litellm.completion_cost(completion_response=full_response) or 0.0
        except Exception:  # pylint: disable=broad-exception-caught
            cost = 0.0

        yield ModelChunk(is_final=True, cost=cost, model=model)
