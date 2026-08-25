"""Test-only ``ModelClient`` double.

Shipped as part of the package (not just the harness's own tests) so agent
developers building on ``NativeAgentWorker`` can script deterministic model
turns in their own tests without calling a real provider.
"""

from __future__ import annotations

from typing import Any, AsyncIterator

from .model_client import ModelChunk


class StubModelClient:
    """Replays scripted chunk sequences, one sequence per ``complete()`` call."""

    def __init__(self, turns: list[list[ModelChunk]]):
        self._turns = list(turns)
        self.calls: list[dict[str, Any]] = []

    async def complete(
        self,
        messages: list[dict[str, Any]],
        *,
        model: str,
        tools: list[dict[str, Any]] | None = None,
        **params: Any,
    ) -> AsyncIterator[ModelChunk]:
        self.calls.append(
            {"messages": messages, "model": model, "tools": tools, "params": params}
        )
        if not self._turns:
            raise AssertionError("StubModelClient: no more scripted turns")
        turn = self._turns.pop(0)
        for chunk in turn:
            yield chunk
