"""Model-calling seam for the native agent harness.

``ModelClient`` is the harness's sole external, non-deterministic dependency
— every other integration point (tool execution, agent-as-tool dispatch,
suspend/resume) runs through real ``by_framework`` code. Tests substitute a
stub implementation here instead of calling a real model provider.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Protocol


@dataclass
class ModelChunk:
    """One increment of a streamed model turn.

    A turn is a sequence of chunks carrying content/tool-call deltas,
    terminated by exactly one chunk with ``is_final=True`` carrying the
    aggregated ``usage``/``cost``/``finish_reason`` for the whole turn.
    """

    content: str = ""
    tool_call_deltas: list[dict[str, Any]] = field(default_factory=list)
    finish_reason: str = ""
    is_final: bool = False
    usage: dict[str, int] | None = None
    cost: float = 0.0
    model: str = ""


class ModelClient(Protocol):
    """Protocol for the harness's model-calling boundary."""

    def complete(
        self,
        messages: list[dict[str, Any]],
        *,
        model: str,
        tools: list[dict[str, Any]] | None = None,
        **params: Any,
    ) -> AsyncIterator[ModelChunk]:
        """Stream a single model turn as a sequence of ``ModelChunk``s."""
        ...  # pylint: disable=unnecessary-ellipsis
