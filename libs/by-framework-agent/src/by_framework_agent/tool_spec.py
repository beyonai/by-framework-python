"""Typed local-tool declarations for the harness's tool-calling loop."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Awaitable, Callable

if TYPE_CHECKING:
    from by_framework.worker.context import AgentContext

ToolHandler = Callable[["AgentContext", dict[str, Any]], Awaitable[Any]]


def _default_parameters() -> dict[str, Any]:
    return {"type": "object", "properties": {}}


@dataclass
class ToolSpec:
    """A single locally-executable tool the model can call.

    ``handler`` receives the current ``AgentContext`` and the tool call's
    parsed JSON arguments, and may return a string (used as-is) or any
    JSON-serializable value (serialized before being sent back to the model).
    """

    name: str
    handler: ToolHandler
    description: str = ""
    parameters: dict[str, Any] = field(default_factory=_default_parameters)

    def to_openai_schema(self) -> dict[str, Any]:
        """Return this tool's definition in OpenAI/litellm function-schema shape."""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }
