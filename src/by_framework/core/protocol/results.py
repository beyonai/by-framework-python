"""Typed result contract for worker command processing."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TypeAlias

from .agent_state import AgentState, AgentStateLiteral
from .content_codec import WireContent

JsonValue: TypeAlias = (
    str | int | float | bool | None | list["JsonValue"] | dict[str, "JsonValue"]
)
ProcessCommandResult: TypeAlias = "AgentTaskResult | JsonValue"

_RESULT_FIELDS = {
    "status",
    "content",
    "reply_data",
    "metadata",
    "extra_payload",
    "finalAnswer",
    "final_answer",
}


@dataclass(frozen=True)
class AgentTaskResult:
    """Structured return value for ``GatewayWorker.process_command``.

    Fields intentionally mirror ``ResumeCommand`` body fields. ``metadata`` is
    merged into ``ResumeCommand.header.metadata`` when a child agent returns to
    its caller.
    """

    status: str = AgentState.COMPLETED.value
    content: WireContent = ""
    reply_data: JsonValue = None
    metadata: dict[str, JsonValue] = field(default_factory=dict)
    extra_payload: dict[str, JsonValue] = field(default_factory=dict)


def ensure_json_serializable(value: object, path: str = "value") -> JsonValue:
    """Validate that a value can safely cross the JSON Redis boundary."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [
            ensure_json_serializable(item, f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, dict):
        serialized: dict[str, JsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(
                    "process_command return value must be JSON serializable; "
                    f"got non-string key {key!r} at {path}"
                )
            serialized[key] = ensure_json_serializable(item, f"{path}.{key}")
        return serialized
    raise TypeError(
        "process_command return value must be JSON serializable; "
        f"got {type(value).__name__} at {path}"
    )


def ensure_wire_content(value: object, path: str = "content") -> WireContent:
    """Validate ResumeCommand-compatible content."""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        items = ensure_json_serializable(value, path)
        if isinstance(items, list) and all(isinstance(item, dict) for item in items):
            return items
    raise TypeError(
        "process_command return content must be a string or list of dicts; "
        f"got {type(value).__name__} at {path}"
    )


def normalize_process_result(result: object) -> AgentTaskResult:
    """Convert a worker return value into the structured result contract."""
    if isinstance(result, AgentTaskResult):
        return AgentTaskResult(
            status=result.status,
            content=ensure_wire_content(result.content),
            reply_data=ensure_json_serializable(result.reply_data, "reply_data"),
            metadata=_ensure_json_object(result.metadata, "metadata"),
            extra_payload=_ensure_json_object(result.extra_payload, "extra_payload"),
        )

    if isinstance(result, str) and result in AgentStateLiteral.__args__:
        return AgentTaskResult(status=result)

    if isinstance(result, dict):
        metadata = _extract_metadata(result)
        is_structured = (
            "reply_data" in result
            or "content" in result
            or "finalAnswer" in result
            or "final_answer" in result
            or (
                set(result).issubset(_RESULT_FIELDS)
                and bool(set(result) & _RESULT_FIELDS)
            )
        )
        if is_structured:
            return AgentTaskResult(
                status=str(result.get("status", AgentState.COMPLETED.value)),
                content=ensure_wire_content(
                    result.get("content")
                    or result.get("finalAnswer")
                    or result.get("final_answer")
                    or ""
                ),
                reply_data=ensure_json_serializable(
                    result.get("reply_data"), "reply_data"
                ),
                metadata=metadata,
                extra_payload=_ensure_json_object(
                    result.get("extra_payload", {}), "extra_payload"
                ),
            )
        return AgentTaskResult(
            status=str(result.get("status", AgentState.COMPLETED.value)),
            reply_data=ensure_json_serializable(result, "reply_data"),
            metadata=metadata,
        )

    return AgentTaskResult(
        reply_data=ensure_json_serializable(result, "reply_data"),
    )


def _extract_metadata(result: dict) -> dict[str, JsonValue]:
    if "metadata" not in result:
        return {}
    return _ensure_json_object(result["metadata"], "metadata")


def _ensure_json_object(value: object, path: str) -> dict[str, JsonValue]:
    serialized = ensure_json_serializable(value, path)
    if not isinstance(serialized, dict):
        raise TypeError(
            "process_command return metadata fields must be JSON objects; "
            f"got {type(value).__name__} at {path}"
        )
    return serialized
