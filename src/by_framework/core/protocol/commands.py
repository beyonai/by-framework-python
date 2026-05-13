"""
Command definitions for Gateway protocol.

Contains command dataclasses for all command types
(AskAgent, Resume, CancelTask, ReloadPlugins)
and command registry for dynamic command dispatch.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, List, TypeVar, Union

from .action_type import ActionType
from .message_header import MessageHeader
from .results import JsonValue


class CancelMode:
    """Cancel mode values for task cancellation."""

    GRACEFUL = "graceful"
    FORCE = "force"

    @classmethod
    def values(cls) -> tuple[str, str]:
        return (cls.GRACEFUL, cls.FORCE)


def _has_content(value: Union[str, List[Dict[str, Any]]]) -> bool:
    if isinstance(value, str):
        return value.strip() != ""
    if isinstance(value, list):
        return len(value) > 0
    return value is not None


@dataclass
class BaseCommand:
    """Command base class, parent of all command types.

    Attributes:
        header: Message header
        action_type: Action type
    """

    header: MessageHeader

    action_type: ClassVar[str]

    def to_dict(self) -> dict[str, Any]:
        raise NotImplementedError

    def to_redis_payload(self) -> dict[str, str]:
        return {"data": json.dumps(self.to_dict())}

    def inject_runtime_payload(self, payload: dict[str, Any]) -> None:
        """Allow runners to enrich command context at runtime."""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BaseCommand":
        raise NotImplementedError


@dataclass
class AskAgentCommand(BaseCommand):
    """Command to send a message to an agent and wait for a reply.

    Attributes:
        content: Message content
        wait_for_reply: Whether to wait for a reply
        extra_payload: Extra payload
    """

    action_type: ClassVar[str] = ActionType.ASK_AGENT.value

    content: Union[str, List[Dict[str, Any]]]
    wait_for_reply: bool = False
    extra_payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not _has_content(self.content):
            raise ValueError("AskAgentCommand requires non-empty content")

    def to_dict(self) -> dict[str, Any]:
        body = {
            "content": self.content,
            "wait_for_reply": self.wait_for_reply,
        }
        if self.extra_payload:
            body["extra_payload"] = dict(self.extra_payload)
        return {
            "action_type": self.action_type,
            "header": self.header.to_dict(),
            "body": body,
        }

    def inject_runtime_payload(self, payload: dict[str, Any]) -> None:
        self.extra_payload.update(payload)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AskAgentCommand":
        body = dict(data.get("body", {}))
        return cls(
            header=MessageHeader.from_dict(data["header"]),
            content=body.get("content", ""),
            wait_for_reply=bool(body.get("wait_for_reply", False)),
            extra_payload=dict(body.get("extra_payload", {})),
        )


@dataclass
class ResumeCommand(BaseCommand):
    """Command to resume suspended task execution.

    Attributes:
        content: Message content
        status: Status
        reply_data: Reply data
        extra_payload: Extra payload
    """

    action_type: ClassVar[str] = ActionType.RESUME.value

    content: Union[str, List[Dict[str, Any]]] = ""
    status: str = ""
    reply_data: JsonValue = None
    extra_payload: dict[str, JsonValue] = field(default_factory=dict)

    def __post_init__(self):
        if not self.status and not _has_content(self.content):
            raise ValueError("ResumeCommand requires status or content")

    def to_dict(self) -> dict[str, Any]:
        body = {
            "content": self.content,
            "status": self.status,
            "reply_data": self.reply_data,
        }
        if self.extra_payload:
            body["extra_payload"] = dict(self.extra_payload)
        return {
            "action_type": self.action_type,
            "header": self.header.to_dict(),
            "body": body,
        }

    def inject_runtime_payload(self, payload: dict[str, Any]) -> None:
        self.extra_payload.update(payload)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ResumeCommand":
        body = dict(data.get("body", {}))
        return cls(
            header=MessageHeader.from_dict(data["header"]),
            content=body.get("content", ""),
            status=body.get("status", ""),
            reply_data=body.get("reply_data"),
            extra_payload=dict(body.get("extra_payload", {})),
        )


@dataclass
class CancelTaskCommand(BaseCommand):
    """Command to cancel an executing task.

    Attributes:
        target_message_id: Target message ID
        target_execution_id: Target execution ID
        target_worker_id: Target worker ID
        reason: Cancellation reason
        requested_by: Requester
        cancel_mode: Cancel mode (graceful or force)
    """

    action_type: ClassVar[str] = ActionType.CANCEL_TASK.value

    target_message_id: str
    target_execution_id: str = ""
    target_worker_id: str = ""
    reason: str = ""
    requested_by: str = ""
    cancel_mode: str = CancelMode.GRACEFUL

    def __post_init__(self):
        if not self.target_message_id:
            raise ValueError("CancelTaskCommand requires target_message_id")
        if self.cancel_mode not in CancelMode.values():
            raise ValueError("CancelTaskCommand cancel_mode must be graceful or force")

    def to_dict(self) -> dict[str, Any]:
        return {
            "action_type": self.action_type,
            "header": self.header.to_dict(),
            "body": {
                "target_message_id": self.target_message_id,
                "target_execution_id": self.target_execution_id,
                "target_worker_id": self.target_worker_id,
                "reason": self.reason,
                "requested_by": self.requested_by,
                "cancel_mode": self.cancel_mode,
            },
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CancelTaskCommand":
        body = dict(data.get("body", {}))
        return cls(
            header=MessageHeader.from_dict(data["header"]),
            target_message_id=body.get("target_message_id", ""),
            target_execution_id=body.get("target_execution_id", ""),
            target_worker_id=body.get("target_worker_id", ""),
            reason=body.get("reason", ""),
            requested_by=body.get("requested_by", ""),
            cancel_mode=body.get("cancel_mode", CancelMode.GRACEFUL),
        )


@dataclass
class ReloadPluginsCommand(BaseCommand):
    """Command to trigger ordered plugin reload on a worker."""

    action_type: ClassVar[str] = ActionType.RELOAD_PLUGINS.value

    reload_id: str
    reason: str = ""

    def __post_init__(self):
        if not self.reload_id:
            raise ValueError("ReloadPluginsCommand requires reload_id")

    def to_dict(self) -> dict[str, Any]:
        return {
            "action_type": self.action_type,
            "header": self.header.to_dict(),
            "body": {
                "reload_id": self.reload_id,
                "reason": self.reason,
            },
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ReloadPluginsCommand":
        body = dict(data.get("body", {}))
        return cls(
            header=MessageHeader.from_dict(data["header"]),
            reload_id=body.get("reload_id", ""),
            reason=body.get("reason", ""),
        )


GatewayCommand = BaseCommand
CommandT = TypeVar("CommandT", bound=BaseCommand)
_COMMAND_REGISTRY: dict[str, type[BaseCommand]] = {}


def register_command(command_cls: type[CommandT]) -> type[CommandT]:
    action_type = getattr(command_cls, "action_type", "")
    if not action_type:
        raise ValueError(f"{command_cls.__name__} must define action_type")
    _COMMAND_REGISTRY[action_type] = command_cls
    return command_cls


def unregister_command(action_type: str) -> None:
    _COMMAND_REGISTRY.pop(action_type, None)


def get_registered_command(action_type: str) -> type[BaseCommand] | None:
    return _COMMAND_REGISTRY.get(action_type)


def command_from_dict(data: dict[str, Any]) -> BaseCommand:
    action_type = data.get("action_type")
    command_cls = get_registered_command(str(action_type))
    if command_cls is None:
        raise ValueError(f"Unsupported action_type: {action_type}")
    return command_cls.from_dict(data)


register_command(AskAgentCommand)
register_command(ResumeCommand)
register_command(CancelTaskCommand)
register_command(ReloadPluginsCommand)
