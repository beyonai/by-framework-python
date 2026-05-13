"""Typed Byai command subclasses used by Byai workers."""

from dataclasses import dataclass, field
from typing import Any

from .byai_codec import serialize_byai_content
from .byai_types import ByaiContent
from .commands import AskAgentCommand, ResumeCommand
from .results import JsonValue


@dataclass
class ByaiAskAgentCommand(AskAgentCommand):
    """AskAgentCommand with Byai-specific content typing."""

    content: ByaiContent

    def to_dict(self) -> dict[str, Any]:
        body = {
            "content": serialize_byai_content(self.content),
            "wait_for_reply": self.wait_for_reply,
        }
        if self.extra_payload:
            body["extra_payload"] = dict(self.extra_payload)
        return {
            "action_type": self.action_type,
            "header": self.header.to_dict(),
            "body": body,
        }


@dataclass
class ByaiResumeCommand(ResumeCommand):
    """ResumeCommand with Byai-specific content typing."""

    content: ByaiContent = ""
    status: str = ""
    reply_data: JsonValue = None
    extra_payload: dict[str, JsonValue] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        body = {
            "content": serialize_byai_content(self.content),
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
