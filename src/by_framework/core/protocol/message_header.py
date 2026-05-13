"""
Message header definitions for Gateway protocol.

Contains the MessageHeader class which carries routing and context information
for all messages exchanged within the Gateway system.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class MessageHeader:
    """Message header definition, containing message routing and context information.

    Attributes:
        message_id: Message unique identifier
        session_id: Session unique identifier, used to associate all messages
            under the same session
        trace_id: Trace ID, used for distributed tracing and log correlation
        source_agent_type: Source agent type, identifies the sender of the message
        target_agent_type: Target agent type, identifies the receiver of the message
        parent_message_id: Parent message ID, used to construct message chains
        task_group_id: Task group ID, used for batch task tracking
        user_code: User code, used for multi-user isolation
        user_name: User name
        metadata: Extra metadata dictionary
    """

    message_id: str
    session_id: str
    trace_id: str
    source_agent_type: str = ""
    target_agent_type: str = ""
    parent_message_id: str = ""
    task_group_id: str = ""
    user_code: str = ""
    user_name: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "session_id": self.session_id,
            "trace_id": self.trace_id,
            "source_agent_type": self.source_agent_type,
            "target_agent_type": self.target_agent_type,
            "parent_message_id": self.parent_message_id,
            "task_group_id": self.task_group_id,
            "user_code": self.user_code,
            "user_name": self.user_name,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MessageHeader":
        return cls(
            message_id=data["message_id"],
            session_id=data["session_id"],
            trace_id=data["trace_id"],
            source_agent_type=data.get("source_agent_type", ""),
            target_agent_type=data.get("target_agent_type", ""),
            parent_message_id=data.get("parent_message_id", ""),
            task_group_id=data.get("task_group_id", ""),
            user_code=data.get("user_code", ""),
            user_name=data.get("user_name", ""),
            metadata=dict(data.get("metadata", {})),
        )
