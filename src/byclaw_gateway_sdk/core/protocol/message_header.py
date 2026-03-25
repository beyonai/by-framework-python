"""
Message header definitions for Gateway protocol.

Contains the MessageHeader class which carries routing and context information
for all messages exchanged within the Gateway system.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class MessageHeader:
    """消息头定义，包含消息的路由和上下文信息。

    Attributes:
        message_id: 消息唯一标识符
        session_id: 会话唯一标识符，用于关联同一会话下的所有消息
        trace_id: 追踪ID，用于分布式追踪和日志关联
        source_agent_id: 源智能体ID，标识消息的发送方
        target_agent_type: 目标智能体类型，标识消息的接收方
        parent_message_id: 父消息ID，用于构建消息链条
        task_group_id: 任务组ID，用于批量任务追踪
        tenant_id: 租户ID，用于多租户隔离
        metadata: 附加元数据字典
    """

    message_id: str
    session_id: str
    trace_id: str
    source_agent_id: str = ""
    target_agent_type: str = ""
    parent_message_id: str = ""
    task_group_id: str = ""
    tenant_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "session_id": self.session_id,
            "trace_id": self.trace_id,
            "source_agent_id": self.source_agent_id,
            "target_agent_type": self.target_agent_type,
            "parent_message_id": self.parent_message_id,
            "task_group_id": self.task_group_id,
            "tenant_id": self.tenant_id,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MessageHeader":
        return cls(
            message_id=data["message_id"],
            session_id=data["session_id"],
            trace_id=data["trace_id"],
            source_agent_id=data.get("source_agent_id", ""),
            target_agent_type=data.get("target_agent_type", ""),
            parent_message_id=data.get("parent_message_id", ""),
            task_group_id=data.get("task_group_id", ""),
            tenant_id=data.get("tenant_id", ""),
            metadata=dict(data.get("metadata", {})),
        )
