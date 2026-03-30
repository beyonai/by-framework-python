"""
Data message definitions for Redis stream transport.

Contains the DataMessage class used for sending structured data events
through Redis streams.
"""

import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class DataMessage:
    """DataMessage 用于通过 Redis streams 发送结构化数据事件。

    Attributes:
        trace_id: 追踪ID
        session_id: 会话ID
        event_type: 事件类型
        source_agent_id: 源智能体ID
        message_id: 消息ID
        timestamp: 时间戳（毫秒）
        data: 事件数据负载
        state_msg: 状态消息
        artifact_url: 产物URL
        metadata: 附加元数据
    """

    trace_id: str
    session_id: str
    event_type: str
    source_agent_id: str = ""
    message_id: str = ""
    parent_message_id: str = ""
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))
    data: Dict[str, Any] = field(default_factory=dict)
    state_msg: str = ""
    artifact_url: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_redis_payload(self) -> Dict[str, str]:
        """Pack the message into a format ready for Redis streams."""
        return {"data": json.dumps(self.__dict__)}
