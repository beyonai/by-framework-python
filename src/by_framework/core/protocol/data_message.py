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
    """DataMessage used for sending structured data events via Redis streams.

    Attributes:
        trace_id: Trace ID
        session_id: Session ID
        event_type: Event type
        source_agent_type: Source agent type
        message_id: Message ID
        timestamp: Timestamp (milliseconds)
        data: Event data payload
        state_msg: State message
        artifact_url: Artifact URL
        metadata: Extra metadata
    """

    trace_id: str
    session_id: str
    event_type: str
    source_agent_type: str = ""
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
