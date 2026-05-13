# pylint: disable=C0103
"""
Message definitions for BaiYing protocol.

Contains message role types, content structures, and message classes
used in the BaiYing messaging protocol.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union


class BaiYingMessageRole(str, Enum):
    """BaiYing message role enum.

    Attributes:
        USER: User message role
        ASSISTANT: Assistant message role
        SYSTEM: System message role
        TOOL_CALL: Tool call message role
        TOOL_RESPONSE: Tool response message role
        RESPONSE_TO_SUB_AGENT: Sub-agent response message role
    """

    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"
    TOOL_CALL = "tool-call"
    TOOL_RESPONSE = "tool-response"
    RESPONSE_TO_SUB_AGENT = "response-to-sub-agent"


@dataclass(frozen=True)
class MessageFile:
    """File object carried in the message.

    Attributes:
        fileId: File ID
        fileUrl: File URL
        fileType: File type
        fileName: File name
    """

    fileId: int
    fileUrl: str
    fileType: str
    fileName: str


@dataclass(frozen=True)
class Resource:
    """Resource object carried in the message.

    Attributes:
        resourceId: Resource ID
        resourceName: Resource name
        resourceType: Resource type
        id: Resource identifier
        path: Resource path
        resourceDesc: Resource description
        resourceMetaData: Resource metadata
    """

    resourceId: str
    resourceName: str
    resourceType: str
    id: Optional[str] = ""
    path: Optional[str] = ""
    resourceDesc: Optional[str] = ""
    resourceMetaData: Optional[Dict[str, Any]] = field(default_factory=dict)


@dataclass(frozen=True)
class MessageContent:
    """Message content structure, supporting text, files, and resources.

    Attributes:
        text: Text content
        files: File list
        resources: Resource list
    """

    text: str
    files: List[MessageFile] = field(default_factory=list)
    resources: List[Resource] = field(default_factory=list)


@dataclass(frozen=True)
class BaiYingMessage:
    """BaiYing message structure.

    Attributes:
        role: Message role
        content: Message content (text or MessageContent structure)
    """

    role: BaiYingMessageRole
    content: Union[str, MessageContent]
