"""
Message definitions for BaiYing protocol.

Contains message role types, content structures, and message classes
used in the BaiYing messaging protocol.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union


class BaiYingMessageRole(str, Enum):
    """BaiYing 消息角色枚举。

    Attributes:
        USER: 用户消息角色
        ASSISTANT: 助手消息角色
        SYSTEM: 系统消息角色
        TOOL_CALL: 工具调用消息角色
        TOOL_RESPONSE: 工具响应消息角色
        RESPONSE_TO_SUB_AGENT: 子智能体响应消息角色
    """

    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"
    TOOL_CALL = "tool-call"
    TOOL_RESPONSE = "tool-response"
    RESPONSE_TO_SUB_AGENT = "response-to-sub-agent"


@dataclass(frozen=True)
class MessageFile:
    """消息中携带的文件对象。

    Attributes:
        fileId: 文件ID
        fileUrl: 文件URL
        fileType: 文件类型
        fileName: 文件名
    """

    fileId: int
    fileUrl: str
    fileType: str
    fileName: str


@dataclass(frozen=True)
class Resource:
    """消息中携带的资源对象。

    Attributes:
        resourceId: 资源ID
        resourceName: 资源名称
        resourceType: 资源类型
        id: 资源标识
        path: 资源路径
        resourceDesc: 资源描述
        resourceMetaData: 资源元数据
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
    """消息内容结构，支持文本、文件和资源。

    Attributes:
        text: 文本内容
        files: 文件列表
        resources: 资源列表
    """

    text: str
    files: List[MessageFile] = field(default_factory=list)
    resources: List[Resource] = field(default_factory=list)


@dataclass(frozen=True)
class BaiYingMessage:
    """BaiYing 消息结构。

    Attributes:
        role: 消息角色
        content: 消息内容（文本或 MessageContent 结构）
    """

    role: BaiYingMessageRole
    content: Union[str, MessageContent]
