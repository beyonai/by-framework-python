"""
Event structures for Gateway protocol.

Contains immutable dataclasses for all event types that can be emitted
through the Gateway event system.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class StateChangeEvent:
    """状态变更事件。

    Attributes:
        state: 新的状态值
        metadata: 附加元数据
    """

    state: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StreamChunkEvent:
    """流式内容块事件。

    Attributes:
        content: 文本内容块
        role: 消息角色
        function_call: 函数调用信息
        tool_calls: 工具调用列表
        metadata: 附加元数据
    """

    content: Optional[str] = None
    role: str = "assistant"
    function_call: Optional[Dict[str, Any]] = None
    tool_calls: Optional[List[Dict[str, Any]]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ArtifactEvent:
    """产物/artifact 事件。

    Attributes:
        url: 产物 URL
        metadata: 附加元数据
    """

    url: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AskUserEvent:
    """向用户请求输入事件。

    Attributes:
        prompt: 提示文本
        metadata: 附加元数据
    """

    prompt: str
    metadata: Dict[str, Any] = field(default_factory=dict)
