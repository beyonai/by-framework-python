"""
Core module of the Gateway SDK.

Provides core components including protocol definitions, worker registry,
workspace management, and history provider.
"""

from .history import HistoryProvider
from .protocol import (
    ActionType,
    AgentState,
    ArtifactEvent,
    AskAgentCommand,
    AskUserEvent,
    BaiYingMessage,
    BaiYingMessageRole,
    BaseCommand,
    CancelTaskCommand,
    CancelTaskResponse,
    DataMessage,
    EventType,
    GatewayCommand,
    MessageContent,
    MessageFile,
    MessageHeader,
    Resource,
    ResumeCommand,
    SendMessageResponse,
    SseMessageType,
    SseReasonMessageType,
    StateChangeEvent,
    StreamChunkEvent,
    command_from_dict,
    get_registered_command,
    register_command,
    unregister_command,
)
from .registry import WorkerRegistry
from .workspace import WorkspaceManager

__all__ = [
    "SendMessageResponse",
    "CancelTaskResponse",
    "BaiYingMessage",
    "BaiYingMessageRole",
    "MessageContent",
    "MessageFile",
    "Resource",
    "DataMessage",
    "ActionType",
    "AgentState",
    "EventType",
    "StateChangeEvent",
    "StreamChunkEvent",
    "ArtifactEvent",
    "AskUserEvent",
    "SseMessageType",
    "SseReasonMessageType",
    "MessageHeader",
    "BaseCommand",
    "AskAgentCommand",
    "ResumeCommand",
    "CancelTaskCommand",
    "GatewayCommand",
    "command_from_dict",
    "register_command",
    "unregister_command",
    "get_registered_command",
    "WorkerRegistry",
    "WorkspaceManager",
    "HistoryProvider",
]
