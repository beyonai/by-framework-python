"""
Gateway Protocol module.

This module contains all protocol-related definitions for the Gateway system,
including:
- Message types and headers
- Command types (AskAgent, Resume, CancelTask)
- Event types and event structures
- Agent state definitions
- Response types

All components in this module are designed to be immutable (frozen dataclasses)
where possible to ensure protocol stability.
"""

from .action_type import ActionType, ActionTypeLiteral
from .agent_state import (
    TERMINAL_STATES,
    AgentState,
    AgentStateLiteral,
    is_terminal_state,
)
from .commands import (
    AskAgentCommand,
    BaseCommand,
    CancelTaskCommand,
    GatewayCommand,
    ResumeCommand,
    command_from_dict,
    get_registered_command,
    register_command,
    unregister_command,
)
from .content_type import SseMessageType, SseReasonMessageType
from .data_message import DataMessage
from .data_shapes import (
    AskAgentBodyDict,
    CancelTaskBodyDict,
    CommandBodyDict,
    CommandHeaderDict,
    ExecutionDataDict,
    RedisCommandDict,
    ResumeBodyDict,
)
from .event_type import EventType
from .events import (ArtifactEvent, AskUserEvent, StateChangeEvent, StreamChunkEvent)
from .message import (
    BaiYingMessage,
    BaiYingMessageRole,
    MessageContent,
    MessageFile,
    Resource,
)
from .message_header import MessageHeader
from .responses import (
    CancelTaskResponse,
    CancelTaskResponseDict,
    SendMessageResponse,
    SendMessageResponseDict,
)

__all__ = [
    "SendMessageResponse",
    "CancelTaskResponse",
    "SendMessageResponseDict",
    "CancelTaskResponseDict",
    "BaiYingMessage",
    "BaiYingMessageRole",
    "MessageContent",
    "MessageFile",
    "Resource",
    "DataMessage",
    "ActionType",
    "ActionTypeLiteral",
    "AgentState",
    "AgentStateLiteral",
    "is_terminal_state",
    "TERMINAL_STATES",
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
    "ExecutionDataDict",
    "CommandHeaderDict",
    "CommandBodyDict",
    "AskAgentBodyDict",
    "ResumeBodyDict",
    "CancelTaskBodyDict",
    "RedisCommandDict",
]
