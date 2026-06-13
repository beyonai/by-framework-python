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
from .byai_codec import (
    ByaiContentCodec,
    deserialize_byai_content,
    serialize_byai_content,
)
from .byai_command import ByaiAskAgentCommand, ByaiResumeCommand
from .byai_types import ByaiContent
from .commands import (
    AskAgentCommand,
    BaseCommand,
    CancelTaskCommand,
    EvictWorkerCommand,
    GatewayCommand,
    ReloadPluginsCommand,
    ResumeCommand,
    ResumeWorkerCommand,
    SuspendWorkerCommand,
    command_from_dict,
    get_registered_command,
    register_command,
    unregister_command,
)
from .content_codec import ContentCodec, WireContent
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
from .results import (
    AgentTaskResult,
    JsonValue,
    ProcessCommandResult,
    ensure_json_serializable,
    normalize_process_result,
)

__all__ = [
    "SendMessageResponse",
    "CancelTaskResponse",
    "SendMessageResponseDict",
    "CancelTaskResponseDict",
    "AgentTaskResult",
    "JsonValue",
    "ProcessCommandResult",
    "ensure_json_serializable",
    "normalize_process_result",
    "BaiYingMessage",
    "BaiYingMessageRole",
    "MessageContent",
    "MessageFile",
    "Resource",
    "ContentCodec",
    "WireContent",
    "ByaiContent",
    "ByaiContentCodec",
    "ByaiAskAgentCommand",
    "ByaiResumeCommand",
    "serialize_byai_content",
    "deserialize_byai_content",
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
    "ReloadPluginsCommand",
    "SuspendWorkerCommand",
    "ResumeWorkerCommand",
    "EvictWorkerCommand",
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
