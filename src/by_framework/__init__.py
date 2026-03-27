"""
Gateway SDK

允许开发人员通过继承 `GatewayWorker` 并运行 `run_worker` 快速启动基于 Redis 的代理节点。
"""

from .client.byai_client import ByaiGatewayClient
from .client.client import (CancelTaskResponse, GatewayClient, SendMessageResponse)
from .client.interceptors import ByaiMessageInterceptor, GatewayInterceptor
from .common.constants import RedisKeys
from .common.emitter import GatewayDataEmitter
from .common.logger import logger, setup_logging
from .common.redis_client import Redis, close_redis, get_redis, init_redis
from .core.extensions import (
    AgentConfig,
    CallbackType,
    Plugin,
    PluginBuildContext,
    PluginManifest,
    PluginRegistry,
    PromptTemplate,
)
from .core.protocol import DataMessage, SseMessageType, SseReasonMessageType
from .core.protocol.action_type import ActionType
from .core.protocol.agent_state import AgentState
from .core.protocol.commands import (
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
from .core.protocol.event_type import EventType
from .core.protocol.events import (
    ArtifactEvent,
    AskUserEvent,
    StateChangeEvent,
    StreamChunkEvent,
)
from .core.protocol.message import (
    BaiYingMessage,
    BaiYingMessageRole,
    MessageContent,
    MessageFile,
    Resource,
)
from .core.protocol.message_header import MessageHeader
from .core.registry import WorkerRegistry
from .core.workspace import WorkspaceManager
from .worker.app import run_worker
from .worker.context import AgentContext
from .worker.heartbeat import WorkerHeartbeat
from .worker.processor import GatewayProcessor
from .worker.runner import RunningExecution, WorkerRunner
from .worker.worker import GatewayWorker

__all__ = [
    "GatewayWorker",
    "BaiYingMessage",
    "BaiYingMessageRole",
    "MessageContent",
    "MessageFile",
    "Resource",
    "AgentContext",
    "AgentConfig",
    "CallbackType",
    "PluginBuildContext",
    "PluginManifest",
    "Plugin",
    "PromptTemplate",
    "PluginRegistry",
    "GatewayDataEmitter",
    "GatewayClient",
    "ByaiGatewayClient",
    "GatewayInterceptor",
    "ByaiMessageInterceptor",
    "SendMessageResponse",
    "CancelTaskResponse",
    "run_worker",
    "logger",
    "setup_logging",
    "get_redis",
    "init_redis",
    "close_redis",
    "Redis",
    "WorkerRegistry",
    "RedisKeys",
    "ActionType",
    "AgentState",
    "EventType",
    "StateChangeEvent",
    "StreamChunkEvent",
    "ArtifactEvent",
    "AskUserEvent",
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
    "WorkerRunner",
    "WorkspaceManager",
    "WorkerHeartbeat",
    "GatewayProcessor",
    "DataMessage",
    "SseMessageType",
    "SseReasonMessageType",
    "RunningExecution",
]
