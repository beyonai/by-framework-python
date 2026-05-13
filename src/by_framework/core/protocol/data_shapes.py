"""
TypedDict definitions for external data shapes.

These are used for validating data that comes from external sources
like Redis, JSON files, or API responses.
"""

from typing import Any, Dict, List, NotRequired, Required, TypedDict


class ExecutionDataDict(TypedDict):
    """Shape of execution data stored in Redis."""

    execution_id: Required[str]
    message_id: Required[str]
    session_id: Required[str]
    worker_id: Required[str]
    target_agent_type: Required[str]
    stream_name: Required[str]
    redis_message_id: Required[str]
    status: Required[str]
    cancel_requested: Required[bool]
    cancel_reason: Required[str]
    created_at: Required[int]
    started_at: Required[int]
    finished_at: Required[int]


class CommandHeaderDict(TypedDict):
    """Shape of command header in Redis messages."""

    message_id: Required[str]
    session_id: Required[str]
    trace_id: Required[str]
    source_agent_type: NotRequired[str]
    target_agent_type: Required[str]
    parent_message_id: NotRequired[str]
    user_code: NotRequired[str]
    user_name: NotRequired[str]
    task_group_id: NotRequired[str]
    metadata: NotRequired[Dict[str, Any]]


class CommandBodyDict(TypedDict):
    """Base shape of command body in Redis messages."""

    pass


class AskAgentBodyDict(CommandBodyDict):
    """Body shape for AskAgentCommand."""

    content: Required[str | List[Dict[str, Any]]]
    wait_for_reply: NotRequired[bool]
    extra_payload: NotRequired[Dict[str, Any]]


class ResumeBodyDict(CommandBodyDict):
    """Body shape for ResumeCommand."""

    content: NotRequired[str | List[Dict[str, Any]]]
    status: NotRequired[str]
    reply_data: NotRequired[Any]
    extra_payload: NotRequired[Dict[str, Any]]


class CancelTaskBodyDict(CommandBodyDict):
    """Body shape for CancelTaskCommand."""

    target_message_id: Required[str]
    target_execution_id: NotRequired[str]
    target_worker_id: NotRequired[str]
    reason: NotRequired[str]
    requested_by: NotRequired[str]
    cancel_mode: NotRequired[str]


class RedisCommandDict(TypedDict):
    """Full shape of command stored in Redis."""

    action_type: Required[str]
    header: Required[CommandHeaderDict]
    body: Required[CommandBodyDict]
