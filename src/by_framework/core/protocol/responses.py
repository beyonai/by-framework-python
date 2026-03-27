"""
Response type definitions for Gateway protocol.

Contains response dataclasses and TypedDict definitions for
send message and cancel task operations.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, NotRequired, Required, TypedDict

# --- API Response Status Constants ---
# These represent the status of an API operation, not the agent state


class ExecutionStatus:
    """API response status values for execution operations."""

    # Success statuses
    SUCCESS = "SUCCESS"
    QUEUED = "QUEUED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"

    # Error statuses
    NOT_FOUND = "NOT_FOUND"
    ALREADY_FINISHED = "ALREADY_FINISHED"
    FAILED = "FAILED"
    SESSION_MISMATCH = "SESSION_MISMATCH"


# TypedDict definitions for external data shapes (Redis, JSON)


class SendMessageResponseDict(TypedDict):
    """External format for SendMessageResponse serialization."""

    success: Required[bool]
    message_id: Required[str]
    trace_id: Required[str]
    target_worker_id: Required[str]
    timestamp: Required[int]
    status: Required[str]


class CancelTaskResponseDict(TypedDict):
    """External format for CancelTaskResponse serialization."""

    success: Required[bool]
    message_id: Required[str]
    execution_id: Required[str]
    worker_id: Required[str]
    status: Required[str]
    timestamp: Required[int]
    error: NotRequired[str]


SendMessageStatus = Literal["SUCCESS", "QUEUED", "FAILED"]
CancelTaskStatus = Literal[
    "NOT_FOUND", "ALREADY_FINISHED", "CANCEL_REQUESTED", "SESSION_MISMATCH"
]


@dataclass(frozen=True)
class SendMessageResponse:
    success: bool
    message_id: str
    trace_id: str
    target_worker_id: str
    timestamp: int
    status: str


@dataclass(frozen=True)
class CancelTaskResponse:
    success: bool
    message_id: str
    execution_id: str
    worker_id: str
    status: str
    timestamp: int
    error: str = ""
