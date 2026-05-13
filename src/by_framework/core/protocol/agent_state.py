"""
Agent state definitions for Gateway protocol.

Contains the AgentState enum which defines all possible states
for an agent task throughout its lifecycle.
"""

from enum import Enum
from typing import Literal


class AgentState(str, Enum):
    """Agent state enum, defining all possible states for agent tasks.

    Basic lifecycle states:
        STARTING: Task is starting
        COMPLETED: Task has completed
        FAILED: Task execution failed
        CANCELLING: Task is being cancelled
        CANCELLED: Task has been cancelled

    Suspension and resumption states:
        RESUMED: Task has resumed execution
        WAITING_AGENT: Waiting for other agent response
        WAITING_USER: Waiting for user input
        QUEUED: Task has been queued
        CALLING_AGENT: Calling another agent
    """

    # Basic lifecycle states
    STARTING = "STARTING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"

    # Suspension and resumption states
    RESUMED = "RESUMED"
    WAITING_AGENT = "WAITING_AGENT"
    WAITING_USER = "WAITING_USER"
    QUEUED = "QUEUED"
    CALLING_AGENT = "CALLING_AGENT"


# Literal type alias for exhaustive type checking
AgentStateLiteral = Literal[
    "STARTING",
    "COMPLETED",
    "FAILED",
    "CANCELLING",
    "CANCELLED",
    "RESUMED",
    "WAITING_AGENT",
    "WAITING_USER",
    "QUEUED",
    "CALLING_AGENT",
]

# Terminal states - states that represent a completed execution
TERMINAL_STATES: frozenset[str] = frozenset(
    {
        AgentState.COMPLETED.value,
        AgentState.FAILED.value,
        AgentState.CANCELLED.value,
    }
)


def is_terminal_state(state: str) -> bool:
    """Determine if the given state is a terminal state.

    Args:
        state: State string to check

    Returns:
        True if it's a terminal state, otherwise False
    """
    return state in TERMINAL_STATES
