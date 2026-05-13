"""
Action type definitions for Gateway protocol commands.

Contains the ActionType enum which defines all possible command action types
in the Gateway command system.
"""

from enum import Enum
from typing import Literal


class ActionType(str, Enum):
    """Command action type enum, defining all possible action types in Gateway commands.

    Attributes:
        ASK_AGENT: Send message to agent and wait for reply
        RESUME: Resume suspended task execution
        ASK_USER: Request input from user
        CANCEL_TASK: Cancel executing task
        RELOAD_PLUGINS: Reload plugin config chain on a worker
    """

    ASK_AGENT = "ASK_AGENT"
    RESUME = "RESUME"
    ASK_USER = "ASK_USER"
    CANCEL_TASK = "CANCEL_TASK"
    RELOAD_PLUGINS = "RELOAD_PLUGINS"


# Literal type alias for exhaustive type checking
ActionTypeLiteral = Literal[
    "ASK_AGENT", "RESUME", "ASK_USER", "CANCEL_TASK", "RELOAD_PLUGINS"
]
