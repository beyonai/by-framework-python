"""
Agent state definitions for Gateway protocol.

Contains the AgentState enum which defines all possible states
for an agent task throughout its lifecycle.
"""

from enum import Enum
from typing import Literal


class AgentState(str, Enum):
    """智能体状态枚举，定义智能体任务在其生命周期中的所有可能状态。

    Basic lifecycle states:
        STARTING: 任务正在启动
        COMPLETED: 任务已完成
        FAILED: 任务执行失败
        CANCELLING: 任务正在被取消
        CANCELLED: 任务已被取消

    Suspension and resumption states:
        RESUMED: 任务已恢复执行
        WAITING_AGENT: 等待其他智能体响应
        WAITING_USER: 等待用户输入
        QUEUED: 任务已加入队列
        CALLING_AGENT: 正在调用其他智能体
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
    """判断给定状态是否为终态。

    Args:
        state: 要检查的状态字符串

    Returns:
        如果是终态返回 True，否则返回 False
    """
    return state in TERMINAL_STATES
