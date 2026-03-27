"""
Action type definitions for Gateway protocol commands.

Contains the ActionType enum which defines all possible command action types
in the Gateway command system.
"""

from enum import Enum
from typing import Literal


class ActionType(str, Enum):
    """命令动作类型枚举，定义 Gateway 命令系统中的所有可能动作类型。

    Attributes:
        ASK_AGENT: 向智能体发送消息并等待回复
        RESUME: 恢复挂起的任务执行
        ASK_USER: 向用户请求输入
        CANCEL_TASK: 取消正在执行的任务
    """

    ASK_AGENT = "ASK_AGENT"
    RESUME = "RESUME"
    ASK_USER = "ASK_USER"
    CANCEL_TASK = "CANCEL_TASK"


# Literal type alias for exhaustive type checking
ActionTypeLiteral = Literal["ASK_AGENT", "RESUME", "ASK_USER", "CANCEL_TASK"]
