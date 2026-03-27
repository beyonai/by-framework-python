"""
Event type definitions for Gateway protocol.

Contains the EventType enum which defines all possible event types
that can be emitted through the Gateway system.
"""

from enum import Enum


class EventType(str, Enum):
    """事件类型枚举，定义 Gateway 系统中所有可能的事件类型。

    Attributes:
        ANSWER_DELTA: 回答内容增量事件
        REASONING_LOG_DELTA: 推理日志增量事件
        REASONING_LOG_START: 推理日志开始事件
        REASONING_LOG_END: 推理日志结束事件
        APP_STREAM_RESPONSE: 应用流式响应事件
        TASK_CREATE: 任务创建事件
        STEP_COMPLETE: 步骤完成事件
        TASK_STOP: 任务停止事件
    """

    ANSWER_DELTA = "answerDelta"
    REASONING_LOG_DELTA = "reasoningLogDelta"
    REASONING_LOG_START = "reasoningLogStart"
    REASONING_LOG_END = "reasoningLogEnd"
    APP_STREAM_RESPONSE = "appStreamResponse"
    TASK_CREATE = "taskCreate"
    STEP_COMPLETE = "stepComplete"
    TASK_STOP = "taskStop"
