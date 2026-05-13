"""
Event type definitions for Gateway protocol.

Contains the EventType enum which defines all possible event types
that can be emitted through the Gateway system.
"""

from enum import Enum


class EventType(str, Enum):
    """Event type enum, defining all possible event types in the Gateway system.

    Attributes:
        ANSWER_DELTA: Answer content delta event
        REASONING_LOG_DELTA: Reasoning log delta event
        REASONING_LOG_START: Reasoning log start event
        REASONING_LOG_END: Reasoning log end event
        FINAL_ANSWER: Final answer event
        APP_STREAM_RESPONSE: Application streaming response event
        TASK_CREATE: Task creation event
        STEP_COMPLETE: Step completion event
        TASK_STOP: Task stop event
    """

    ANSWER_DELTA = "answerDelta"
    REASONING_LOG_DELTA = "reasoningLogDelta"
    REASONING_LOG_START = "reasoningLogStart"
    REASONING_LOG_END = "reasoningLogEnd"
    FINAL_ANSWER = "finalAnswer"
    APP_STREAM_RESPONSE = "appStreamResponse"
    TASK_CREATE = "taskCreate"
    STEP_COMPLETE = "stepComplete"
    TASK_STOP = "taskStop"
