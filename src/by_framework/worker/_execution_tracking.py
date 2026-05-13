"""
Execution tracking module for WorkerRunner.

Contains RunningExecution dataclass and execution state management.
"""

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from by_framework.worker.context import AgentContext


@dataclass
class RunningExecution:
    """Tracks an active execution within a worker."""

    execution_id: str
    message_id: str
    session_id: str
    worker_id: str
    task: asyncio.Task
    cancel_event: asyncio.Event
    parent_message_id: str = ""
    context: Optional["AgentContext"] = None
    cancel_reason: str = ""
    is_resumed: bool = False
    existing_data: Optional[dict[str, Any]] = None


class ExecutionTracker:
    """Manages active executions and message-to-execution mappings."""

    def __init__(self):
        self._active_executions: dict[str, RunningExecution] = {}
        self._message_to_execution: dict[str, str] = {}

    def add_execution(self, execution: RunningExecution) -> None:
        """Register a new active execution."""
        self._active_executions[execution.execution_id] = execution
        self._message_to_execution[execution.message_id] = execution.execution_id

    def get_execution(self, execution_id: str) -> Optional[RunningExecution]:
        """Get an active execution by ID."""
        return self._active_executions.get(execution_id)

    def get_execution_by_message(self, message_id: str) -> Optional[RunningExecution]:
        """Get an active execution by message ID."""
        execution_id = self._message_to_execution.get(message_id)
        if execution_id:
            return self._active_executions.get(execution_id)
        return None

    def remove_execution(self, execution_id: str) -> Optional[RunningExecution]:
        """Remove and return an execution by ID."""
        execution = self._active_executions.pop(execution_id, None)
        if execution:
            self._message_to_execution.pop(execution.message_id, None)
        return execution

    def remove_by_message(self, message_id: str) -> Optional[RunningExecution]:
        """Remove and return an execution by message ID."""
        execution_id = self._message_to_execution.pop(message_id, None)
        if execution_id:
            return self._active_executions.pop(execution_id, None)
        return None

    def get_active_count(self) -> int:
        """Return the number of active executions."""
        return len(self._active_executions)
