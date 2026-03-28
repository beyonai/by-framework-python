"""
Control message handling module for WorkerRunner.

Handles control commands like CancelTaskCommand.
"""

import asyncio

from by_framework.common.exceptions import UnsupportedCommandError
from by_framework.core.protocol.commands import (
    AskAgentCommand,
    CancelTaskCommand,
    ResumeCommand,
    command_from_dict,
)


async def parse_control_command(data_dict: dict) -> CancelTaskCommand | AskAgentCommand | ResumeCommand:
    """
    Parse and validate a control or task command from the worker control stream.

    Args:
        data_dict: Parsed JSON data

    Returns:
        CancelTaskCommand, AskAgentCommand, or ResumeCommand instance

    Raises:
        UnsupportedCommandError: If command type is not supported on the control stream
    """
    try:
        command = command_from_dict(data_dict)
    except ValueError as e:
        raise UnsupportedCommandError(str(e)) from e
    if isinstance(command, CancelTaskCommand):
        return command
    if isinstance(command, (AskAgentCommand, ResumeCommand)):
        # AskAgentCommand/ResumeCommand on worker_ctrl_stream means direct routing
        return command
    raise UnsupportedCommandError(type(command).__name__)


async def handle_cancel_task(
    command: CancelTaskCommand,
    active_executions: dict,
    message_to_execution: dict,
    redis_client,
    group_name: str,
    worker,
) -> None:
    """
    Handle a CancelTaskCommand.

    Triggers cancellation for the target execution and notifies plugins.
    """
    # Find execution ID
    execution_id = command.target_execution_id or message_to_execution.get(
        command.target_message_id
    )
    reason = command.reason
    running = active_executions.get(execution_id) if execution_id else None

    registry = getattr(worker, "registry", None)
    target_session_id = running.session_id if running else command.header.session_id

    # Mark execution as cancelling
    if execution_id and registry and hasattr(registry, "mark_execution_cancelling"):
        await registry.mark_execution_cancelling(
            execution_id, target_session_id, reason
        )

    # Trigger cancellation
    if running:
        running.cancel_reason = reason
        running.cancel_event.set()

        # Notify worker plugins
        if running.context and worker.plugin_registry:
            asyncio.create_task(
                worker.plugin_registry.on_task_cancel(running.context, command)
            )
        asyncio.create_task(worker.on_cancel_task(command))

        # Cancel the task
        running.task.cancel()
