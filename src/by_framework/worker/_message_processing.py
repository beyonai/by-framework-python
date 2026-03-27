"""
Message processing module for WorkerRunner.

Handles message fetching, parsing, and processing logic.
"""

import asyncio
import json
from typing import Optional

from by_framework.common.exceptions import (
    MessageDataNotFoundError,
    MessageParseError,
)
from by_framework.common.logger import logger
from by_framework.core.protocol.commands import (AskAgentCommand, ResumeCommand)


async def parse_message_data(msg_data: dict) -> dict:
    """
    Parse message data from Redis format.

    Args:
        msg_data: Raw message data from Redis

    Returns:
        Parsed data dictionary

    Raises:
        MessageDataNotFoundError: If no data field is found
        MessageParseError: If JSON parsing fails
    """
    # Support both bytes and string keys
    if b"data" in msg_data:
        data_bytes = msg_data[b"data"]
        data_str = data_bytes.decode() if isinstance(data_bytes, bytes) else data_bytes
    elif "data" in msg_data:
        data_str = msg_data["data"]
    else:
        raise MessageDataNotFoundError()

    try:
        return json.loads(data_str)
    except json.JSONDecodeError as err:
        raise MessageParseError(cause=err) from err


def decode_message_id(msg_id_bytes: bytes | str) -> str:
    """Decode message ID from bytes or return as-is."""
    return msg_id_bytes.decode() if isinstance(msg_id_bytes, bytes) else msg_id_bytes


# NOTE: Only used in tests.
async def process_command(
    command: AskAgentCommand | ResumeCommand,
    worker,
    cancel_event: asyncio.Event,
    cancel_reason: str,
    existing_execution: Optional[dict],
    execution_id: str,
    stream_name: str,
    msg_id: str,
    redis_client,
    group_name: str,
    terminal_states: frozenset[str],
) -> str:
    """
    Process a business command (AskAgentCommand or ResumeCommand).

    NOTE: This function is strictly for testing purposes.

    Returns:
        Final status string
    """
    header = command.header

    # Skip if execution already in terminal state
    if existing_execution and existing_execution.get("status") in terminal_states:
        await redis_client.xack(stream_name, group_name, msg_id)
        logger.info(
            "[%s] Skipping terminal execution replay: %s -> %s",
            worker.worker_id,
            header.message_id,
            existing_execution.get("status"),
        )
        return existing_execution.get("status", "")

    # Call worker message handler
    final_status = await worker._handle_message(
        command,
        cancel_event=cancel_event,
        cancel_reason=cancel_reason,
    )

    return final_status
