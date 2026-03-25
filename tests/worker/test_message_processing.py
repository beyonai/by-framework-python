"""
Tests for byclaw_gateway_sdk.worker._message_processing module.
"""

import asyncio
import json
import unittest
from unittest.mock import AsyncMock, Mock

from byclaw_gateway_sdk.common.exceptions import (
    MessageDataNotFoundError,
    MessageParseError,
)
from byclaw_gateway_sdk.core.protocol.commands import (AskAgentCommand, ResumeCommand)
from byclaw_gateway_sdk.core.protocol.message_header import MessageHeader
from byclaw_gateway_sdk.worker._message_processing import (
    decode_message_id,
    inject_history,
    parse_message_data,
    process_command,
)


class TestDecodeMessageId(unittest.TestCase):
    """Tests for decode_message_id function."""

    def test_with_bytes(self):
        """Test decoding bytes message ID."""
        result = decode_message_id(b"12345-0")
        self.assertEqual(result, "12345-0")

    def test_with_string(self):
        """Test with string message ID returns as-is."""
        result = decode_message_id("12345-0")
        self.assertEqual(result, "12345-0")


class TestParseMessageData(unittest.IsolatedAsyncioTestCase):
    """Tests for parse_message_data function."""

    async def test_with_bytes_key(self):
        """Test parsing message with bytes key."""
        msg_data = {b"data": json.dumps({"key": "value"}).encode()}
        result = await parse_message_data(msg_data)
        self.assertEqual(result, {"key": "value"})

    async def test_with_string_key(self):
        """Test parsing message with string key."""
        msg_data = {"data": json.dumps({"key": "value"})}
        result = await parse_message_data(msg_data)
        self.assertEqual(result, {"key": "value"})

    async def test_missing_data_key_raises(self):
        """Test that missing data key raises MessageDataNotFoundError."""
        msg_data = {"other": "data"}
        with self.assertRaises(MessageDataNotFoundError):
            await parse_message_data(msg_data)

    async def test_invalid_json_raises(self):
        """Test that invalid JSON raises MessageParseError."""
        msg_data = {"data": "not valid json {{{"}
        with self.assertRaises(MessageParseError) as ctx:
            await parse_message_data(msg_data)
        self.assertIsInstance(ctx.exception.cause, json.JSONDecodeError)


class MockHistoryProvider:
    """Mock HistoryProvider for testing."""

    history_data = [
        {"role": "user", "content": "Hello"},
        {"role": "assistant", "content": "Hi there!"},
    ]

    @classmethod
    async def get_session_history(cls, session_id: str):
        return cls.history_data


class TestInjectHistory(unittest.IsolatedAsyncioTestCase):
    """Tests for inject_history function."""

    async def test_injects_history_into_command(self):
        """Test that history is injected into command extra_payload."""
        # Patch HistoryProvider
        import byclaw_gateway_sdk.worker._message_processing as mp

        original = mp.HistoryProvider
        mp.HistoryProvider = MockHistoryProvider

        try:
            command = AskAgentCommand(
                header=MessageHeader(
                    message_id="msg-1",
                    session_id="sess-123",
                    trace_id="trace-1",
                    target_agent_type="test_agent",
                ),
                content="Hello",
            )

            # History should not be in extra_payload initially
            self.assertNotIn("history", command.extra_payload)

            await inject_history(command)

            # Now history should be injected
            self.assertIn("history", command.extra_payload)
            self.assertEqual(
                command.extra_payload["history"], MockHistoryProvider.history_data
            )
        finally:
            mp.HistoryProvider = original


class MockRedis:
    """Mock Redis client for testing."""

    def __init__(self):
        self.acked = False
        self.ack_stream = None
        self.ack_group = None
        self.ack_ids = None

    async def xack(self, stream: str, group: str, *ids):
        self.acked = True
        self.ack_stream = stream
        self.ack_group = group
        self.ack_ids = ids


class TestProcessCommand(unittest.IsolatedAsyncioTestCase):
    """Tests for process_command function."""

    async def test_skips_terminal_state_execution(self):
        """Test that terminal state executions are skipped and acked."""
        mock_redis = MockRedis()
        mock_worker = Mock()
        mock_worker.worker_id = "worker-1"

        existing_execution = {"status": "COMPLETED"}

        command = AskAgentCommand(
            header=MessageHeader(
                message_id="msg-1",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="test_agent",
            ),
            content="Hello",
        )

        result = await process_command(
            command=command,
            worker=mock_worker,
            cancel_event=asyncio.Event(),
            cancel_reason="",
            existing_execution=existing_execution,
            execution_id="exec-1",
            stream_name="test_stream",
            msg_id="1-0",
            redis_client=mock_redis,
            group_name="test_group",
            terminal_states=frozenset({"COMPLETED", "FAILED", "CANCELLED"}),
        )

        # Should return the terminal status
        self.assertEqual(result, "COMPLETED")

        # Should have acked the message
        self.assertTrue(mock_redis.acked)

        # Worker handler should NOT have been called
        mock_worker._handle_message.assert_not_called()

    async def test_processes_normal_execution(self):
        """Test that normal executions are processed."""
        mock_redis = MockRedis()
        mock_worker = Mock()
        mock_worker.worker_id = "worker-1"
        mock_worker._handle_message = AsyncMock(return_value="COMPLETED")

        command = AskAgentCommand(
            header=MessageHeader(
                message_id="msg-1",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="test_agent",
            ),
            content="Hello",
        )

        # Patch HistoryProvider to avoid real DB calls
        import byclaw_gateway_sdk.worker._message_processing as mp

        original = mp.HistoryProvider
        mp.HistoryProvider = MockHistoryProvider

        try:
            result = await process_command(
                command=command,
                worker=mock_worker,
                cancel_event=asyncio.Event(),
                cancel_reason="",
                existing_execution=None,
                execution_id="exec-1",
                stream_name="test_stream",
                msg_id="1-0",
                redis_client=mock_redis,
                group_name="test_group",
                terminal_states=frozenset({"COMPLETED", "FAILED", "CANCELLED"}),
            )

            self.assertEqual(result, "COMPLETED")
            mock_worker._handle_message.assert_awaited_once()
        finally:
            mp.HistoryProvider = original

    async def test_resume_command_processed(self):
        """Test that ResumeCommand is processed without history injection."""
        mock_redis = MockRedis()
        mock_worker = Mock()
        mock_worker.worker_id = "worker-1"
        mock_worker._handle_message = AsyncMock(return_value="RESUMED")

        command = ResumeCommand(
            header=MessageHeader(
                message_id="msg-1",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="test_agent",
            ),
            content="Continuing",
            status="RESUMED",
        )

        # Patch HistoryProvider to detect if it's called
        class TrackingHistoryProvider:
            called = False

            @classmethod
            async def get_session_history(cls, session_id):
                cls.called = True
                return []

        import byclaw_gateway_sdk.worker._message_processing as mp

        original = mp.HistoryProvider
        mp.HistoryProvider = TrackingHistoryProvider

        try:
            result = await process_command(
                command=command,
                worker=mock_worker,
                cancel_event=asyncio.Event(),
                cancel_reason="",
                existing_execution=None,
                execution_id="exec-1",
                stream_name="test_stream",
                msg_id="1-0",
                redis_client=mock_redis,
                group_name="test_group",
                terminal_states=frozenset({"COMPLETED", "FAILED", "CANCELLED"}),
            )

            self.assertEqual(result, "RESUMED")
            mock_worker._handle_message.assert_awaited_once()

            # ResumeCommand should NOT call get_session_history (no history injection)
            self.assertFalse(TrackingHistoryProvider.called)
        finally:
            mp.HistoryProvider = original


if __name__ == "__main__":
    unittest.main()
