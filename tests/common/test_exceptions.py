"""
Tests for byclaw_gateway_sdk.common.exceptions module.
"""

import json
import unittest

from byclaw_gateway_sdk.common.exceptions import (
    CommandValidationError,
    ExecutionDataError,
    ExecutionNotFoundError,
    GatewaySDKError,
    MessageDataNotFoundError,
    MessageParseError,
    RedisConnectionError,
    SessionMismatchError,
    StreamGroupExistsError,
    TerminalStateError,
    UnsupportedCommandError,
    WorkerLockError,
    WorkerNotFoundError,
    WorkerRegistryNotSetError,
)


class TestGatewaySDKError(unittest.TestCase):
    """Tests for base GatewaySDKError class."""

    def test_error_with_message(self):
        err = GatewaySDKError("Test error")
        self.assertEqual(str(err), "Test error")
        self.assertIsNone(err.cause)

    def test_error_with_cause(self):
        original = ValueError("original error")
        err = GatewaySDKError("Test error", cause=original)
        self.assertEqual(str(err), "Test error")
        self.assertIs(err.cause, original)


class TestRedisConnectionError(unittest.TestCase):
    """Tests for RedisConnectionError."""

    def test_default_message(self):
        err = RedisConnectionError()
        self.assertEqual(str(err), "Failed to connect to Redis")

    def test_custom_message(self):
        err = RedisConnectionError("Connection refused")
        self.assertEqual(str(err), "Connection refused")

    def test_with_cause(self):
        original = OSError("Network unreachable")
        err = RedisConnectionError(cause=original)
        self.assertIn("Failed to connect to Redis", str(err))
        self.assertIs(err.cause, original)


class TestStreamGroupExistsError(unittest.TestCase):
    """Tests for StreamGroupExistsError."""

    def test_error_message(self):
        err = StreamGroupExistsError("my_group", "my_stream")
        self.assertIn("my_group", str(err))
        self.assertIn("my_stream", str(err))
        self.assertEqual(err.group_name, "my_group")
        self.assertEqual(err.stream_name, "my_stream")


class TestExecutionNotFoundError(unittest.TestCase):
    """Tests for ExecutionNotFoundError."""

    def test_with_execution_id_only(self):
        err = ExecutionNotFoundError("exec-123")
        self.assertIn("exec-123", str(err))
        self.assertEqual(err.execution_id, "exec-123")
        self.assertEqual(err.session_id, "")

    def test_with_session_id(self):
        err = ExecutionNotFoundError("exec-123", "sess-456")
        self.assertIn("exec-123", str(err))
        self.assertIn("sess-456", str(err))


class TestExecutionDataError(unittest.TestCase):
    """Tests for ExecutionDataError."""

    def test_with_cause(self):
        original = ValueError("invalid json")
        err = ExecutionDataError("exec-123", cause=original)
        self.assertIn("exec-123", str(err))
        self.assertIs(err.cause, original)


class TestSessionMismatchError(unittest.TestCase):
    """Tests for SessionMismatchError."""

    def test_error_message(self):
        err = SessionMismatchError("msg-1", "sess-expected", "sess-actual")
        msg = str(err)
        self.assertIn("msg-1", msg)
        self.assertIn("sess-expected", msg)
        self.assertIn("sess-actual", msg)
        self.assertEqual(err.message_id, "msg-1")
        self.assertEqual(err.expected_session, "sess-expected")
        self.assertEqual(err.actual_session, "sess-actual")


class TestTerminalStateError(unittest.TestCase):
    """Tests for TerminalStateError."""

    def test_error_message(self):
        err = TerminalStateError("exec-123", "COMPLETED")
        self.assertIn("exec-123", str(err))
        self.assertIn("COMPLETED", str(err))
        self.assertEqual(err.execution_id, "exec-123")
        self.assertEqual(err.current_status, "COMPLETED")


class TestUnsupportedCommandError(unittest.TestCase):
    """Tests for UnsupportedCommandError."""

    def test_error_message(self):
        err = UnsupportedCommandError("CustomCommand")
        self.assertIn("CustomCommand", str(err))
        self.assertEqual(err.command_type, "CustomCommand")


class TestMessageParseError(unittest.TestCase):
    """Tests for MessageParseError."""

    def test_without_message_id(self):
        err = MessageParseError()
        self.assertIn("Failed to parse message", str(err))

    def test_with_message_id(self):
        err = MessageParseError("msg-123")
        self.assertIn("msg-123", str(err))

    def test_with_cause(self):
        original = json.JSONDecodeError("Expecting value", "", 0)
        err = MessageParseError("msg-123", cause=original)
        self.assertIn("msg-123", str(err))


class TestMessageDataNotFoundError(unittest.TestCase):
    """Tests for MessageDataNotFoundError."""

    def test_without_message_id(self):
        err = MessageDataNotFoundError()
        self.assertIn("Message data not found", str(err))

    def test_with_message_id(self):
        err = MessageDataNotFoundError("msg-123")
        self.assertIn("msg-123", str(err))
        self.assertEqual(err.message_id, "msg-123")


class TestWorkerNotFoundError(unittest.TestCase):
    """Tests for WorkerNotFoundError."""

    def test_error_message(self):
        err = WorkerNotFoundError("my_agent")
        self.assertIn("my_agent", str(err))
        self.assertEqual(err.agent_type, "my_agent")


class TestWorkerLockError(unittest.TestCase):
    """Tests for WorkerLockError."""

    def test_error_message(self):
        err = WorkerLockError("worker-1")
        self.assertIn("worker-1", str(err))
        self.assertEqual(err.worker_id, "worker-1")


class TestWorkerRegistryNotSetError(unittest.TestCase):
    """Tests for WorkerRegistryNotSetError."""

    def test_error_message(self):
        err = WorkerRegistryNotSetError("send messages")
        self.assertIn("send messages", str(err))
        self.assertEqual(err.operation, "send messages")


class TestCommandValidationError(unittest.TestCase):
    """Tests for CommandValidationError."""

    def test_error_message(self):
        err = CommandValidationError("CancelTaskCommand", "missing target_message_id")
        msg = str(err)
        self.assertIn("CancelTaskCommand", msg)
        self.assertIn("missing target_message_id", msg)
        self.assertEqual(err.command_type, "CancelTaskCommand")
        self.assertEqual(err.reason, "missing target_message_id")


if __name__ == "__main__":
    unittest.main()
