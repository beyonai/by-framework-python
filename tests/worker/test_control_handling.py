"""
Tests for by_framework.worker._control_handling module.
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, Mock

from by_framework.common.exceptions import UnsupportedCommandError
from by_framework.core.protocol.commands import CancelTaskCommand
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.worker._control_handling import (
    handle_cancel_task,
    parse_control_command,
)


class TestParseControlCommand(unittest.IsolatedAsyncioTestCase):
    """Tests for parse_control_command function."""

    async def test_valid_cancel_task_command(self):
        """Test parsing a valid CancelTaskCommand."""
        data = {
            "action_type": "CANCEL_TASK",
            "header": {
                "message_id": "ctl-1",
                "session_id": "sess-1",
                "trace_id": "trace-1",
                "target_agent_type": "test_agent",
                "source_agent_id": "",
                "parent_message_id": "",
                "tenant_id": "",
                "metadata": {},
            },
            "body": {
                "target_message_id": "msg-1",
                "target_execution_id": "exec-1",
                "reason": "user request",
                "cancel_mode": "graceful",
            },
        }

        result = await parse_control_command(data)

        self.assertIsInstance(result, CancelTaskCommand)
        self.assertEqual(result.header.message_id, "ctl-1")
        self.assertEqual(result.target_message_id, "msg-1")
        self.assertEqual(result.target_execution_id, "exec-1")
        self.assertEqual(result.reason, "user request")

    async def test_unsupported_command_raises_error(self):
        """Test that an unregistered action_type raises UnsupportedCommandError."""
        data = {
            "action_type": "UNSUPPORTED_FAKE_COMMAND",
            "header": {
                "message_id": "msg-1",
                "session_id": "sess-1",
                "trace_id": "trace-1",
                "target_agent_type": "test_agent",
                "source_agent_id": "",
                "parent_message_id": "",
                "tenant_id": "",
                "metadata": {},
            },
            "body": {},
        }

        with self.assertRaises(UnsupportedCommandError) as ctx:
            await parse_control_command(data)

        self.assertIn("UNSUPPORTED_FAKE_COMMAND", str(ctx.exception))

    async def test_ask_agent_command_is_accepted(self):
        """Test that AskAgentCommand is now accepted on the control stream."""
        data = {
            "action_type": "ASK_AGENT",
            "header": {
                "message_id": "msg-1",
                "session_id": "sess-1",
                "trace_id": "trace-1",
                "target_agent_type": "test_agent",
                "source_agent_id": "",
                "parent_message_id": "",
                "tenant_id": "",
                "metadata": {},
            },
            "body": {
                "content": "Hello",
            },
        }

        command = await parse_control_command(data)
        self.assertEqual(command.header.message_id, "msg-1")
        self.assertEqual(command.content, "Hello")


class MockExecution:
    """Mock execution object for testing."""

    def __init__(self):
        self.cancel_reason = ""
        self.cancel_event = asyncio.Event()
        self.session_id = "sess-1"
        self.context = None
        self.task = Mock()  # Mock task with cancel method


class TestHandleCancelTask(unittest.IsolatedAsyncioTestCase):
    """Tests for handle_cancel_task function."""

    async def test_cancel_existing_execution(self):
        """Test cancelling an existing execution."""
        mock_execution = MockExecution()
        active_executions = {"exec-1": mock_execution}
        message_to_execution = {"msg-1": "exec-1"}

        mock_registry = AsyncMock()
        mock_worker = Mock()
        mock_worker.registry = mock_registry
        mock_worker.plugin_registry = None
        mock_worker.on_cancel_task = AsyncMock()

        mock_redis = AsyncMock()

        command = CancelTaskCommand(
            header=MessageHeader(
                message_id="ctl-1",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="test_agent",
            ),
            target_message_id="msg-1",
            target_execution_id="exec-1",
            reason="user cancelled",
        )

        await handle_cancel_task(
            command=command,
            active_executions=active_executions,
            message_to_execution=message_to_execution,
            redis_client=mock_redis,
            group_name="test_group",
            worker=mock_worker,
        )

        # Yield control to let asyncio.create_task callbacks execute
        await asyncio.sleep(0)

        # Verify execution was cancelled
        self.assertEqual(mock_execution.cancel_reason, "user cancelled")
        self.assertTrue(mock_execution.cancel_event.is_set())

        # Verify registry was called
        mock_registry.mark_execution_cancelling.assert_awaited_once()

        # Verify worker.on_cancel_task was called
        mock_worker.on_cancel_task.assert_awaited_once()

    async def test_cancel_with_only_message_id(self):
        """Test cancelling when only message_id is available (no execution_id)."""
        mock_execution = MockExecution()
        active_executions = {"exec-1": mock_execution}
        message_to_execution = {"msg-1": "exec-1"}

        mock_registry = AsyncMock()
        mock_worker = Mock()
        mock_worker.registry = mock_registry
        mock_worker.plugin_registry = None
        mock_worker.on_cancel_task = AsyncMock()

        mock_redis = AsyncMock()

        command = CancelTaskCommand(
            header=MessageHeader(
                message_id="ctl-1",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="test_agent",
            ),
            target_message_id="msg-1",
            # No target_execution_id - should look up via message_to_execution
            reason="user cancelled",
        )

        await handle_cancel_task(
            command=command,
            active_executions=active_executions,
            message_to_execution=message_to_execution,
            redis_client=mock_redis,
            group_name="test_group",
            worker=mock_worker,
        )

        self.assertEqual(mock_execution.cancel_reason, "user cancelled")
        self.assertTrue(mock_execution.cancel_event.is_set())

    async def test_cancel_nonexistent_execution(self):
        """Test cancelling when execution doesn't exist in active_executions."""
        active_executions = {}  # Empty - execution not found
        message_to_execution = {}

        mock_registry = AsyncMock()
        mock_worker = Mock()
        mock_worker.registry = mock_registry
        mock_worker.plugin_registry = None
        mock_worker.on_cancel_task = AsyncMock()

        mock_redis = AsyncMock()

        command = CancelTaskCommand(
            header=MessageHeader(
                message_id="ctl-1",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="test_agent",
            ),
            target_message_id="msg-nonexistent",
            target_execution_id="exec-nonexistent",
            reason="user cancelled",
        )

        # Should not raise - just no-ops for non-existent execution
        await handle_cancel_task(
            command=command,
            active_executions=active_executions,
            message_to_execution=message_to_execution,
            redis_client=mock_redis,
            group_name="test_group",
            worker=mock_worker,
        )

        # Registry should still be called (mark_execution_cancelling)
        mock_registry.mark_execution_cancelling.assert_awaited_once()

    async def test_cancel_with_plugin_context(self):
        """Test cancelling execution with plugin context."""
        mock_execution = MockExecution()
        mock_execution.context = AsyncMock()  # Has plugin context
        active_executions = {"exec-1": mock_execution}
        message_to_execution = {"msg-1": "exec-1"}

        mock_registry = AsyncMock()
        mock_plugin_registry = AsyncMock()
        mock_worker = Mock()
        mock_worker.registry = mock_registry
        mock_worker.plugin_registry = mock_plugin_registry
        mock_worker.on_cancel_task = AsyncMock()

        mock_redis = AsyncMock()

        command = CancelTaskCommand(
            header=MessageHeader(
                message_id="ctl-1",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="test_agent",
            ),
            target_message_id="msg-1",
            target_execution_id="exec-1",
            reason="user cancelled",
        )

        await handle_cancel_task(
            command=command,
            active_executions=active_executions,
            message_to_execution=message_to_execution,
            redis_client=mock_redis,
            group_name="test_group",
            worker=mock_worker,
        )

        # Yield control to let asyncio.create_task callbacks execute
        await asyncio.sleep(0)

        # Both plugin hook and worker hook should be called
        mock_plugin_registry.on_task_cancel.assert_awaited_once()
        mock_worker.on_cancel_task.assert_awaited_once()

    async def test_cancel_without_registry(self):
        """Test cancelling when worker has no registry."""
        mock_execution = MockExecution()
        active_executions = {"exec-1": mock_execution}
        message_to_execution = {"msg-1": "exec-1"}

        mock_worker = Mock()
        mock_worker.registry = None  # No registry
        mock_worker.plugin_registry = None
        mock_worker.on_cancel_task = AsyncMock()

        mock_redis = AsyncMock()

        command = CancelTaskCommand(
            header=MessageHeader(
                message_id="ctl-1",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="test_agent",
            ),
            target_message_id="msg-1",
            target_execution_id="exec-1",
            reason="user cancelled",
        )

        # Should not raise
        await handle_cancel_task(
            command=command,
            active_executions=active_executions,
            message_to_execution=message_to_execution,
            redis_client=mock_redis,
            group_name="test_group",
            worker=mock_worker,
        )

        # Execution should still be cancelled
        self.assertTrue(mock_execution.cancel_event.is_set())


if __name__ == "__main__":
    unittest.main()
