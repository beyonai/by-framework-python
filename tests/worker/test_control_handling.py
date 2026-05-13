"""
Tests for by_framework.worker._control_handling module.
"""

import asyncio
import json
import unittest
from unittest.mock import AsyncMock, Mock

from by_framework.common.constants import RedisKeys
from by_framework.core.protocol.commands import (
    CancelTaskCommand,
    ReloadPluginsCommand,
)
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.errors import UnsupportedCommandError
from by_framework.worker._control_handling import (
    handle_cancel_task,
    handle_reload_plugins,
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
                "source_agent_type": "",
                "parent_message_id": "",
                "task_group_id": "",
                "user_code": "",
                "user_name": "",
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
                "source_agent_type": "",
                "parent_message_id": "",
                "task_group_id": "",
                "user_code": "",
                "user_name": "",
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
                "source_agent_type": "",
                "parent_message_id": "",
                "task_group_id": "",
                "user_code": "",
                "user_name": "",
                "metadata": {},
            },
            "body": {
                "content": "Hello",
            },
        }

        command = await parse_control_command(data)
        self.assertEqual(command.header.message_id, "msg-1")
        self.assertEqual(command.content, "Hello")

    async def test_reload_plugins_command_is_accepted(self):
        """Test that ReloadPluginsCommand is accepted on the control stream."""
        data = {
            "action_type": "RELOAD_PLUGINS",
            "header": {
                "message_id": "ctl-reload-1",
                "session_id": "sess-1",
                "trace_id": "trace-1",
                "target_agent_type": "test_agent",
                "source_agent_type": "",
                "parent_message_id": "",
                "task_group_id": "",
                "user_code": "",
                "user_name": "",
                "metadata": {},
            },
            "body": {
                "reload_id": "reload-1",
                "reason": "refresh configs",
            },
        }

        command = await parse_control_command(data)
        self.assertIsInstance(command, ReloadPluginsCommand)
        self.assertEqual(command.reload_id, "reload-1")
        self.assertEqual(command.reason, "refresh configs")


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


class TestHandleReloadPlugins(unittest.IsolatedAsyncioTestCase):
    """Tests for reload control handling."""

    async def test_reload_plugins_without_registry_raises(self):
        mock_worker = Mock()
        mock_worker.plugin_registry = None

        command = ReloadPluginsCommand(
            header=MessageHeader(
                message_id="ctl-reload-2",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="test_agent",
            ),
            reload_id="reload-2",
            reason="reload",
        )

        with self.assertRaises(UnsupportedCommandError):
            await handle_reload_plugins(command, mock_worker)

    async def test_reload_plugins_duplicate_command_calls_registry_once(self):
        mock_worker = Mock()
        mock_worker.plugin_registry = AsyncMock()
        mock_worker.plugin_registry.reload_plugins = AsyncMock()

        command = ReloadPluginsCommand(
            header=MessageHeader(
                message_id="ctl-reload-3",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="test_agent",
            ),
            reload_id="reload-3",
            reason="reload",
        )

        await handle_reload_plugins(command, mock_worker)

        mock_worker.plugin_registry.reload_plugins.assert_awaited_once_with(
            reload_id="reload-3",
            reason="reload",
        )

    async def test_reload_plugins_publishes_success_ack(self):
        mock_worker = Mock()
        mock_worker.worker_id = "worker-1"
        mock_worker.redis = AsyncMock()
        mock_worker.redis.xadd = AsyncMock()
        mock_worker.plugin_registry = AsyncMock()
        mock_worker.plugin_registry.agent_configs_version = 2
        mock_worker.plugin_registry.reload_plugins = AsyncMock()
        mock_worker.plugin_registry.get_reload_status.return_value = {
            "status": "success",
            "reason": "reload",
            "version_before": 1,
            "version_after": 2,
            "error": "",
        }

        command = ReloadPluginsCommand(
            header=MessageHeader(
                message_id="ctl-reload-4",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="test_agent",
            ),
            reload_id="reload-4",
            reason="reload",
        )

        await handle_reload_plugins(command, mock_worker)

        mock_worker.redis.xadd.assert_awaited_once()
        stream_name, payload = mock_worker.redis.xadd.await_args.args
        self.assertEqual(stream_name, RedisKeys.plugin_reload_ack_stream("reload-4"))
        body = json.loads(payload["data"])
        self.assertEqual(body["worker_id"], "worker-1")
        self.assertEqual(body["status"], "success")
        self.assertEqual(body["version_before"], 1)
        self.assertEqual(body["version_after"], 2)

    async def test_reload_plugins_publishes_failure_ack_and_reraises(self):
        mock_worker = Mock()
        mock_worker.worker_id = "worker-1"
        mock_worker.redis = AsyncMock()
        mock_worker.redis.xadd = AsyncMock()
        mock_worker.plugin_registry = AsyncMock()
        mock_worker.plugin_registry.agent_configs_version = 1
        mock_worker.plugin_registry.reload_plugins = AsyncMock(
            side_effect=RuntimeError("reload boom")
        )
        mock_worker.plugin_registry.get_reload_status.return_value = {
            "status": "failure",
            "reason": "reload",
            "version_before": 1,
            "version_after": 1,
            "error": "reload boom",
        }

        command = ReloadPluginsCommand(
            header=MessageHeader(
                message_id="ctl-reload-5",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="test_agent",
            ),
            reload_id="reload-5",
            reason="reload",
        )

        with self.assertRaisesRegex(RuntimeError, "reload boom"):
            await handle_reload_plugins(command, mock_worker)

        mock_worker.redis.xadd.assert_awaited_once()
        stream_name, payload = mock_worker.redis.xadd.await_args.args
        self.assertEqual(stream_name, RedisKeys.plugin_reload_ack_stream("reload-5"))
        body = json.loads(payload["data"])
        self.assertEqual(body["worker_id"], "worker-1")
        self.assertEqual(body["status"], "failure")
        self.assertEqual(body["error"], "reload boom")


if __name__ == "__main__":
    unittest.main()
