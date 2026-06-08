import asyncio
import json
import unittest
from typing import Any
from unittest.mock import ANY, AsyncMock, Mock, patch

from by_framework import (
    AgentTaskResult,
    GatewayWorker,
    RedisKeys,
    RunningExecution,
    WorkerRunner,
)
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.protocol.commands import (
    AskAgentCommand,
    CancelTaskCommand,
    ReloadPluginsCommand,
)
from by_framework.core.protocol.message_header import MessageHeader


class MockRedisRunner:

    def __init__(self, message_to_return):
        self.msg = message_to_return
        self.called_xreadgroup = False
        self.acked = False
        self.ack_calls = []
        self.group_create_calls = []

    async def xgroup_create(self, name, groupname, id="0", mkstream=False):
        self.group_create_calls.append((name, groupname, id, mkstream))

    async def xreadgroup(self, groupname, consumername, streams, count=1, block=0):
        self.called_xreadgroup = True
        if self.msg:
            res = self.msg
            self.msg = None  # only return once
            return res
        return []

    async def xack(self, name, groupname, *ids):
        self.acked = True
        self.ack_calls.append((name, groupname, ids))


class DummyWorker(GatewayWorker):

    def __init__(self):
        super().__init__("worker-1", None, None, None)
        self.processed = False

    def get_agent_types(self) -> list[str]:
        return ["dummy_agent"]

    async def process_command(self, command: Any, context: Any) -> None:
        self.processed = True

    async def _handle_message(self, command, **kwargs):
        await self.process_command(command, None)
        return AgentTaskResult(status=AgentState.COMPLETED.value)


class ExecutionInspectWorker(DummyWorker):

    def __init__(self):
        super().__init__()
        self.seen_execution = None

    async def _handle_message(self, command, **kwargs):
        self.seen_execution = kwargs.get("execution")
        return AgentTaskResult(status=AgentState.COMPLETED.value)


class MultiCapWorker(GatewayWorker):

    def __init__(self):
        super().__init__("worker-multi", None, None, None)

    def get_agent_types(self) -> list[str]:
        return ["agent-b", "agent-a"]

    async def process_command(self, command: Any, context: Any) -> None:
        return None


class DuplicateIdRegistry:

    async def claim_worker_id(self, worker_id: str):
        raise ValueError(f"worker_id already in use: {worker_id}")


class TestWorkerRunner(unittest.IsolatedAsyncioTestCase):

    async def test_runner_pull_and_dispatch(self):
        """Test that WorkerRunner pulls a message and dispatches it to the worker."""
        mock_msg = AskAgentCommand(
            header=MessageHeader(
                message_id="msg-1",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="dummy_agent",
            ),
            content="test",
        )

        mock_redis_data = [
            [
                RedisKeys.ctrl_stream("dummy_agent").encode(),
                [
                    (
                        b"1600000000000-0",
                        {b"data": json.dumps(mock_msg.to_dict()).encode()},
                    )
                ],
            ]
        ]

        redis_mock = MockRedisRunner(message_to_return=mock_redis_data)
        worker = DummyWorker()

        runner = WorkerRunner(
            redis_client=redis_mock, worker=worker, group_name="test_group"
        )

        # Run one single iteration manually
        await runner._run_once()
        await runner.wait_for_tasks()

        self.assertTrue(redis_mock.called_xreadgroup)
        self.assertTrue(redis_mock.acked)
        self.assertTrue(worker.processed)

    def test_runner_auto_group_name_when_not_specified(self):
        """Test that WorkerRunner generates a deterministic auto
        group name when not specified."""
        worker = MultiCapWorker()
        redis_mock = MockRedisRunner(message_to_return=[])

        runner = WorkerRunner(redis_client=redis_mock, worker=worker, group_name=None)
        self.assertTrue(runner.group_name.startswith(f"{RedisKeys.CG_AGENT_ENGINES}:"))

        runner2 = WorkerRunner(redis_client=redis_mock, worker=worker, group_name=None)
        self.assertEqual(runner.group_name, runner2.group_name)

    async def test_runner_rejects_duplicate_worker_id_on_start(self):
        """Test that WorkerRunner raises ValueError when
        worker_id is already claimed."""
        worker = MultiCapWorker()
        worker.registry = DuplicateIdRegistry()
        redis_mock = MockRedisRunner(message_to_return=[])
        runner = WorkerRunner(
            redis_client=redis_mock, worker=worker, group_name="test_group"
        )

        with self.assertRaisesRegex(ValueError, "worker_id already in use"):
            await runner.start()

    async def test_runner_shutdown_releases_presence_and_unregisters_membership(
        self,
    ):
        """Test graceful shutdown removes owned presence and membership."""
        worker = DummyWorker()
        worker.registry = AsyncMock()
        worker.registry.release_worker_id.return_value = True
        worker.stop_heartbeat = AsyncMock()
        redis_mock = MockRedisRunner(message_to_return=[])
        runner = WorkerRunner(
            redis_client=redis_mock, worker=worker, group_name="test_group"
        )
        runner._lock_token = "lock-token"

        await runner._shutdown()

        worker.stop_heartbeat.assert_awaited_once()
        worker.registry.release_worker_id.assert_awaited_once_with(
            "worker-1", "lock-token"
        )
        worker.registry.mark_worker_inactive.assert_not_awaited()
        worker.registry.unregister_worker_membership.assert_awaited_once_with(
            "worker-1"
        )

    async def test_runner_shutdown_keeps_membership_when_presence_owned_elsewhere(
        self,
    ):
        """Test stale shutdown does not remove another owner's membership."""
        worker = DummyWorker()
        worker.registry = AsyncMock()
        worker.registry.release_worker_id.return_value = False
        worker.stop_heartbeat = AsyncMock()
        redis_mock = MockRedisRunner(message_to_return=[])
        runner = WorkerRunner(
            redis_client=redis_mock, worker=worker, group_name="test_group"
        )
        runner._lock_token = "stale-token"

        await runner._shutdown()

        worker.registry.release_worker_id.assert_awaited_once_with(
            "worker-1", "stale-token"
        )
        worker.registry.unregister_worker_membership.assert_not_awaited()

    async def test_runner_registers_execution_and_acks_processed_message(self):
        """Test that _process_message_from_dict registers execution
        and acks the message."""
        redis_mock = MockRedisRunner(message_to_return=[])
        worker = DummyWorker()
        worker.registry = AsyncMock()
        worker.registry.get_execution_by_message_id.return_value = None

        runner = WorkerRunner(
            redis_client=redis_mock, worker=worker, group_name="test_group"
        )
        payload = AskAgentCommand(
            header=MessageHeader(
                message_id="msg-registered",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="dummy_agent",
            ),
            content="test",
        ).to_dict()

        await runner._process_message_from_dict(
            RedisKeys.ctrl_stream("dummy_agent"), "1-0", payload
        )

        self.assertTrue(worker.registry.save_execution.await_count == 1)
        worker.registry.mark_execution_finished.assert_awaited()
        self.assertTrue(redis_mock.acked)

    async def test_runner_records_worker_execute_span(self):
        """Processed worker commands write an execution span for trace drilldown."""
        redis_mock = MockRedisRunner(message_to_return=[])
        worker = DummyWorker()
        worker.registry = AsyncMock()
        worker.registry.get_execution_by_message_id.return_value = {
            "execution_id": "exec-worker",
            "message_id": "msg-worker",
            "session_id": "sess-1",
            "trace_id": "trace-worker",
            "parent_message_id": "parent-msg",
            "target_agent_type": "dummy_agent",
            "created_at": 100,
        }
        span_recorder = AsyncMock()

        runner = WorkerRunner(
            redis_client=redis_mock,
            worker=worker,
            group_name="test_group",
            span_recorder=span_recorder,
        )
        payload = AskAgentCommand(
            header=MessageHeader(
                message_id="msg-worker",
                session_id="sess-1",
                trace_id="trace-worker",
                target_agent_type="dummy_agent",
                parent_message_id="parent-msg",
            ),
            content="test",
        ).to_dict()

        await runner._process_message_from_dict(
            RedisKeys.ctrl_stream("dummy_agent"), "1-0", payload
        )

        span_recorder.record_span.assert_awaited_once()
        span = span_recorder.record_span.await_args.args[0]
        self.assertEqual(span.trace_id, "trace-worker")
        self.assertEqual(span.span_id, "exec-worker:worker.execute")
        self.assertEqual(span.parent_span_id, "msg-worker:client.dispatch")
        self.assertEqual(span.operation, "worker.execute")
        self.assertEqual(span.component, "worker")
        self.assertEqual(span.session_id, "sess-1")
        self.assertEqual(span.execution_id, "exec-worker")
        self.assertEqual(span.message_id, "msg-worker")
        self.assertEqual(span.parent_message_id, "parent-msg")
        self.assertEqual(span.worker_id, "worker-1")
        self.assertEqual(span.target_agent_type, "dummy_agent")
        self.assertEqual(span.status, AgentState.COMPLETED.value)

    async def test_runner_uses_propagated_trace_parent_for_worker_execute_span(self):
        """worker.execute should attach to the header-propagated client span id."""
        redis_mock = MockRedisRunner(message_to_return=[])
        worker = DummyWorker()
        worker.registry = AsyncMock()
        worker.registry.get_execution_by_message_id.return_value = {
            "execution_id": "exec-worker",
            "message_id": "msg-worker",
            "session_id": "sess-1",
            "trace_id": "trace-worker",
            "parent_message_id": "",
            "target_agent_type": "dummy_agent",
            "created_at": 100,
        }
        span_recorder = AsyncMock()

        runner = WorkerRunner(
            redis_client=redis_mock,
            worker=worker,
            group_name="test_group",
            span_recorder=span_recorder,
        )
        payload = AskAgentCommand(
            header=MessageHeader(
                message_id="msg-worker",
                session_id="sess-1",
                trace_id="trace-worker",
                target_agent_type="dummy_agent",
                trace_parent_span_id="0123456789abcdef",
            ),
            content="test",
        ).to_dict()

        with patch("by_framework.worker.runner.live_execution_otel_span") as live_span:
            execute_span = Mock()
            live_span.return_value.__aenter__ = AsyncMock(return_value=execute_span)
            live_span.return_value.__aexit__ = AsyncMock(return_value=None)
            await runner._process_message_from_dict(
                RedisKeys.ctrl_stream("dummy_agent"), "1-0", payload
            )

        live_span.assert_called_once()
        self.assertEqual(
            live_span.call_args.kwargs["parent_span_id"], "0123456789abcdef"
        )
        span = span_recorder.record_span.await_args.args[0]
        self.assertEqual(span.parent_span_id, "0123456789abcdef")

    async def test_runner_persists_structured_failure_details(self):
        """Test terminal execution updates include structured failure fields."""
        redis_mock = MockRedisRunner(message_to_return=[])
        worker = DummyWorker()
        worker.registry = AsyncMock()
        worker.registry.get_execution_by_message_id.return_value = None
        worker._handle_message = AsyncMock(
            return_value=AgentTaskResult(
                status=AgentState.FAILED.value,
                reply_data={"error": "boom"},
                metadata={
                    "error_type": "RuntimeError",
                    "error_message": "boom",
                    "error_code": "E_BOOM",
                    "failed_stage": "process_command",
                    "retryable": False,
                },
            )
        )

        runner = WorkerRunner(
            redis_client=redis_mock, worker=worker, group_name="test_group"
        )
        payload = AskAgentCommand(
            header=MessageHeader(
                message_id="msg-failed",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="dummy_agent",
            ),
            content="test",
        ).to_dict()

        await runner._process_message_from_dict(
            RedisKeys.ctrl_stream("dummy_agent"), "1-0", payload
        )

        worker.registry.mark_execution_finished.assert_awaited_once_with(
            ANY,
            "sess-1",
            AgentState.FAILED.value,
            {
                "error_type": "RuntimeError",
                "error_message": "boom",
                "error_code": "E_BOOM",
                "failed_stage": "process_command",
                "retryable": False,
            },
        )

    async def test_runner_treats_existing_queued_execution_as_new_request(self):
        """Test sender-created QUEUED executions are not treated as resumes."""
        redis_mock = MockRedisRunner(message_to_return=[])
        worker = ExecutionInspectWorker()
        worker.registry = AsyncMock()
        worker.registry.get_execution_by_message_id.return_value = {
            "execution_id": "exec-queued",
            "message_id": "msg-queued",
            "session_id": "sess-1",
            "parent_message_id": "",
            "status": "QUEUED",
        }

        runner = WorkerRunner(
            redis_client=redis_mock, worker=worker, group_name="test_group"
        )
        payload = AskAgentCommand(
            header=MessageHeader(
                message_id="msg-queued",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="dummy_agent",
            ),
            content="test",
        ).to_dict()

        await runner._process_message_from_dict(
            RedisKeys.ctrl_stream("dummy_agent"), "1-1", payload
        )

        self.assertIsNotNone(worker.seen_execution)
        self.assertFalse(worker.seen_execution.is_resumed)
        worker.registry.update_execution_status.assert_awaited_once_with(
            "exec-queued",
            "sess-1",
            "RUNNING",
            worker_id="worker-1",
        )
        self.assertTrue(redis_mock.acked)

    async def test_runner_skips_replayed_cancelled_message_and_acks_it(self):
        """Test that cancelled replayed messages are skipped
        without processing and are acked."""
        redis_mock = MockRedisRunner(message_to_return=[])
        worker = DummyWorker()
        worker.registry = AsyncMock()
        worker.registry.get_execution_by_message_id.return_value = {
            "execution_id": "exec-1",
            "status": "CANCELLED",
        }

        runner = WorkerRunner(
            redis_client=redis_mock, worker=worker, group_name="test_group"
        )
        payload = AskAgentCommand(
            header=MessageHeader(
                message_id="msg-cancelled",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="dummy_agent",
            ),
            content="test",
        ).to_dict()

        await runner._process_message_from_dict(
            RedisKeys.ctrl_stream("dummy_agent"), "2-0", payload
        )

        self.assertFalse(worker.processed)
        self.assertTrue(redis_mock.acked)
        worker.registry.save_execution.assert_not_called()

    async def test_runner_control_message_cancels_local_execution_and_acks_control_message(  # pylint: disable=C0301
        self,
    ) -> None:
        """Test that a CancelTaskCommand from control stream
        cancels the local execution task."""
        redis_mock = MockRedisRunner(message_to_return=[])
        worker = DummyWorker()
        worker.registry = AsyncMock()
        runner = WorkerRunner(
            redis_client=redis_mock, worker=worker, group_name="test_group"
        )

        cancel_event = asyncio.Event()

        class FakeTask:

            def __init__(self):
                self.cancel_called = False

            def cancel(self, msg=None):
                self.cancel_called = True

        fake_task = FakeTask()
        execution = RunningExecution(
            execution_id="exec-1",
            message_id="msg-1",
            session_id="sess-1",
            worker_id="worker-1",
            task=fake_task,
            cancel_event=cancel_event,
            context=None,
            cancel_reason="",
        )
        runner._tracker.add_execution(execution)

        control_msg = CancelTaskCommand(
            header=MessageHeader(
                message_id="ctl-1",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="dummy_agent",
                parent_message_id="msg-1",
            ),
            target_message_id="msg-1",
            target_execution_id="exec-1",
            reason="user aborted",
            cancel_mode="graceful",
        )

        await runner._handle_control_message(
            RedisKeys.worker_ctrl_stream("worker-1"),
            "3-0",
            control_msg,
        )

        self.assertTrue(cancel_event.is_set())
        self.assertTrue(fake_task.cancel_called)
        self.assertTrue(redis_mock.acked)
        worker.registry.mark_execution_cancelling.assert_awaited_with(
            "exec-1", "sess-1", "user aborted"
        )

    async def test_runner_triggers_on_cancel_task_hook(self):
        """Test that worker.on_cancel_task hook is triggered
        when handling cancel command."""
        redis_mock = MockRedisRunner(message_to_return=[])
        worker = DummyWorker()
        worker.on_cancel_task = AsyncMock()
        runner = WorkerRunner(
            redis_client=redis_mock, worker=worker, group_name="test_group"
        )

        cancel_event = asyncio.Event()
        fake_task = Mock()

        execution = RunningExecution(
            execution_id="exec-hook",
            message_id="msg-hook",
            session_id="sess-hook",
            worker_id="worker-1",
            task=fake_task,
            cancel_event=cancel_event,
            context=AsyncMock(),  # Mock context
        )
        runner._tracker.add_execution(execution)

        control_msg = CancelTaskCommand(
            header=MessageHeader(
                message_id="ctl-hook",
                session_id="sess-hook",
                trace_id="trace-hook",
                target_agent_type="dummy_agent",
            ),
            target_message_id="msg-hook",
            reason="hook test",
        )

        await runner._handle_control_message(
            RedisKeys.worker_ctrl_stream("worker-1"),
            "6-0",
            control_msg,
        )

        # Wait for the async task spawned by runner to finish
        await asyncio.sleep(0.1)

        worker.on_cancel_task.assert_awaited_once()
        call_args = worker.on_cancel_task.call_args[0][0]
        self.assertEqual(call_args.reason, "hook test")

    async def test_runner_sets_up_worker_control_stream(self):
        """Test that setup_control_streams creates the worker control stream group."""
        redis_mock = MockRedisRunner(message_to_return=[])
        worker = DummyWorker()
        runner = WorkerRunner(
            redis_client=redis_mock, worker=worker, group_name="test_group"
        )

        await runner.setup_control_streams()

        self.assertIn(
            (RedisKeys.worker_ctrl_stream("worker-1"), "test_group", "0", True),
            redis_mock.group_create_calls,
        )

    async def test_runner_control_loop_reads_worker_control_stream(self):
        """Test that _run_control_once reads from worker control
        stream and calls handler."""
        control_msg = CancelTaskCommand(
            header=MessageHeader(
                message_id="ctl-2",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="dummy_agent",
                parent_message_id="msg-1",
            ),
            target_message_id="msg-1",
            reason="user aborted",
            cancel_mode="graceful",
        )
        redis_mock = MockRedisRunner(
            message_to_return=[
                [
                    RedisKeys.worker_ctrl_stream("worker-1").encode(),
                    [(b"4-0", {b"data": json.dumps(control_msg.to_dict()).encode()})],
                ]
            ]
        )
        worker = DummyWorker()
        runner = WorkerRunner(
            redis_client=redis_mock, worker=worker, group_name="test_group"
        )
        runner._handle_control_message = AsyncMock()

        handled = await runner._run_control_once(block=1)

        self.assertTrue(handled)
        runner._handle_control_message.assert_awaited_once()
        self.assertTrue(redis_mock.called_xreadgroup)

    async def test_runner_control_loop_triggers_plugin_reload(self):
        """Test that reload control messages call plugin_registry.reload_plugins."""
        control_msg = ReloadPluginsCommand(
            header=MessageHeader(
                message_id="ctl-reload-2",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="dummy_agent",
            ),
            reload_id="reload-2",
            reason="runner test",
        )
        redis_mock = MockRedisRunner(
            message_to_return=[
                [
                    RedisKeys.worker_ctrl_stream("worker-1").encode(),
                    [(b"7-0", {b"data": json.dumps(control_msg.to_dict()).encode()})],
                ]
            ]
        )
        worker = DummyWorker()
        worker.plugin_registry = AsyncMock()
        runner = WorkerRunner(
            redis_client=redis_mock, worker=worker, group_name="test_group"
        )

        handled = await runner._run_control_once(block=1)

        self.assertTrue(handled)
        worker.plugin_registry.reload_plugins.assert_awaited_once_with(
            reload_id="reload-2",
            reason="runner test",
        )
        self.assertTrue(redis_mock.acked)

    async def test_shutdown_calls_worker_plugin_registry_shutdown_hooks(self):
        """Test that runner shutdown notifies the worker's plugin registry."""
        redis_mock = MockRedisRunner(message_to_return=None)
        worker = DummyWorker()
        worker.plugin_registry = Mock()
        worker.plugin_registry.log_hook_stats_on_shutdown = True
        worker.plugin_registry.log_hook_stats = Mock()
        worker.plugin_registry.on_worker_shutdown = AsyncMock()
        runner = WorkerRunner(
            redis_client=redis_mock, worker=worker, group_name="test_group"
        )

        await runner._shutdown()

        worker.plugin_registry.log_hook_stats.assert_called_once()
        worker.plugin_registry.on_worker_shutdown.assert_awaited_once_with(worker)

    async def test_runner_invalid_control_message_is_acked_without_handler(self):
        """Test that invalid control messages are acked without calling the handler."""
        invalid_control_msg = {
            "action_type": "CANCEL_TASK",
            "header": {
                "message_id": "ctl-3",
                "session_id": "sess-1",
                "trace_id": "trace-1",
                "target_agent_type": "dummy_agent",
                "source_agent_type": "",
                "parent_message_id": "",
                "task_group_id": "",
                "user_code": "",
                "user_name": "",
                "metadata": {},
            },
            "body": {},
        }
        redis_mock = MockRedisRunner(
            message_to_return=[
                [
                    RedisKeys.worker_ctrl_stream("worker-1").encode(),
                    [(b"5-0", {b"data": json.dumps(invalid_control_msg).encode()})],
                ]
            ]
        )
        worker = DummyWorker()
        runner = WorkerRunner(
            redis_client=redis_mock, worker=worker, group_name="test_group"
        )
        runner._handle_control_message = AsyncMock()

        handled = await runner._run_control_once(block=1)

        self.assertTrue(handled)
        runner._handle_control_message.assert_not_called()
        self.assertTrue(redis_mock.acked)


if __name__ == "__main__":
    unittest.main()
