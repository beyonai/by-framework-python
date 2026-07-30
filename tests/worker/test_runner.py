import asyncio
import http.client
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
from by_framework.common.config import WorkerConfig
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.protocol.commands import (
    AskAgentCommand,
    CancelTaskCommand,
    EvictWorkerCommand,
    ReloadPluginsCommand,
    ResumeCommand,
    SuspendWorkerCommand,
)
from by_framework.core.protocol.message_header import MessageHeader


def _request_readyz(port: int):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
    try:
        connection.request("GET", "/readyz")
        response = connection.getresponse()
        body = json.loads(response.read().decode("utf-8"))
        return response.status, body
    finally:
        connection.close()


async def _wait_until(predicate, timeout=2.0, interval=0.02):
    """Poll predicate (which may itself raise, e.g. ConnectionRefusedError
    while a server is still binding) until it returns truthy or timeout."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    last_exc = None
    while loop.time() < deadline:
        try:
            if predicate():
                return
        except Exception as exc:  # pylint: disable=broad-exception-caught
            last_exc = exc
        await asyncio.sleep(interval)
    if last_exc is not None:
        raise last_exc
    raise AssertionError("condition not met within timeout")


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

    async def claim_worker_id(self, worker_id: str, ttl_seconds: int = 15):
        raise ValueError(f"worker_id already in use: {worker_id}")


class DuplicateIdRegistryWithCleanupMethods(DuplicateIdRegistry):

    def __init__(self):
        self.marked_inactive = False
        self.unregistered_membership = False

    async def mark_worker_inactive(self, worker_id: str):
        self.marked_inactive = True
        return True

    async def unregister_worker_membership(self, worker_id: str):
        self.unregistered_membership = True


class StartupOrderStop(RuntimeError):
    """Sentinel used to stop the infinite runner loop in startup-order tests."""


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

        with patch.object(WorkerConfig, "worker_id_claim_max_wait_seconds", 0):
            with self.assertRaisesRegex(ValueError, "worker_id already in use"):
                await runner.start()

    async def test_runner_does_not_cleanup_presence_when_claim_never_succeeded(self):
        """A duplicate worker must not delete the active owner's lease or membership."""
        worker = MultiCapWorker()
        registry = DuplicateIdRegistryWithCleanupMethods()
        worker.registry = registry
        worker.stop_heartbeat = AsyncMock()
        redis_mock = MockRedisRunner(message_to_return=[])
        runner = WorkerRunner(
            redis_client=redis_mock, worker=worker, group_name="test_group"
        )

        with patch.object(WorkerConfig, "worker_id_claim_max_wait_seconds", 0):
            with self.assertRaisesRegex(ValueError, "worker_id already in use"):
                await runner.start()

        self.assertFalse(registry.marked_inactive)
        self.assertFalse(registry.unregistered_membership)

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

    async def test_runner_marks_consumer_ready_before_starting_heartbeat(self):
        """Online lease should not be visible before the reader side is ready."""
        worker = DummyWorker()
        worker.registry = Mock()
        worker.registry.claim_worker_id = AsyncMock(return_value="lock-token")
        worker.registry.release_worker_id = AsyncMock(return_value=True)
        worker.registry.unregister_worker_membership = AsyncMock()
        redis_mock = MockRedisRunner(message_to_return=[])
        runner = WorkerRunner(
            redis_client=redis_mock, worker=worker, group_name="test_group"
        )
        runner._control_loop = AsyncMock()
        reader_ready_seen_by_heartbeat = False

        async def start_heartbeat(**kwargs):
            nonlocal reader_ready_seen_by_heartbeat
            self.assertIn("health_check", kwargs)
            reader_ready_seen_by_heartbeat = runner._is_consumer_healthy()
            raise StartupOrderStop("stop after heartbeat order check")

        worker.start_heartbeat = start_heartbeat
        worker.stop_heartbeat = AsyncMock()

        with self.assertRaisesRegex(StartupOrderStop, "order check"):
            await runner.start()

        self.assertTrue(reader_ready_seen_by_heartbeat)
        self.assertFalse(redis_mock.called_xreadgroup)

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

    async def test_runner_prefers_framework_parent_for_redis_trace_tree(self):
        """Redis worker.execute spans attach to the framework call span id."""
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
                metadata={"framework_parent_span_id": "call-msg:client.dispatch"},
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

        self.assertEqual(
            live_span.call_args.kwargs["parent_span_id"], "0123456789abcdef"
        )
        span = span_recorder.record_span.await_args.args[0]
        self.assertEqual(span.parent_span_id, "call-msg:client.dispatch")

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

    async def test_runner_warns_when_resume_command_execution_lookup_misses(self):
        """A ResumeCommand whose message_id/session_id don't resolve to an
        existing execution silently starts a brand-new, disconnected
        execution today. That failure mode should at least be visible in
        logs instead of passing unnoticed."""
        redis_mock = MockRedisRunner(message_to_return=[])
        worker = DummyWorker()
        worker.registry = AsyncMock()
        worker.registry.get_execution_by_message_id.return_value = None

        runner = WorkerRunner(
            redis_client=redis_mock,
            worker=worker,
            group_name="test_group",
            span_recorder=AsyncMock(),
        )
        runner._trace_writer = AsyncMock()
        payload = ResumeCommand(
            header=MessageHeader(
                message_id="msg-orphan",
                session_id="sess-orphan",
                trace_id="trace-1",
                target_agent_type="dummy_agent",
            ),
            content="user reply",
        ).to_dict()

        with self.assertLogs("by-framework", level="WARNING") as captured:
            await runner._process_message_from_dict(
                RedisKeys.ctrl_stream("dummy_agent"), "1-0", payload
            )

        self.assertTrue(
            any(
                "msg-orphan" in record and "sess-orphan" in record
                for record in captured.output
            ),
            captured.output,
        )

    async def test_runner_does_not_warn_when_resume_command_execution_resolves(self):
        """A ResumeCommand that correctly resolves to its suspended execution
        should not trip the orphan-resume warning."""
        redis_mock = MockRedisRunner(message_to_return=[])
        worker = DummyWorker()
        worker.registry = AsyncMock()
        worker.registry.get_execution_by_message_id.return_value = {
            "execution_id": "exec-1",
            "message_id": "msg-1",
            "session_id": "sess-1",
            "status": "WAITING_USER",
        }

        runner = WorkerRunner(
            redis_client=redis_mock,
            worker=worker,
            group_name="test_group",
            span_recorder=AsyncMock(),
        )
        runner._trace_writer = AsyncMock()
        payload = ResumeCommand(
            header=MessageHeader(
                message_id="msg-1",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="dummy_agent",
            ),
            content="user reply",
        ).to_dict()

        with self.assertNoLogs("by-framework", level="WARNING"):
            await runner._process_message_from_dict(
                RedisKeys.ctrl_stream("dummy_agent"), "1-0", payload
            )

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

    async def test_runner_health_endpoint_is_wired_into_startup_and_shutdown(self):
        """Supplying health_port starts a real /readyz server during real
        startup, tied to the real consume loop's liveness signal via
        _mark_consumer_tick(), and tears it down on shutdown."""
        worker = DummyWorker()
        worker.start_heartbeat = AsyncMock()
        worker.stop_heartbeat = AsyncMock()
        redis_mock = MockRedisRunner(message_to_return=[])
        runner = WorkerRunner(
            redis_client=redis_mock,
            worker=worker,
            group_name="test_group",
            health_port=0,
        )
        runner._control_loop = AsyncMock()

        run_task = asyncio.ensure_future(runner.start())
        try:
            await _wait_until(lambda: runner._health_server.is_running)
            port = runner._health_server.port
            await _wait_until(lambda: _request_readyz(port)[0] in (200, 503))

            status, body = _request_readyz(port)
            self.assertEqual(status, 200)
            self.assertTrue(body["ready"])
            self.assertEqual(body["reason"], "serving")
        finally:
            run_task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await run_task

        with self.assertRaises(ConnectionRefusedError):
            _request_readyz(port)

    async def test_runner_shutdown_reports_draining_before_health_server_stops(self):
        """_shutdown() must flip to draining as its first step - before the
        rest of the drain runs, not after - and the readiness server must
        stay reachable (still reporting draining) throughout the drain,
        not disappear before it. Hooks into stop_heartbeat, which
        _shutdown() calls partway through, to observe /readyz mid-drain."""
        worker = DummyWorker()
        redis_mock = MockRedisRunner(message_to_return=[])
        runner = WorkerRunner(
            redis_client=redis_mock,
            worker=worker,
            group_name="test_group",
            health_port=0,
        )
        runner._health_server.start()
        runner._mark_consumer_tick()  # simulate "already past starting"
        observed = {}

        async def observe_mid_shutdown():
            port = runner._health_server.port
            observed["status"], observed["body"] = _request_readyz(port)

        worker.stop_heartbeat = observe_mid_shutdown

        await runner._shutdown()

        self.assertEqual(observed["status"], 503)
        self.assertEqual(observed["body"]["reason"], "draining")
        # And the server itself is torn down only once the drain is done.
        self.assertFalse(runner._health_server.is_running)

    async def test_runner_without_health_port_starts_no_server(self):
        """Omitting health_port must leave _health_server unset - the
        default (opted-out) case must have zero new behavior."""
        worker = DummyWorker()
        redis_mock = MockRedisRunner(message_to_return=[])
        runner = WorkerRunner(
            redis_client=redis_mock, worker=worker, group_name="test_group"
        )

        self.assertIsNone(runner._health_server)

    async def test_runner_health_endpoint_reflects_admin_suspend_and_evict_live(self):
        """Real SuspendWorkerCommand/EvictWorkerCommand messages, dispatched
        through the Worker's actual control-message pipeline
        (_run_control_once -> parse_control_command ->
        handle_suspend_worker/handle_evict_worker - not just the
        _set_admin_lifecycle callback directly), must be reflected in
        /readyz immediately, without a process restart."""
        worker = DummyWorker()
        redis_mock = MockRedisRunner(message_to_return=[])
        runner = WorkerRunner(
            redis_client=redis_mock,
            worker=worker,
            group_name="test_group",
            health_port=0,
        )
        runner._health_server.start()
        runner._mark_consumer_tick()
        # Fake a live _consumer_task so _is_consumer_healthy() reflects
        # staleness, not "no task at all" - not what this test is about.
        runner._consumer_task = asyncio.ensure_future(asyncio.sleep(10))
        try:
            _, body = _request_readyz(runner._health_server.port)
            self.assertEqual(body["reason"], "serving")

            suspend_cmd = SuspendWorkerCommand(
                header=MessageHeader(session_id="s1", trace_id="t1", message_id="m1"),
                reason="maintenance",
            )
            redis_mock.msg = [
                [
                    RedisKeys.worker_ctrl_stream("worker-1").encode(),
                    [(b"1-0", {b"data": json.dumps(suspend_cmd.to_dict()).encode()})],
                ]
            ]
            await runner._run_control_once(block=1)
            _, body = _request_readyz(runner._health_server.port)
            self.assertEqual(body["reason"], "suspended")

            evict_cmd = EvictWorkerCommand(
                header=MessageHeader(session_id="s1", trace_id="t1", message_id="m2"),
                force=False,
            )
            redis_mock.msg = [
                [
                    RedisKeys.worker_ctrl_stream("worker-1").encode(),
                    [(b"2-0", {b"data": json.dumps(evict_cmd.to_dict()).encode()})],
                ]
            ]
            await runner._run_control_once(block=1)
            _, body = _request_readyz(runner._health_server.port)
            self.assertEqual(body["reason"], "evicted")
        finally:
            runner._health_server.stop()
            runner._consumer_task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await runner._consumer_task

    async def test_runner_health_endpoint_reflects_consumer_staleness_live(self):
        """Transition from serving -> consumer_stalled -> serving must be
        observable through /readyz without a restart, driven by the same
        staleness signal (_consumer_last_tick_monotonic vs
        _consumer_health_timeout_seconds) _is_consumer_healthy() already
        uses internally."""
        worker = DummyWorker()
        redis_mock = MockRedisRunner(message_to_return=[])
        runner = WorkerRunner(
            redis_client=redis_mock,
            worker=worker,
            group_name="test_group",
            health_port=0,
        )
        runner._health_server.start()
        runner._mark_consumer_tick()
        # Fake a _consumer_task so _is_consumer_healthy() doesn't
        # short-circuit on "no task" - only staleness is under test here.
        runner._consumer_task = asyncio.ensure_future(asyncio.sleep(10))
        try:
            _, body = _request_readyz(runner._health_server.port)
            self.assertEqual(body["reason"], "serving")

            runner._consumer_last_tick_monotonic -= (
                runner._consumer_health_timeout_seconds + 1
            )
            _, body = _request_readyz(runner._health_server.port)
            self.assertEqual(body["reason"], "consumer_stalled")

            runner._mark_consumer_tick()
            _, body = _request_readyz(runner._health_server.port)
            self.assertEqual(body["reason"], "serving")
        finally:
            runner._health_server.stop()
            runner._consumer_task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await runner._consumer_task


if __name__ == "__main__":
    unittest.main()
