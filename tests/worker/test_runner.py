import asyncio
import json
import unittest
from typing import Any
from unittest.mock import AsyncMock, Mock

from by_framework import (
    GatewayWorker,
    RedisKeys,
    RunningExecution,
    WorkerRunner,
)
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.protocol.commands import (
    AskAgentCommand,
    CancelTaskCommand,
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

    def get_capabilities(self) -> list[str]:
        return ["dummy_agent"]

    async def process_command(self, command: Any, context: Any) -> None:
        self.processed = True

    async def _handle_message(self, command, **kwargs):
        await self.process_command(command, None)
        return AgentState.COMPLETED.value


class MultiCapWorker(GatewayWorker):

    def __init__(self):
        super().__init__("worker-multi", None, None, None)

    def get_capabilities(self) -> list[str]:
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
        """Test that WorkerRunner generates a deterministic auto group name when not specified."""
        worker = MultiCapWorker()
        redis_mock = MockRedisRunner(message_to_return=[])

        runner = WorkerRunner(redis_client=redis_mock, worker=worker, group_name=None)
        self.assertTrue(runner.group_name.startswith(f"{RedisKeys.CG_AGENT_ENGINES}:"))

        runner2 = WorkerRunner(redis_client=redis_mock, worker=worker, group_name=None)
        self.assertEqual(runner.group_name, runner2.group_name)

    async def test_runner_rejects_duplicate_worker_id_on_start(self):
        """Test that WorkerRunner raises ValueError when worker_id is already claimed."""
        worker = MultiCapWorker()
        worker.registry = DuplicateIdRegistry()
        redis_mock = MockRedisRunner(message_to_return=[])
        runner = WorkerRunner(
            redis_client=redis_mock, worker=worker, group_name="test_group"
        )

        with self.assertRaisesRegex(ValueError, "worker_id already in use"):
            await runner.start()

    async def test_runner_registers_execution_and_acks_processed_message(self):
        """Test that _process_message_from_dict registers execution and acks the message."""
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

    async def test_runner_skips_replayed_cancelled_message_and_acks_it(self):
        """Test that cancelled replayed messages are skipped without processing and are acked."""
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

    async def test_runner_control_message_cancels_local_execution_and_acks_control_message(
        self,
    ):
        """Test that a CancelTaskCommand from control stream cancels the local execution task."""
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

            def cancel(self):
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
        """Test that worker.on_cancel_task hook is triggered when handling cancel command."""
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
        """Test that _run_control_once reads from worker control stream and calls handler."""
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

    async def test_runner_invalid_control_message_is_acked_without_handler(self):
        """Test that invalid control messages are acked without calling the handler."""
        invalid_control_msg = {
            "action_type": "CANCEL_TASK",
            "header": {
                "message_id": "ctl-3",
                "session_id": "sess-1",
                "trace_id": "trace-1",
                "target_agent_type": "dummy_agent",
                "source_agent_id": "",
                "parent_message_id": "",
                "tenant_id": "",
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
