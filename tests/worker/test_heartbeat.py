"""
Tests for by_framework.worker.heartbeat module.
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, Mock

from by_framework.worker.heartbeat import WorkerHeartbeat


class MockRegistry:
    """Mock WorkerRegistry for testing."""

    def __init__(self):
        self.membership_calls = []
        self.heartbeat_calls = []
        self.worker_id = ""
        self.agent_types = []

    async def register_worker_membership(self, worker_id: str, agent_types: list):
        self.membership_calls.append((worker_id, agent_types))
        self.worker_id = worker_id
        self.agent_types = agent_types

    async def heartbeat_worker(self, worker_id: str, lease_ttl_seconds: int = 15):
        self.heartbeat_calls.append((worker_id, lease_ttl_seconds))
        self.worker_id = worker_id
        return True


class FailingAfterStartRegistry(MockRegistry):
    """Registry that allows startup, then fails periodic heartbeat."""

    async def heartbeat_worker(self, worker_id: str, lease_ttl_seconds: int = 15):
        if self.heartbeat_calls:
            self.heartbeat_calls.append((worker_id, lease_ttl_seconds))
            raise RuntimeError("redis unavailable")
        return await super().heartbeat_worker(worker_id, lease_ttl_seconds)


class TestWorkerHeartbeat(unittest.IsolatedAsyncioTestCase):
    """Tests for WorkerHeartbeat."""

    async def test_initialization(self):
        """Test basic initialization."""
        mock_registry = MockRegistry()
        heartbeat = WorkerHeartbeat(
            worker_id="worker-1",
            agent_types=["agent-a", "agent-b"],
            registry=mock_registry,
            interval=15,
        )

        self.assertEqual(heartbeat.worker_id, "worker-1")
        self.assertEqual(heartbeat.agent_types, ["agent-a", "agent-b"])
        self.assertEqual(heartbeat.interval, 15)
        self.assertEqual(heartbeat.lease_ttl_seconds, 30)
        self.assertIsNone(heartbeat._task)

    async def test_start_initial_registration(self):
        """Test that start() performs initial registration."""
        mock_registry = MockRegistry()
        heartbeat = WorkerHeartbeat(
            worker_id="worker-1",
            agent_types=["agent-a"],
            registry=mock_registry,
        )

        await heartbeat.start()

        # Should have registered membership and initial heartbeat
        self.assertEqual(len(mock_registry.membership_calls), 1)
        self.assertEqual(mock_registry.membership_calls[0][0], "worker-1")
        self.assertEqual(mock_registry.heartbeat_calls, [("worker-1", 30)])

        # Task should be created
        self.assertIsNotNone(heartbeat._task)

        # Clean up
        await heartbeat.stop()

    async def test_start_twice_noops(self):
        """Test that calling start() twice doesn't register twice."""
        mock_registry = MockRegistry()
        heartbeat = WorkerHeartbeat(
            worker_id="worker-1",
            agent_types=["agent-a"],
            registry=mock_registry,
        )

        await heartbeat.start()
        first_task = heartbeat._task

        await heartbeat.start()  # Should be no-op
        second_task = heartbeat._task

        # Should still be the same task
        self.assertIs(first_task, second_task)

        # Should have registered membership and heartbeat only once
        self.assertEqual(len(mock_registry.membership_calls), 1)
        self.assertEqual(len(mock_registry.heartbeat_calls), 1)

        # Clean up
        await heartbeat.stop()

    async def test_stop_cancels_task(self):
        """Test that stop() cancels the heartbeat task."""
        mock_registry = MockRegistry()
        heartbeat = WorkerHeartbeat(
            worker_id="worker-1",
            agent_types=["agent-a"],
            registry=mock_registry,
        )

        await heartbeat.start()
        self.assertIsNotNone(heartbeat._task)

        await heartbeat.stop()

        # Task should be cancelled
        self.assertIsNone(heartbeat._task)

    async def test_stop_without_start_noops(self):
        """Test that stop() without start() is a no-op."""
        mock_registry = MockRegistry()
        heartbeat = WorkerHeartbeat(
            worker_id="worker-1",
            agent_types=["agent-a"],
            registry=mock_registry,
        )

        # Should not raise
        await heartbeat.stop()

        # No lifecycle calls should have occurred
        self.assertEqual(len(mock_registry.membership_calls), 0)
        self.assertEqual(len(mock_registry.heartbeat_calls), 0)

    async def test_stop_multiple_times_noops(self):
        """Test that calling stop() multiple times is safe."""
        mock_registry = MockRegistry()
        heartbeat = WorkerHeartbeat(
            worker_id="worker-1",
            agent_types=["agent-a"],
            registry=mock_registry,
        )

        await heartbeat.start()
        await heartbeat.stop()
        await heartbeat.stop()  # Should not raise

        self.assertIsNone(heartbeat._task)

    async def test_heartbeat_loop_repairs_membership_periodically(self):
        """Test heartbeat loop refreshes liveness and repairs membership."""
        mock_registry = MockRegistry()
        heartbeat = WorkerHeartbeat(
            worker_id="worker-1",
            agent_types=["agent-a"],
            registry=mock_registry,
            interval=0.01,  # Very short interval for testing
        )

        await heartbeat.start()

        # Wait for a couple of heartbeat cycles
        await asyncio.sleep(0.035)

        # Membership is reconciled periodically so Redis set loss can self-heal.
        self.assertGreaterEqual(len(mock_registry.membership_calls), 2)
        self.assertTrue(
            all(
                call == ("worker-1", ["agent-a"])
                for call in mock_registry.membership_calls
            )
        )
        # Liveness should be refreshed repeatedly.
        self.assertGreaterEqual(len(mock_registry.heartbeat_calls), 2)
        self.assertTrue(
            all(call == ("worker-1", 30) for call in mock_registry.heartbeat_calls)
        )

        await heartbeat.stop()

    async def test_heartbeat_error_handling(self):
        """Test that heartbeat handles registry errors gracefully in loop."""
        mock_registry = Mock()
        error_count = 0

        async def failing_heartbeat(*args):
            nonlocal error_count
            error_count += 1
            raise RuntimeError(f"Error {error_count}")

        mock_registry.register_worker_membership = AsyncMock()
        mock_registry.heartbeat_worker = failing_heartbeat

        heartbeat = WorkerHeartbeat(
            worker_id="worker-1",
            agent_types=["agent-a"],
            registry=mock_registry,
            interval=0.01,
        )

        with self.assertRaisesRegex(RuntimeError, "Error 1"):
            await heartbeat.start()
        self.assertEqual(error_count, 1)

    async def test_heartbeat_exits_after_failure_deadline(self):
        """Test sustained heartbeat failures trigger the watcher task."""
        mock_registry = FailingAfterStartRegistry()
        heartbeat = WorkerHeartbeat(
            worker_id="worker-1",
            agent_types=["agent-a"],
            registry=mock_registry,
            interval=0.01,
            lease_ttl_seconds=0.03,
        )

        await heartbeat.start()

        with self.assertRaisesRegex(RuntimeError, "lock was stolen"):
            await asyncio.wait_for(heartbeat.task, timeout=0.5)

        await heartbeat.stop()


if __name__ == "__main__":
    unittest.main()
