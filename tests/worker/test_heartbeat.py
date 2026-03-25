"""
Tests for byclaw_gateway_sdk.worker.heartbeat module.
"""

import asyncio
import unittest
from unittest.mock import Mock

from byclaw_gateway_sdk.worker.heartbeat import WorkerHeartbeat


class MockRegistry:
    """Mock WorkerRegistry for testing."""

    def __init__(self):
        self.register_calls = []
        self.worker_id = ""
        self.capabilities = []

    async def register_worker(self, worker_id: str, capabilities: list):
        self.register_calls.append((worker_id, capabilities))
        self.worker_id = worker_id
        self.capabilities = capabilities


class TestWorkerHeartbeat(unittest.IsolatedAsyncioTestCase):
    """Tests for WorkerHeartbeat."""

    async def test_initialization(self):
        """Test basic initialization."""
        mock_registry = MockRegistry()
        heartbeat = WorkerHeartbeat(
            worker_id="worker-1",
            capabilities=["agent-a", "agent-b"],
            registry=mock_registry,
            interval=15,
        )

        self.assertEqual(heartbeat.worker_id, "worker-1")
        self.assertEqual(heartbeat.capabilities, ["agent-a", "agent-b"])
        self.assertEqual(heartbeat.interval, 15)
        self.assertIsNone(heartbeat._task)

    async def test_start_initial_registration(self):
        """Test that start() performs initial registration."""
        mock_registry = MockRegistry()
        heartbeat = WorkerHeartbeat(
            worker_id="worker-1",
            capabilities=["agent-a"],
            registry=mock_registry,
        )

        await heartbeat.start()

        # Should have registered on start
        self.assertEqual(len(mock_registry.register_calls), 1)
        self.assertEqual(mock_registry.register_calls[0][0], "worker-1")

        # Task should be created
        self.assertIsNotNone(heartbeat._task)

        # Clean up
        await heartbeat.stop()

    async def test_start_twice_noops(self):
        """Test that calling start() twice doesn't register twice."""
        mock_registry = MockRegistry()
        heartbeat = WorkerHeartbeat(
            worker_id="worker-1",
            capabilities=["agent-a"],
            registry=mock_registry,
        )

        await heartbeat.start()
        first_task = heartbeat._task

        await heartbeat.start()  # Should be no-op
        second_task = heartbeat._task

        # Should still be the same task
        self.assertIs(first_task, second_task)

        # Should have registered only once
        self.assertEqual(len(mock_registry.register_calls), 1)

        # Clean up
        await heartbeat.stop()

    async def test_stop_cancels_task(self):
        """Test that stop() cancels the heartbeat task."""
        mock_registry = MockRegistry()
        heartbeat = WorkerHeartbeat(
            worker_id="worker-1",
            capabilities=["agent-a"],
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
            capabilities=["agent-a"],
            registry=mock_registry,
        )

        # Should not raise
        await heartbeat.stop()

        # No registrations should have occurred
        self.assertEqual(len(mock_registry.register_calls), 0)

    async def test_stop_multiple_times_noops(self):
        """Test that calling stop() multiple times is safe."""
        mock_registry = MockRegistry()
        heartbeat = WorkerHeartbeat(
            worker_id="worker-1",
            capabilities=["agent-a"],
            registry=mock_registry,
        )

        await heartbeat.start()
        await heartbeat.stop()
        await heartbeat.stop()  # Should not raise

        self.assertIsNone(heartbeat._task)

    async def test_heartbeat_loop_registers_periodically(self):
        """Test that heartbeat sends periodic registrations."""
        mock_registry = MockRegistry()
        heartbeat = WorkerHeartbeat(
            worker_id="worker-1",
            capabilities=["agent-a"],
            registry=mock_registry,
            interval=0.01,  # Very short interval for testing
        )

        await heartbeat.start()

        # Wait for a couple of heartbeat cycles
        await asyncio.sleep(0.035)

        # Should have initial registration + periodic registrations
        # Account for timing variations - at minimum we should have 2 registrations
        self.assertGreaterEqual(len(mock_registry.register_calls), 2)

        await heartbeat.stop()

    async def test_heartbeat_error_handling(self):
        """Test that heartbeat handles registry errors gracefully in loop."""
        mock_registry = Mock()
        error_count = 0

        async def failing_register(*args):
            nonlocal error_count
            error_count += 1
            # First call (initial) succeeds, subsequent calls fail
            if error_count > 1:
                raise RuntimeError(f"Error {error_count}")
            return

        mock_registry.register_worker = failing_register

        heartbeat = WorkerHeartbeat(
            worker_id="worker-1",
            capabilities=["agent-a"],
            registry=mock_registry,
            interval=0.01,
        )

        await heartbeat.start()

        # Wait for heartbeat cycles
        await asyncio.sleep(0.035)

        # Should have initial registration + periodic registrations (some failed)
        self.assertGreaterEqual(error_count, 2)

        await heartbeat.stop()


if __name__ == "__main__":
    unittest.main()
