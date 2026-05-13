import asyncio
import logging
import unittest

from by_framework import logger


class TestLoggerIntegration(unittest.TestCase):
    """Test logging functionality integration with other modules."""

    def setUp(self):
        """Reset logger configuration before each test."""
        # logger is now pre-configured or exported
        self.logger = logger

    def tearDown(self):
        """Clean up logger configuration after each test."""
        logger = logging.getLogger("test-integration-logger")
        logger.handlers = []

    def test_logger_with_client(self):
        """Test that logger can work properly in the Client module."""
        from unittest.mock import AsyncMock, MagicMock

        from by_framework import ByaiGatewayClient, SendMessageResponse

        redis_mock = AsyncMock()
        redis_mock.pipeline = MagicMock(
            return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
        )
        registry_mock = AsyncMock()
        registry_mock.get_target_worker = AsyncMock(return_value="worker-1")
        registry_mock.has_online_agent_type = AsyncMock(
            return_value=(True, ["worker-1"])
        )

        client = ByaiGatewayClient(redis_client=redis_mock, registry=registry_mock)
        # Call a method that generates logs
        result = asyncio.run(
            client.send_message(
                "test-agent", "test-session", "test-user", "test-content"
            )
        )
        self.assertIsInstance(result, SendMessageResponse)

    def test_logger_with_worker(self):
        """Test that logger can work properly in the Worker module."""
        from unittest.mock import AsyncMock, MagicMock, Mock

        from by_framework import GatewayWorker

        # Create a simple Worker subclass for testing
        class TestWorker(GatewayWorker):

            def get_agent_types(self):
                return ["test-agent"]

            async def process_command(self, command, context):
                from by_framework import StateChangeEvent

                self.logger.debug("Test worker processing task")
                await context.emit_state(StateChangeEvent(state="COMPLETED"))

        # Configure appropriate mock objects
        workspace_manager_mock = Mock()
        workspace_manager_mock.setup_workspace = AsyncMock(
            return_value={
                "root": "/tmp/test",
                "public": "/tmp/test/public",
                "private": "/tmp/test/private",
            }
        )
        workspace_manager_mock.cleanup_task = AsyncMock(return_value=None)

        # Configure msg mock object
        from by_framework.core.protocol.commands import AskAgentCommand
        from by_framework.core.protocol.message_header import MessageHeader

        msg_mock = AskAgentCommand(
            header=MessageHeader(
                message_id="test-msg-id",
                session_id="test-session-id",
                trace_id="trace-logger",
                source_agent_type="test-source",
                target_agent_type="test-agent",
            ),
            content="test-content",
        )

        registry_mock = AsyncMock()
        redis_mock = AsyncMock()
        redis_mock.pipeline = MagicMock(
            return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
        )
        worker = TestWorker(
            "test-worker", redis_mock, registry_mock, workspace_manager_mock
        )

        async def run_worker():
            # Simulate processing message
            await worker.start_heartbeat()
            await worker._handle_message(msg_mock)

        try:
            asyncio.run(run_worker())
            self.assertTrue(True, "Worker can process tasks normally")
        except Exception as e:
            self.fail(f"Worker threw exception when processing task: {e}")

    def test_logger_file_output(self):
        """Test that logs can be written to file."""
        import os

        log_file = "by-framework.log"
        # Save original logger level
        original_level = self.logger.level
        try:
            # Set logger level to DEBUG so debug logs can be captured by file handler
            self.logger.setLevel(logging.DEBUG)
            self.logger.info("Test log file output")
            self.logger.debug("Debug log entry")
            self.logger.warning("Warning log entry")

            # Ensure logs are flushed to file
            for handler in self.logger.handlers:
                handler.flush()

            self.assertTrue(os.path.exists(log_file), "Log file was not created")
            self.assertTrue(os.path.getsize(log_file) > 0, "Log file content is empty")

            # Verify log content
            with open(log_file, "r") as f:
                content = f.read()
                self.assertIn("Test log file output", content)
                self.assertIn("Debug log entry", content)
                self.assertIn("Warning log entry", content)
        finally:
            # Restore original logger level
            self.logger.setLevel(original_level)


if __name__ == "__main__":
    unittest.main()
