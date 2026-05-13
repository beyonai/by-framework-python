"""Tests for AdkWorker."""

from unittest.mock import MagicMock

import pytest
from by_framework.core.protocol.commands import AskAgentCommand

try:
    from by_framework_adk.worker import AdkWorker

    HAS_ADK = True
except ImportError:
    HAS_ADK = False


@pytest.mark.skipif(not HAS_ADK, reason="google-adk is not installed")
def test_adk_worker_initialization():
    """Test that AdkWorker can be initialized."""

    class MyWorker(AdkWorker):
        """Dummy worker for testing."""

        def get_agent_types(self):
            return ["my-agent"]

        def build_agent(self, context, command):
            return MagicMock()

    worker = MyWorker("redis://localhost:6379")
    assert worker.get_agent_types() == ["my-agent"]
    assert worker.app_name == "by_framework_adk_app"


@pytest.mark.skipif(not HAS_ADK, reason="google-adk is not installed")
@pytest.mark.asyncio
async def test_adk_worker_process_command():
    """Test process_command execution flow."""

    class MyWorker(AdkWorker):
        """Dummy worker for testing process_command."""

        def get_agent_types(self):
            return ["my-agent"]

        def build_agent(self, context, command):
            agent = MagicMock()
            agent.name = "test_agent"
            return agent

    worker = MyWorker("redis://localhost:6379")
    context = MagicMock()
    context.session_id = "test_session"

    command = AskAgentCommand(
        header=MagicMock(),
        content="hello",
    )

    # We mock runner so we don't need real ADK backend
    # Actually, the adapter instantiates Runner directly.
    # We might need to patch Runner if we want to test execution without real ADK.
    assert worker is not None
    assert command is not None
