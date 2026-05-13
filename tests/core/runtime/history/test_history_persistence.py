from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from by_framework.core.protocol.commands import AskAgentCommand
from by_framework.core.protocol.event_type import EventType
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.core.runtime.history import (BaseHistoryBackend, HistoryManager)
from by_framework.worker.context import AgentContext
from by_framework.worker.worker import GatewayWorker


class MockWorker(GatewayWorker):

    def get_agent_types(self):
        return ["mock-agent"]

    async def process_command(self, command, context):
        # Simulate business logic emitting streaming content
        await context.emit_chunk("Hello", event_type=EventType.ANSWER_DELTA.name)
        await context.emit_chunk(" World", event_type=EventType.ANSWER_DELTA.name)
        # Do not proactively send appStreamResponse, test fallback logic
        return {"status": "ok"}


@pytest.fixture
def mock_redis():
    mock = MagicMock()
    # Simulate redis.pipeline().execute() is async
    pipeline = MagicMock()
    pipeline.execute = AsyncMock(return_value=[])
    mock.pipeline.return_value = pipeline
    return mock


@pytest.fixture
def mock_history_manager():
    with patch(
        "by_framework.core.runtime.history.history_manager.HistoryManager.save_message",
        new_callable=AsyncMock,
    ) as mocked:
        yield mocked


@pytest.mark.asyncio
async def test_context_accumulates_and_flushes(mock_redis, mock_history_manager):
    """Verify AgentContext chunk accumulation and appStreamResponse
    triggered persistence."""
    context = AgentContext(
        session_id="s1", trace_id="t1", redis_client=mock_redis, current_agent_id="a1"
    )

    # Simulate sending multiple chunks
    await context.emit_chunk("Hello")
    await context.emit_chunk(" World")

    # At this point, save should not be triggered
    mock_history_manager.assert_not_called()

    # Send stream end marker
    await context.emit_chunk("", event_type=EventType.APP_STREAM_RESPONSE.value)

    # Verify saved content
    mock_history_manager.assert_awaited_once()
    args, kwargs = mock_history_manager.call_args
    assert kwargs["role"] == "assistant"
    assert kwargs["content"] == "Hello World"


@pytest.mark.asyncio
async def test_worker_saves_user_and_assistant_history(
    mock_redis, mock_history_manager
):
    """Verify automatic history tracking of user messages in Worker lifecycle."""
    registry = MagicMock()
    ws_manager = AsyncMock()
    ws_manager.setup_workspace.return_value = {
        "private": "/tmp",
        "public": "/tmp/public",
    }

    worker = MockWorker(
        worker_id="w1",
        redis_client=mock_redis,
        registry=registry,
        workspace_manager=ws_manager,
    )

    command = AskAgentCommand(
        header=MessageHeader(
            session_id="s2",
            trace_id="t2",
            target_agent_type="mock-agent",
            message_id="m1",
            user_code="default",  # Fill in missing parameter
        ),
        content="User Question",
    )

    # Execute processing logic
    await worker._handle_message(command)

    # Verify save was called twice: once for user, once for assistant (fallback trigger)
    assert mock_history_manager.call_count == 2

    # Check user message
    user_call = mock_history_manager.call_args_list[0]
    assert user_call.kwargs["role"] == "user"
    assert user_call.kwargs["content"] == "User Question"

    # Check assistant reply (accumulated by MockWorker of "Hello World")
    assistant_call = mock_history_manager.call_args_list[1]
    assert assistant_call.kwargs["role"] == "assistant"
    assert assistant_call.kwargs["content"] == "Hello World"


@pytest.mark.asyncio
async def test_duplicate_save_prevention(mock_redis, mock_history_manager):
    """Verify that history records are not saved multiple times
    (stream end + Worker end fallback)."""
    context = AgentContext(session_id="s3", trace_id="t3", redis_client=mock_redis)

    await context.emit_chunk("Data")
    # Manually trigger one send end
    await context.emit_chunk("", event_type=EventType.APP_STREAM_RESPONSE.value)
    # Logically try to flush again (simulating Worker end fallback call)
    await context.flush_to_history()

    # Should only have called save once
    assert mock_history_manager.call_count == 1


@pytest.mark.asyncio
async def test_in_memory_storage_isolation():
    """Verify InMemoryHistoryBackend multi-session isolation."""
    from by_framework.core.runtime.history import InMemoryHistoryBackend

    storage = InMemoryHistoryBackend()

    await storage.save_message("session-A", "user", "Hello A")
    await storage.save_message("session-B", "user", "Hello B")

    history_a = await storage.get_history("session-A")
    history_b = await storage.get_history("session-B")

    assert len(history_a) == 1
    assert history_a[0]["content"] == "Hello A"
    assert len(history_b) == 1
    assert history_b[0]["content"] == "Hello B"


@pytest.mark.asyncio
async def test_history_manager_backend_switch(mock_redis):
    """Verify HistoryManager dynamic backend switching logic."""

    class MyCustomBackend(BaseHistoryBackend):

        def __init__(self):
            self.saved = False

        async def get_history(self, session_id, limit=10):
            return []

        async def save_message(self, session_id, role, content, metadata=None):
            self.saved = True

        async def list_sessions(self):
            return []

    custom_backend = MyCustomBackend()
    HistoryManager.set_default_backend(custom_backend)

    # Trigger save (via instance)
    manager = HistoryManager(session_id="s4")
    await manager.save_message("assistant", "test")
    assert custom_backend.saved is True

    # Restore default to avoid affecting other tests
    from by_framework.core.runtime.history import InMemoryHistoryBackend

    HistoryManager.set_default_backend(InMemoryHistoryBackend())
