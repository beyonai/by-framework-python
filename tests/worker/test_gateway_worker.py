import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from byclaw_gateway_sdk import GatewayWorker
from byclaw_gateway_sdk.core.protocol.commands import (AskAgentCommand, ResumeCommand)
from byclaw_gateway_sdk.core.protocol.message_header import MessageHeader


class DummyWorker(GatewayWorker):

    def get_capabilities(self):
        return []

    async def process_command(self, command, context):
        pass


class CancelWorker(GatewayWorker):

    def get_capabilities(self):
        return ["cancel_agent"]

    async def process_command(self, command, context):
        raise asyncio.CancelledError("user aborted")


class RecordingWorker(GatewayWorker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_command = None

    def get_capabilities(self):
        return ["recording_agent"]

    async def process_command(self, command, context):
        self.last_command = command
        return {"ok": True}


def test_worker_persist_metadata(tmp_path):
    """Test that _persist_agent_return_state_sync correctly persists command metadata to disk."""
    worker = DummyWorker(worker_id="test")
    paths = {"public": str(tmp_path)}
    msg = AskAgentCommand(
        header=MessageHeader(
            message_id="m1",
            session_id="s1",
            trace_id="trace-1",
            target_agent_type="t1",
            metadata={"user_data": "123"},
        ),
        content="metadata payload",
    )
    worker._persist_agent_return_state_sync(paths, msg)

    state_file = tmp_path / "session" / "agent_returns" / "m1.json"
    data = json.loads(state_file.read_text())
    assert data.get("metadata") == {"user_data": "123"}


@pytest.mark.asyncio
async def test_worker_cancelled_emits_cancelled_state(tmp_path):
    """Test that when a worker task raises CancelledError, it emits CANCELLED state (not FAILED)."""
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }

    worker = CancelWorker(
        worker_id="test-cancel",
        redis_client=redis_mock,
        registry=AsyncMock(),
        workspace_manager=workspace_manager,
    )

    msg = AskAgentCommand(
        header=MessageHeader(
            message_id="m2",
            session_id="s2",
            trace_id="trace-2",
            target_agent_type="cancel_agent",
        ),
        content="hello",
    )

    await worker._handle_message(msg)

    payloads = [
        json.loads(call.args[1]["data"])
        for call in redis_mock.pipeline.return_value.xadd.call_args_list
        if len(call.args) >= 2
        and isinstance(call.args[1], dict)
        and "data" in call.args[1]
    ]
    state_messages = [
        payload.get("data", {})
        .get("choices", [{}])[0]
        .get("delta", {})
        .get("content", "")
        for payload in payloads
    ]

    assert any("CANCELLED" in state for state in state_messages)
    assert not any("FAILED" in state for state in state_messages)


@pytest.mark.asyncio
async def test_worker_resume_message_round_trips_as_resume_command(tmp_path):
    """Test that a ResumeCommand is correctly handled and stored as a ResumeCommand on the worker."""
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }

    worker = RecordingWorker(
        worker_id="test-resume",
        redis_client=redis_mock,
        registry=AsyncMock(),
        workspace_manager=workspace_manager,
    )

    msg = ResumeCommand(
        header=MessageHeader(
            message_id="m3",
            session_id="s3",
            trace_id="trace-3",
            target_agent_type="recording_agent",
        ),
        status="SUCCESS",
        reply_data={"answer": 42},
    )

    await worker._handle_message(msg)

    assert isinstance(worker.last_command, ResumeCommand)
    assert worker.last_command.status == "SUCCESS"
    assert worker.last_command.reply_data == {"answer": 42}


@pytest.mark.asyncio
async def test_worker_injects_decoded_command_into_context(tmp_path):
    """Test that the decoded command is injected into the context as current_command."""
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }

    observed = {}

    class ContextInspectWorker(GatewayWorker):

        def get_capabilities(self):
            return ["inspect_agent"]

        async def process_command(self, command, context):
            observed["command"] = getattr(context, "current_command", None)
            return {"ok": True}

    worker = ContextInspectWorker(
        worker_id="test-inspect",
        redis_client=redis_mock,
        registry=AsyncMock(),
        workspace_manager=workspace_manager,
    )

    msg = ResumeCommand(
        header=MessageHeader(
            message_id="m4",
            session_id="s4",
            trace_id="trace-4",
            target_agent_type="inspect_agent",
        ),
        status="SUCCESS",
        reply_data={"answer": 7},
    )

    await worker._handle_message(msg)

    assert isinstance(observed["command"], ResumeCommand)
    assert observed["command"].reply_data == {"answer": 7}


@pytest.mark.asyncio
async def test_worker_without_process_command_returns_failed(tmp_path):
    """Test that a worker without process_command override returns FAILED status."""
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }

    class LegacyOnlyWorker(GatewayWorker):

        def get_capabilities(self):
            return ["legacy_agent"]

    worker = LegacyOnlyWorker(
        worker_id="test-legacy",
        redis_client=redis_mock,
        registry=AsyncMock(),
        workspace_manager=workspace_manager,
    )
    msg = AskAgentCommand(
        header=MessageHeader(
            message_id="m5",
            session_id="s5",
            trace_id="trace-5",
            target_agent_type="legacy_agent",
        ),
        content="hello",
    )

    result = await worker._handle_message(msg)

    assert result == "FAILED"


@pytest.mark.asyncio
async def test_worker_process_command_override_takes_precedence(tmp_path):
    """Test that worker's process_command override receives the original command."""
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }
    observed = {}

    class CommandWorker(GatewayWorker):

        def get_capabilities(self):
            return ["command_agent"]

        async def process_command(self, command, context):
            observed["command"] = command
            return {"ok": True}

    worker = CommandWorker(
        worker_id="test-command",
        redis_client=redis_mock,
        registry=AsyncMock(),
        workspace_manager=workspace_manager,
    )
    msg = AskAgentCommand(
        header=MessageHeader(
            message_id="m6",
            session_id="s6",
            trace_id="trace-6",
            target_agent_type="command_agent",
        ),
        content="hello command",
    )

    await worker._handle_message(msg)

    assert isinstance(observed["command"], AskAgentCommand)
