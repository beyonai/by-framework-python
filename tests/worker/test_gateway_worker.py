import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from by_framework import (
    AgentConfig,
    AgentTaskResult,
    GatewayWorker,
    PluginRegistry,
    RunningExecution,
)
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.protocol.commands import AskAgentCommand, ResumeCommand
from by_framework.core.protocol.content_type import SseMessageType
from by_framework.core.protocol.message_header import MessageHeader


class DummyWorker(GatewayWorker):

    def get_agent_types(self):
        return []

    async def process_command(self, command, context):
        pass


class RecordingWorker(GatewayWorker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_command = None

    def get_agent_types(self):
        return ["recording_agent"]

    async def process_command(self, command, context):
        self.last_command = command
        return {"ok": True}


class SnapshotInspectWorker(GatewayWorker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_agent_ids = None
        self.seen_agent_configs_version = None

    def get_agent_types(self):
        return ["recording_agent"]

    async def process_command(self, command, context):
        self.seen_agent_ids = [config.agent_id for config in context.agent_configs]
        self.seen_agent_configs_version = context.agent_configs_version
        return {"ok": True}


class CustomLayoutBuilder:

    def build(self, content, role, content_type, source_agent_type, **kwargs):
        return {
            "content": content,
            "content_type": content_type,
            "agent": source_agent_type,
            "message_id": kwargs["order_id"],
        }


class StructuredResultWorker(GatewayWorker):

    def get_agent_types(self):
        return ["structured_agent"]

    async def process_command(self, command, context):
        return AgentTaskResult(
            status=AgentState.COMPLETED.value,
            content="structured content",
            reply_data={"answer": 42},
            metadata={"tokens": 123, "caller": "overridden"},
            extra_payload={"debug_id": "dbg-1"},
        )


def test_worker_persist_metadata(tmp_path):
    """Test that _persist_agent_return_state_sync correctly persists
    command metadata to disk."""
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
async def test_worker_agent_task_result_maps_to_resume_callback(tmp_path):
    redis_mock = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.xadd = MagicMock()
    mock_pipe.expire = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    redis_mock.pipeline = MagicMock(return_value=mock_pipe)

    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }

    worker = StructuredResultWorker(
        worker_id="test-structured",
        redis_client=redis_mock,
        registry=AsyncMock(),
        workspace_manager=workspace_manager,
    )
    msg = AskAgentCommand(
        header=MessageHeader(
            message_id="child-msg",
            session_id="sess-structured",
            trace_id="trace-structured",
            source_agent_type="agent-a",
            target_agent_type="structured_agent",
            parent_message_id="parent-msg",
            metadata={"caller": "original", "request_id": "req-1"},
        ),
        content="hello",
    )

    result = await worker._handle_message(msg)

    assert result.status == AgentState.COMPLETED.value
    args, _ = redis_mock.xadd.call_args
    raw = json.loads(args[1]["data"])
    callback = ResumeCommand.from_dict(raw)
    assert callback.status == AgentState.COMPLETED.value
    assert callback.content == "structured content"
    assert callback.reply_data == {"answer": 42}
    assert callback.extra_payload == {"debug_id": "dbg-1"}
    assert callback.header.metadata == {
        "caller": "overridden",
        "request_id": "req-1",
        "tokens": 123,
    }


@pytest.mark.asyncio
async def test_worker_resume_message_round_trips_as_resume_command(tmp_path):
    """Test that a ResumeCommand is correctly handled and stored as a
    ResumeCommand on the worker."""
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

        def get_agent_types(self):
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
async def test_worker_passes_layout_builder_to_agent_context():
    redis_mock = AsyncMock()
    mock_pipe = MagicMock()
    mock_pipe.xadd = MagicMock()
    mock_pipe.expire = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[])
    redis_mock.pipeline = MagicMock(return_value=mock_pipe)
    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": "/tmp",
        "public": "/tmp/public",
    }

    class LayoutInspectWorker(GatewayWorker):

        def get_agent_types(self):
            return ["layout_agent"]

        async def process_command(self, command, context):
            await context.emit_chunk("custom-layout")
            return {"ok": True}

    worker = LayoutInspectWorker(
        worker_id="test-layout",
        redis_client=redis_mock,
        workspace_manager=workspace_manager,
        layout_builder=CustomLayoutBuilder(),
    )
    command = AskAgentCommand(
        header=MessageHeader(
            message_id="msg-layout",
            session_id="sess-layout",
            trace_id="trace-layout",
            target_agent_type="layout_agent",
        ),
        content="hello",
    )

    await worker._handle_message(command)

    args, _ = mock_pipe.xadd.call_args_list[0]
    raw = json.loads(args[1]["data"])
    assert raw["data"] == {
        "content": "custom-layout",
        "content_type": SseMessageType.text.value,
        "agent": "layout_agent",
        "message_id": "msg-layout",
    }


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

        def get_agent_types(self):
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

    assert result.status == "FAILED"


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

        def get_agent_types(self):
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


@pytest.mark.asyncio
async def test_worker_persists_agent_configs_snapshot_for_new_execution(tmp_path):
    """Test that a new execution snapshots the latest registry configs."""
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }
    plugin_registry = PluginRegistry()
    plugin_registry._set_agent_configs([AgentConfig(agent_id="agent_v1")])  # pylint: disable=protected-access
    registry = AsyncMock()
    registry.persist_agent_configs_snapshot.return_value = "snapshot-key-1"

    worker = SnapshotInspectWorker(
        worker_id="test-snapshot-persist",
        redis_client=redis_mock,
        registry=registry,
        workspace_manager=workspace_manager,
        plugin_registry=plugin_registry,
    )
    msg = AskAgentCommand(
        header=MessageHeader(
            message_id="m7",
            session_id="s7",
            trace_id="trace-7",
            target_agent_type="recording_agent",
        ),
        content="persist snapshot",
    )
    execution = RunningExecution(
        execution_id="exec-7",
        message_id="m7",
        session_id="s7",
        worker_id="test-snapshot-persist",
        task=AsyncMock(),
        cancel_event=AsyncMock(),
    )

    await worker._handle_message(msg, execution=execution)

    registry.persist_agent_configs_snapshot.assert_awaited_once()
    persisted_snapshot = registry.persist_agent_configs_snapshot.await_args.args[1]
    assert persisted_snapshot.version == 1
    assert [config.agent_id for config in persisted_snapshot.configs] == ["agent_v1"]
    registry.update_execution_fields.assert_awaited_once_with(
        "exec-7",
        "s7",
        agent_configs_version=1,
        agent_configs_snapshot_key="snapshot-key-1",
    )


@pytest.mark.asyncio
async def test_worker_persists_agent_configs_snapshot_when_execution_suspends(tmp_path):
    """Test that suspended executions reuse their request-bound snapshot."""
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }
    plugin_registry = PluginRegistry()
    plugin_registry._set_agent_configs([AgentConfig(agent_id="agent_v1")])  # pylint: disable=protected-access
    registry = AsyncMock()
    registry.persist_agent_configs_snapshot.return_value = "snapshot-key-10"

    class SuspendedWorker(SnapshotInspectWorker):

        async def process_command(self, command, context):
            self.seen_agent_ids = [config.agent_id for config in context.agent_configs]
            self.seen_agent_configs_version = context.agent_configs_version
            context._is_suspended = True  # pylint: disable=protected-access
            return {"status": "WAITING_USER"}

    worker = SuspendedWorker(
        worker_id="test-snapshot-suspended",
        redis_client=redis_mock,
        registry=registry,
        workspace_manager=workspace_manager,
        plugin_registry=plugin_registry,
    )
    msg = AskAgentCommand(
        header=MessageHeader(
            message_id="m10",
            session_id="s10",
            trace_id="trace-10",
            target_agent_type="recording_agent",
        ),
        content="suspend snapshot",
    )
    execution = RunningExecution(
        execution_id="exec-10",
        message_id="m10",
        session_id="s10",
        worker_id="test-snapshot-suspended",
        task=AsyncMock(),
        cancel_event=AsyncMock(),
    )

    result = await worker._handle_message(msg, execution=execution)

    assert result.status == "WAITING_USER"
    registry.persist_agent_configs_snapshot.assert_awaited_once()
    persisted_snapshot = registry.persist_agent_configs_snapshot.await_args.args[1]
    assert persisted_snapshot.version == 1
    assert [config.agent_id for config in persisted_snapshot.configs] == ["agent_v1"]
    registry.update_execution_fields.assert_awaited_once_with(
        "exec-10",
        "s10",
        agent_configs_version=1,
        agent_configs_snapshot_key="snapshot-key-10",
    )


@pytest.mark.asyncio
async def test_worker_restores_persisted_agent_configs_snapshot_for_resumed_execution(
    tmp_path,
):
    """Test that resumed execution uses the persisted snapshot instead of latest."""
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }
    plugin_registry = PluginRegistry()
    plugin_registry._set_agent_configs([AgentConfig(agent_id="agent_v1")])  # pylint: disable=protected-access
    persisted_snapshot = plugin_registry.get_agent_configs_snapshot()
    plugin_registry._set_agent_configs([AgentConfig(agent_id="agent_v2")])  # pylint: disable=protected-access

    registry = AsyncMock()
    registry.load_agent_configs_snapshot.return_value = persisted_snapshot
    worker = SnapshotInspectWorker(
        worker_id="test-snapshot-restore",
        redis_client=redis_mock,
        registry=registry,
        workspace_manager=workspace_manager,
        plugin_registry=plugin_registry,
    )
    msg = ResumeCommand(
        header=MessageHeader(
            message_id="m8",
            session_id="s8",
            trace_id="trace-8",
            target_agent_type="recording_agent",
        ),
        status="SUCCESS",
        reply_data={"answer": 1},
    )
    execution = RunningExecution(
        execution_id="exec-8",
        message_id="m8",
        session_id="s8",
        worker_id="test-snapshot-restore",
        task=AsyncMock(),
        cancel_event=AsyncMock(),
        parent_message_id="parent-8",
        is_resumed=True,
        existing_data={
            "agent_configs_snapshot_key": "snapshot-key-8",
            "agent_configs_version": persisted_snapshot.version,
        },
    )

    await worker._handle_message(msg, execution=execution)

    registry.load_agent_configs_snapshot.assert_awaited_once_with("snapshot-key-8")
    assert worker.seen_agent_ids == ["agent_v1"]
    assert worker.seen_agent_configs_version == persisted_snapshot.version


@pytest.mark.asyncio
async def test_worker_logs_context_when_persisted_snapshot_is_missing(tmp_path):
    """Test snapshot restore failures emit contextual error logs."""
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }
    registry = AsyncMock()
    registry.load_agent_configs_snapshot.return_value = None
    worker = SnapshotInspectWorker(
        worker_id="test-snapshot-missing",
        redis_client=redis_mock,
        registry=registry,
        workspace_manager=workspace_manager,
        plugin_registry=PluginRegistry(),
    )
    msg = ResumeCommand(
        header=MessageHeader(
            message_id="m9",
            session_id="s9",
            trace_id="trace-9",
            target_agent_type="recording_agent",
        ),
        status="SUCCESS",
        reply_data={"answer": 9},
    )
    execution = RunningExecution(
        execution_id="exec-9",
        message_id="m9",
        session_id="s9",
        worker_id="test-snapshot-missing",
        task=AsyncMock(),
        cancel_event=AsyncMock(),
        is_resumed=True,
        existing_data={
            "agent_configs_snapshot_key": "snapshot-key-9",
            "agent_configs_version": 7,
        },
    )

    with patch("by_framework.worker.worker.logger.error") as mock_logger_error:
        with pytest.raises(
            RuntimeError,
            match="Persisted agent config snapshot not found: snapshot-key-9",
        ):
            await worker._handle_message(msg, execution=execution)

    logged_args = mock_logger_error.call_args.args
    assert "snapshot restore failed" in logged_args[0]
    assert logged_args[1] == "test-snapshot-missing"
    assert logged_args[2] == "exec-9"
    assert logged_args[3] == "s9"
    assert logged_args[4] == "m9"
    assert logged_args[5] == "snapshot-key-9"
