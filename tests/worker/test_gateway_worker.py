import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from by_framework import (
    AgentConfig,
    AgentContext,
    AgentTaskResult,
    GatewayWorker,
    PluginRegistry,
    RunningExecution,
)
from by_framework.common.constants import RedisKeys
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


def test_worker_agent_return_langfuse_parent_uses_context_trace_parent():
    header = MessageHeader(
        message_id="child-msg",
        session_id="sess-structured",
        trace_id="trace-structured",
        target_agent_type="structured_agent",
        metadata={"langfuse_parent_observation_id": "metadata-parent"},
        langfuse_parent_observation_id="header-parent",
    )
    context = AgentContext(
        session_id="sess-structured",
        trace_id="trace-structured",
        redis_client=object(),
        current_agent_id="structured_agent",
    )
    context.set_trace_parent_observation_id("context-agent-task")

    assert (
        GatewayWorker._agent_return_langfuse_parent_id(header, context)
        == "context-agent-task"
    )


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
            trace_parent_span_id="parent-span-1",
            langfuse_parent_observation_id="parent-observation-1",
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
    assert callback.header.metadata["caller"] == "overridden"
    assert callback.header.metadata["request_id"] == "req-1"
    assert callback.header.metadata["tokens"] == 123
    assert callback.header.metadata["framework_parent_span_id"] == (
        "child-msg:agent.return"
    )
    assert callback.header.metadata["trace_parent_span_id"] == (
        callback.header.trace_parent_span_id
    )
    assert callback.header.trace_parent_span_id != "parent-span-1"
    assert callback.header.langfuse_parent_observation_id == "parent-observation-1"


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
async def test_worker_received_message_log_uses_header_trace_id(tmp_path):
    """Received-message logs should show the propagated trace id."""
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
        worker_id="test-log-trace",
        redis_client=redis_mock,
        registry=AsyncMock(),
        workspace_manager=workspace_manager,
    )
    msg = ResumeCommand(
        header=MessageHeader(
            message_id="m-log",
            session_id="s-log",
            trace_id="trace-from-header",
            target_agent_type="recording_agent",
        ),
        status="SUCCESS",
        reply_data={"answer": 42},
    )

    with patch("by_framework.worker.worker.logger.info") as info_mock:
        await worker._handle_message(msg)

    received_calls = [
        call
        for call in info_mock.call_args_list
        if call.args and call.args[0] == "[%s] Received message: %s (Trace: %s)"
    ]
    assert received_calls
    assert received_calls[0].args[3] == "trace-from-header"


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
            observed["worker_id"] = getattr(context, "worker_id", "")
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
    assert observed["worker_id"] == "test-inspect"


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
    registry.update_execution_fields.assert_awaited_once()
    args = registry.update_execution_fields.await_args.args
    kwargs = registry.update_execution_fields.await_args.kwargs
    assert args == ("exec-7", "s7")
    assert kwargs["agent_configs_version"] == 1
    assert kwargs["agent_configs_snapshot_key"] == "snapshot-key-1"
    assert kwargs["agent_config_audit"]["target_agent_type"] == "recording_agent"
    assert kwargs["agent_config_audit"]["target_agent_registered"] is False
    assert kwargs["agent_config_audit"]["target_agent_config"]["agent_id"] == (
        "recording_agent"
    )
    assert kwargs["agent_config_audit"]["target_agent_config"]["registered"] is False


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
    registry.update_execution_fields.assert_awaited_once()
    args = registry.update_execution_fields.await_args.args
    kwargs = registry.update_execution_fields.await_args.kwargs
    assert args == ("exec-10", "s10")
    assert kwargs["agent_configs_version"] == 1
    assert kwargs["agent_configs_snapshot_key"] == "snapshot-key-10"
    assert kwargs["agent_config_audit"]["target_agent_type"] == "recording_agent"
    assert kwargs["agent_config_audit"]["target_agent_registered"] is False
    assert kwargs["agent_config_audit"]["target_agent_config"]["agent_id"] == (
        "recording_agent"
    )
    assert kwargs["agent_config_audit"]["target_agent_config"]["registered"] is False


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


def _single_call_dispatch(task_group_id=""):
    return AskAgentCommand(
        header=MessageHeader(
            message_id="child-msg",
            session_id="sess-persist",
            trace_id="trace-persist",
            source_agent_type="agent-a",
            target_agent_type="structured_agent",
            parent_message_id="parent-msg",
            task_group_id=task_group_id,
            metadata={"request_id": "req-1"},
        ),
        content="hello",
    )


def _make_persist_worker(tmp_path, redis_mock):
    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }
    return StructuredResultWorker(
        worker_id="test-persist",
        redis_client=redis_mock,
        registry=AsyncMock(),
        workspace_manager=workspace_manager,
    )


@pytest.mark.asyncio
async def test_worker_persists_single_call_result_before_replying(tmp_path):
    """A single call_agent's result must survive its reply message.

    mark_execution_finished() only stores error fields, so without this the
    answer exists nowhere once the reply is lost. The copy reuses the Task
    Group results Hash as a group of size one, and must be byte-compatible
    with what the group-join path writes.
    """
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    worker = _make_persist_worker(tmp_path, redis_mock)

    await worker._handle_message(_single_call_dispatch())

    results_key = RedisKeys.task_group_results("tg-single-child-msg")
    hset_calls = [
        c for c in redis_mock.hset.await_args_list if c.args[0] == results_key
    ]
    assert len(hset_calls) == 1
    field, raw = hset_calls[0].args[1], hset_calls[0].args[2]
    assert field == "child-msg"
    stored = json.loads(raw)

    callback = ResumeCommand.from_dict(
        json.loads(redis_mock.xadd.call_args.args[1]["data"])
    )
    assert stored == {
        "status": callback.status,
        "reply_data": callback.reply_data,
        "content": callback.content,
        "target_agent_type": callback.header.source_agent_type,
        "metadata": callback.header.metadata,
        "extra_payload": callback.extra_payload,
    }
    assert stored["status"] == AgentState.COMPLETED.value
    assert stored["reply_data"] == {"answer": 42}
    assert stored["target_agent_type"] == "structured_agent"
    redis_mock.expire.assert_any_await(results_key, 86400)


@pytest.mark.asyncio
async def test_worker_result_persist_failure_does_not_block_callback(tmp_path):
    """Losing the recovery copy only costs recoverability — the reply, which
    is the only thing that resumes the caller, must still be sent."""
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    redis_mock.hset = AsyncMock(side_effect=RuntimeError("redis down"))
    worker = _make_persist_worker(tmp_path, redis_mock)

    result = await worker._handle_message(_single_call_dispatch())

    assert result.status == AgentState.COMPLETED.value
    callback = ResumeCommand.from_dict(
        json.loads(redis_mock.xadd.call_args.args[1]["data"])
    )
    assert callback.status == AgentState.COMPLETED.value
    assert callback.header.target_agent_type == "agent-a"


@pytest.mark.asyncio
async def test_worker_does_not_write_single_call_result_for_group_member(tmp_path):
    """A Task Group member's result is stored by the group-join path keyed
    by the group id — writing a second copy here would fork the accounting."""
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    worker = _make_persist_worker(tmp_path, redis_mock)

    await worker._handle_message(_single_call_dispatch(task_group_id="tg-abc"))

    single_key = RedisKeys.task_group_results("tg-single-child-msg")
    assert not [c for c in redis_mock.hset.await_args_list if c.args[0] == single_key]
    assert redis_mock.xadd.await_count == 1


class SuspendingWorker(GatewayWorker):
    """Stands in for a middle-of-chain agent: dispatches and unwinds."""

    def get_agent_types(self):
        return ["middle_agent"]

    async def process_command(self, command, context):
        context._is_suspended = True  # pylint: disable=protected-access
        context._suspended_state = AgentState.WAITING_AGENT.value  # pylint: disable=protected-access
        return {"status": AgentState.QUEUED.value}


def _worker_with_workspace(cls, tmp_path, redis_mock, **kwargs):
    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }
    return cls(
        worker_id="test-suspend",
        redis_client=redis_mock,
        registry=AsyncMock(),
        workspace_manager=workspace_manager,
        **kwargs,
    )


def _mock_redis():
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    return redis_mock


def _resumable_worker(cls, tmp_path, redis_mock):
    """Worker plus the execution fields a resumed execution must carry.

    A resume restores its agent-config snapshot from the execution record
    (and fails loudly without one), so any resume test has to supply both.
    """
    plugin_registry = PluginRegistry()
    plugin_registry._set_agent_configs([AgentConfig(agent_id="agent_v1")])  # pylint: disable=protected-access
    snapshot = plugin_registry.get_agent_configs_snapshot()
    worker = _worker_with_workspace(
        cls, tmp_path, redis_mock, plugin_registry=plugin_registry
    )
    worker.registry.load_agent_configs_snapshot.return_value = snapshot
    return worker, {
        "agent_configs_snapshot_key": "snapshot-key",
        "agent_configs_version": snapshot.version,
    }


def _ctrl_stream_replies(redis_mock, agent_type):
    return [
        ResumeCommand.from_dict(json.loads(call.args[1]["data"]))
        for call in redis_mock.xadd.await_args_list
        if call.args[0] == RedisKeys.ctrl_stream(agent_type)
    ]


@pytest.mark.asyncio
async def test_suspended_sub_agent_does_not_reply_to_its_caller(tmp_path):
    """A suspended execution has no result yet.

    Replying with the value the handler returned so it could unwind wakes the
    caller early AND burns the single reply it was waiting for — the real
    result then has nothing left to deliver it with.
    """
    redis_mock = _mock_redis()
    worker = _worker_with_workspace(SuspendingWorker, tmp_path, redis_mock)

    result = await worker._handle_message(
        AskAgentCommand(
            header=MessageHeader(
                message_id="msg-b",
                session_id="sess-chain",
                trace_id="trace-chain",
                source_agent_type="agent-a",
                target_agent_type="middle_agent",
                parent_message_id="msg-a",
            ),
            content="do it",
        )
    )

    assert _ctrl_stream_replies(redis_mock, "agent-a") == []
    assert result.status == AgentState.WAITING_AGENT.value


@pytest.mark.asyncio
async def test_suspended_execution_persists_as_waiting_agent(tmp_path):
    """The status the framework persists comes from why it suspended, not from
    whatever non-terminal value the handler happened to return."""
    redis_mock = _mock_redis()
    worker = _worker_with_workspace(SuspendingWorker, tmp_path, redis_mock)

    result = await worker._handle_message(
        AskAgentCommand(
            header=MessageHeader(
                message_id="msg-root",
                session_id="sess-chain",
                trace_id="trace-chain",
                target_agent_type="middle_agent",
            ),
            content="do it",
        )
    )

    assert result.status == AgentState.WAITING_AGENT.value


class TransferringWorker(SuspendingWorker):
    """Suspends the context but returns a terminal status anyway."""

    async def process_command(self, command, context):
        context._is_suspended = True  # pylint: disable=protected-access
        context._suspended_state = (  # pylint: disable=protected-access
            AgentState.WAITING_AGENT.value
        )
        return {"status": AgentState.COMPLETED.value}


@pytest.mark.asyncio
async def test_terminal_business_status_wins_over_suspension(tmp_path):
    """A handler that reached a terminal state is finished, whatever it
    dispatched along the way."""
    redis_mock = _mock_redis()
    worker = _worker_with_workspace(TransferringWorker, tmp_path, redis_mock)

    result = await worker._handle_message(
        AskAgentCommand(
            header=MessageHeader(
                message_id="msg-root",
                session_id="sess-chain",
                trace_id="trace-chain",
                target_agent_type="middle_agent",
            ),
            content="do it",
        )
    )

    assert result.status == AgentState.COMPLETED.value


@pytest.mark.asyncio
async def test_terminal_status_despite_suspension_still_replies(tmp_path):
    """The other half of the rule above, and it has to be the same half.

    Terminal wins for the persisted status, so nothing will ever resume this
    execution to deliver the reply later. Skipping the reply on the suspension
    flag alone would leave the caller parked on a reply that provably cannot
    come — until a sweep bails it out, which is a fallback, not a design.
    """
    redis_mock = _mock_redis()
    worker = _worker_with_workspace(TransferringWorker, tmp_path, redis_mock)

    await worker._handle_message(
        AskAgentCommand(
            header=MessageHeader(
                message_id="msg-b",
                session_id="sess-chain",
                trace_id="trace-chain",
                source_agent_type="agent-a",
                target_agent_type="middle_agent",
                parent_message_id="msg-a",
            ),
            content="do it",
        )
    )

    replies = _ctrl_stream_replies(redis_mock, "agent-a")
    assert len(replies) == 1
    assert replies[0].status == AgentState.COMPLETED.value


@pytest.mark.asyncio
async def test_resumed_execution_replies_to_its_own_caller_not_the_sub_agent(tmp_path):
    """The resume that wakes B describes C, the hop below it.

    Routing B's reply off that header sends B's result back down to C and
    leaves A — the agent actually waiting — with nothing. The caller has to
    come from the execution record the original dispatch wrote.
    """
    redis_mock = _mock_redis()
    # No group tracker for B's own group, so the join path stays out of the way.
    redis_mock.hget = AsyncMock(return_value=None)
    worker, snapshot_fields = _resumable_worker(
        StructuredResultWorker, tmp_path, redis_mock
    )
    resume_from_c = ResumeCommand(
        header=MessageHeader(
            message_id="msg-b",  # B's own id: what B is reattached by
            session_id="sess-chain",
            trace_id="trace-chain",
            source_agent_type="agent-c",  # the sub-agent that just finished
            target_agent_type="structured_agent",
            parent_message_id="msg-c",  # B's sub-task
            task_group_id="tg-of-c",  # B's OWN group, not the one B belongs to
        ),
        status=AgentState.COMPLETED.value,
        reply_data={"from": "c"},
    )
    execution = RunningExecution(
        execution_id="exec-b",
        message_id="msg-b",
        session_id="sess-chain",
        worker_id="test-suspend",
        task=AsyncMock(),
        cancel_event=AsyncMock(),
        is_resumed=True,
        parent_message_id="msg-a",
        existing_data={
            "execution_id": "exec-b",
            "message_id": "msg-b",
            "session_id": "sess-chain",
            "status": AgentState.WAITING_AGENT.value,
            "source_agent_type": "agent-a",
            "parent_message_id": "msg-a",
            "task_group_id": "tg-of-a",
            **snapshot_fields,
        },
    )

    await worker._handle_message(resume_from_c, execution=execution)

    assert _ctrl_stream_replies(redis_mock, "agent-c") == []
    replies = _ctrl_stream_replies(redis_mock, "agent-a")
    assert len(replies) == 1
    reply = replies[0]
    # A reattaches its suspended execution by message_id, and Group Join keys
    # results by parent_message_id — both must describe the A->B hop.
    assert reply.header.message_id == "msg-a"
    assert reply.header.parent_message_id == "msg-b"
    assert reply.header.task_group_id == "tg-of-a"
    assert reply.header.source_agent_type == "structured_agent"
    assert reply.reply_data == {"answer": 42}


@pytest.mark.asyncio
async def test_resumed_root_execution_replies_to_nobody(tmp_path):
    """A root execution's record names no caller — inventing one from the
    resume header would send its final answer to its own sub-agent."""
    redis_mock = _mock_redis()
    worker, snapshot_fields = _resumable_worker(
        StructuredResultWorker, tmp_path, redis_mock
    )
    execution = RunningExecution(
        execution_id="exec-root",
        message_id="msg-root",
        session_id="sess-chain",
        worker_id="test-suspend",
        task=AsyncMock(),
        cancel_event=AsyncMock(),
        is_resumed=True,
        existing_data={
            "source_agent_type": "",
            "parent_message_id": "",
            **snapshot_fields,
        },
    )

    await worker._handle_message(
        ResumeCommand(
            header=MessageHeader(
                message_id="msg-root",
                session_id="sess-chain",
                trace_id="trace-chain",
                source_agent_type="agent-b",
                target_agent_type="structured_agent",
                parent_message_id="msg-b",
            ),
            status=AgentState.COMPLETED.value,
            reply_data={"from": "b"},
        ),
        execution=execution,
    )

    assert _ctrl_stream_replies(redis_mock, "agent-b") == []


@pytest.mark.asyncio
async def test_failed_suspended_execution_still_replies_to_its_caller(tmp_path):
    """Suspension only defers a reply while the execution can still produce
    one. A crash means no resume is coming, so the caller must be told now."""

    class FailingAfterDispatchWorker(GatewayWorker):

        def get_agent_types(self):
            return ["middle_agent"]

        async def process_command(self, command, context):
            context._is_suspended = True  # pylint: disable=protected-access
            context._suspended_state = (  # pylint: disable=protected-access
                AgentState.WAITING_AGENT.value
            )
            raise RuntimeError("boom")

    redis_mock = _mock_redis()
    worker = _worker_with_workspace(FailingAfterDispatchWorker, tmp_path, redis_mock)

    await worker._handle_message(
        AskAgentCommand(
            header=MessageHeader(
                message_id="msg-b",
                session_id="sess-chain",
                trace_id="trace-chain",
                source_agent_type="agent-a",
                target_agent_type="middle_agent",
                parent_message_id="msg-a",
            ),
            content="do it",
        )
    )

    replies = _ctrl_stream_replies(redis_mock, "agent-a")
    assert len(replies) == 1
    assert replies[0].status == AgentState.FAILED.value
