"""Tests for Byai worker command decoding behavior."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from by_framework.core.protocol.byai_command import (
    ByaiAskAgentCommand,
    ByaiResumeCommand,
)
from by_framework.core.protocol.commands import AskAgentCommand, ResumeCommand
from by_framework.core.protocol.message import (BaiYingMessage, BaiYingMessageRole)
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.worker.byai_context import ByaiAgentContext
from by_framework.worker.byai_worker import ByaiWorker


def _build_worker_dependencies(tmp_path):
    redis_mock = AsyncMock()
    redis_mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }
    return redis_mock, workspace_manager


@pytest.mark.asyncio
async def test_byai_worker_process_command_receives_decoded_message(tmp_path):
    redis_mock, workspace_manager = _build_worker_dependencies(tmp_path)
    observed = {}

    class DemoWorker(ByaiWorker):

        def get_agent_types(self):
            return ["demo"]

        async def process_command(self, command, context):
            observed["command"] = command
            observed["context"] = context
            return {"ok": True}

    worker = DemoWorker(
        worker_id="demo-worker",
        redis_client=redis_mock,
        registry=AsyncMock(),
        workspace_manager=workspace_manager,
    )

    msg = AskAgentCommand(
        header=MessageHeader(
            message_id="m1",
            session_id="s1",
            trace_id="t1",
            target_agent_type="demo",
        ),
        content=[{"role": "user", "content": "hello"}],
    )

    await worker._handle_message(msg)

    assert isinstance(observed["command"], ByaiAskAgentCommand)
    assert isinstance(observed["command"].content, BaiYingMessage)
    assert observed["command"].content.role == BaiYingMessageRole.USER
    assert observed["command"].content.content == "hello"
    assert isinstance(observed["context"], ByaiAgentContext)


@pytest.mark.asyncio
async def test_byai_worker_resume_command_receives_decoded_message(tmp_path):
    redis_mock, workspace_manager = _build_worker_dependencies(tmp_path)
    observed = {}

    class DemoWorker(ByaiWorker):

        def get_agent_types(self):
            return ["demo"]

        async def process_command(self, command, context):
            observed["command"] = command
            return {"ok": True}

    worker = DemoWorker(
        worker_id="demo-worker",
        redis_client=redis_mock,
        registry=AsyncMock(),
        workspace_manager=workspace_manager,
    )

    msg = ResumeCommand(
        header=MessageHeader(
            message_id="m2",
            session_id="s2",
            trace_id="t2",
            target_agent_type="demo",
        ),
        content=[{"role": "assistant", "content": "done"}],
        status="SUCCESS",
    )

    await worker._handle_message(msg)

    assert isinstance(observed["command"], ByaiResumeCommand)
    assert isinstance(observed["command"].content, BaiYingMessage)
    assert observed["command"].content.role == BaiYingMessageRole.ASSISTANT
    assert observed["command"].content.content == "done"
