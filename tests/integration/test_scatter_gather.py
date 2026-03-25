from typing import List

import pytest

from byclaw_gateway_sdk.core.protocol.commands import ResumeCommand
from byclaw_gateway_sdk.core.protocol.message_header import MessageHeader
from byclaw_gateway_sdk.worker.worker import GatewayWorker


class DummyWorker(GatewayWorker):

    def get_capabilities(self) -> List[str]:
        return ["dummy"]

    async def process_command(self, command, context):
        return {"status": "ok"}


@pytest.mark.asyncio
async def test_persist_agent_return_state_scatter_gather(tmp_path):
    """Test that scatter-gather agent return states are persisted without overwriting each other."""
    worker = DummyWorker(worker_id="test_worker", redis_client=None)

    parent_id = "parent-123"
    group_id = "group-123"

    # Simulate B returning
    cmd_b = ResumeCommand(
        header=MessageHeader(
            message_id="msg-b",
            session_id="session-1",
            trace_id="trace-1",
            parent_message_id=parent_id,
            task_group_id=group_id,
        ),
        status="COMPLETED",
        content="B result",
    )

    # Simulate C returning
    cmd_c = ResumeCommand(
        header=MessageHeader(
            message_id="msg-c",
            session_id="session-1",
            trace_id="trace-1",
            parent_message_id=parent_id,
            task_group_id=group_id,
        ),
        status="COMPLETED",
        content="C result",
    )

    paths = {"public": str(tmp_path)}
    worker._persist_agent_return_state_sync(paths, cmd_b)
    worker._persist_agent_return_state_sync(paths, cmd_c)

    returns_dir = tmp_path / "session" / "agent_returns" / group_id
    assert returns_dir.exists()

    # Ensure they didn't overwrite each other
    assert (returns_dir / "msg-b.json").exists()
    assert (returns_dir / "msg-c.json").exists()

    content_b = (returns_dir / "msg-b.json").read_text()
    assert "B result" in content_b
    content_c = (returns_dir / "msg-c.json").read_text()
    assert "C result" in content_c
