from typing import List
from unittest.mock import AsyncMock

import pytest

from by_framework.common.constants import (
    TASK_GROUP_FIELD_ABORTED,
    TASK_GROUP_FIELD_COMPLETED,
    RedisKeys,
)
from by_framework.core.protocol.commands import ResumeCommand
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.worker.context import AgentContext
from by_framework.worker.worker import GatewayWorker


class DummyWorker(GatewayWorker):

    def get_agent_types(self) -> List[str]:
        return ["dummy"]

    async def process_command(self, command, context):
        return {"status": "ok"}


class MockRedis:
    """Minimal in-memory Redis hash store for scatter-gather join tests."""

    def __init__(self):
        self.data = {}

    async def hset(self, name, key=None, value=None, mapping=None):
        bucket = self.data.setdefault(name, {})
        if mapping:
            bucket.update(mapping)
        else:
            bucket[key] = value

    async def hget(self, name, key):
        return self.data.get(name, {}).get(key)

    async def hgetall(self, name):
        return dict(self.data.get(name, {}))

    async def hincrby(self, name, key, amount=1):
        bucket = self.data.setdefault(name, {})
        value = int(bucket.get(key, 0)) + amount
        bucket[key] = value
        return value

    async def expire(self, name, ttl):
        return 1

    async def xadd(self, name, fields):
        return "0-1"

    async def smembers(self, name):
        # Every agent type is treated as having one online worker, so
        # dispatch-time availability checks always pass in these tests.
        return {b"worker-1"}

    async def get(self, name):
        return b"1"

    def pipeline(self):
        return _MockPipeline(self)


class _MockPipeline:
    """Minimal pipeline shim: queues (method, args) and applies them on execute()."""

    def __init__(self, redis):
        self._redis = redis
        self._ops = []

    def __getattr__(self, name):

        def queue(*args, **kwargs):
            self._ops.append((name, args, kwargs))
            return self

        return queue

    async def execute(self):
        results = []
        for name, args, kwargs in self._ops:
            results.append(await getattr(self._redis, name)(*args, **kwargs))
        self._ops = []
        return results


class RecordingProcessCommandWorker(GatewayWorker):
    """Worker that records every command handed to process_command."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.received_commands = []

    def get_agent_types(self) -> List[str]:
        return ["caller_agent"]

    async def process_command(self, command, context):
        self.received_commands.append(command)
        return {"status": "ok"}


@pytest.mark.asyncio
async def test_persist_agent_return_state_scatter_gather(tmp_path):
    """Test that scatter-gather agent return states are persisted
    without overwriting each other."""
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


@pytest.mark.asyncio
async def test_group_join_delivers_aggregated_results_on_resume(tmp_path):
    """process_command is resumed with every sub-task's result once the
    Task Group completes, not with whichever reply happened to arrive last."""
    redis = MockRedis()
    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }

    worker = RecordingProcessCommandWorker(
        worker_id="test-join",
        redis_client=redis,
        registry=AsyncMock(),
        workspace_manager=workspace_manager,
    )

    caller_context = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=redis,
        current_agent_id="caller_agent",
        message_id="parent-msg",
    )
    dispatch_result = await caller_context.dispatch_group(
        tasks=[
            {"target_agent_type": "agent-b", "content": "task one"},
            {"target_agent_type": "agent-c", "content": "task two"},
        ],
    )
    task_group_id = dispatch_result["task_group_id"]
    msg_b, msg_c = (t["message_id"] for t in dispatch_result["dispatched_tasks"])

    # _enqueue_agent_return sets a reply's header.message_id to the caller's
    # own message_id (shared by every sibling reply in this Task Group) and
    # header.parent_message_id to the sub-task's own dispatch-time message_id
    # (msg_b/msg_c here, distinct per task) — mirror that real relationship,
    # not a hand-picked distinct message_id per reply, or this test can't
    # catch a Group Join bug that only shows up when siblings' replies
    # collide on the shared header.message_id.
    reply_b = ResumeCommand(
        header=MessageHeader(
            message_id="parent-msg",
            session_id="s1",
            trace_id="t1",
            source_agent_type="agent-b",
            target_agent_type="caller_agent",
            parent_message_id=msg_b,
            task_group_id=task_group_id,
        ),
        status="COMPLETED",
        content="B result",
        reply_data={"value": "b"},
    )
    await worker._handle_message(reply_b)
    assert worker.received_commands == []

    reply_c = ResumeCommand(
        header=MessageHeader(
            message_id="parent-msg",
            session_id="s1",
            trace_id="t1",
            source_agent_type="agent-c",
            target_agent_type="caller_agent",
            parent_message_id=msg_c,
            task_group_id=task_group_id,
        ),
        status="COMPLETED",
        content="C result",
        reply_data={"value": "c"},
    )
    await worker._handle_message(reply_c)

    assert len(worker.received_commands) == 1
    resumed = worker.received_commands[0]
    aggregate = resumed.reply_data
    assert isinstance(aggregate, list)
    assert len(aggregate) == 2

    by_message_id = {item["message_id"]: item for item in aggregate}
    assert by_message_id[msg_b]["target_agent_type"] == "agent-b"
    assert by_message_id[msg_b]["reply_data"] == {"value": "b"}
    assert by_message_id[msg_b]["status"] == "COMPLETED"
    assert by_message_id[msg_c]["target_agent_type"] == "agent-c"
    assert by_message_id[msg_c]["reply_data"] == {"value": "c"}


@pytest.mark.asyncio
async def test_group_join_delivers_aggregate_even_with_partial_failure(tmp_path):
    """A failed sub-task still completes the group; the caller sees both
    outcomes in the aggregate instead of the group hanging or erroring."""
    redis = MockRedis()
    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }

    worker = RecordingProcessCommandWorker(
        worker_id="test-join-partial",
        redis_client=redis,
        registry=AsyncMock(),
        workspace_manager=workspace_manager,
    )

    caller_context = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=redis,
        current_agent_id="caller_agent",
        message_id="parent-msg",
    )
    dispatch_result = await caller_context.dispatch_group(
        tasks=[
            {"target_agent_type": "agent-b", "content": "task one"},
            {"target_agent_type": "agent-c", "content": "task two"},
        ],
    )
    task_group_id = dispatch_result["task_group_id"]
    msg_b, msg_c = (t["message_id"] for t in dispatch_result["dispatched_tasks"])

    await worker._handle_message(
        ResumeCommand(
            header=MessageHeader(
                message_id="parent-msg",
                session_id="s1",
                trace_id="t1",
                source_agent_type="agent-b",
                target_agent_type="caller_agent",
                parent_message_id=msg_b,
                task_group_id=task_group_id,
            ),
            status="FAILED",
            reply_data={"error": "boom"},
        )
    )
    await worker._handle_message(
        ResumeCommand(
            header=MessageHeader(
                message_id="parent-msg",
                session_id="s1",
                trace_id="t1",
                source_agent_type="agent-c",
                target_agent_type="caller_agent",
                parent_message_id=msg_c,
                task_group_id=task_group_id,
            ),
            status="COMPLETED",
            reply_data={"value": "c"},
        )
    )

    assert len(worker.received_commands) == 1
    aggregate = worker.received_commands[0].reply_data
    by_message_id = {item["message_id"]: item for item in aggregate}
    assert by_message_id[msg_b]["status"] == "FAILED"
    assert by_message_id[msg_c]["status"] == "COMPLETED"


@pytest.mark.asyncio
async def test_collect_group_results_still_works_outside_process_command(tmp_path):
    """collect_group_results remains a valid manual-polling path, unchanged
    by the automatic aggregation delivered through resume."""
    redis = MockRedis()
    caller_context = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=redis,
        current_agent_id="caller_agent",
        message_id="parent-msg",
    )
    dispatch_result = await caller_context.dispatch_group(
        tasks=[{"target_agent_type": "agent-b", "content": "task one"}],
    )
    task_group_id = dispatch_result["task_group_id"]
    msg_b = dispatch_result["dispatched_tasks"][0]["message_id"]

    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }
    worker = RecordingProcessCommandWorker(
        worker_id="test-manual-poll",
        redis_client=redis,
        registry=AsyncMock(),
        workspace_manager=workspace_manager,
    )
    await worker._handle_message(
        ResumeCommand(
            header=MessageHeader(
                message_id="parent-msg",
                session_id="s1",
                trace_id="t1",
                source_agent_type="agent-b",
                target_agent_type="caller_agent",
                parent_message_id=msg_b,
                task_group_id=task_group_id,
            ),
            status="COMPLETED",
            reply_data={"value": "b"},
        )
    )

    results = await caller_context.collect_group_results(task_group_id, timeout=1.0)
    assert len(results) == 1
    assert results[0]["message_id"] == msg_b
    assert results[0]["reply_data"] == {"value": "b"}


@pytest.mark.asyncio
async def test_collect_group_results_times_out_with_partial_results(tmp_path):
    """A sub-task that never replies still lets manual polling time out
    and return whatever partial results were collected."""
    redis = MockRedis()
    caller_context = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=redis,
        current_agent_id="caller_agent",
        message_id="parent-msg",
    )
    dispatch_result = await caller_context.dispatch_group(
        tasks=[
            {"target_agent_type": "agent-b", "content": "task one"},
            {"target_agent_type": "agent-c", "content": "task two"},
        ],
    )
    task_group_id = dispatch_result["task_group_id"]
    msg_b = dispatch_result["dispatched_tasks"][0]["message_id"]

    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }
    worker = RecordingProcessCommandWorker(
        worker_id="test-timeout",
        redis_client=redis,
        registry=AsyncMock(),
        workspace_manager=workspace_manager,
    )
    # Only agent-b ever replies; agent-c never does.
    await worker._handle_message(
        ResumeCommand(
            header=MessageHeader(
                message_id="parent-msg",
                session_id="s1",
                trace_id="t1",
                source_agent_type="agent-b",
                target_agent_type="caller_agent",
                parent_message_id=msg_b,
                task_group_id=task_group_id,
            ),
            status="COMPLETED",
            reply_data={"value": "b"},
        )
    )

    results = await caller_context.collect_group_results(task_group_id, timeout=0.3)
    assert len(results) == 1
    assert results[0]["message_id"] == msg_b


@pytest.mark.asyncio
async def test_group_join_discards_reply_for_aborted_task_group(tmp_path):
    """A reply for a Task Group that was aborted mid-dispatch is dropped,
    never counted, and never used to resume the (already-failed) caller."""
    redis = MockRedis()
    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }
    worker = RecordingProcessCommandWorker(
        worker_id="test-abort",
        redis_client=redis,
        registry=AsyncMock(),
        workspace_manager=workspace_manager,
    )

    caller_context = AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=redis,
        current_agent_id="caller_agent",
        message_id="parent-msg",
    )
    dispatch_result = await caller_context.dispatch_group(
        tasks=[{"target_agent_type": "agent-b", "content": "one"}],
    )
    task_group_id = dispatch_result["task_group_id"]
    msg_b = dispatch_result["dispatched_tasks"][0]["message_id"]

    # Simulate a mid-dispatch failure (e.g. a later sibling's xadd raised)
    # that marked this group aborted after agent-b had already been sent.
    await redis.hset(RedisKeys.task_group(task_group_id), TASK_GROUP_FIELD_ABORTED, "1")

    await worker._handle_message(
        ResumeCommand(
            header=MessageHeader(
                message_id="parent-msg",
                session_id="s1",
                trace_id="t1",
                source_agent_type="agent-b",
                target_agent_type="caller_agent",
                parent_message_id=msg_b,
                task_group_id=task_group_id,
            ),
            status="COMPLETED",
            reply_data={"value": "b"},
        )
    )

    assert worker.received_commands == []
    group_hash = redis.data[RedisKeys.task_group(task_group_id)]
    assert group_hash.get(TASK_GROUP_FIELD_COMPLETED, "0") == "0"
