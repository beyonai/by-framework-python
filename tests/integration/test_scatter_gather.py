import json
from typing import List
from unittest.mock import AsyncMock, patch

import pytest

from by_framework.common.constants import (
    TASK_GROUP_FIELD_ABORTED,
    TASK_GROUP_FIELD_COMPLETED,
    TASK_GROUP_FIELD_JOIN_CLAIM_EXPIRES_AT,
    TASK_GROUP_FIELD_JOINED,
    TASK_GROUP_FIELD_TOTAL,
    TASK_GROUP_JOIN_CLAIM_TTL_MS,
    RedisKeys,
)
from by_framework.core.protocol.commands import (ResumeCommand, command_from_dict)
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.worker.context import AgentContext
from by_framework.worker.task_group import (
    TASK_GROUP_JOIN_PENDING_STATUS,
    TASK_GROUP_JOINED_STATUS,
    TaskGroupJoinState,
    TaskGroupStore,
)
from by_framework.worker.worker import GatewayWorker


class DummyWorker(GatewayWorker):

    def get_agent_types(self) -> List[str]:
        return ["dummy"]

    async def process_command(self, command, context):
        return {"status": "ok"}


class MockRedis:
    """Minimal in-memory Redis hash store for scatter-gather join tests."""

    def __init__(self, offline_agent_types=None):
        self.data = {}
        # Agent types with no online worker, so dispatch-time availability
        # checks reject them the way they would in a real deployment.
        self.offline_agent_types = set(offline_agent_types or ())
        self.streams = {}

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

    async def xadd(self, name, fields, **_kwargs):
        self.streams.setdefault(name, []).append(fields)
        return "0-1"

    async def eval(self, script, numkeys, *keys_and_args):
        if numkeys == 1:
            group_key, claim_field, claim_token, joined_field, expires_field = (
                keys_and_args
            )
            group = self.data.setdefault(group_key, {})
            if group.get(claim_field) != claim_token:
                return 0
            group[joined_field] = "1"
            group.pop(claim_field, None)
            group.pop(expires_field, None)
            return 1

        group_key, results_key, *args = keys_and_args
        (
            total_field,
            completed_field,
            aborted_field,
            result_field,
            result_json,
            ttl,
            joined_field,
            claim_field,
            expires_field,
            now_ms,
            claim_token,
            claim_ttl_ms,
        ) = args
        assert "HSETNX" in script
        assert int(ttl) > 0
        group = self.data.setdefault(group_key, {})
        total = int(group.get(total_field, 0))
        completed = int(group.get(completed_field, 0))
        if not total:
            return [0, completed, total]
        if group.get(aborted_field):
            return [1, completed, total]

        results = self.data.setdefault(results_key, {})
        if result_field not in results:
            results[result_field] = result_json
            completed += 1
            group[completed_field] = completed
        if completed < total:
            return [2, completed, total]
        if group.get(joined_field) == "1":
            return [5, completed, total]
        if group.get(claim_field) and int(group.get(expires_field, 0)) > int(now_ms):
            return [4, completed, total]
        group[claim_field] = claim_token
        group[expires_field] = int(now_ms) + int(claim_ttl_ms)
        return [3, completed, total]

    async def smembers(self, name):
        # Every agent type is treated as having one online worker unless the
        # test declared it offline, so dispatch-time availability checks pass
        # by default and can be made to fail on demand.
        for agent_type in self.offline_agent_types:
            if name == RedisKeys.agent_type_members(agent_type):
                return set()
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
    assert by_message_id[msg_b]["error"] == "boom"
    assert by_message_id[msg_b]["error_code"] is None
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


def _make_join_worker(redis, tmp_path, worker_id):
    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }
    return RecordingProcessCommandWorker(
        worker_id=worker_id,
        redis_client=redis,
        registry=AsyncMock(),
        workspace_manager=workspace_manager,
    )


def _make_caller_context(redis):
    return AgentContext(
        session_id="s1",
        trace_id="t1",
        redis_client=redis,
        current_agent_id="caller_agent",
        message_id="parent-msg",
    )


def _reply(*, task_group_id, task_message_id, source_agent_type, **kwargs):
    """A sub-agent's reply, shaped the way _enqueue_agent_return shapes it."""
    return ResumeCommand(
        header=MessageHeader(
            message_id="parent-msg",
            session_id="s1",
            trace_id="t1",
            source_agent_type=source_agent_type,
            target_agent_type="caller_agent",
            parent_message_id=task_message_id,
            task_group_id=task_group_id,
        ),
        **kwargs,
    )


@pytest.mark.asyncio
async def test_group_with_every_target_offline_still_resumes_caller(tmp_path):
    """A group whose only target is offline must resume the caller, not hang.

    Regression for the deadlock where the dispatcher itself counted the
    failure: `completed` reached `total` inside call_agents, where nothing
    knows how to resume anyone, so no reply was ever left to trigger Group
    Join and the caller stayed suspended forever.
    """
    redis = MockRedis(offline_agent_types={"agent-b"})
    worker = _make_join_worker(redis, tmp_path, "test-all-offline")
    caller_context = _make_caller_context(redis)

    dispatch_result = await caller_context.call_agents(
        tasks=[{"target_agent_type": "agent-b", "content": "one"}],
    )
    task_group_id = dispatch_result["task_group_id"]
    msg_b = dispatch_result["dispatched_tasks"][0]["message_id"]

    # Nothing was dispatched and the group is untouched until the worker
    # flushes the synthetic reply.
    assert dispatch_result["dispatched_tasks"][0]["status"] == "FAILED"
    group_hash = redis.data[RedisKeys.task_group(task_group_id)]
    assert group_hash.get(TASK_GROUP_FIELD_COMPLETED) == "0"

    await worker._flush_pending_group_replies(caller_context)

    # The flushed reply lands on the caller's own control stream, exactly
    # where a real sub-agent's return would have gone.
    caller_stream = redis.streams[RedisKeys.ctrl_stream("caller_agent")]
    assert len(caller_stream) == 1
    flushed = command_from_dict(json.loads(caller_stream[0]["data"]))
    await worker._handle_message(flushed)

    assert len(worker.received_commands) == 1
    aggregate = worker.received_commands[0].reply_data
    assert len(aggregate) == 1
    assert aggregate[0]["message_id"] == msg_b
    assert aggregate[0]["status"] == "FAILED"
    assert aggregate[0]["target_agent_type"] == "agent-b"
    assert aggregate[0]["reply_data"]["error_code"] == "AGENT_TYPE_UNAVAILABLE"
    assert aggregate[0]["error_code"] == "AGENT_TYPE_UNAVAILABLE"
    assert aggregate[0]["error"] == aggregate[0]["reply_data"]["error"]


@pytest.mark.asyncio
async def test_group_resumes_once_when_fast_reply_precedes_offline_sibling(tmp_path):
    """The second deadlock shape: a sibling replies before a later sibling
    fails its availability check, so the dispatcher's own counting would have
    been the one to complete the group."""
    redis = MockRedis(offline_agent_types={"agent-c"})
    worker = _make_join_worker(redis, tmp_path, "test-fast-then-offline")
    caller_context = _make_caller_context(redis)

    dispatch_result = await caller_context.call_agents(
        tasks=[
            {"target_agent_type": "agent-b", "content": "one"},
            {"target_agent_type": "agent-c", "content": "two"},
        ],
    )
    task_group_id = dispatch_result["task_group_id"]
    msg_b, msg_c = (t["message_id"] for t in dispatch_result["dispatched_tasks"])

    # agent-b's reply arrives first: 1/2, caller not resumed yet.
    await worker._handle_message(
        _reply(
            task_group_id=task_group_id,
            task_message_id=msg_b,
            source_agent_type="agent-b",
            status="COMPLETED",
            content="B result",
            reply_data={"value": "b"},
        )
    )
    assert worker.received_commands == []

    # Then the offline sibling's synthetic reply completes the group.
    await worker._flush_pending_group_replies(caller_context)
    flushed = command_from_dict(
        json.loads(redis.streams[RedisKeys.ctrl_stream("caller_agent")][0]["data"])
    )
    await worker._handle_message(flushed)

    assert len(worker.received_commands) == 1
    aggregate = worker.received_commands[0].reply_data
    assert [item["message_id"] for item in aggregate] == [msg_b, msg_c]
    assert aggregate[0]["status"] == "COMPLETED"
    assert aggregate[1]["status"] == "FAILED"


@pytest.mark.asyncio
async def test_group_aggregate_follows_dispatch_order_not_completion_order(tmp_path):
    """Aggregation order is the tasks' dispatch order, so callers can index
    results positionally instead of matching by hand."""
    redis = MockRedis()
    worker = _make_join_worker(redis, tmp_path, "test-order")
    caller_context = _make_caller_context(redis)

    dispatch_result = await caller_context.call_agents(
        tasks=[
            {"target_agent_type": "agent-b", "content": "one"},
            {"target_agent_type": "agent-c", "content": "two"},
            {"target_agent_type": "agent-d", "content": "three"},
        ],
    )
    task_group_id = dispatch_result["task_group_id"]
    msg_b, msg_c, msg_d = (t["message_id"] for t in dispatch_result["dispatched_tasks"])

    # Complete in reverse order.
    for msg_id, agent_type in (
        (msg_d, "agent-d"),
        (msg_c, "agent-c"),
        (msg_b, "agent-b"),
    ):
        await worker._handle_message(
            _reply(
                task_group_id=task_group_id,
                task_message_id=msg_id,
                source_agent_type=agent_type,
                status="COMPLETED",
                content=f"{agent_type} result",
            )
        )

    assert len(worker.received_commands) == 1
    resumed = worker.received_commands[0]
    assert [item["message_id"] for item in resumed.reply_data] == [msg_b, msg_c, msg_d]
    assert [item["target_agent_type"] for item in resumed.reply_data] == [
        "agent-b",
        "agent-c",
        "agent-d",
    ]
    # reply_data is the single aggregation channel; content must not also
    # carry whichever sibling replied last.
    assert resumed.content == ""


@pytest.mark.asyncio
async def test_group_join_counts_each_subtask_once_under_redelivery(tmp_path):
    redis = MockRedis()
    worker = _make_join_worker(redis, tmp_path, "test-redelivery")
    caller_context = _make_caller_context(redis)

    dispatch_result = await caller_context.call_agents(
        tasks=[
            {"target_agent_type": "agent-b", "content": "one"},
            {"target_agent_type": "agent-c", "content": "two"},
        ]
    )
    task_group_id = dispatch_result["task_group_id"]
    msg_b, msg_c = (task["message_id"] for task in dispatch_result["dispatched_tasks"])
    reply_b = _reply(
        task_group_id=task_group_id,
        task_message_id=msg_b,
        source_agent_type="agent-b",
        status="COMPLETED",
        reply_data={"value": "b"},
    )

    await worker._handle_message(reply_b)
    await worker._handle_message(reply_b)

    assert worker.received_commands == []
    assert (
        redis.data[RedisKeys.task_group(task_group_id)][TASK_GROUP_FIELD_COMPLETED] == 1
    )

    reply_c = _reply(
        task_group_id=task_group_id,
        task_message_id=msg_c,
        source_agent_type="agent-c",
        status="COMPLETED",
        reply_data={"value": "c"},
    )
    await worker._handle_message(reply_c)
    await worker._handle_message(reply_c)

    assert len(worker.received_commands) == 1
    assert [item["message_id"] for item in worker.received_commands[0].reply_data] == [
        msg_b,
        msg_c,
    ]
    assert (
        redis.data[RedisKeys.task_group(task_group_id)][TASK_GROUP_FIELD_COMPLETED] == 2
    )


@pytest.mark.asyncio
async def test_group_join_claim_can_be_recovered_after_owner_crash():
    redis = MockRedis()
    store = TaskGroupStore(redis)
    task_group_id = "tg-claim-recovery"
    await store.create(
        task_group_id,
        message_ids=["msg-b", "msg-c"],
        source_agent_type="caller-agent",
    )

    waiting = await store.record_reply(
        task_group_id,
        task_message_id="msg-b",
        result={"status": "COMPLETED"},
        now_ms=100,
        claim_token="worker-one",
    )
    ready = await store.record_reply(
        task_group_id,
        task_message_id="msg-c",
        result={"status": "COMPLETED"},
        now_ms=100,
        claim_token="worker-one",
    )
    still_claimed = await store.record_reply(
        task_group_id,
        task_message_id="msg-c",
        result={"status": "COMPLETED"},
        now_ms=100 + TASK_GROUP_JOIN_CLAIM_TTL_MS - 1,
        claim_token="worker-two",
    )
    recovered = await store.record_reply(
        task_group_id,
        task_message_id="msg-c",
        result={"status": "COMPLETED"},
        now_ms=100 + TASK_GROUP_JOIN_CLAIM_TTL_MS + 1,
        claim_token="worker-two",
    )

    assert waiting.state == TaskGroupJoinState.WAITING
    assert ready.state == TaskGroupJoinState.READY
    assert still_claimed.state == TaskGroupJoinState.CLAIMED
    assert recovered.state == TaskGroupJoinState.READY
    assert await store.mark_joined(task_group_id, recovered.claim_token) is True

    joined = await store.record_reply(
        task_group_id,
        task_message_id="msg-c",
        result={"status": "COMPLETED"},
        now_ms=100 + TASK_GROUP_JOIN_CLAIM_TTL_MS + 2,
        claim_token="worker-three",
    )
    assert joined.state == TaskGroupJoinState.JOINED


@pytest.mark.asyncio
async def test_group_join_does_not_ack_while_another_owner_holds_claim(tmp_path):
    redis = MockRedis()
    worker = _make_join_worker(redis, tmp_path, "test-claimed-redelivery")
    caller_context = _make_caller_context(redis)
    dispatch_result = await caller_context.call_agents(
        tasks=[
            {"target_agent_type": "agent-b", "content": "one"},
            {"target_agent_type": "agent-c", "content": "two"},
        ]
    )
    task_group_id = dispatch_result["task_group_id"]
    msg_b, msg_c = (task["message_id"] for task in dispatch_result["dispatched_tasks"])
    store = TaskGroupStore(redis)
    await store.record_reply(
        task_group_id,
        task_message_id=msg_b,
        result={"status": "COMPLETED"},
        claim_token="crashed-worker",
    )
    claimed = await store.record_reply(
        task_group_id,
        task_message_id=msg_c,
        result={"status": "COMPLETED"},
        claim_token="crashed-worker",
    )
    assert claimed.state == TaskGroupJoinState.READY

    reply_c = _reply(
        task_group_id=task_group_id,
        task_message_id=msg_c,
        source_agent_type="agent-c",
        status="COMPLETED",
    )
    pending = await worker._handle_message(reply_c)
    assert pending.status == TASK_GROUP_JOIN_PENDING_STATUS
    assert worker.received_commands == []

    # Simulate lease expiry after the original owner died. The same pending
    # Redis Stream entry can now be reclaimed and completes the caller once.
    redis.data[RedisKeys.task_group(task_group_id)][
        TASK_GROUP_FIELD_JOIN_CLAIM_EXPIRES_AT
    ] = 0
    await worker._handle_message(reply_c)
    assert len(worker.received_commands) == 1


@pytest.mark.asyncio
async def test_group_join_commits_terminal_caller_failure(tmp_path):
    redis = MockRedis()
    worker = _make_join_worker(redis, tmp_path, "test-terminal-failure")
    worker.process_command = AsyncMock(side_effect=ValueError("caller failed"))
    caller_context = _make_caller_context(redis)
    dispatch_result = await caller_context.call_agents(
        tasks=[
            {"target_agent_type": "agent-b", "content": "one"},
            {"target_agent_type": "agent-c", "content": "two"},
        ]
    )
    task_group_id = dispatch_result["task_group_id"]
    msg_b, msg_c = (task["message_id"] for task in dispatch_result["dispatched_tasks"])
    await worker._handle_message(
        _reply(
            task_group_id=task_group_id,
            task_message_id=msg_b,
            source_agent_type="agent-b",
            status="COMPLETED",
        )
    )
    reply_c = _reply(
        task_group_id=task_group_id,
        task_message_id=msg_c,
        source_agent_type="agent-c",
        status="COMPLETED",
    )

    failed = await worker._handle_message(reply_c)
    assert failed.status == "FAILED"
    assert (
        redis.data[RedisKeys.task_group(task_group_id)][TASK_GROUP_FIELD_JOINED] == "1"
    )

    duplicate = await worker._handle_message(reply_c)
    assert duplicate.status == TASK_GROUP_JOINED_STATUS
    assert worker.process_command.await_count == 1


@pytest.mark.asyncio
async def test_group_without_protocol_stamp_keeps_legacy_join(tmp_path):
    """A group created by a pre-v2 dispatcher — an old worker still running
    during a rolling upgrade — must be joined the old way: results keyed by
    the caller's own message_id, no aggregation, content left alone."""
    redis = MockRedis()
    worker = _make_join_worker(redis, tmp_path, "test-legacy")

    task_group_id = "tg-legacy01"
    group_key = RedisKeys.task_group(task_group_id)
    # Exactly what a pre-v2 dispatcher wrote: no protocol_version, no
    # task_order.
    await redis.hset(
        group_key,
        mapping={
            TASK_GROUP_FIELD_TOTAL: "1",
            TASK_GROUP_FIELD_COMPLETED: "0",
        },
    )

    await worker._handle_message(
        _reply(
            task_group_id=task_group_id,
            task_message_id="msg-b",
            source_agent_type="agent-b",
            status="COMPLETED",
            content="B result",
            reply_data={"value": "b"},
        )
    )

    assert len(worker.received_commands) == 1
    resumed = worker.received_commands[0]
    # Legacy behavior: the caller sees the single reply as-is.
    assert resumed.reply_data == {"value": "b"}
    assert resumed.content == "B result"
    # And the result was stored under the legacy (caller message_id) key.
    results = redis.data[RedisKeys.task_group_results(task_group_id)]
    assert set(results) == {"parent-msg"}


@pytest.mark.asyncio
async def test_group_join_logs_loudly_when_a_result_never_arrived(tmp_path):
    """A short result set resumes the caller but must never do so silently."""
    redis = MockRedis()
    worker = _make_join_worker(redis, tmp_path, "test-incomplete")
    caller_context = _make_caller_context(redis)

    dispatch_result = await caller_context.call_agents(
        tasks=[
            {"target_agent_type": "agent-b", "content": "one"},
            {"target_agent_type": "agent-c", "content": "two"},
        ],
    )
    task_group_id = dispatch_result["task_group_id"]
    msg_b, msg_c = (t["message_id"] for t in dispatch_result["dispatched_tasks"])

    # agent-c's result is lost (expired hash field, failed write, ...) but the
    # completion counter still reaches total.
    await worker._handle_message(
        _reply(
            task_group_id=task_group_id,
            task_message_id=msg_b,
            source_agent_type="agent-b",
            status="COMPLETED",
            content="B result",
        )
    )
    await redis.hincrby(RedisKeys.task_group(task_group_id), TASK_GROUP_FIELD_COMPLETED)
    # by-framework's logger sets propagate=False, so caplog's root handler
    # never sees these records; assert on the module logger directly.
    with patch("by_framework.worker.task_group.logger.error") as mock_error:
        aggregate = await TaskGroupStore(redis, worker_id=worker.worker_id).aggregate(
            task_group_id,
            total=2,
        )

    assert [item["message_id"] for item in aggregate] == [msg_b]
    assert mock_error.call_count == 1
    logged = mock_error.call_args.args
    assert "expected %d" in logged[0]
    assert msg_c in logged[-1]


@pytest.mark.asyncio
async def test_flush_is_skipped_when_process_command_raises(tmp_path):
    """A dispatch-time infrastructure failure aborts the group; the synthetic
    replies queued before it must never be delivered, or a late reply would
    resume a caller execution that already failed."""
    redis = MockRedis(offline_agent_types={"agent-b"})
    caller_context = _make_caller_context(redis)

    original_xadd = redis.xadd

    async def failing_xadd(name, fields):
        if name == RedisKeys.ctrl_stream("agent-c"):
            raise RuntimeError("stream unavailable")
        return await original_xadd(name, fields)

    redis.xadd = failing_xadd

    with pytest.raises(RuntimeError):
        await caller_context.call_agents(
            tasks=[
                {"target_agent_type": "agent-b", "content": "one"},
                {"target_agent_type": "agent-c", "content": "two"},
            ],
        )

    # agent-b's synthetic failure reply was built but never handed to the
    # context, so there is nothing for the worker to flush.
    assert caller_context.drain_pending_group_replies() == []

    # The group itself is marked aborted, so even the sibling that WAS sent
    # cannot resume the caller when its reply lands.
    group_hashes = [
        bucket for key, bucket in redis.data.items() if TASK_GROUP_FIELD_TOTAL in bucket
    ]
    assert len(group_hashes) == 1
    assert group_hashes[0][TASK_GROUP_FIELD_ABORTED] == "1"
