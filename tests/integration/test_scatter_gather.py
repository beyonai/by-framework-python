import json
from typing import List
from unittest.mock import AsyncMock

import pytest

from by_framework.common.constants import (
    TASK_GROUP_FIELD_ABORTED,
    TASK_GROUP_FIELD_COMPLETED,
    RedisKeys,
)
from by_framework.core.extensions.plugin import (
    AgentConfigsSnapshot,
    Plugin,
    PluginManifest,
)
from by_framework.core.extensions.registry import PluginRegistry
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.protocol.commands import AskAgentCommand, ResumeCommand
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.core.wait_index import member_from_resume, wait_index_key
from by_framework.worker.context import AgentContext
from by_framework.worker.runner import WorkerRunner
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
        self.zsets = {}
        self.strings = {}
        self.acked = []
        self.xadds = []

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

    async def xadd(self, name, fields, **kwargs):
        self.xadds.append((name, fields))
        return "0-1"

    async def smembers(self, name):
        # Every agent type is treated as having one online worker, so
        # dispatch-time availability checks always pass in these tests.
        return {b"worker-1"}

    async def get(self, name):
        return b"1"

    # --- wait index + idempotency gate ---------------------------------
    async def zadd(self, name, mapping):
        self.zsets.setdefault(name, {}).update(mapping)
        return len(mapping)

    async def zrem(self, name, *members):
        stored = self.zsets.get(name, {})
        return sum(1 for member in members if stored.pop(member, None) is not None)

    async def set(self, name, value, ex=None):  # pylint: disable=invalid-name
        self.strings[name] = value
        return True

    async def exists(self, *names):
        return sum(1 for name in names if name in self.strings)

    async def delete(self, name):
        self.strings.pop(name, None)
        self.data.pop(name, None)
        self.zsets.pop(name, None)

    async def xack(self, name, group, *ids):
        self.acked.append((name, group, ids))

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
async def test_duplicate_sibling_reply_cannot_overshoot_the_group_total(tmp_path):
    """Group Join counts replies, so a duplicated one is not merely
    redundant: it pushes `completed` past `total`, which makes the
    `completed < total` guard false a second time and aggregates (and wakes
    the caller) twice. The gate has to stop the duplicate upstream of the
    HINCRBY, which is why it lives in the runner rather than in the worker.
    """
    redis = MockRedis()
    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }

    worker = RecordingProcessCommandWorker(
        worker_id="test-join-duplicate",
        redis_client=redis,
        registry=AsyncMock(),
        workspace_manager=workspace_manager,
    )
    worker.registry.get_execution_by_message_id.return_value = {
        "execution_id": "exec-caller",
        "message_id": "parent-msg",
        "session_id": "s1",
        "parent_message_id": "",
        "source_agent_type": "",
        "task_group_id": "",
        "status": AgentState.WAITING_AGENT.value,
        "agent_configs_snapshot_key": "snapshot-key",
        "agent_configs_version": 1,
    }
    worker.registry.load_agent_configs_snapshot.return_value = AgentConfigsSnapshot(
        version=1, configs=()
    )
    runner = WorkerRunner(
        redis_client=redis,
        worker=worker,
        group_name="test_group",
        span_recorder=AsyncMock(),
    )
    runner._trace_writer = AsyncMock()

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

    def reply(child_message_id, agent_type, value):
        return ResumeCommand(
            header=MessageHeader(
                message_id="parent-msg",
                session_id="s1",
                trace_id="t1",
                source_agent_type=agent_type,
                target_agent_type="caller_agent",
                parent_message_id=child_message_id,
                task_group_id=task_group_id,
            ),
            status="COMPLETED",
            reply_data={"value": value},
        ).to_dict()

    stream = RedisKeys.ctrl_stream("caller_agent")
    await runner._process_message_from_dict(stream, "1-0", reply(msg_b, "agent-b", "b"))
    # The same reply again — a sweep-synthesized copy and the real one.
    await runner._process_message_from_dict(stream, "1-1", reply(msg_b, "agent-b", "b"))
    await runner._process_message_from_dict(stream, "1-2", reply(msg_c, "agent-c", "c"))

    group = redis.data[RedisKeys.task_group(task_group_id)]
    assert int(group[TASK_GROUP_FIELD_COMPLETED]) == 2
    assert len(worker.received_commands) == 1
    aggregate = worker.received_commands[0].reply_data
    assert {item["message_id"] for item in aggregate} == {msg_b, msg_c}


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


class OfflineAwareRedis(MockRedis):
    """MockRedis where only the named agent types have an online worker.

    MockRedis reports every agent type as online, which is what makes the
    dispatch-time availability rejection — the case a group member can never
    get a reply for — unreachable in the tests above.
    """

    def __init__(self, online=()):
        super().__init__()
        self.online = set(online)

    async def smembers(self, name):
        for agent_type in self.online:
            if name == RedisKeys.agent_type_members(agent_type):
                return {b"worker-1"}
        return set()


class GroupDispatchingWorker(GatewayWorker):
    """Fans out to `targets` on first contact, records what it is resumed with."""

    def __init__(self, targets, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.targets = targets
        self.dispatch_result = None
        self.resumed_with = []

    def get_agent_types(self) -> List[str]:
        return ["caller_agent"]

    async def process_command(self, command, context):
        if isinstance(command, ResumeCommand):
            self.resumed_with.append(command.reply_data)
            return {"status": AgentState.COMPLETED.value}
        self.dispatch_result = await context.call_agents(
            tasks=[
                {"target_agent_type": target, "content": "go"}
                for target in self.targets
            ],
        )
        return {"status": AgentState.QUEUED.value}


def _root_command(message_id="parent-msg"):
    return AskAgentCommand(
        header=MessageHeader(
            message_id=message_id,
            session_id="s1",
            trace_id="t1",
            target_agent_type="caller_agent",
        ),
        content="start",
    )


def _stand_ins(redis):
    """Replies the worker flushed onto the caller's own control stream."""
    stream = RedisKeys.ctrl_stream("caller_agent")
    return [
        ResumeCommand.from_dict(json.loads(fields["data"]))
        for name, fields in redis.xadds
        if name == stream
    ]


def _group_counters(redis, task_group_id):
    return dict(redis.data.get(RedisKeys.task_group(task_group_id), {}))


async def _dispatching_worker(redis, tmp_path, targets, plugin_registry=None):
    workspace_manager = AsyncMock()
    workspace_manager.setup_workspace.return_value = {
        "private": str(tmp_path),
        "public": str(tmp_path),
    }
    return GroupDispatchingWorker(
        targets,
        worker_id="test-unavailable",
        redis_client=redis,
        registry=AsyncMock(),
        workspace_manager=workspace_manager,
        plugin_registry=plugin_registry,
    )


@pytest.mark.asyncio
async def test_group_where_every_target_is_offline_still_resumes_the_caller(tmp_path):
    """Nothing was dispatched, so nothing will reply — and the dispatcher must
    still not close the group itself.

    Booking the failures inline fills `completed` to `total` inside the
    dispatch loop, at which point there is no reply left anywhere to trigger
    Group Join: the caller aggregates never, and stays suspended forever. The
    compensation therefore has to BE a reply.
    """
    redis = OfflineAwareRedis(online=())
    worker = await _dispatching_worker(redis, tmp_path, ["agent-b", "agent-c"])

    await worker._handle_message(_root_command())

    task_group_id = worker.dispatch_result["task_group_id"]
    # The dispatcher books nothing: no result, no increment.
    assert _group_counters(redis, task_group_id)[TASK_GROUP_FIELD_COMPLETED] == "0"
    assert redis.data.get(RedisKeys.task_group_results(task_group_id), {}) == {}

    stand_ins = _stand_ins(redis)
    assert len(stand_ins) == 2
    assert {reply.header.source_agent_type for reply in stand_ins} == {
        "agent-b",
        "agent-c",
    }

    for reply in stand_ins:
        await worker._handle_message(reply)

    counters = _group_counters(redis, task_group_id)
    assert counters[TASK_GROUP_FIELD_COMPLETED] == int(counters["total"])
    # Resumed exactly once, by the last stand-in, with the whole aggregate.
    assert len(worker.resumed_with) == 1
    aggregate = worker.resumed_with[0]
    assert len(aggregate) == 2
    assert {item["status"] for item in aggregate} == {AgentState.FAILED.value}
    assert {item["reply_data"]["error_code"] for item in aggregate} == {
        "AGENT_TYPE_UNAVAILABLE"
    }


@pytest.mark.asyncio
async def test_a_sibling_joined_mid_dispatch_leaves_a_reply_to_close_the_group(
    tmp_path,
):
    """The racy half of the same bug: a sibling replies fast.

    Nothing serialises Group Join against the dispatch loop — the reply lands
    on the caller's agent type's control stream and any worker may join it
    while the caller is still fanning out. If a later member's failure is
    booked inline, THAT increment reaches `total` and, again, no reply is left
    to run the join. The interleaving is forced here by joining agent-b's
    reply from the plugin hook that fires as agent-c is about to be dispatched.
    """
    redis = OfflineAwareRedis(online={"agent-b"})
    joined = {}

    class JoinSiblingMidDispatch(Plugin):
        """Joins agent-b's reply just before agent-c's dispatch is attempted."""

        def __init__(self):
            super().__init__(PluginManifest(plugin_id="join-mid-dispatch"))
            self.worker = None

        async def register_agent_configs(self, build_context):
            return None

        async def on_call_agent_start(self, context, command):
            if command.header.target_agent_type != "agent-c" or joined.get("done"):
                return
            joined["done"] = True
            await self.worker._handle_message(
                ResumeCommand(
                    header=MessageHeader(
                        message_id="parent-msg",
                        session_id="s1",
                        trace_id="t1",
                        source_agent_type="agent-b",
                        target_agent_type="caller_agent",
                        parent_message_id=joined["sibling"],
                        task_group_id=command.header.task_group_id,
                    ),
                    status=AgentState.COMPLETED.value,
                    reply_data={"value": "b"},
                )
            )

        async def on_call_agent_complete(self, context, command, result):
            if command.header.target_agent_type == "agent-b":
                joined["sibling"] = command.header.message_id

    plugin = JoinSiblingMidDispatch()
    registry = PluginRegistry()
    registry.register_bundle(plugin)
    worker = await _dispatching_worker(
        redis, tmp_path, ["agent-b", "agent-c"], plugin_registry=registry
    )
    plugin.worker = worker

    await worker._handle_message(_root_command())

    task_group_id = worker.dispatch_result["task_group_id"]
    # agent-b's reply was counted mid-dispatch; the group is one short and the
    # caller has not been resumed.
    assert _group_counters(redis, task_group_id)[TASK_GROUP_FIELD_COMPLETED] == 1
    assert worker.resumed_with == []

    stand_ins = _stand_ins(redis)
    assert len(stand_ins) == 1
    await worker._handle_message(stand_ins[0])

    counters = _group_counters(redis, task_group_id)
    assert counters[TASK_GROUP_FIELD_COMPLETED] == int(counters["total"])
    assert len(worker.resumed_with) == 1
    by_agent = {item["target_agent_type"]: item for item in worker.resumed_with[0]}
    assert by_agent["agent-b"]["status"] == AgentState.COMPLETED.value
    assert by_agent["agent-c"]["status"] == AgentState.FAILED.value


@pytest.mark.asyncio
async def test_offline_member_stand_in_passes_the_idempotency_gate(tmp_path):
    """The stand-in is a reply like any other, so it goes through the gate.

    call_agents registers a wait-index entry for the member it could not
    dispatch, so the stand-in CLAIMS that entry rather than being waved
    through as unregistered — which is what lets a sweep compensate the same
    member if the stand-in is never delivered, without the two colliding.
    """
    redis = OfflineAwareRedis(online=())
    worker = await _dispatching_worker(redis, tmp_path, ["agent-b"])
    worker.registry.get_execution_by_message_id.return_value = {
        "execution_id": "exec-caller",
        "message_id": "parent-msg",
        "session_id": "s1",
        "parent_message_id": "",
        "source_agent_type": "",
        "task_group_id": "",
        "status": AgentState.WAITING_AGENT.value,
        "agent_configs_snapshot_key": "snapshot-key",
        "agent_configs_version": 1,
    }
    worker.registry.load_agent_configs_snapshot.return_value = AgentConfigsSnapshot(
        version=1, configs=()
    )
    runner = WorkerRunner(
        redis_client=redis,
        worker=worker,
        group_name="test_group",
        span_recorder=AsyncMock(),
    )
    runner._trace_writer = AsyncMock()

    await worker._handle_message(_root_command())
    stand_in = _stand_ins(redis)[0]
    member = member_from_resume(stand_in)
    assert member in redis.zsets[wait_index_key("s1")]

    stream = RedisKeys.ctrl_stream("caller_agent")
    await runner._process_message_from_dict(stream, "1-0", stand_in.to_dict())
    assert len(worker.resumed_with) == 1
    assert member not in redis.zsets[wait_index_key("s1")]

    # A second copy (a sweep's, say) is recognized as a duplicate and dropped
    # instead of aggregating the group again.
    await runner._process_message_from_dict(stream, "1-1", stand_in.to_dict())
    assert len(worker.resumed_with) == 1
