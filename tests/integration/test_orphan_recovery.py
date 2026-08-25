"""Suspended callers that would otherwise wait forever.

Everything here starts from the same shape: a caller dispatched a sub-task
with ``wait_for_reply=True``, ended its own execution, and the reply it is
parked on is never going to arrive on its own. `WaitIndexSweeper` is the only
thing left that can move it, so these tests pin what it must and — just as
importantly — must NOT resolve.

The waits are registered with ``reply_timeout_ms=0`` so they are due
immediately; the deadline arithmetic itself is not what is under test here.
"""

import asyncio
import json
import time
import unittest
from typing import Any

from by_framework import GatewayWorker, RedisKeys, WorkerRegistry, WorkerRunner
from by_framework.common.constants import (
    TASK_GROUP_FIELD_ABORTED,
    LivenessErrorCode,
    single_call_task_group_id,
)
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.protocol.commands import AskAgentCommand, ResumeCommand
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.core.wait_index import encode_member, wait_index_key
from by_framework.core.wait_sweeper import (
    OUTCOME_ASK_USER_SKIPPED,
    OUTCOME_CALLER_TERMINAL,
    OUTCOME_CHILD_ALIVE,
    OUTCOME_CHILD_WAITING,
    OUTCOME_GROUP_ABORTED,
    OUTCOME_RECOVERED,
    OUTCOME_TIMED_OUT,
    OUTCOME_WORKER_LOST,
    WaitIndexSweeper,
)


class FakePipeline:

    def __init__(self, redis):
        self.redis = redis
        self.commands = []

    def xadd(self, name, fields, maxlen=None, approximate=True):
        self.commands.append(("xadd", name, fields))
        return self

    def hset(self, name, key, value):
        self.commands.append(("hset", name, key, value))
        return self

    def hdel(self, name, *keys):
        self.commands.append(("hdel", name, keys))
        return self

    def hincrby(self, name, key, amount=1):
        self.commands.append(("hincrby", name, key, amount))
        return self

    def zadd(self, name, mapping):
        self.commands.append(("zadd", name, mapping))
        return self

    def zrem(self, name, *values):
        self.commands.append(("zrem", name, values))
        return self

    def expire(self, name, ttl):
        self.commands.append(("expire", name, ttl))
        return self

    async def execute(self):
        for command in self.commands:
            op, name, rest = command[0], command[1], command[2:]
            if op == "xadd":
                await self.redis.xadd(name, rest[0])
            elif op == "hset":
                await self.redis.hset(name, key=rest[0], value=rest[1])
            elif op == "hdel":
                await self.redis.hdel(name, *rest[0])
            elif op == "hincrby":
                await self.redis.hincrby(name, rest[0], rest[1])
            elif op == "zadd":
                await self.redis.zadd(name, rest[0])
            elif op == "zrem":
                await self.redis.zrem(name, *rest[0])
            elif op == "expire":
                await self.redis.expire(name, rest[0])
        return []


class FakeRedis:
    """In-memory Redis covering the surface this flow touches.

    The real `WorkerRegistry` and `WorkerRunner` run against it, because the
    execution records and worker leases the sweep interrogates are written by
    them — stubbing the registry would stub away the evidence under test.
    """

    def __init__(self):
        self.streams: dict[str, list] = {}
        # Consumption pops from `streams`; keep an append-only log of
        # everything ever published for assertions.
        self.published: dict[str, list] = {}
        self.hashes: dict[str, dict] = {}
        self.sets: dict[str, set] = {}
        self.zsets: dict[str, dict] = {}
        self.kv: dict[str, Any] = {}

    # --- streams ---
    async def xgroup_create(self, name, groupname, id="0", mkstream=False):
        pass

    async def xadd(self, name, fields, maxlen=None, approximate=True):
        entries = self.streams.setdefault(name, [])
        log = self.published.setdefault(name, [])
        msg_id = f"{len(log) + 1}-0".encode()
        entry = (
            msg_id,
            {
                (k.encode() if isinstance(k, str) else k): (
                    v.encode() if isinstance(v, str) else v
                )
                for k, v in fields.items()
            },
        )
        entries.append(entry)
        log.append(entry)
        return msg_id

    async def xreadgroup(self, groupname, consumername, streams, count=1, block=0):
        results = []
        for name in streams:
            entries = self.streams.get(name)
            if entries:
                results.append((name.encode(), [entries.pop(0)]))
        return results

    async def xack(self, name, groupname, *ids):
        return len(ids)

    # --- hashes / sets / zsets / kv ---
    async def hset(self, name, key=None, value=None, mapping=None):
        # Argument order mirrors redis-py's: callers pass (name, key, value)
        # positionally, and a fake that swaps them silently drops the write.
        bucket = self.hashes.setdefault(name, {})
        if mapping:
            bucket.update(mapping)
        else:
            bucket[key] = value

    async def hget(self, name, key):
        return self.hashes.get(name, {}).get(key)

    async def hgetall(self, name):
        return dict(self.hashes.get(name, {}))

    async def hincrby(self, name, key, amount=1):
        bucket = self.hashes.setdefault(name, {})
        bucket[key] = int(bucket.get(key, 0)) + amount
        return bucket[key]

    async def hdel(self, name, *keys):
        bucket = self.hashes.get(name, {})
        return sum(1 for key in keys if bucket.pop(key, None) is not None)

    async def sadd(self, name, value):
        self.sets.setdefault(name, set()).add(value)
        return 1

    async def smembers(self, name):
        return self.sets.get(name, set())

    async def sismember(self, name, value):
        return value in self.sets.get(name, set())

    async def zadd(self, name, mapping):
        self.zsets.setdefault(name, {}).update(mapping)

    async def zrem(self, name, *values):
        bucket = self.zsets.get(name, {})
        return sum(1 for value in values if bucket.pop(value, None) is not None)

    async def zremrangebyscore(self, name, min_score, max_score):
        bucket = self.zsets.get(name, {})
        doomed = [k for k, v in bucket.items() if min_score <= v <= max_score]
        for key in doomed:
            bucket.pop(key)
        return len(doomed)

    async def zrangebyscore(
        self, name, min_score, max_score, start=None, num=None, withscores=False
    ):
        bucket = self.zsets.get(name, {})
        upper = float("inf") if max_score == "+inf" else max_score
        items = sorted(
            ((k, v) for k, v in bucket.items() if min_score <= v <= upper),
            key=lambda kv: kv[1],
        )
        if start is not None or num is not None:
            begin = start or 0
            items = items[begin : begin + num] if num is not None else items[begin:]
        return items if withscores else [k for k, _ in items]

    async def zrevrange(self, name, start, end):
        items = sorted(
            self.zsets.get(name, {}).items(), key=lambda kv: kv[1], reverse=True
        )
        selected = items[start:] if end == -1 else items[start : end + 1]
        return [k for k, _ in selected]

    async def set(self, name, value, nx=False, ex=None):
        if nx and name in self.kv:
            return False
        self.kv[name] = value
        return True

    async def get(self, name):
        return self.kv.get(name)

    async def exists(self, name):
        return 1 if name in self.kv else 0

    async def eval(self, script, numkeys, *args):
        """Just enough Lua to run the token-verified lock scripts.

        Dispatches on what the script does rather than executing it, but
        keeps the semantics the callers rely on: a release only succeeds for
        the holder of the token.
        """
        key, token = args[0], args[1]
        raw = self.kv.get(key)
        if "DEL" not in script:
            raise NotImplementedError(script)
        if raw is None:
            return 1 if token == "" else 0
        if token == "":
            self.kv.pop(key, None)
            return 1
        try:
            stored = json.loads(raw).get("token")
        except (TypeError, ValueError):
            return 0
        if stored != token:
            return 0
        self.kv.pop(key, None)
        return 1

    async def delete(self, name):
        self.kv.pop(name, None)
        self.hashes.pop(name, None)
        self.sets.pop(name, None)
        self.zsets.pop(name, None)

    async def expire(self, name, ttl):
        return 1

    def pipeline(self):
        return FakePipeline(self)


class WorkspaceManagerStub:

    async def setup_workspace(
        self, session_id, task_id, user_code="default", agent_id=""
    ):
        return {"public": "/tmp/pub", "private": "/tmp/priv"}

    async def cleanup_task(self, session_id, task_id, user_code="default", agent_id=""):
        return None


class CallerAgent(GatewayWorker):
    """Dispatches `next_agent_type` on first contact, answers on resume."""

    def __init__(self, agent_type, next_agent_type, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.agent_type = agent_type
        self.next_agent_type = next_agent_type
        self.resume_payloads: list[Any] = []
        self.resume_metadata: list[dict] = []

    def get_agent_types(self) -> list[str]:
        return [self.agent_type]

    async def process_command(self, command, context: Any):
        if isinstance(command, ResumeCommand):
            self.resume_payloads.append(command.reply_data)
            self.resume_metadata.append(dict(command.header.metadata))
            return {
                "status": AgentState.COMPLETED.value,
                "reply_data": {"from": self.agent_type, "sub": command.reply_data},
            }
        await context.call_agent(
            target_agent_type=self.next_agent_type,
            content="delegate",
            wait_for_reply=True,
            reply_timeout_ms=0,
        )
        return {"status": AgentState.QUEUED.value}


class HangingAgent(GatewayWorker):
    """Picks a task up and never finishes it — a worker about to be killed."""

    def __init__(self, agent_type, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.agent_type = agent_type
        self.started = asyncio.Event()

    def get_agent_types(self) -> list[str]:
        return [self.agent_type]

    async def process_command(self, command, context: Any):
        self.started.set()
        await asyncio.Event().wait()  # pragma: no cover - cancelled in teardown
        return {"status": AgentState.COMPLETED.value}


class AnsweringAgent(GatewayWorker):

    def __init__(self, agent_type, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.agent_type = agent_type

    def get_agent_types(self) -> list[str]:
        return [self.agent_type]

    async def process_command(self, command, context: Any):
        return {
            "status": AgentState.COMPLETED.value,
            "reply_data": {"from": self.agent_type, "answer": 42},
        }


class GroupCallerAgent(GatewayWorker):
    """Fans out to several agents at once and waits for the whole group."""

    def __init__(self, agent_type, targets, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.agent_type = agent_type
        self.targets = targets
        self.resume_payloads: list[Any] = []

    def get_agent_types(self) -> list[str]:
        return [self.agent_type]

    async def process_command(self, command, context: Any):
        if isinstance(command, ResumeCommand):
            self.resume_payloads.append(command.reply_data)
            return {"status": AgentState.COMPLETED.value}
        await context.call_agents(
            [
                {"target_agent_type": target, "content": "delegate"}
                for target in self.targets
            ],
            wait_for_reply=True,
            reply_timeout_ms=0,
        )
        return {"status": AgentState.QUEUED.value}


class AskingAgent(GatewayWorker):
    """Suspends on a human instead of on an agent."""

    def __init__(self, agent_type, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.agent_type = agent_type

    def get_agent_types(self) -> list[str]:
        return [self.agent_type]

    async def process_command(self, command, context: Any):
        if isinstance(command, ResumeCommand):
            return {
                "status": AgentState.COMPLETED.value,
                "reply_data": {"from": self.agent_type, "user_said": command.content},
            }
        await context.ask_user("are you there?", reply_timeout_ms=0)
        return {"status": AgentState.QUEUED.value}


def _ctrl_messages(redis, agent_type):
    return [
        json.loads(entry[1][b"data"].decode())
        for entry in redis.published.get(RedisKeys.ctrl_stream(agent_type), [])
    ]


def _replies(redis, agent_type):
    return [
        ResumeCommand.from_dict(message)
        for message in _ctrl_messages(redis, agent_type)
        if message["action_type"] == ResumeCommand.action_type
    ]


def _dispatched_message_id(redis, agent_type):
    return _ctrl_messages(redis, agent_type)[0]["header"]["message_id"]


def _just_due_ms():
    """A deadline that has only just passed.

    Deliberately not 0: every real score is a writer's own clock plus a
    non-negative offset, so an epoch-0 entry is not "due" — it is one the
    sweep's prune half deletes as abandoned before triage ever sees it.
    """
    return int(time.time() * 1000) - 1000


class OrphanRecoveryTestBase(unittest.IsolatedAsyncioTestCase):

    session_id = "sess-orphan"

    async def asyncSetUp(self):
        self.redis = FakeRedis()
        self.registry = WorkerRegistry(self.redis)
        self.workspace = WorkspaceManagerStub()
        self.runners: dict[str, WorkerRunner] = {}
        self.sweeper = WaitIndexSweeper(
            self.redis,
            worker_id="worker-sweeper",
            registry=self.registry,
            enabled=True,
        )

    async def asyncTearDown(self):
        for runner in self.runners.values():
            for task in list(runner._running_tasks):
                task.cancel()
            await asyncio.gather(*runner._running_tasks, return_exceptions=True)

    async def _register(self, worker: GatewayWorker):
        runner = WorkerRunner(self.redis, worker, group_name="test-group")
        await runner.setup_streams()
        for agent_type in worker.get_agent_types():
            self.runners[agent_type] = runner
            await self.redis.sadd(
                RedisKeys.agent_type_members(agent_type), worker.worker_id
            )
        await self.redis.set(RedisKeys.worker_online_lease(worker.worker_id), "1")
        return runner

    async def _seed_root_message(self, agent_type: str, message_id: str = "msg-root"):
        await self.redis.xadd(
            RedisKeys.ctrl_stream(agent_type),
            AskAgentCommand(
                header=MessageHeader(
                    message_id=message_id,
                    session_id=self.session_id,
                    trace_id="trace-orphan",
                    target_agent_type=agent_type,
                ),
                content="start",
            ).to_redis_payload(),
        )

    async def _step(self, agent_type: str):
        runner = self.runners[agent_type]
        await runner._run_once()
        await runner.wait_for_tasks()

    async def _start_and_hang(self, agent: HangingAgent):
        """Let a worker claim a task, then leave it mid-flight."""
        runner = self.runners[agent.agent_type]
        await runner._run_once()
        await asyncio.wait_for(agent.started.wait(), timeout=1)

    async def _kill_worker(self, worker_id: str):
        """What a `kill -9` leaves behind: a RUNNING record, no lease."""
        self.redis.kv.pop(RedisKeys.worker_online_lease(worker_id), None)

    def _wait_entries(self):
        return dict(self.redis.zsets.get(wait_index_key(self.session_id), {}))

    async def _execution(self, message_id: str):
        return await self.registry.get_execution_by_message_id(
            message_id, session_id=self.session_id
        )


class TestDeadChildWorker(OrphanRecoveryTestBase):
    """The callee's worker dies mid-task."""

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.agent_a = CallerAgent(
            "agent-a", "agent-b", "worker-a", self.redis, self.registry, self.workspace
        )
        self.agent_b = HangingAgent(
            "agent-b", "worker-b", self.redis, self.registry, self.workspace
        )
        await self._register(self.agent_a)
        await self._register(self.agent_b)
        await self._seed_root_message("agent-a")

    async def test_caller_is_woken_with_child_worker_lost(self):
        await self._step("agent-a")
        await self._start_and_hang(self.agent_b)
        b_dispatch = _dispatched_message_id(self.redis, "agent-b")
        await self._kill_worker("worker-b")

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_WORKER_LOST), 1)
        replies = _replies(self.redis, "agent-a")
        self.assertEqual(len(replies), 1)
        reply = replies[0]
        # Addressed exactly like a real reply: the caller's own message_id is
        # what its suspended execution is reattached by, and the sub-task's id
        # is what the wait-index gate rebuilds the entry from.
        self.assertEqual(reply.header.message_id, "msg-root")
        self.assertEqual(reply.header.parent_message_id, b_dispatch)
        self.assertEqual(reply.status, AgentState.FAILED.value)
        self.assertEqual(
            reply.reply_data["error_code"], LivenessErrorCode.CHILD_WORKER_LOST
        )
        self.assertEqual(reply.reply_data["child_message_id"], b_dispatch)

        await self._step("agent-a")

        self.assertEqual(len(self.agent_a.resume_payloads), 1)
        self.assertEqual(
            self.agent_a.resume_payloads[0]["error_code"],
            LivenessErrorCode.CHILD_WORKER_LOST,
        )
        self.assertEqual(
            self.agent_a.resume_metadata[0]["synthesized_by"], "wait_sweeper"
        )

    async def test_sweeper_leaves_the_wait_entry_for_the_gate_to_claim(self):
        await self._step("agent-a")
        await self._start_and_hang(self.agent_b)
        await self._kill_worker("worker-b")

        await self.sweeper.sweep_once()
        # Still registered: the entry is the token the reply competes for, so
        # clearing it here would mean the synthesized reply is the one copy
        # nothing can arbitrate.
        self.assertEqual(len(self._wait_entries()), 1)

        await self._step("agent-a")
        self.assertEqual(self._wait_entries(), {})

    async def test_a_real_reply_arriving_late_cannot_wake_the_caller_twice(self):
        await self._step("agent-a")
        await self._start_and_hang(self.agent_b)
        b_dispatch = _dispatched_message_id(self.redis, "agent-b")
        await self._kill_worker("worker-b")
        await self.sweeper.sweep_once()
        await self._step("agent-a")

        # The worker was not as dead as it looked and eventually replies.
        await self.redis.xadd(
            RedisKeys.ctrl_stream("agent-a"),
            ResumeCommand(
                header=MessageHeader(
                    message_id="msg-root",
                    session_id=self.session_id,
                    trace_id="trace-orphan",
                    source_agent_type="agent-b",
                    target_agent_type="agent-a",
                    parent_message_id=b_dispatch,
                ),
                status=AgentState.COMPLETED.value,
                reply_data={"from": "agent-b"},
            ).to_redis_payload(),
        )
        await self._step("agent-a")

        self.assertEqual(len(self.agent_a.resume_payloads), 1)


class TestLostReplyRecovery(OrphanRecoveryTestBase):
    """The callee finished; only its reply message was lost."""

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.agent_a = CallerAgent(
            "agent-a", "agent-b", "worker-a", self.redis, self.registry, self.workspace
        )
        self.agent_b = AnsweringAgent(
            "agent-b", "worker-b", self.redis, self.registry, self.workspace
        )
        await self._register(self.agent_a)
        await self._register(self.agent_b)
        await self._seed_root_message("agent-a")

    async def test_caller_gets_the_stored_answer_not_a_failure(self):
        await self._step("agent-a")
        await self._step("agent-b")
        b_dispatch = _dispatched_message_id(self.redis, "agent-b")

        # Lose the reply in transit, after the callee stored its result.
        self.redis.streams[RedisKeys.ctrl_stream("agent-a")].clear()
        stored = await self.redis.hget(
            RedisKeys.task_group_results(single_call_task_group_id(b_dispatch)),
            b_dispatch,
        )
        self.assertIsNotNone(stored)

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_RECOVERED), 1)
        await self._step("agent-a")

        # What is lost is a message, not an answer: the caller resumes with
        # the callee's real payload and status.
        self.assertEqual(
            self.agent_a.resume_payloads, [{"from": "agent-b", "answer": 42}]
        )
        recovered = _replies(self.redis, "agent-a")[-1]
        self.assertEqual(recovered.status, AgentState.COMPLETED.value)
        self.assertEqual(
            recovered.header.metadata["liveness_error_code"],
            LivenessErrorCode.REPLY_LOST_RECOVERED,
        )


class TestHealthyChildIsNotKilled(OrphanRecoveryTestBase):
    """A slow-but-alive callee must survive its deadline."""

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.agent_a = CallerAgent(
            "agent-a", "agent-b", "worker-a", self.redis, self.registry, self.workspace
        )
        self.agent_b = HangingAgent(
            "agent-b", "worker-b", self.redis, self.registry, self.workspace
        )
        await self._register(self.agent_a)
        await self._register(self.agent_b)
        await self._seed_root_message("agent-a")

    async def test_live_lease_renews_instead_of_failing(self):
        await self._step("agent-a")
        await self._start_and_hang(self.agent_b)
        before = self._wait_entries()

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_CHILD_ALIVE), 1)
        self.assertEqual(_replies(self.redis, "agent-a"), [])
        after = self._wait_entries()
        self.assertEqual(set(after), set(before))
        member = next(iter(before))
        self.assertGreater(after[member], before[member])


class TestNestedChainPropagation(OrphanRecoveryTestBase):
    """A -> B -> C, C's worker dies. Failure must climb, not cascade."""

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.agent_a = CallerAgent(
            "agent-a", "agent-b", "worker-a", self.redis, self.registry, self.workspace
        )
        self.agent_b = CallerAgent(
            "agent-b", "agent-c", "worker-b", self.redis, self.registry, self.workspace
        )
        self.agent_c = HangingAgent(
            "agent-c", "worker-c", self.redis, self.registry, self.workspace
        )
        await self._register(self.agent_a)
        await self._register(self.agent_b)
        await self._register(self.agent_c)
        await self._seed_root_message("agent-a")

    async def test_failure_travels_up_one_hop_at_a_time(self):
        await self._step("agent-a")
        await self._step("agent-b")
        await self._start_and_hang(self.agent_c)
        c_dispatch = _dispatched_message_id(self.redis, "agent-c")
        await self._kill_worker("worker-c")

        outcomes = await self.sweeper.sweep_once()

        # Only the innermost wait fails. B is suspended on C, so A's wait on B
        # is renewed — timing out both would report a fabricated cause at the
        # top and lose the actual one.
        self.assertEqual(outcomes.get(OUTCOME_WORKER_LOST), 1)
        self.assertEqual(outcomes.get(OUTCOME_CHILD_WAITING), 1)
        self.assertEqual(_replies(self.redis, "agent-a"), [])
        replies_to_b = _replies(self.redis, "agent-b")
        self.assertEqual(len(replies_to_b), 1)
        self.assertEqual(
            replies_to_b[0].reply_data["error_code"],
            LivenessErrorCode.CHILD_WORKER_LOST,
        )
        self.assertEqual(replies_to_b[0].reply_data["child_message_id"], c_dispatch)

        await self._step("agent-b")  # B resumes and answers A itself
        await self._step("agent-a")

        self.assertEqual(len(self.agent_a.resume_payloads), 1)
        self.assertEqual(self.agent_a.resume_payloads[0]["from"], "agent-b")
        self.assertEqual(
            self.agent_a.resume_payloads[0]["sub"]["error_code"],
            LivenessErrorCode.CHILD_WORKER_LOST,
        )
        # Neither link is left parked once the chain has unwound.
        self.assertEqual(self._wait_entries(), {})


class TestAskUserIsNotCompensated(OrphanRecoveryTestBase):
    """A human being slow is not a fault, so there is nothing to fix."""

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.agent_a = AskingAgent(
            "agent-a", "worker-a", self.redis, self.registry, self.workspace
        )
        await self._register(self.agent_a)
        await self._seed_root_message("agent-a")

    async def test_due_ask_user_entry_is_skipped_untouched(self):
        await self._step("agent-a")
        before = self._wait_entries()
        self.assertEqual(len(before), 1)

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_ASK_USER_SKIPPED), 1)
        self.assertEqual(_replies(self.redis, "agent-a"), [])
        # Left exactly as it was: it is not renewed either, because its only
        # remaining job is to let the gate recognize a repeated answer.
        self.assertEqual(self._wait_entries(), before)


class TestStrandedEntries(OrphanRecoveryTestBase):
    """Entries whose caller cannot be woken must be cleaned, not answered."""

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.agent_a = CallerAgent(
            "agent-a", "agent-b", "worker-a", self.redis, self.registry, self.workspace
        )
        await self._register(self.agent_a)

    async def _register_entry(self, caller_message_id: str, child_message_id: str):
        member = encode_member(
            session_id=self.session_id,
            parent_message_id=caller_message_id,
            child_message_id=child_message_id,
            task_group_id="",
        )
        await self.redis.zadd(wait_index_key(self.session_id), {member: _just_due_ms()})
        return member

    async def test_terminal_caller_entry_is_dropped_without_a_reply(self):
        # The window this covers: the wait is registered before the dispatch
        # xadd, so an xadd that raises leaves an entry behind for a caller
        # that has already failed.
        await self.registry.save_execution(
            {
                "execution_id": "exec-caller",
                "message_id": "msg-dead-caller",
                "session_id": self.session_id,
                "trace_id": "trace-orphan",
                "worker_id": "worker-a",
                "source_agent_type": "",
                "target_agent_type": "agent-a",
                "status": AgentState.FAILED.value,
            }
        )
        member = await self._register_entry("msg-dead-caller", "msg-child")

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_CALLER_TERMINAL), 1)
        self.assertNotIn(member, self._wait_entries())
        self.assertEqual(_replies(self.redis, "agent-a"), [])

    async def test_entry_without_any_execution_record_is_dropped(self):
        await self._register_entry("msg-vanished", "msg-child")

        await self.sweeper.sweep_once()

        self.assertEqual(self._wait_entries(), {})
        self.assertEqual(_replies(self.redis, "agent-a"), [])


class TestRenewalCeiling(OrphanRecoveryTestBase):
    """A callee whose worker is alive but which never finishes.

    A live lease says "the process is up", not "the work is moving", so
    renewing on it alone would suspend the caller forever — a hung model
    call and a deadlock look identical to a slow one. The ceiling is what
    turns that from an unbounded hang into a bounded failure.

    The scenarios are built from explicit execution records because what is
    under test is the deadline arithmetic itself: the original deadline, the
    sub-task's `created_at`, and the multiple between them.
    """

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.agent_a = CallerAgent(
            "agent-a", "agent-b", "worker-a", self.redis, self.registry, self.workspace
        )
        await self._register(self.agent_a)
        await self.redis.set(RedisKeys.worker_online_lease("worker-b"), "1")

    async def _park(self, *, timeout_ms: int, waited_ms: int) -> str:
        """A caller suspended `waited_ms` past a `timeout_ms` deadline."""
        now_ms = int(time.time() * 1000)
        deadline_ms = now_ms - waited_ms
        await self.registry.save_execution(
            {
                "execution_id": "exec-caller",
                "message_id": "msg-caller",
                "session_id": self.session_id,
                "trace_id": "trace-orphan",
                "worker_id": "worker-a",
                "target_agent_type": "agent-a",
                "status": AgentState.WAITING_AGENT.value,
            }
        )
        await self.registry.save_execution(
            {
                "execution_id": "exec-child",
                "message_id": "msg-child",
                "session_id": self.session_id,
                "trace_id": "trace-orphan",
                "worker_id": "worker-b",
                "source_agent_type": "agent-a",
                "target_agent_type": "agent-b",
                "status": "RUNNING",
                # Written by initialize_execution immediately before the wait
                # is registered, so it stands in for "when the wait started".
                "created_at": deadline_ms - timeout_ms,
            }
        )
        member = encode_member(
            session_id=self.session_id,
            parent_message_id="msg-caller",
            child_message_id="msg-child",
            task_group_id="",
        )
        await self.redis.zadd(wait_index_key(self.session_id), {member: deadline_ms})
        return member

    async def test_a_callee_inside_its_budget_is_renewed_not_killed(self):
        # One minute asked for, one minute overdue: well inside 3x, and the
        # worker is alive. Killing here is the misfire the ceiling must avoid.
        member = await self._park(timeout_ms=60_000, waited_ms=60_000)

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_CHILD_ALIVE), 1)
        self.assertEqual(_replies(self.redis, "agent-a"), [])
        self.assertGreater(self._wait_entries()[member], time.time() * 1000)

    async def test_a_callee_past_its_budget_is_failed_with_child_timeout(self):
        # Ten minutes overdue on a one-minute timeout is past 3x, so the
        # caller is told the sub-task timed out even though the worker is up.
        await self._park(timeout_ms=60_000, waited_ms=600_000)

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_TIMED_OUT), 1)
        replies = _replies(self.redis, "agent-a")
        self.assertEqual(len(replies), 1)
        self.assertEqual(replies[0].status, AgentState.FAILED.value)
        self.assertEqual(
            replies[0].reply_data["error_code"], LivenessErrorCode.CHILD_TIMEOUT
        )

    async def test_the_budget_survives_the_renewals_that_overwrite_the_deadline(self):
        # Every renewal overwrites the deadline, destroying the evidence of
        # when the wait actually started. Re-measuring from the deadline a
        # sweep just pushed out would put the ceiling permanently in the
        # future, which is the shape of the bug this saved origin prevents.
        member = await self._park(timeout_ms=60_000, waited_ms=600_000)
        self.assertEqual((await self.sweeper.sweep_once()).get(OUTCOME_TIMED_OUT), 1)

        # Come due again on a *fresh* deadline — one that, taken as the
        # origin, would sit comfortably inside the budget.
        await self.redis.zadd(
            wait_index_key(self.session_id), {member: int(time.time() * 1000)}
        )
        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_TIMED_OUT), 1)
        self.assertEqual(
            _replies(self.redis, "agent-a")[0].reply_data["error_code"],
            LivenessErrorCode.CHILD_TIMEOUT,
        )

    async def test_a_callee_waiting_on_a_human_is_exempt(self):
        # The same exemption, for the wait that can legitimately last days.
        # A caller whose sub-agent is parked on `ask_user` must not be failed
        # for it: the person is not late, and the sub-agent will reply on its
        # own the moment they answer.
        await self._park(timeout_ms=60_000, waited_ms=600_000)
        await self.registry.update_execution_status_by_message(
            "msg-child", self.session_id, AgentState.WAITING_USER.value
        )

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_CHILD_WAITING), 1)
        self.assertEqual(_replies(self.redis, "agent-a"), [])

    async def test_a_callee_suspended_on_its_own_callee_is_exempt(self):
        # This wait was registered before the deeper one it is blocked on, so
        # its ceiling comes first — applying it here would fail the chain from
        # the top down and report the wrong cause everywhere.
        await self._park(timeout_ms=60_000, waited_ms=600_000)
        await self.registry.update_execution_status_by_message(
            "msg-child", self.session_id, AgentState.WAITING_AGENT.value
        )

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_CHILD_WAITING), 1)
        self.assertEqual(_replies(self.redis, "agent-a"), [])


class TestTaskGroupOrphan(OrphanRecoveryTestBase):
    """One member of a fan-out loses its worker; the group must still close."""

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.agent_a = GroupCallerAgent(
            "agent-a",
            ["agent-b", "agent-c"],
            "worker-a",
            self.redis,
            self.registry,
            self.workspace,
        )
        self.agent_b = AnsweringAgent(
            "agent-b", "worker-b", self.redis, self.registry, self.workspace
        )
        self.agent_c = HangingAgent(
            "agent-c", "worker-c", self.redis, self.registry, self.workspace
        )
        await self._register(self.agent_a)
        await self._register(self.agent_b)
        await self._register(self.agent_c)
        await self._seed_root_message("agent-a")

    async def _fan_out_and_lose_agent_c(self) -> str:
        await self._step("agent-a")
        await self._step("agent-b")  # one sibling answers normally
        await self._step("agent-a")  # ...and is joined: 1 of 2
        await self._start_and_hang(self.agent_c)
        await self._kill_worker("worker-c")
        return _dispatched_message_id(self.redis, "agent-c")

    def _group_id(self):
        return _ctrl_messages(self.redis, "agent-b")[0]["header"]["task_group_id"]

    def _group_counters(self):
        return dict(self.redis.hashes[RedisKeys.task_group(self._group_id())])

    async def _make_due_again(self):
        """Simulate the renewal window elapsing so the entry comes due again."""
        index_key = wait_index_key(self.session_id)
        for member in list(self.redis.zsets.get(index_key, {})):
            await self.redis.zadd(index_key, {member: _just_due_ms()})

    async def test_the_group_still_fills_and_the_caller_is_resumed(self):
        c_dispatch = await self._fan_out_and_lose_agent_c()
        self.assertEqual(self.agent_a.resume_payloads, [])

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_WORKER_LOST), 1)
        # The stand-in is an ordinary group reply, not a private accounting
        # write: it carries the group id and lets the existing join count it.
        orphan_reply = _replies(self.redis, "agent-a")[-1]
        self.assertEqual(orphan_reply.header.task_group_id, self._group_id())
        self.assertEqual(orphan_reply.header.parent_message_id, c_dispatch)

        await self._step("agent-a")

        counters = self._group_counters()
        self.assertEqual(counters["completed"], int(counters["total"]))
        self.assertEqual(len(self.agent_a.resume_payloads), 1)
        results = {
            result["target_agent_type"]: result
            for result in self.agent_a.resume_payloads[0]
        }
        self.assertEqual(results["agent-b"]["status"], AgentState.COMPLETED.value)
        self.assertEqual(results["agent-c"]["status"], AgentState.FAILED.value)
        self.assertEqual(
            results["agent-c"]["reply_data"]["error_code"],
            LivenessErrorCode.CHILD_WORKER_LOST,
        )

    async def test_repeated_sweeps_cannot_push_completed_past_total(self):
        # The entry survives a sweep on purpose, so every renewal window
        # produces another stand-in. Only one may ever be counted: a second
        # would take `completed` past `total` and aggregate the group again,
        # resuming a caller that has already finished.
        await self._fan_out_and_lose_agent_c()
        for _ in range(3):
            await self.sweeper.sweep_once()
            await self._make_due_again()

        self.assertGreater(len(_replies(self.redis, "agent-a")), 3)
        for _ in range(5):
            await self._step("agent-a")

        counters = self._group_counters()
        self.assertEqual(counters["completed"], int(counters["total"]))
        self.assertEqual(len(self.agent_a.resume_payloads), 1)

    async def test_a_sweep_after_the_group_closed_adds_nothing(self):
        await self._fan_out_and_lose_agent_c()
        await self.sweeper.sweep_once()
        await self._step("agent-a")
        replies_before = len(_replies(self.redis, "agent-a"))

        await self._make_due_again()
        await self.sweeper.sweep_once()

        # The caller is terminal and its entries are gone, so there is nothing
        # left to compensate.
        self.assertEqual(len(_replies(self.redis, "agent-a")), replies_before)
        self.assertEqual(self._wait_entries(), {})

    async def test_an_aborted_group_is_cleaned_up_instead(self):
        await self._fan_out_and_lose_agent_c()
        await self.redis.hset(
            RedisKeys.task_group(self._group_id()), TASK_GROUP_FIELD_ABORTED, "1"
        )
        replies_before = len(_replies(self.redis, "agent-a"))

        outcomes = await self.sweeper.sweep_once()

        # Its caller was already failed by the dispatch that aborted it, and
        # every reply for the group is discarded on arrival anyway.
        self.assertEqual(outcomes.get(OUTCOME_GROUP_ABORTED), 1)
        self.assertEqual(len(_replies(self.redis, "agent-a")), replies_before)
        self.assertEqual(self._wait_entries(), {})


class TestCallerOfAnAskingSubAgent(OrphanRecoveryTestBase):
    """A -> B, and B is waiting on a human.

    Two waits exist at once and they must be treated as opposites: B's own
    `ask_user` entry is never compensated, while A's entry — a machine waiting
    on a machine — is live and would normally be a candidate. It has to be
    renewed rather than failed, or every human-in-the-loop sub-agent kills the
    caller that delegated to it.
    """

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.agent_a = CallerAgent(
            "agent-a", "agent-b", "worker-a", self.redis, self.registry, self.workspace
        )
        self.agent_b = AskingAgent(
            "agent-b", "worker-b", self.redis, self.registry, self.workspace
        )
        await self._register(self.agent_a)
        await self._register(self.agent_b)
        await self._seed_root_message("agent-a")

    async def test_the_caller_is_renewed_and_the_ask_user_wait_is_skipped(self):
        await self._step("agent-a")
        await self._step("agent-b")
        b_dispatch = _dispatched_message_id(self.redis, "agent-b")
        self.assertEqual(
            (await self._execution(b_dispatch))["status"],
            AgentState.WAITING_USER.value,
        )
        before = self._wait_entries()
        self.assertEqual(len(before), 2)

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_CHILD_WAITING), 1)
        self.assertEqual(outcomes.get(OUTCOME_ASK_USER_SKIPPED), 1)
        self.assertEqual(_replies(self.redis, "agent-a"), [])
        after = self._wait_entries()
        self.assertEqual(set(after), set(before))

    async def test_the_answer_still_reaches_the_caller_afterwards(self):
        await self._step("agent-a")
        await self._step("agent-b")
        b_dispatch = _dispatched_message_id(self.redis, "agent-b")
        await self.sweeper.sweep_once()

        # The person answers. A client's resume reuses the suspended
        # execution's own message_id and names no source agent type.
        await self.redis.xadd(
            RedisKeys.ctrl_stream("agent-b"),
            ResumeCommand(
                header=MessageHeader(
                    message_id=b_dispatch,
                    session_id=self.session_id,
                    trace_id="trace-orphan",
                    target_agent_type="agent-b",
                ),
                content="yes",
            ).to_redis_payload(),
        )
        await self._step("agent-b")
        await self._step("agent-a")

        self.assertEqual(
            self.agent_a.resume_payloads,
            [{"from": "agent-b", "user_said": "yes"}],
        )
        self.assertEqual(self._wait_entries(), {})


class TestTimedOutChildIsCancelled(OrphanRecoveryTestBase):
    """A callee that outlived its renewal budget on a live worker.

    Cancellation is the one action a sweep takes beyond resolving the caller,
    and it is best-effort by construction: it is delivered after the reply,
    it is cooperative (so the wedged callee that caused the timeout is
    precisely the one it may not reach), and it does not stop the callee from
    replying later. None of the caller's liveness may depend on it.
    """

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.agent_a = CallerAgent(
            "agent-a", "agent-b", "worker-a", self.redis, self.registry, self.workspace
        )
        self.agent_b = HangingAgent(
            "agent-b", "worker-b", self.redis, self.registry, self.workspace
        )
        await self._register(self.agent_a)
        await self._register(self.agent_b)
        await self._seed_root_message("agent-a")

    async def _park_past_the_ceiling(self) -> str:
        """Let B claim the task and hang, then age the wait past its budget.

        The callee is genuinely RUNNING on a genuinely live worker — the
        situation the ceiling exists for, where nothing but elapsed time
        distinguishes a wedged callee from a slow one.
        """
        await self._step("agent-a")
        await self._start_and_hang(self.agent_b)
        child_message_id = _dispatched_message_id(self.redis, "agent-b")

        deadline_ms = int(time.time() * 1000) - 600_000
        child = await self._execution(child_message_id)
        # The span between created_at and the original deadline is the
        # caller's own timeout, which is what the budget is a multiple of.
        child["created_at"] = deadline_ms - 60_000
        await self.registry.save_execution(child)
        index_key = wait_index_key(self.session_id)
        for member in list(self.redis.zsets.get(index_key, {})):
            await self.redis.zadd(index_key, {member: deadline_ms})
        return child_message_id

    def _cancel_commands(self, worker_id: str = "worker-b"):
        return [
            json.loads(entry[1][b"data"].decode())
            for entry in self.redis.published.get(
                RedisKeys.worker_ctrl_stream(worker_id), []
            )
        ]

    async def test_cancel_is_addressed_to_the_worker_running_the_callee(self):
        child_message_id = await self._park_past_the_ceiling()

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_TIMED_OUT), 1)
        # Delivered to the worker's own stream, not the agent type's
        # competitive one: handle_cancel_task looks the execution up in its
        # worker's in-memory table, so a cancel any worker may claim records
        # that it cancelled something while cancelling nothing.
        commands = self._cancel_commands()
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0]["body"]["target_message_id"], child_message_id)
        self.assertEqual(
            (await self._execution(child_message_id))["cancel_requested"], True
        )

    async def test_the_caller_is_resumed_even_when_cancellation_fails(self):
        # AC9. Cancellation is cooperative and the archetypal CHILD_TIMEOUT is
        # a callee wedged in a blocking call, so the case that most needs
        # cancelling is the case it cannot reach. Here it cannot even be sent.
        await self._park_past_the_ceiling()
        original_xadd = self.redis.xadd

        async def refuse_cancel_stream(name, fields, **kwargs):
            if name == RedisKeys.worker_ctrl_stream("worker-b"):
                raise RuntimeError("cancel could not be delivered")
            return await original_xadd(name, fields, **kwargs)

        self.redis.xadd = refuse_cancel_stream

        outcomes = await self.sweeper.sweep_once()

        self.redis.xadd = original_xadd
        self.assertEqual(outcomes.get(OUTCOME_TIMED_OUT), 1)
        replies = _replies(self.redis, "agent-a")
        self.assertEqual(len(replies), 1)
        self.assertEqual(
            replies[0].reply_data["error_code"], LivenessErrorCode.CHILD_TIMEOUT
        )
        self.assertEqual(self._cancel_commands(), [])

    async def test_a_lost_worker_is_not_cancelled(self):
        # Nothing is running to cancel, and the execution's worker_id names a
        # stream nobody is consuming.
        await self._park_past_the_ceiling()
        self.redis.kv.pop(RedisKeys.worker_online_lease("worker-b"), None)

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_WORKER_LOST), 1)
        self.assertEqual(self._cancel_commands(), [])

    async def test_cancellation_can_be_switched_off(self):
        self.sweeper.cancel_on_timeout = False
        await self._park_past_the_ceiling()

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_TIMED_OUT), 1)
        self.assertEqual(len(_replies(self.redis, "agent-a")), 1)
        self.assertEqual(self._cancel_commands(), [])

    async def test_the_callees_own_cancelled_reply_is_dropped_by_the_gate(self):
        # Cancelling does not silence the callee: GatewayWorker's
        # CancelledError branch still sends a CANCELLED reply. The stand-in
        # already resolved this wait, so that copy must be dropped rather than
        # wake the caller a second time.
        child_message_id = await self._park_past_the_ceiling()
        await self.sweeper.sweep_once()
        await self._step("agent-a")
        self.assertEqual(len(self.agent_a.resume_payloads), 1)

        await self.redis.xadd(
            RedisKeys.ctrl_stream("agent-a"),
            ResumeCommand(
                header=MessageHeader(
                    message_id="msg-root",
                    session_id=self.session_id,
                    trace_id="trace-orphan",
                    source_agent_type="agent-b",
                    target_agent_type="agent-a",
                    parent_message_id=child_message_id,
                ),
                status=AgentState.CANCELLED.value,
                reply_data={"reason": "cancelled by the sweep"},
            ).to_redis_payload(),
        )
        await self._step("agent-a")

        self.assertEqual(len(self.agent_a.resume_payloads), 1)


if __name__ == "__main__":
    unittest.main()
