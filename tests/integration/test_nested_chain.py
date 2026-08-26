"""Three-level call chain: A -> B -> C.

The two-level flow (`test_callback_flow.py`) never exercises an agent that is
BOTH a callee and a caller. Everything specific to a middle link lives here:
it must stay silent while suspended, and after it resumes it must reply to the
agent that called it — not to the sub-agent whose reply woke it.
"""

import json
import unittest
from typing import Any

from by_framework import GatewayWorker, RedisKeys, WorkerRegistry, WorkerRunner
from by_framework.common.constants import CLIENT_SOURCE_AGENT_TYPE
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.protocol.commands import AskAgentCommand, ResumeCommand
from by_framework.core.protocol.event_type import EventType
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.core.wait_index import decode_member, wait_index_key


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
    """In-memory Redis with the surface this flow touches.

    Enough of streams + hashes + sets + sorted sets that the REAL
    `WorkerRegistry` can run against it — the execution records this chain
    depends on (source_agent_type, snapshot key) are written by the registry,
    so faking the registry instead would fake away the thing under test.
    """

    def __init__(self):
        self.streams: dict[str, list] = {}
        # Consumption pops from `streams`, so keep an append-only log of what
        # was ever published for assertions.
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
        # positionally, and a fake that swaps them silently drops the write —
        # which hid the single-call result persistence from this flow.
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

    async def zrangebyscore(self, name, min_score, max_score, withscores=False):
        bucket = self.zsets.get(name, {})
        upper = float("inf") if max_score == "+inf" else max_score
        items = [(k, v) for k, v in bucket.items() if min_score <= v <= upper]
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


class ChainAgent(GatewayWorker):
    """Calls `next_agent_type` on first contact, answers on resume."""

    def __init__(
        self, agent_type, next_agent_type, *args, dispatch_metadata=None, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.agent_type = agent_type
        self.next_agent_type = next_agent_type
        self.dispatch_metadata = dispatch_metadata
        self.dispatch_calls = 0
        self.resume_payloads: list[Any] = []
        # What each invocation was actually handed, so a test can assert on
        # the inbound direction (what this agent reads) as well as on the
        # outbound one (what it sends).
        self.seen_metadata: list[dict] = []

    def get_agent_types(self) -> list[str]:
        return [self.agent_type]

    async def process_command(self, command, context: Any):
        self.seen_metadata.append(dict(command.header.metadata))
        if isinstance(command, ResumeCommand):
            self.resume_payloads.append(command.reply_data)
            return {
                "status": AgentState.COMPLETED.value,
                "reply_data": {"from": self.agent_type, "sub": command.reply_data},
                # Own metadata this link contributes: must override
                # same-named keys from whatever it was dispatched with, while
                # leaving other inherited keys untouched.
                "metadata": {
                    "agent": self.agent_type,
                    "tag": f"from-{self.agent_type}",
                },
            }
        self.dispatch_calls += 1
        await context.call_agent(
            target_agent_type=self.next_agent_type,
            content="delegate",
            wait_for_reply=True,
            metadata=self.dispatch_metadata,
        )
        return {"status": AgentState.QUEUED.value}


class AskingMiddleAgent(GatewayWorker):
    """A middle link that suspends on a human rather than on another agent."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_answers: list[Any] = []
        self.seen_metadata: list[dict] = []

    def get_agent_types(self) -> list[str]:
        return ["agent-b"]

    async def process_command(self, command, context: Any):
        self.seen_metadata.append(dict(command.header.metadata))
        if isinstance(command, ResumeCommand):
            self.user_answers.append(command.content)
            return {
                "status": AgentState.COMPLETED.value,
                "reply_data": {"from": "agent-b", "user_said": command.content},
                # Own metadata this link contributes: must override
                # same-named keys from A's original dispatch metadata.
                "metadata": {"agent": "agent-b", "tag": "from-agent-b"},
            }
        return await context.ask_user("which colour?")


class LeafAgent(GatewayWorker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls = 0

    def get_agent_types(self) -> list[str]:
        return ["agent-c"]

    async def process_command(self, command, context: Any):
        self.calls += 1
        return {
            "status": AgentState.COMPLETED.value,
            "reply_data": {"from": "agent-c"},
        }


def _ctrl_messages(redis, agent_type):
    return [
        json.loads(entry[1][b"data"].decode())
        for entry in redis.published.get(RedisKeys.ctrl_stream(agent_type), [])
    ]


def _replies(redis, agent_type):
    """RESUME commands addressed to an agent type (its callees' answers)."""
    return [
        ResumeCommand.from_dict(message)
        for message in _ctrl_messages(redis, agent_type)
        if message["action_type"] == ResumeCommand.action_type
    ]


def _dispatched_message_id(redis, agent_type):
    """message_id of the first command ever published to an agent type."""
    return _ctrl_messages(redis, agent_type)[0]["header"]["message_id"]


def _wait_members(redis, session_id):
    return [
        decode_member(member)
        for member in redis.zsets.get(wait_index_key(session_id), {})
    ]


class TestNestedChain(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.redis = FakeRedis()
        self.registry = WorkerRegistry(self.redis)
        workspace = WorkspaceManagerStub()

        self.agent_a = ChainAgent(
            "agent-a",
            "agent-b",
            "worker-a",
            self.redis,
            self.registry,
            workspace,
            # A's own dispatch metadata to B: the "A's original metadata"
            # this task is about. Must reach A's own reply intact even
            # though B suspends (via a nested call_agent to C) before B
            # finally replies.
            dispatch_metadata={"caller": "agent-a", "tag": "keep"},
        )
        self.agent_b = ChainAgent(
            "agent-b", "agent-c", "worker-b", self.redis, self.registry, workspace
        )
        self.agent_c = LeafAgent("worker-c", self.redis, self.registry, workspace)

        self.runners = {}
        for worker in (self.agent_a, self.agent_b, self.agent_c):
            runner = WorkerRunner(self.redis, worker, group_name="test-group")
            await runner.setup_streams()
            self.runners[worker.get_agent_types()[0]] = runner
            for agent_type in worker.get_agent_types():
                await self.redis.sadd(
                    RedisKeys.agent_type_members(agent_type), worker.worker_id
                )
            await self.redis.set(RedisKeys.worker_online_lease(worker.worker_id), "1")

        await self.redis.xadd(
            RedisKeys.ctrl_stream("agent-a"),
            AskAgentCommand(
                header=MessageHeader(
                    message_id="msg-root",
                    session_id="sess-chain",
                    trace_id="trace-chain",
                    target_agent_type="agent-a",
                ),
                content="start",
            ).to_redis_payload(),
        )

    async def _step(self, agent_type):
        runner = self.runners[agent_type]
        await runner._run_once()
        await runner.wait_for_tasks()

    async def test_middle_agent_stays_silent_until_it_has_a_real_result(self):
        await self._step("agent-a")  # A dispatches B
        await self._step("agent-b")  # B dispatches C and suspends

        # The whole defect in one assertion: B unwound with a placeholder
        # status, and forwarding that to A would wake A with a non-answer and
        # consume the single reply A was waiting for.
        self.assertEqual(_replies(self.redis, "agent-a"), [])
        self.assertEqual(self.agent_a.resume_payloads, [])

        await self._step("agent-c")  # C answers B
        await self._step("agent-b")  # B resumes and answers A

        replies_to_a = _replies(self.redis, "agent-a")
        self.assertEqual(len(replies_to_a), 1)
        reply = replies_to_a[0]
        # Addressed to A's suspended execution, and identifying B's sub-task.
        self.assertEqual(reply.header.message_id, "msg-root")
        self.assertEqual(reply.header.source_agent_type, "agent-b")
        # A's own dispatch metadata to B survives B suspending on a nested
        # call_agent to C, and B's own returned metadata overrides same-named
        # keys instead of being discarded or replacing the whole dict.
        self.assertEqual(reply.header.metadata["caller"], "agent-a")
        self.assertEqual(reply.header.metadata["tag"], "from-agent-b")
        self.assertEqual(reply.header.metadata["agent"], "agent-b")

        await self._step("agent-a")  # A resumes with B's answer

        self.assertEqual(self.agent_c.calls, 1)
        self.assertEqual(len(self.agent_a.resume_payloads), 1)
        # A hears from B, and C's result reaches A only nested inside B's —
        # never as a reply of its own.
        self.assertEqual(
            self.agent_a.resume_payloads[0],
            {"from": "agent-b", "sub": {"from": "agent-c"}},
        )
        self.assertEqual(self.agent_b.resume_payloads, [{"from": "agent-c"}])

    async def test_each_suspended_link_registers_its_own_wait_entry(self):
        await self._step("agent-a")
        await self._step("agent-b")

        members = sorted(
            _wait_members(self.redis, "sess-chain"), key=lambda m: m.parent_message_id
        )
        self.assertEqual(len(members), 2)
        # A waits on the message it sent to B; B waits on the one it sent to C.
        # Every entry names its own link, so the deepest one expires first and
        # a failure travels up hop by hop instead of timing out the whole chain.
        b_dispatch = _dispatched_message_id(self.redis, "agent-b")
        c_dispatch = _dispatched_message_id(self.redis, "agent-c")
        by_parent = {member.parent_message_id: member for member in members}
        self.assertEqual(by_parent["msg-root"].child_message_id, b_dispatch)
        self.assertEqual(by_parent[b_dispatch].child_message_id, c_dispatch)

    async def test_suspended_middle_agent_is_recorded_as_waiting_agent(self):
        await self._step("agent-a")
        await self._step("agent-b")

        b_dispatch = _dispatched_message_id(self.redis, "agent-b")
        execution = await self.registry.get_execution_by_message_id(
            b_dispatch, session_id="sess-chain"
        )
        # Not QUEUED: nothing is going to pick this execution up, it is parked
        # on a reply. That distinction is what lets a sweep tell a suspended
        # caller from a task still waiting for a worker.
        self.assertEqual(execution["status"], AgentState.WAITING_AGENT.value)
        self.assertEqual(execution["source_agent_type"], "agent-a")


class TestSubAgentAsksTheUser(unittest.IsolatedAsyncioTestCase):
    """A -> B, where B suspends on `ask_user` instead of on another agent.

    Structurally this is the middle-link case again, but the resume comes from
    a *client* rather than from `_enqueue_agent_return`, so its header is
    shaped differently — and that seam is exactly where the caller's identity
    gets lost. Nothing covered it: the chain tests above resume from a
    sub-agent, and `test_ask_user_flow.py` asks from a root agent that has no
    caller to owe a reply to.
    """

    session_id = "sess-ask"

    async def asyncSetUp(self):
        self.redis = FakeRedis()
        self.registry = WorkerRegistry(self.redis)
        workspace = WorkspaceManagerStub()

        self.agent_a = ChainAgent(
            "agent-a",
            "agent-b",
            "worker-a",
            self.redis,
            self.registry,
            workspace,
            # A's own dispatch metadata to B: must reach A's reply intact
            # even though B suspends on ask_user before it replies.
            dispatch_metadata={"caller": "agent-a", "tag": "keep"},
        )
        self.agent_b = AskingMiddleAgent(
            "worker-b", self.redis, self.registry, workspace
        )

        self.runners = {}
        for worker in (self.agent_a, self.agent_b):
            runner = WorkerRunner(self.redis, worker, group_name="test-group")
            await runner.setup_streams()
            self.runners[worker.get_agent_types()[0]] = runner
            for agent_type in worker.get_agent_types():
                await self.redis.sadd(
                    RedisKeys.agent_type_members(agent_type), worker.worker_id
                )
            await self.redis.set(RedisKeys.worker_online_lease(worker.worker_id), "1")

        await self.redis.xadd(
            RedisKeys.ctrl_stream("agent-a"),
            AskAgentCommand(
                header=MessageHeader(
                    message_id="msg-root",
                    session_id=self.session_id,
                    trace_id="trace-ask",
                    target_agent_type="agent-a",
                    # A's OWN request metadata — distinct from the metadata A
                    # passes down to B, and never forwarded to B. A must be
                    # able to read it again after B wakes it back up.
                    metadata={"root": "from-client"},
                ),
                content="start",
            ).to_redis_payload(),
        )

    async def _step(self, agent_type):
        runner = self.runners[agent_type]
        await runner._run_once()
        await runner.wait_for_tasks()

    async def _answer_the_user_prompt(self, message_id, answer="Pink", metadata=None):
        """What a client sends when the person replies.

        `GatewayClient.send_message(action_type=RESUME)` reuses the suspended
        execution's own message_id as `header.message_id` — that is what the
        runner reattaches by — and names no source agent type.
        """
        await self.redis.xadd(
            RedisKeys.ctrl_stream("agent-b"),
            ResumeCommand(
                header=MessageHeader(
                    message_id=message_id,
                    session_id=self.session_id,
                    trace_id="trace-ask",
                    target_agent_type="agent-b",
                    metadata=metadata or {},
                ),
                content=answer,
            ).to_redis_payload(),
        )

    async def test_b_stays_silent_while_it_waits_for_the_human(self):
        await self._step("agent-a")
        await self._step("agent-b")

        # Suspended on a person is still suspended: replying now would hand A
        # the WAITING_USER placeholder B unwound with and burn the single reply
        # A is parked on.
        self.assertEqual(_replies(self.redis, "agent-a"), [])
        self.assertEqual(self.agent_a.resume_payloads, [])
        b_dispatch = _dispatched_message_id(self.redis, "agent-b")
        execution = await self.registry.get_execution_by_message_id(
            b_dispatch, session_id=self.session_id
        )
        self.assertEqual(execution["status"], AgentState.WAITING_USER.value)

    async def test_b_replies_to_a_once_the_human_answers(self):
        await self._step("agent-a")
        await self._step("agent-b")
        b_dispatch = _dispatched_message_id(self.redis, "agent-b")

        await self._answer_the_user_prompt(
            b_dispatch,
            # The answering client's own metadata for this hop: transient
            # plumbing that must not leak through to A.
            metadata={"caller": "should-not-leak", "client_tag": "should-not-leak"},
        )
        await self._step("agent-b")

        self.assertEqual(self.agent_b.user_answers, ["Pink"])
        replies_to_a = _replies(self.redis, "agent-a")
        self.assertEqual(len(replies_to_a), 1)
        # The client's resume names no caller — A is found on the execution
        # record its own dispatch wrote, which is the whole point.
        self.assertEqual(replies_to_a[0].header.message_id, "msg-root")
        self.assertEqual(replies_to_a[0].header.source_agent_type, "agent-b")
        # A's own dispatch metadata to B survives the ask_user round-trip —
        # it is NOT replaced by the answering client's own resume metadata —
        # and B's own returned metadata overrides same-named keys.
        reply_metadata = replies_to_a[0].header.metadata
        self.assertEqual(reply_metadata["caller"], "agent-a")
        self.assertEqual(reply_metadata["tag"], "from-agent-b")
        self.assertEqual(reply_metadata["agent"], "agent-b")
        self.assertNotIn("client_tag", reply_metadata)

        # The INBOUND direction, which is a different rule: B is the addressee
        # of the client's answer, so that answer's metadata is payload for B
        # rather than someone else's plumbing. B reads its own dispatch
        # metadata AND the answer's, with the answer winning collisions.
        b_resumed_with = self.agent_b.seen_metadata[1]
        self.assertEqual(b_resumed_with["tag"], "keep")
        self.assertEqual(b_resumed_with["caller"], "should-not-leak")
        self.assertEqual(b_resumed_with["client_tag"], "should-not-leak")

        await self._step("agent-a")

        self.assertEqual(
            self.agent_a.resume_payloads,
            [{"from": "agent-b", "user_said": "Pink"}],
        )
        # Both links have unwound: nothing is left parked in the wait index.
        self.assertEqual(_wait_members(self.redis, self.session_id), [])

    async def test_a_reads_its_own_root_metadata_after_b_wakes_it(self):
        """The reported bug: A's own metadata vanished once B woke it.

        A never gets its request metadata back from B — B replies with what A
        dispatched B *with*, not with what A itself was dispatched with. The
        only copy is on A's own execution record, which is why the worker
        picking a message up records its metadata rather than relying on
        whoever sent it (a client root dispatch writes no such field).
        """
        await self._step("agent-a")
        await self._step("agent-b")
        b_dispatch = _dispatched_message_id(self.redis, "agent-b")
        await self._answer_the_user_prompt(b_dispatch)
        await self._step("agent-b")
        await self._step("agent-a")

        a_resumed_with = self.agent_a.seen_metadata[1]
        # What the client dispatched A with, still readable after the suspend.
        self.assertEqual(a_resumed_with["root"], "from-client")
        # And B's reply metadata layered on top.
        self.assertEqual(a_resumed_with["agent"], "agent-b")
        self.assertEqual(a_resumed_with["tag"], "from-agent-b")

    async def test_a_resume_naming_the_wrong_execution_cannot_reach_a(self):
        """The contract the whole path rests on, pinned as a test.

        A client answering must send the suspended execution's own message_id.
        With anything else `get_execution_by_message_id` resolves nothing, and
        since there is no execution record there is no record of who called
        either — the answer cannot reach A no matter what B does with it. The
        runner logs the unresolved resume, and this one dies one step further
        on (a resumed execution with no persisted config snapshot is refused),
        so B's handler never even runs: it fails loudly instead of quietly
        answering the wrong person.

        A therefore stays suspended and the wait entry it is parked on
        survives, which is what leaves the sweep able to resolve it.
        """
        await self._step("agent-a")
        await self._step("agent-b")

        await self._answer_the_user_prompt("msg-not-an-execution")
        await self._step("agent-b")

        self.assertEqual(self.agent_b.user_answers, [])
        self.assertEqual(_replies(self.redis, "agent-a"), [])
        self.assertEqual(self.agent_a.resume_payloads, [])
        parked = _wait_members(self.redis, self.session_id)
        self.assertIn("msg-root", [member.parent_message_id for member in parked])


class TestRootExecutionAsksTheUser(unittest.IsolatedAsyncioTestCase):
    """The bottom of the chain: an agent a *client* called, with nobody above.

    Recovering a resumed execution's caller from its own execution record is
    what makes the middle-link cases above work — but a root execution has a
    record too, and `GatewayClient` stamps it with `CLIENT_SOURCE_AGENT_TYPE`.
    Read as an agent type, that turns every root `ask_user` round into a reply
    posted to a control stream no worker consumes, and suppresses the
    end-of-stream event the caller is actually waiting for, because the
    execution now believes it owes its result to an agent instead of to the
    session data plane. Neither failure raises anything.
    """

    session_id = "sess-root"

    async def asyncSetUp(self):
        self.redis = FakeRedis()
        self.registry = WorkerRegistry(self.redis)
        self.agent = AskingMiddleAgent(
            "worker-root", self.redis, self.registry, WorkspaceManagerStub()
        )
        self.runner = WorkerRunner(self.redis, self.agent, group_name="test-group")
        await self.runner.setup_streams()
        await self.redis.sadd(
            RedisKeys.agent_type_members("agent-b"), self.agent.worker_id
        )
        await self.redis.set(RedisKeys.worker_online_lease(self.agent.worker_id), "1")

        # Field-for-field what GatewayClient.send_message writes for a root
        # dispatch, including the source_agent_type marker that is the point.
        await self.registry.initialize_execution(
            {
                "execution_id": "exec-root",
                "message_id": "msg-root",
                "session_id": self.session_id,
                "trace_id": "trace-root",
                "parent_message_id": "",
                "source_agent_type": CLIENT_SOURCE_AGENT_TYPE,
                "target_agent_type": "agent-b",
                "stream_name": RedisKeys.ctrl_stream("agent-b"),
                "status": "QUEUED",
            }
        )
        await self.redis.xadd(
            RedisKeys.ctrl_stream("agent-b"),
            AskAgentCommand(
                header=MessageHeader(
                    message_id="msg-root",
                    session_id=self.session_id,
                    trace_id="trace-root",
                    target_agent_type="agent-b",
                ),
                content="start",
            ).to_redis_payload(),
        )

    async def _step(self):
        await self.runner._run_once()
        await self.runner.wait_for_tasks()

    async def _answer(self, answer="Pink"):
        await self.redis.xadd(
            RedisKeys.ctrl_stream("agent-b"),
            ResumeCommand(
                header=MessageHeader(
                    message_id="msg-root",
                    session_id=self.session_id,
                    trace_id="trace-root",
                    target_agent_type="agent-b",
                ),
                content=answer,
            ).to_redis_payload(),
        )

    async def test_the_client_marker_is_not_treated_as_a_caller(self):
        await self._step()
        await self._answer()
        await self._step()

        self.assertEqual(self.agent.user_answers, ["Pink"])
        self.assertEqual(
            self.redis.published.get(
                RedisKeys.ctrl_stream(CLIENT_SOURCE_AGENT_TYPE), []
            ),
            [],
        )

    async def test_the_session_stream_is_still_closed_after_the_answer(self):
        await self._step()
        await self._answer()
        await self._step()

        events = [
            json.loads(entry[1][b"data"].decode())["event_type"]
            for entry in self.redis.published.get(
                RedisKeys.session_data_stream(self.session_id), []
            )
        ]
        # Nobody else can close this stream: believing it owes an agent a reply
        # is exactly what stops the root from emitting the end event.
        self.assertIn(EventType.APP_STREAM_RESPONSE.value, events)


class AskThenDelegateAgent(GatewayWorker):
    """Suspends on a human first, then calls a sub-agent once resumed."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_metadata: list[dict] = []

    def get_agent_types(self) -> list[str]:
        return ["agent-b"]

    async def process_command(self, command, context: Any):
        self.seen_metadata.append(dict(command.header.metadata))
        if isinstance(command, ResumeCommand):
            await context.call_agent(
                target_agent_type="agent-c",
                content="delegate",
                wait_for_reply=True,
            )
            return {"status": AgentState.QUEUED.value}
        return await context.ask_user("which colour?")


class TestTracePlumbingSurvivesTheInboundRestore(unittest.IsolatedAsyncioTestCase):
    """Restoring a hop's metadata must not restore its span ids with it.

    The framework injects `trace_parent_span_id`, `framework_parent_span_id`
    and `langfuse_parent_observation_id` into every dispatch's metadata, so a
    stored copy describes the dispatch that created the execution — not the
    hop resuming it now. `AgentContext._resolve_call_langfuse_parent_id()`
    reads that key straight off the current command as its last-resort
    fallback, so letting the stale value back in would parent a post-resume
    call to an observation from before the suspend.
    """

    session_id = "sess-trace"

    async def asyncSetUp(self):
        self.redis = FakeRedis()
        self.registry = WorkerRegistry(self.redis)
        self.agent = AskThenDelegateAgent(
            "worker-b", self.redis, self.registry, WorkspaceManagerStub()
        )
        self.runner = WorkerRunner(self.redis, self.agent, group_name="test-group")
        await self.runner.setup_streams()
        for agent_type in ("agent-b", "agent-c"):
            await self.redis.sadd(
                RedisKeys.agent_type_members(agent_type), self.agent.worker_id
            )
        await self.redis.set(RedisKeys.worker_online_lease(self.agent.worker_id), "1")

        await self.redis.xadd(
            RedisKeys.ctrl_stream("agent-b"),
            AskAgentCommand(
                header=MessageHeader(
                    message_id="msg-root",
                    session_id=self.session_id,
                    trace_id="trace-plumbing",
                    target_agent_type="agent-b",
                    metadata={
                        "tenant": "acme",
                        "langfuse_parent_observation_id": "observation-before-suspend",
                    },
                ),
                content="start",
            ).to_redis_payload(),
        )

    async def _step(self):
        await self.runner._run_once()
        await self.runner.wait_for_tasks()

    async def test_a_call_made_after_a_resume_does_not_reuse_the_stale_parent(self):
        await self._step()
        await self.redis.xadd(
            RedisKeys.ctrl_stream("agent-b"),
            ResumeCommand(
                header=MessageHeader(
                    message_id="msg-root",
                    session_id=self.session_id,
                    trace_id="trace-plumbing",
                    target_agent_type="agent-b",
                ),
                content="Pink",
            ).to_redis_payload(),
        )
        await self._step()

        dispatched = _ctrl_messages(self.redis, "agent-c")
        self.assertEqual(len(dispatched), 1)
        header = dispatched[0]["header"]
        self.assertNotEqual(
            header["langfuse_parent_observation_id"], "observation-before-suspend"
        )
        self.assertNotEqual(
            header["metadata"].get("langfuse_parent_observation_id"),
            "observation-before-suspend",
        )
        # The business half of the same stored metadata IS restored, which is
        # what makes the exclusion a filter rather than a switch: `tenant`
        # comes back, the span id sitting next to it in the same dict does not.
        resumed_with = self.agent.seen_metadata[1]
        self.assertEqual(resumed_with["tenant"], "acme")
        self.assertNotIn("langfuse_parent_observation_id", resumed_with)


if __name__ == "__main__":
    unittest.main()
