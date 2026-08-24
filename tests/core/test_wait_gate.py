"""Idempotency gate: which copy of a reply gets to wake the caller.

The gate is the one place in this subsystem where a mistake *loses* a
message rather than duplicating one, so the tests here are weighted
towards the "must be let through" cases.
"""

import hashlib
import json

import pytest

from by_framework.common.constants import (
    DEFAULT_ASK_USER_TIMEOUT_MS,
    WAIT_CONSUMED_TTL_SECONDS,
    RedisKeys,
)
from by_framework.core.protocol.commands import ResumeCommand
from by_framework.core.protocol.event_type import EventType
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.core.wait_gate import (
    ALLOW_CLAIMED,
    ALLOW_GATE_ERROR,
    ALLOW_UNREGISTERED,
    DENY_ALREADY_CONSUMED,
    consume_wait_entry,
    consumed_marker_key,
    emit_orphaned_reply,
)
from by_framework.core.wait_index import encode_member, wait_index_key

SESSION = "sess-1"


class FakeRedis:
    """In-memory ZSET/string store, plus a switch to make it fail."""

    def __init__(self, fail_on: str = ""):
        self.zsets: dict[str, dict[str, float]] = {}
        self.strings: dict[str, str] = {}
        self.streams: list[tuple] = []
        self.fail_on = fail_on
        self.set_calls: list[tuple] = []

    def _maybe_fail(self, command: str):
        if self.fail_on == command:
            raise ConnectionError(f"redis is down ({command})")

    async def zadd(self, name, mapping):
        self._maybe_fail("zadd")
        self.zsets.setdefault(name, {}).update(mapping)
        return len(mapping)

    async def zrem(self, name, *members):
        self._maybe_fail("zrem")
        stored = self.zsets.get(name, {})
        return sum(1 for member in members if stored.pop(member, None) is not None)

    async def set(self, name, value, ex=None):  # pylint: disable=invalid-name
        self._maybe_fail("set")
        self.set_calls.append((name, value, ex))
        self.strings[name] = value
        return True

    async def exists(self, *names):
        self._maybe_fail("exists")
        return sum(1 for name in names if name in self.strings)

    def pipeline(self):
        return _FakePipeline(self)


class _FakePipeline:

    def __init__(self, redis):
        self.redis = redis
        self.ops = []

    def xadd(self, name, fields, **kwargs):
        self.ops.append(("xadd", name, fields))
        return self

    def expire(self, name, ttl):
        self.ops.append(("expire", name, ttl))
        return self

    async def execute(self):
        self.redis._maybe_fail("execute")
        for op in self.ops:
            if op[0] == "xadd":
                self.redis.streams.append((op[1], op[2]))
        return [None] * len(self.ops)


def sub_agent_reply(
    child_message_id="child-msg", task_group_id="", caller="parent-msg"
):
    """A reply shaped exactly like GatewayWorker._enqueue_agent_return's."""
    return ResumeCommand(
        header=MessageHeader(
            message_id=caller,
            session_id=SESSION,
            trace_id="trace-1",
            source_agent_type="agent-b",
            target_agent_type="agent-a",
            parent_message_id=child_message_id,
            task_group_id=task_group_id,
        ),
        status="COMPLETED",
        reply_data={"value": 1},
    )


def register(redis, *, parent_message_id, child_message_id, task_group_id=""):
    member = encode_member(
        session_id=SESSION,
        parent_message_id=parent_message_id,
        child_message_id=child_message_id,
        task_group_id=task_group_id,
    )
    redis.zsets.setdefault(wait_index_key(SESSION), {})[member] = 1.0
    return member


@pytest.mark.asyncio
async def test_first_reply_claims_the_entry_and_marks_it_consumed():
    redis = FakeRedis()
    member = register(
        redis, parent_message_id="parent-msg", child_message_id="child-msg"
    )

    decision = await consume_wait_entry(redis, sub_agent_reply())

    assert decision.allow
    assert decision.reason == ALLOW_CLAIMED
    assert decision.member == member
    assert redis.zsets[wait_index_key(SESSION)] == {}
    assert redis.set_calls == [
        (consumed_marker_key(SESSION, member), "1", WAIT_CONSUMED_TTL_SECONDS)
    ]


@pytest.mark.asyncio
async def test_duplicate_reply_is_dropped_after_the_entry_was_claimed():
    """The whole point: a synthesized failure and the real reply that shows
    up afterwards must not both wake the caller."""
    redis = FakeRedis()
    register(redis, parent_message_id="parent-msg", child_message_id="child-msg")

    first = await consume_wait_entry(redis, sub_agent_reply())
    second = await consume_wait_entry(redis, sub_agent_reply())

    assert first.allow
    assert not second.allow
    assert second.reason == DENY_ALREADY_CONSUMED


@pytest.mark.asyncio
async def test_unregistered_reply_is_allowed_through():
    """RED LINE: ZREM returning 0 because nothing was ever registered is not
    a duplicate. During a rolling upgrade every in-flight reply looks like
    this — dropping them loses real work permanently."""
    redis = FakeRedis()

    decision = await consume_wait_entry(redis, sub_agent_reply())

    assert decision.allow
    assert decision.reason == ALLOW_UNREGISTERED
    assert redis.set_calls == []  # nothing claimed, so nothing to remember


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "broken_command,registered",
    # zrem breaks the claim itself; exists breaks the "is this a duplicate?"
    # lookup, which is only reached when the claim came back empty.
    [("zrem", True), ("exists", False)],
)
async def test_gate_failure_allows_the_reply(broken_command, registered):
    """RED LINE: the gate fails open. A gate that drops messages when Redis
    hiccups is worse than the duplicate it exists to prevent."""
    redis = FakeRedis(fail_on=broken_command)
    if registered:
        register(redis, parent_message_id="parent-msg", child_message_id="child-msg")

    decision = await consume_wait_entry(redis, sub_agent_reply())

    assert decision.allow
    assert decision.reason == ALLOW_GATE_ERROR


@pytest.mark.asyncio
async def test_claim_survives_a_failed_marker_write():
    """Losing the marker costs only the ability to spot a much later
    duplicate; it must not turn a legitimate reply into a dropped one."""
    redis = FakeRedis(fail_on="set")
    register(redis, parent_message_id="parent-msg", child_message_id="child-msg")

    decision = await consume_wait_entry(redis, sub_agent_reply())

    assert decision.allow
    assert redis.zsets[wait_index_key(SESSION)] == {}


@pytest.mark.asyncio
async def test_ask_user_entry_is_claimed_despite_a_client_supplied_parent_id():
    """ask_user registers with an empty child_message_id (there is no
    sub-task), but the user's reply comes from a client that puts its own
    value in header.parent_message_id — so the member cannot be rebuilt
    from the header alone and the empty-child variant must be tried."""
    redis = FakeRedis()
    member = register(redis, parent_message_id="caller-msg", child_message_id="")

    user_reply = ResumeCommand(
        header=MessageHeader(
            message_id="caller-msg",
            session_id=SESSION,
            trace_id="trace-1",
            # Whatever the client happened to put here — not a sub-task id.
            parent_message_id="some-root-msg",
            target_agent_type="agent-a",
        ),
        content="Pink",
    )

    decision = await consume_wait_entry(redis, user_reply)

    assert decision.allow
    assert decision.member == member
    assert redis.zsets[wait_index_key(SESSION)] == {}


@pytest.mark.asyncio
async def test_duplicate_sub_agent_reply_does_not_consume_a_live_ask_user_wait():
    """Candidate order is load-bearing: each candidate must be fully
    resolved (claim, then check its own marker) before the next is tried.
    Otherwise a duplicate sub-agent reply would fall through to the
    ask_user variant and silently clear a wait that is still pending."""
    redis = FakeRedis()
    register(redis, parent_message_id="caller-msg", child_message_id="child-msg")

    first = await consume_wait_entry(redis, sub_agent_reply(caller="caller-msg"))
    assert first.allow

    # The caller resumed and is now waiting on the user.
    ask_user_member = register(
        redis, parent_message_id="caller-msg", child_message_id=""
    )

    duplicate = await consume_wait_entry(redis, sub_agent_reply(caller="caller-msg"))

    assert not duplicate.allow
    assert redis.zsets[wait_index_key(SESSION)] == {ask_user_member: 1.0}


@pytest.mark.asyncio
async def test_task_group_reply_never_falls_back_to_the_ask_user_variant():
    """A reply carrying a task_group_id is a sub-agent reply by
    construction; there is no ask_user wait it could belong to."""
    redis = FakeRedis()
    ask_user_member = register(
        redis, parent_message_id="caller-msg", child_message_id=""
    )

    decision = await consume_wait_entry(
        redis, sub_agent_reply(caller="caller-msg", task_group_id="tg-1")
    )

    assert decision.allow
    assert decision.reason == ALLOW_UNREGISTERED
    assert redis.zsets[wait_index_key(SESSION)] == {ask_user_member: 1.0}


@pytest.mark.asyncio
async def test_task_group_siblings_are_gated_independently():
    redis = FakeRedis()
    register(
        redis,
        parent_message_id="caller-msg",
        child_message_id="child-1",
        task_group_id="tg-1",
    )
    register(
        redis,
        parent_message_id="caller-msg",
        child_message_id="child-2",
        task_group_id="tg-1",
    )

    first = await consume_wait_entry(
        redis, sub_agent_reply("child-1", "tg-1", caller="caller-msg")
    )
    second = await consume_wait_entry(
        redis, sub_agent_reply("child-2", "tg-1", caller="caller-msg")
    )
    duplicate = await consume_wait_entry(
        redis, sub_agent_reply("child-1", "tg-1", caller="caller-msg")
    )

    assert (first.allow, second.allow) == (True, True)
    assert not duplicate.allow


def user_answer(caller="caller-msg", client_parent="some-root-msg"):
    """A user's reply, as a client sends it: the parent id is whatever the
    client put there, not a sub-task id."""
    return ResumeCommand(
        header=MessageHeader(
            message_id=caller,
            session_id=SESSION,
            trace_id="trace-1",
            parent_message_id=client_parent,
            target_agent_type="agent-a",
        ),
        content="Pink",
    )


def test_the_consumed_marker_outlives_every_wait_it_may_arbitrate():
    """The marker is the only thing that separates "already resolved" from
    "never registered", and the gate is required to let the latter through.
    So the moment it expires while entries of the same session are still
    live, both of the failures below become reachable — which is why it is
    sized to the session registry's own lifetime rather than to the task
    group data a late reply happens to carry."""
    assert WAIT_CONSUMED_TTL_SECONDS == RedisKeys.DEFAULT_SESSION_TTL
    # The longest deadline anything registers is ask_user's, and a human may
    # take all of it.
    assert WAIT_CONSUMED_TTL_SECONDS * 1000 >= DEFAULT_ASK_USER_TIMEOUT_MS


@pytest.mark.asyncio
async def test_a_repeated_user_answer_is_recognized_as_a_duplicate():
    """Failure 1 of 2. A person answering twice (a resent client message, a
    double-click) must not resume the caller twice — and the window in which
    that can happen is as long as the person is allowed to take."""
    redis = FakeRedis()
    member = register(redis, parent_message_id="caller-msg", child_message_id="")

    first = await consume_wait_entry(redis, user_answer())
    second = await consume_wait_entry(redis, user_answer())

    assert first.allow and first.member == member
    assert not second.allow
    assert second.reason == DENY_ALREADY_CONSUMED
    # ...and it stays recognizable for the whole ask_user window, not just a
    # day of it.
    assert redis.set_calls[0][2] == WAIT_CONSUMED_TTL_SECONDS


@pytest.mark.asyncio
async def test_an_expired_marker_lets_a_stale_reply_eat_a_live_ask_user_wait():
    """Failure 2 of 2, and the worse one — this is what the marker's TTL is
    actually sized against.

    A duplicate sub-agent reply is supposed to stop at its own candidate.
    Once that candidate's marker is gone it does not stop: it falls through
    to the ask_user candidate for the same caller, claims a wait that is
    still pending, and marks *that* consumed — so the person's real answer is
    then dropped as a duplicate and the caller is never resumed. The entry
    that made this possible outlives a day easily; only a marker living as
    long as the session prevents it.
    """
    redis = FakeRedis()
    register(redis, parent_message_id="caller-msg", child_message_id="child-msg")
    first = await consume_wait_entry(redis, sub_agent_reply(caller="caller-msg"))
    assert first.allow

    # The caller resumed and is now waiting on a human.
    ask_user_member = register(
        redis, parent_message_id="caller-msg", child_message_id=""
    )
    # Simulate the marker expiring mid-wait, which is what a TTL shorter than
    # the ask_user deadline does.
    redis.strings.clear()

    duplicate = await consume_wait_entry(redis, sub_agent_reply(caller="caller-msg"))
    real_answer = await consume_wait_entry(redis, user_answer())

    assert duplicate.allow and duplicate.member == ask_user_member
    assert not real_answer.allow  # the person's answer, lost
    assert redis.zsets[wait_index_key(SESSION)] == {}


def test_consumed_marker_key_is_a_stable_digest_of_the_member():
    """Cross-SDK contract: TS/Java compute this key the same way, so the
    digest has to be a plain SHA-1 of the member string."""
    member = encode_member(SESSION, "parent-msg", "child-msg", "")
    digest = hashlib.sha1(member.encode("utf-8")).hexdigest()
    assert consumed_marker_key(SESSION, member) == RedisKeys.wait_consumed(
        SESSION, digest
    )


@pytest.mark.asyncio
async def test_orphaned_reply_event_is_emitted_on_the_session_data_stream():
    redis = FakeRedis()

    await emit_orphaned_reply(
        redis,
        sub_agent_reply(),
        worker_id="worker-1",
        reason=DENY_ALREADY_CONSUMED,
    )

    assert len(redis.streams) == 1
    stream_name, payload = redis.streams[0]
    assert stream_name == RedisKeys.session_data_stream(SESSION)
    message = json.loads(payload["data"])
    assert message["event_type"] == EventType.ORPHANED_REPLY.value
    assert message["session_id"] == SESSION
    assert message["data"] == {
        "reason": DENY_ALREADY_CONSUMED,
        "caller_message_id": "parent-msg",
        "child_message_id": "child-msg",
        "task_group_id": "",
        "status": "COMPLETED",
        "worker_id": "worker-1",
    }


@pytest.mark.asyncio
async def test_orphaned_reply_event_failure_is_swallowed():
    """Reporting a drop must never change or block the drop itself."""
    redis = FakeRedis(fail_on="execute")

    await emit_orphaned_reply(redis, sub_agent_reply())  # must not raise
