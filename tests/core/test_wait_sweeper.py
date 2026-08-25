"""Sweeper mechanics: the switch, shard claiming, and the odd entries.

The end-to-end triage lives in `tests/integration/test_orphan_recovery.py`
against the real registry. What is left here is everything that has no
business reaching a call chain: the opt-in switch, the shard lock that keeps
two workers off the same entries, and entries that are damaged, deferred, or
unroutable.
"""

import json
import time
import unittest
from typing import Any, Optional

from by_framework.common.constants import (
    DEFAULT_ASK_USER_TIMEOUT_MS,
    TASK_GROUP_FIELD_ABORTED,
    TASK_GROUP_FIELD_TOTAL,
    WAIT_INDEX_SHARDS,
    WAIT_PRUNE_AFTER_SECONDS,
    WAIT_RENEW_INCREMENT_MS,
    LivenessErrorCode,
    RedisKeys,
)
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.wait_index import (
    encode_member,
    wait_index_key,
    wait_index_shard,
)
from by_framework.core.wait_sweeper import (
    OUTCOME_CALLER_LOST,
    OUTCOME_CALLER_NOT_SUSPENDED,
    OUTCOME_GROUP_ABORTED,
    OUTCOME_GROUP_ALREADY_JOINED,
    OUTCOME_GROUP_GONE,
    OUTCOME_MALFORMED,
    OUTCOME_NEVER_STARTED,
    OUTCOME_PRUNED,
    OUTCOME_UNROUTABLE,
    OUTCOME_WORKER_LOST,
    SWEEPER_ENABLED_ENV,
    SWEEPER_PRUNE_ENABLED_ENV,
    WaitIndexSweeper,
)

SESSION = "sess-sweep"


class FakeRedis:

    def __init__(self):
        self.hashes: dict[str, dict] = {}
        self.zsets: dict[str, dict] = {}
        self.kv: dict[str, Any] = {}
        self.published: dict[str, list] = {}

    async def xadd(self, name, fields, maxlen=None, approximate=True):
        self.published.setdefault(name, []).append(dict(fields))
        return b"1-0"

    async def hget(self, name, key):
        return self.hashes.get(name, {}).get(key)

    async def hset(self, name, key=None, value=None, mapping=None):
        bucket = self.hashes.setdefault(name, {})
        if mapping:
            bucket.update(mapping)
        else:
            bucket[key] = value

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
        items = sorted(
            (
                (k, v)
                for k, v in self.zsets.get(name, {}).items()
                if min_score <= v <= max_score
            ),
            key=lambda kv: kv[1],
        )
        if num is not None:
            items = items[(start or 0) : (start or 0) + num]
        return items if withscores else [k for k, _ in items]

    async def set(self, name, value, nx=False, ex=None):
        if nx and name in self.kv:
            return False
        self.kv[name] = value
        return True

    async def get(self, name):
        return self.kv.get(name)

    async def eval(self, script, numkeys, *args):
        key, token = args[0], args[1]
        raw = self.kv.get(key)
        if raw is None:
            return 1 if token == "" else 0
        if token and json.loads(raw).get("token") != token:
            return 0
        self.kv.pop(key, None)
        return 1


class FakeRegistry:
    """Only what a sweep interrogates: execution records and worker leases."""

    def __init__(self):
        self.executions: dict[str, dict] = {}
        self.online: set[str] = set()

    def add_execution(self, message_id: str, **fields):
        self.executions[message_id] = {"message_id": message_id, **fields}

    async def get_execution_by_message_id(
        self, message_id: str, session_id: str = ""
    ) -> Optional[dict]:
        del session_id
        return self.executions.get(message_id)

    async def is_worker_online(self, worker_id: str) -> bool:
        return worker_id in self.online


def _just_due_ms() -> int:
    """A deadline that has only just passed.

    Not 0: a real score is always "some writer's clock plus a non-negative
    offset", so an epoch-0 entry is not a due entry — it is one the prune
    half deletes as abandoned before triage ever sees it.
    """
    return int(time.time() * 1000) - 1000


def _due_member(redis: FakeRedis, **fields) -> str:
    member = encode_member(session_id=SESSION, **fields)
    redis.zsets.setdefault(wait_index_key(SESSION), {})[member] = _just_due_ms()
    return member


class TestSweeperSwitch(unittest.IsolatedAsyncioTestCase):

    def test_compensation_is_disabled_unless_the_env_flag_is_set(self):
        # Off is the rollback position for the whole liveness feature: with
        # no compensation, the wait index is written and cleared but never
        # acted on, and the system behaves as it did before it existed.
        self.assertFalse(WaitIndexSweeper(FakeRedis()).enabled)

    def test_env_flag_enables_compensation(self):
        import os

        os.environ[SWEEPER_ENABLED_ENV] = "true"
        try:
            self.assertTrue(WaitIndexSweeper(FakeRedis()).enabled)
        finally:
            os.environ.pop(SWEEPER_ENABLED_ENV)

    def test_pruning_is_on_by_default(self):
        # Pruning cannot ride on the compensation switch: nothing but a reply
        # or a sweep ever removes a wait-index entry, so with compensation off
        # every call whose reply never arrives would leak one entry forever.
        self.assertTrue(WaitIndexSweeper(FakeRedis()).prune_enabled)

    def test_pruning_can_be_turned_off_by_env(self):
        import os

        os.environ[SWEEPER_PRUNE_ENABLED_ENV] = "false"
        try:
            self.assertFalse(WaitIndexSweeper(FakeRedis()).prune_enabled)
        finally:
            os.environ.pop(SWEEPER_PRUNE_ENABLED_ENV)

    def test_the_loop_slows_to_the_prune_cadence_without_compensation(self):
        # With nothing to triage, the only work left has a multi-day horizon;
        # waking every 30 seconds to do it would be pure noise.
        sweeper = WaitIndexSweeper(FakeRedis(), enabled=False)
        self.assertEqual(sweeper.loop_interval_seconds, sweeper.prune_interval_seconds)
        self.assertEqual(
            WaitIndexSweeper(FakeRedis(), enabled=True).loop_interval_seconds,
            WaitIndexSweeper(FakeRedis(), enabled=True).interval_seconds,
        )

    async def test_run_returns_immediately_only_when_both_halves_are_off(self):
        redis = FakeRedis()
        await WaitIndexSweeper(redis, enabled=False, prune_enabled=False).run()
        self.assertEqual(redis.kv, {})


class TestShardClaiming(unittest.IsolatedAsyncioTestCase):

    async def test_a_held_shard_is_skipped_by_another_sweeper(self):
        redis = FakeRedis()
        registry = FakeRegistry()
        shard = wait_index_shard(SESSION)
        _due_member(
            redis, parent_message_id="msg-caller", child_message_id="", task_group_id=""
        )
        # Somebody else already owns the shard holding this session.
        redis.kv[RedisKeys.wait_sweep_lock(shard)] = json.dumps({"token": "other"})

        outcomes = await WaitIndexSweeper(
            redis, registry=registry, enabled=True
        ).sweep_once()

        self.assertEqual(outcomes, {})

    async def test_lock_is_released_after_the_pass(self):
        redis = FakeRedis()
        sweeper = WaitIndexSweeper(redis, registry=FakeRegistry(), enabled=True)

        await sweeper.sweep_once()

        # Held only for the pass: a sweeper that kept its shards would starve
        # every other worker until its TTL expired.
        self.assertEqual(redis.kv, {})

    async def test_every_shard_is_visited_in_one_pass(self):
        redis = FakeRedis()
        seen = []
        for shard in range(WAIT_INDEX_SHARDS):
            redis.zsets[RedisKeys.wait_index(shard)] = {
                f"bad-member-{shard}": _just_due_ms()
            }
        sweeper = WaitIndexSweeper(redis, registry=FakeRegistry(), enabled=True)
        original = sweeper._resolve_entry

        async def record(index_key, member, deadline_ms=0):
            seen.append(index_key)
            return await original(index_key, member, deadline_ms)

        sweeper._resolve_entry = record
        await sweeper.sweep_once()

        self.assertEqual(len(seen), WAIT_INDEX_SHARDS)


class TestOddEntries(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.redis = FakeRedis()
        self.registry = FakeRegistry()
        self.sweeper = WaitIndexSweeper(
            self.redis, worker_id="worker-sweeper", registry=self.registry, enabled=True
        )
        self.index_key = wait_index_key(SESSION)

    def _add_suspended_caller(self, message_id="msg-caller", agent_type="agent-a"):
        self.registry.add_execution(
            message_id,
            session_id=SESSION,
            trace_id="trace-1",
            worker_id="worker-a",
            target_agent_type=agent_type,
            status=AgentState.WAITING_AGENT.value,
        )

    async def test_unparseable_member_is_dropped(self):
        self.redis.zsets[self.index_key] = {"not-a-member\\": _just_due_ms()}

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_MALFORMED), 1)
        self.assertEqual(self.redis.zsets[self.index_key], {})

    def _open_group(self, group_id="tg-1", total=2):
        self.redis.hashes[RedisKeys.task_group(group_id)] = {
            TASK_GROUP_FIELD_TOTAL: str(total)
        }

    async def test_status_decorated_with_a_reason_still_reads_as_suspended(self):
        # A caller parked on a Task Group persists as
        # "WAITING_AGENT: waiting_for_group". Compared raw it looks like a
        # running execution, which would send the entry down the
        # caller-not-suspended branch and, with a dead worker, delete it.
        self.registry.add_execution(
            "msg-caller",
            session_id=SESSION,
            worker_id="worker-a",
            target_agent_type="agent-a",
            status=f"{AgentState.WAITING_AGENT.value}: waiting_for_group",
        )
        self._open_group()
        member = _due_member(
            self.redis,
            parent_message_id="msg-caller",
            child_message_id="msg-child",
            task_group_id="tg-1",
        )

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_NEVER_STARTED), 1)
        self.assertIn(member, self.redis.zsets[self.index_key])

    async def test_group_that_no_longer_exists_is_cleaned_not_answered(self):
        # Its tracker expired, so a reply would find no group to join and
        # would resume the caller with one sibling's payload in place of the
        # aggregate it is waiting for.
        self._add_suspended_caller()
        member = _due_member(
            self.redis,
            parent_message_id="msg-caller",
            child_message_id="msg-child",
            task_group_id="tg-gone",
        )

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_GROUP_GONE), 1)
        self.assertEqual(self.redis.published, {})
        self.assertNotIn(member, self.redis.zsets[self.index_key])

    async def test_aborted_group_is_cleaned_not_answered(self):
        # The fan-out failed partway, so the caller is already terminated and
        # every reply for this group is discarded on arrival.
        self._add_suspended_caller()
        self._open_group("tg-aborted")
        self.redis.hashes[RedisKeys.task_group("tg-aborted")][
            TASK_GROUP_FIELD_ABORTED
        ] = "1"
        member = _due_member(
            self.redis,
            parent_message_id="msg-caller",
            child_message_id="msg-child",
            task_group_id="tg-aborted",
        )

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_GROUP_ABORTED), 1)
        self.assertEqual(self.redis.published, {})
        self.assertNotIn(member, self.redis.zsets[self.index_key])

    async def test_entry_whose_reply_already_joined_is_cleaned_not_answered(self):
        # A result under this sub-task's id can only have been written by the
        # join, so its reply was already counted. The entry outliving that is
        # a gate ZREM that did not land; answering it would count it twice.
        self._add_suspended_caller()
        self._open_group("tg-joined")
        self.redis.hashes[RedisKeys.task_group_results("tg-joined")] = {
            "msg-child": json.dumps({"status": AgentState.COMPLETED.value})
        }
        member = _due_member(
            self.redis,
            parent_message_id="msg-caller",
            child_message_id="msg-child",
            task_group_id="tg-joined",
        )

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_GROUP_ALREADY_JOINED), 1)
        self.assertEqual(self.redis.published, {})
        self.assertNotIn(member, self.redis.zsets[self.index_key])

    async def test_caller_still_running_is_given_more_time(self):
        # The caller registered its wait and has not finished its handler
        # yet. Replying now would run alongside it.
        self.registry.add_execution(
            "msg-caller",
            session_id=SESSION,
            worker_id="worker-a",
            target_agent_type="agent-a",
            status="RUNNING",
        )
        self.registry.online.add("worker-a")
        member = _due_member(
            self.redis,
            parent_message_id="msg-caller",
            child_message_id="msg-child",
            task_group_id="",
        )

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_CALLER_NOT_SUSPENDED), 1)
        self.assertEqual(self.redis.published, {})
        self.assertIn(member, self.redis.zsets[self.index_key])

    async def test_caller_that_died_before_suspending_is_dropped(self):
        # No reply can ever reattach this caller, so the entry is garbage.
        # Its own caller, one level up, is what rescues the chain.
        self.registry.add_execution(
            "msg-caller",
            session_id=SESSION,
            worker_id="worker-a",
            target_agent_type="agent-a",
            status="RUNNING",
        )
        member = _due_member(
            self.redis,
            parent_message_id="msg-caller",
            child_message_id="msg-child",
            task_group_id="",
        )

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_CALLER_LOST), 1)
        self.assertNotIn(member, self.redis.zsets[self.index_key])
        self.assertEqual(self.redis.published, {})

    async def test_reply_without_a_caller_agent_type_is_not_invented(self):
        self.registry.add_execution(
            "msg-caller",
            session_id=SESSION,
            worker_id="worker-a",
            target_agent_type="",
            status=AgentState.WAITING_AGENT.value,
        )
        self.registry.add_execution(
            "msg-child",
            session_id=SESSION,
            source_agent_type="",
            target_agent_type="agent-b",
            status="RUNNING",
            worker_id="worker-dead",
        )
        member = _due_member(
            self.redis,
            parent_message_id="msg-caller",
            child_message_id="msg-child",
            task_group_id="",
        )

        outcomes = await self.sweeper.sweep_once()

        # Nothing names the stream this reply would go on; guessing one would
        # deliver it to the wrong agent type.
        self.assertEqual(outcomes.get(OUTCOME_UNROUTABLE), 1)
        self.assertEqual(self.redis.published, {})
        self.assertIn(member, self.redis.zsets[self.index_key])

    async def test_finished_child_without_a_stored_result_fails_loudly(self):
        self._add_suspended_caller()
        self.registry.add_execution(
            "msg-child",
            session_id=SESSION,
            source_agent_type="agent-a",
            target_agent_type="agent-b",
            status=AgentState.COMPLETED.value,
            worker_id="worker-b",
        )
        _due_member(
            self.redis,
            parent_message_id="msg-caller",
            child_message_id="msg-child",
            task_group_id="",
        )

        await self.sweeper.sweep_once()

        published = self.redis.published[RedisKeys.ctrl_stream("agent-a")]
        reply = json.loads(published[0]["data"])
        # An empty COMPLETED would be a fabricated answer — the one outcome
        # worse than a reported failure.
        self.assertEqual(reply["body"]["status"], AgentState.FAILED.value)
        self.assertEqual(
            reply["body"]["reply_data"]["error_code"],
            LivenessErrorCode.REPLY_LOST_RECOVERED,
        )

    async def test_dead_worker_reply_mirrors_a_real_agent_return(self):
        self._add_suspended_caller()
        self.registry.add_execution(
            "msg-child",
            session_id=SESSION,
            trace_id="trace-1",
            source_agent_type="agent-a",
            target_agent_type="agent-b",
            status="RUNNING",
            worker_id="worker-b",
        )
        _due_member(
            self.redis,
            parent_message_id="msg-caller",
            child_message_id="msg-child",
            task_group_id="",
        )

        outcomes = await self.sweeper.sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_WORKER_LOST), 1)
        published = self.redis.published[RedisKeys.ctrl_stream("agent-a")]
        reply = json.loads(published[0]["data"])
        header = reply["header"]
        # The id reversal is the contract: message_id reattaches the caller's
        # suspended execution, parent_message_id names the sub-task, and the
        # gate rebuilds the wait-index member from both.
        self.assertEqual(header["message_id"], "msg-caller")
        self.assertEqual(header["parent_message_id"], "msg-child")
        self.assertEqual(header["target_agent_type"], "agent-a")
        self.assertEqual(header["source_agent_type"], "agent-b")
        self.assertEqual(
            header["metadata"]["liveness_error_code"],
            LivenessErrorCode.CHILD_WORKER_LOST,
        )


class TestPruning(unittest.IsolatedAsyncioTestCase):
    """Garbage collection of entries nothing can ever learn anything from.

    Its whole justification is that it decides nothing, so these tests are
    weighted towards the entries it must leave alone — a wait it deletes is
    one nothing will ever compensate.
    """

    def setUp(self):
        self.redis = FakeRedis()
        self.registry = FakeRegistry()
        self.index_key = wait_index_key(SESSION)
        self.now_ms = int(time.time() * 1000)

    def _entry(self, score_ms: float, child_message_id="msg-child") -> str:
        member = encode_member(
            session_id=SESSION,
            parent_message_id="msg-caller",
            child_message_id=child_message_id,
            task_group_id="",
        )
        self.redis.zsets.setdefault(self.index_key, {})[member] = score_ms
        return member

    def _sweeper(self, **kwargs) -> WaitIndexSweeper:
        kwargs.setdefault("enabled", False)
        return WaitIndexSweeper(self.redis, registry=self.registry, **kwargs)

    def test_the_threshold_strictly_exceeds_the_longest_deadline(self):
        # DEFAULT_ASK_USER_TIMEOUT_MS *equals* the session TTL, so a threshold
        # trimmed to the session TTL exactly would sit on the boundary of a
        # live ask_user wait and lose to any clock skew between the worker
        # that registered the entry and the one sweeping it.
        self.assertGreater(WAIT_PRUNE_AFTER_SECONDS * 1000, DEFAULT_ASK_USER_TIMEOUT_MS)

    async def test_prune_runs_while_compensation_is_off(self):
        # The reason this half cannot share the compensation switch: entries
        # are removed only by a reply or by a sweep, so the failures this
        # subsystem exists for would accumulate without bound in the very
        # structure meant to bound them.
        abandoned = self._entry(self.now_ms - (WAIT_PRUNE_AFTER_SECONDS + 60) * 1000)

        outcomes = await self._sweeper().sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_PRUNED), 1)
        self.assertNotIn(abandoned, self.redis.zsets[self.index_key])
        # ...and it stayed a deletion: nothing was interrogated, nothing was
        # answered.
        self.assertEqual(self.redis.published, {})

    async def test_prune_asks_nothing_about_the_entry_it_deletes(self):
        # No execution record exists for this caller at all. Pruning must not
        # care — that is what makes it a deletion rather than a verdict.
        self._entry(self.now_ms - (WAIT_PRUNE_AFTER_SECONDS + 60) * 1000)
        self.registry.executions.clear()

        outcomes = await self._sweeper().sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_PRUNED), 1)

    async def test_a_live_ask_user_wait_is_left_alone(self):
        # RED LINE. A human takes days by right, and this wait's deadline is
        # the session TTL itself — the closest anything legitimately gets to
        # the prune threshold.
        member = encode_member(
            session_id=SESSION,
            parent_message_id="msg-caller",
            child_message_id="",
            task_group_id="",
        )
        self.redis.zsets.setdefault(self.index_key, {})[member] = (
            self.now_ms + DEFAULT_ASK_USER_TIMEOUT_MS
        )

        outcomes = await self._sweeper().sweep_once()

        self.assertNotIn(OUTCOME_PRUNED, outcomes)
        self.assertIn(member, self.redis.zsets[self.index_key])

    async def test_an_overdue_ask_user_wait_is_still_left_alone(self):
        # Being late is the normal state of an ask_user wait: its deadline
        # passes the moment the person is slower than the session TTL, and it
        # is deliberately never compensated, so it also never gets renewed.
        # Pruning at the deadline would delete exactly those.
        member = encode_member(
            session_id=SESSION,
            parent_message_id="msg-caller",
            child_message_id="",
            task_group_id="",
        )
        self.redis.zsets.setdefault(self.index_key, {})[member] = self.now_ms - 1000

        await self._sweeper().sweep_once()

        self.assertIn(member, self.redis.zsets[self.index_key])

    async def test_a_recently_renewed_entry_is_left_alone(self):
        # A renewal rewrites the score to `now + increment`, so a renewed
        # entry looks young no matter how long the wait has actually run.
        renewed = self._entry(self.now_ms + WAIT_RENEW_INCREMENT_MS)

        await self._sweeper().sweep_once()

        self.assertIn(renewed, self.redis.zsets[self.index_key])

    async def test_an_entry_whose_last_renewal_is_ancient_is_pruned(self):
        # The score is evidence about the most recent *write*, whichever it
        # was: a renewal raises it, so an old score on a renewed entry proves
        # nothing has renewed it in longer than a session TTL — its session
        # registry is gone and no triage could reach any verdict but "caller
        # missing", which deletes it anyway.
        stale_renewal_at = self.now_ms - (WAIT_PRUNE_AFTER_SECONDS + 86400) * 1000
        renewed = self._entry(stale_renewal_at + WAIT_RENEW_INCREMENT_MS)

        outcomes = await self._sweeper().sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_PRUNED), 1)
        self.assertNotIn(renewed, self.redis.zsets[self.index_key])

    async def test_prune_and_compensation_coexist_in_one_pass(self):
        self.registry.add_execution(
            "msg-caller",
            session_id=SESSION,
            worker_id="worker-a",
            target_agent_type="agent-a",
            status=AgentState.WAITING_AGENT.value,
        )
        self.registry.add_execution(
            "msg-child",
            session_id=SESSION,
            source_agent_type="agent-a",
            target_agent_type="agent-b",
            status="RUNNING",
            worker_id="worker-dead",
        )
        abandoned = self._entry(
            self.now_ms - (WAIT_PRUNE_AFTER_SECONDS + 60) * 1000,
            child_message_id="msg-forgotten",
        )
        due = self._entry(self.now_ms - 1000)

        outcomes = await self._sweeper(enabled=True).sweep_once()

        self.assertEqual(outcomes.get(OUTCOME_PRUNED), 1)
        self.assertEqual(outcomes.get(OUTCOME_WORKER_LOST), 1)
        self.assertNotIn(abandoned, self.redis.zsets[self.index_key])
        # Compensation leaves its entry for the gate to claim.
        self.assertIn(due, self.redis.zsets[self.index_key])

    async def test_prune_keeps_its_own_coarser_cadence(self):
        sweeper = self._sweeper(prune_interval_seconds=3600)
        await sweeper.sweep_once()

        # Arrives after the first pass; the second pass is inside the prune
        # interval, so it must not run again.
        abandoned = self._entry(self.now_ms - (WAIT_PRUNE_AFTER_SECONDS + 60) * 1000)
        outcomes = await sweeper.sweep_once()

        self.assertNotIn(OUTCOME_PRUNED, outcomes)
        self.assertIn(abandoned, self.redis.zsets[self.index_key])

    async def test_pruning_off_and_compensation_off_touches_nothing(self):
        abandoned = self._entry(self.now_ms - (WAIT_PRUNE_AFTER_SECONDS + 60) * 1000)

        outcomes = await self._sweeper(prune_enabled=False).sweep_once()

        self.assertEqual(outcomes, {})
        self.assertIn(abandoned, self.redis.zsets[self.index_key])
        # Not even a shard lock was taken: there was nothing to do.
        self.assertEqual(self.redis.kv, {})


if __name__ == "__main__":
    unittest.main()
