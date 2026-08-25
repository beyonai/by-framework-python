"""Resolves suspended callers whose reply is never going to arrive.

A caller that suspends on ``call_agent(wait_for_reply=True)`` has *ended*:
its handler returned, the message was acked, and the only thing that can
ever run it again is a ``ResumeCommand`` landing on its agent type's control
stream. If the callee's worker is killed mid-task, nobody sends that
command — there is no timer left anywhere in the process that dispatched it.
This module is that missing timer, and it lives outside any single
execution: the wait index (``core/wait_index.py``) records *who is waiting
on whom*, and this sweeper walks the entries that came due.

Three properties shape the whole design:

* **Pull, don't push.** Whether a callee is still alive is already knowable
  from data the system keeps anyway — its execution record and its worker's
  heartbeat lease. A sweep reads that evidence when a deadline expires, so
  the happy path pays one ZADD and one ZREM and no periodic writes at all.
* **The sweeper never resolves a wait itself.** It synthesizes the reply the
  callee would have sent and puts it on the caller's control stream, then
  leaves the wait-index entry alone. ``core/wait_gate.py`` claims the entry
  when *some* copy of the reply is consumed. Whichever copy arrives first
  wins, in either order, and the loser is dropped — one accounting path, so
  a caller cannot be woken twice or (worse) left with its entry cleared and
  no reply on the way. See ``_emit_reply`` for why the alternative — clearing
  the entry here — quietly needs a second, ungated delivery channel.
* **Deeper waits expire first, and that is load-bearing.** A callee that is
  itself suspended waiting on *its* callee gets renewed rather than failed.
  The innermost wait times out, its failure travels up hop by hop through
  ordinary replies, and each level fails for a reason it can report. Failing
  every level of a chain at once would turn one dead worker into a chain-wide
  outage and lose the causal chain with it.

Compensation is deliberately narrower than cleanup:

* ``ask_user`` waits are **never compensated**. A human taking three
  days is not a fault, so there is nothing to compensate; the entry exists
  only so the gate can recognize a duplicate answer. They are still cleaned
  up once the caller is gone.
* A Task Group orphan is compensated by the *same* synthesized reply as any
  other, carrying the group id — so it is counted by the group's existing
  join and wakes the caller only if it is the last sibling outstanding. The
  sweeper never touches ``task_group_results``/``completed`` itself: a second
  writer of that accounting is what hangs a caller when its increment is the
  one that reaches ``total`` and no reply is left to trigger the join.

One action is taken beyond resolving the caller: a callee that ran past its
renewal ceiling (``CHILD_TIMEOUT``) is asked to stop, since it is the only
triage outcome with a live process on the other end. That request is
best-effort in the strict sense — it happens *after* the reply is emitted,
every failure is swallowed, and nothing about the wake-up depends on it. See
``_cancel_timed_out_child``.

The pass has two halves, switched separately, because only one of them
decides anything:

* **Compensation** — the triage above. It synthesizes replies and changes
  observable behaviour, so it is opt-in and carries the rollback switch for
  the whole liveness feature.
* **Pruning** — deleting entries whose score is old enough to prove nothing
  can be learned from them any more. That is not a decision, and it has to
  run *regardless*: an entry is only ever removed by a reply or by a sweep,
  so with compensation off, every call whose reply never arrives leaks one
  entry forever — the exact failure this subsystem exists for, accumulating
  without bound in the structure meant to bound it. See ``_prune_shard``.

Everything here is fail-soft: a sweep that raises must never take down the
worker hosting it, and any uncertainty resolves toward "leave the entry
alone and look again next cycle".
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from typing import Any, NamedTuple, Optional

from by_framework.common.constants import (
    DEFAULT_REPLY_TIMEOUT_MS,
    TASK_GROUP_FIELD_ABORTED,
    TASK_GROUP_FIELD_TOTAL,
    WAIT_INDEX_SHARDS,
    WAIT_PRUNE_AFTER_SECONDS,
    WAIT_PRUNE_INTERVAL_SECONDS,
    WAIT_RENEW_INCREMENT_MS,
    WAIT_RENEW_MAX_MULTIPLE,
    WAIT_RENEW_ORIGIN_TTL_SECONDS,
    WAIT_SWEEP_BATCH_LIMIT,
    WAIT_SWEEP_INTERVAL_SECONDS,
    WAIT_SWEEP_LOCK_TTL_SECONDS,
    LivenessErrorCode,
    RedisKeys,
    single_call_task_group_id,
)
from by_framework.common.logger import logger
from by_framework.common.redis_client import Redis, get_redis
from by_framework.core.protocol.agent_state import (AgentState, is_terminal_state)
from by_framework.core.registry import (
    WorkerRegistry,
    acquire_scoped_lock,
    release_scoped_lock,
)
from by_framework.core.wait_index import (
    WaitIndexMember,
    decode_member,
    fnv1a32,
    member_digest,
)
from by_framework.core.wait_reply import (
    SYNTHESIZED_BY_SWEEPER,
    failure_reply_data,
    stand_in_reply,
)

SWEEPER_ENABLED_ENV = "BY_FRAMEWORK_WAIT_SWEEPER_ENABLED"
SWEEPER_INTERVAL_ENV = "BY_FRAMEWORK_WAIT_SWEEP_INTERVAL_SECONDS"
SWEEPER_RENEW_MULTIPLE_ENV = "BY_FRAMEWORK_WAIT_RENEW_MAX_MULTIPLE"
SWEEPER_CANCEL_ON_TIMEOUT_ENV = "BY_FRAMEWORK_WAIT_CANCEL_ON_TIMEOUT"
SWEEPER_PRUNE_ENABLED_ENV = "BY_FRAMEWORK_WAIT_PRUNE_ENABLED"
SWEEPER_PRUNE_INTERVAL_ENV = "BY_FRAMEWORK_WAIT_PRUNE_INTERVAL_SECONDS"

# Identifies a reply this module built rather than an agent. Rides on the
# reply's metadata so it stays out of reply_data, which business code reads.
SYNTHETIC_REPLY_MARKER = SYNTHESIZED_BY_SWEEPER

# What a sweep decided about one entry. Returned (and counted) so both the
# logs and the tests can assert on the triage rather than on side effects.
OUTCOME_MALFORMED = "malformed"
OUTCOME_CALLER_MISSING = "caller_missing"
OUTCOME_CALLER_TERMINAL = "caller_terminal"
OUTCOME_CALLER_LOST = "caller_lost"
OUTCOME_CALLER_NOT_SUSPENDED = "caller_not_suspended"
OUTCOME_ASK_USER_SKIPPED = "ask_user_skipped"
OUTCOME_GROUP_GONE = "group_gone"
OUTCOME_GROUP_ABORTED = "group_aborted"
OUTCOME_GROUP_ALREADY_JOINED = "group_already_joined"
OUTCOME_CHILD_WAITING = "child_waiting"
OUTCOME_CHILD_ALIVE = "child_alive"
OUTCOME_RECOVERED = "recovered"
OUTCOME_WORKER_LOST = "worker_lost"
OUTCOME_NEVER_STARTED = "never_started"
OUTCOME_TIMED_OUT = "timed_out"
OUTCOME_UNROUTABLE = "unroutable"
OUTCOME_ERROR = "error"
# Not a triage outcome: counts entries deleted by the prune half of the pass,
# which never looks at an entry's contents.
OUTCOME_PRUNED = "pruned"

# States that mean "this execution is parked on a reply", as opposed to
# queued behind a worker or actively running. Written by
# GatewayWorker._apply_suspended_status.
SUSPENDED_STATES: frozenset[str] = frozenset(
    {AgentState.WAITING_AGENT.value, AgentState.WAITING_USER.value}
)


def _env_int(name: str, default: int) -> int:
    try:
        return int((os.environ.get(name) or "").strip() or default)
    except ValueError:
        return default


def _env_bool(name: str, *, default: bool) -> bool:
    val = (os.environ.get(name) or "").strip().lower()
    if val in {"1", "true", "yes", "on", "enabled"}:
        return True
    if val in {"0", "false", "no", "off", "disabled"}:
        return False
    return default


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value or "")


def _int(value: Any) -> int:
    """Best-effort epoch-ms parse; 0 for anything unusable."""
    try:
        return int(float(_text(value) or 0))
    except (TypeError, ValueError):
        return 0


def _base_status(value: Any) -> str:
    """Strip the ``": reason"`` suffix some statuses are decorated with.

    A caller parked on a Task Group persists as
    ``"WAITING_AGENT: waiting_for_group"``, and an aborted group's reply
    produces ``"CANCELLED: group_aborted"``. Comparing those raw makes a
    suspended caller look running and a cancelled one look live, so every
    status this module branches on goes through here first.
    """
    return _text(value).split(":", 1)[0].strip()


class DueEntry(NamedTuple):
    """One wait-index entry that came due, as this pass found it.

    ``deadline_ms`` is the score the entry carried when it was read — the
    caller's original deadline until a renewal overwrites it, which is why
    the first renewal is what records the renewal budget's origin.
    """

    index_key: str
    member: str
    deadline_ms: int
    entry: WaitIndexMember


class WaitIndexSweeper:
    """Background pass over due wait-index entries.

    One instance runs per worker (see ``WorkerRunner.start``), each claiming
    shards opportunistically, so the whole mechanism inherits the worker
    fleet's availability instead of needing a singleton service.

    Two switches, because the pass does two things of different kinds:

    * ``enabled`` (``BY_FRAMEWORK_WAIT_SWEEPER_ENABLED``, **off** by default)
      governs compensation — the triage, the synthesized replies, renewals
      and cancellation. This is the first thing in the liveness chain that
      changes observable behaviour, so it carries the rollback switch for all
      of it: with it off, the wait index is written and cleared but never
      acted on, and the system behaves exactly as it did before.
    * ``prune_enabled`` (``BY_FRAMEWORK_WAIT_PRUNE_ENABLED``, **on** by
      default) governs garbage collection only. It cannot ride on the switch
      above, because an entry is removed only by a reply or by a sweep: with
      compensation off, every call whose reply never arrives — the very
      failures this subsystem addresses — leaks one entry permanently. Nor
      does it need a switch of its own for safety: it deletes only entries
      whose score proves no interrogation could still succeed, so it decides
      nothing (see ``_prune_shard``).

    It is also runtime-agnostic on purpose: everything it reads and writes is
    plain Redis data, so a Python sweeper resolves suspended callers created
    by the TS and Java SDKs too, including cross-language call chains.
    """

    def __init__(
        self,
        redis_client: Optional[Redis] = None,
        *,
        worker_id: str = "",
        registry: Optional[WorkerRegistry] = None,
        interval_seconds: Optional[int] = None,
        enabled: Optional[bool] = None,
        renew_max_multiple: Optional[int] = None,
        cancel_on_timeout: Optional[bool] = None,
        prune_enabled: Optional[bool] = None,
        prune_interval_seconds: Optional[int] = None,
    ) -> None:
        self.redis = redis_client or get_redis()
        self.worker_id = worker_id or f"sweeper-{id(self)}"
        self.registry = registry or WorkerRegistry(self.redis)
        self.interval_seconds = (
            interval_seconds
            if interval_seconds is not None
            else _env_int(SWEEPER_INTERVAL_ENV, WAIT_SWEEP_INTERVAL_SECONDS)
        )
        self.enabled = (
            enabled
            if enabled is not None
            else _env_bool(SWEEPER_ENABLED_ENV, default=False)
        )
        self.prune_enabled = (
            prune_enabled
            if prune_enabled is not None
            else _env_bool(SWEEPER_PRUNE_ENABLED_ENV, default=True)
        )
        self.prune_interval_seconds = max(
            1,
            (
                prune_interval_seconds
                if prune_interval_seconds is not None
                else _env_int(SWEEPER_PRUNE_INTERVAL_ENV, WAIT_PRUNE_INTERVAL_SECONDS)
            ),
        )
        # Never below 1: a multiple of 1 means "the deadline is the ceiling",
        # i.e. no renewal ever happens and every callee that is merely slow is
        # killed at its first deadline.
        self.renew_max_multiple = max(
            1,
            (
                renew_max_multiple
                if renew_max_multiple is not None
                else _env_int(SWEEPER_RENEW_MULTIPLE_ENV, WAIT_RENEW_MAX_MULTIPLE)
            ),
        )
        # Deployment-wide, not per call. See _cancel_timed_out_child for why
        # this knob cannot be a call_agent argument without paying for it on
        # every happy-path dispatch.
        self.cancel_on_timeout = (
            cancel_on_timeout
            if cancel_on_timeout is not None
            else _env_bool(SWEEPER_CANCEL_ON_TIMEOUT_ENV, default=True)
        )
        self._lock_ttl_seconds = max(
            self.interval_seconds * 3, WAIT_SWEEP_LOCK_TTL_SECONDS
        )
        # Start each worker at a different shard so a fleet spreads out
        # instead of every member queuing on shard 0's lock every cycle.
        self._shard_cursor = _stable_offset(self.worker_id)
        # None means "never pruned in this process", so the first pass does.
        self._last_prune_monotonic: Optional[float] = None

    @property
    def loop_interval_seconds(self) -> int:
        """How long the background loop sleeps between passes.

        Compensation is what needs a short cadence (a due entry should not
        wait long for its triage). With it off, the only work left is a
        garbage collector with a multi-day horizon, so the loop drops to the
        prune cadence rather than waking every 30 seconds to do nothing.
        """
        return self.interval_seconds if self.enabled else self.prune_interval_seconds

    async def run(self) -> None:
        """Sweep every ``loop_interval_seconds`` until cancelled."""
        if not self.enabled and not self.prune_enabled:
            logger.debug("WaitIndexSweeper fully disabled, not starting.")
            return
        logger.info(
            "WaitIndexSweeper started (worker_id=%s, interval=%ds, "
            "compensate=%s, prune=%s)",
            self.worker_id,
            self.loop_interval_seconds,
            self.enabled,
            self.prune_enabled,
        )
        try:
            while True:
                await self.sweep_once()
                await asyncio.sleep(self.loop_interval_seconds)
        except asyncio.CancelledError:
            pass
        finally:
            logger.debug("WaitIndexSweeper stopped (worker_id=%s)", self.worker_id)

    async def sweep_once(self) -> dict[str, int]:
        """Run one pass over every shard this worker can claim right now.

        Returns a count per triage outcome, which is also what makes the
        pass observable in tests.
        """
        outcomes: dict[str, int] = {}
        pruning = self._prune_is_due()
        start = self._shard_cursor
        for offset in range(WAIT_INDEX_SHARDS):
            shard = (start + offset) % WAIT_INDEX_SHARDS
            for outcome, count in (await self._sweep_shard(shard, pruning)).items():
                outcomes[outcome] = outcomes.get(outcome, 0) + count
        # Rotate so the shard a busy worker starts on (and therefore claims
        # first) differs each cycle; nothing is pinned to one owner.
        self._shard_cursor = (start + 1) % WAIT_INDEX_SHARDS
        return outcomes

    def _prune_is_due(self) -> bool:
        """Whether this pass should prune, on the coarser prune cadence."""
        if not self.prune_enabled:
            return False
        now = time.monotonic()
        last = self._last_prune_monotonic
        if last is not None and now - last < self.prune_interval_seconds:
            return False
        self._last_prune_monotonic = now
        return True

    async def _sweep_shard(self, shard: int, pruning: bool = False) -> dict[str, int]:
        """Claim one shard, prune it and/or resolve its due entries.

        The lock is taken for both halves even though pruning is idempotent
        (``ZREMRANGEBYSCORE`` over a fixed range yields the same state no
        matter how many workers run it): holding it just keeps a fleet from
        issuing the same command N times a cycle. Correctness does not depend
        on it, which is why a lost or expired claim costs nothing here.
        """
        if not pruning and not self.enabled:
            return {}
        lock_key = RedisKeys.wait_sweep_lock(shard)
        token = uuid.uuid4().hex
        try:
            claimed = await acquire_scoped_lock(
                self.redis, lock_key, token, self._lock_ttl_seconds
            )
        except Exception as error:  # pylint: disable=broad-exception-caught
            logger.warning("Wait sweep could not claim shard %d: %s", shard, error)
            return {}
        if not claimed:
            return {}
        outcomes: dict[str, int] = {}
        try:
            if pruning:
                pruned = await self._prune_shard(shard)
                if pruned:
                    outcomes[OUTCOME_PRUNED] = pruned
            if self.enabled:
                for outcome, count in (await self._resolve_due_entries(shard)).items():
                    outcomes[outcome] = outcomes.get(outcome, 0) + count
            return outcomes
        except Exception as error:  # pylint: disable=broad-exception-caught
            # A sweep is a safety net; it must never be able to take down the
            # worker that hosts it.
            logger.warning("Wait sweep failed on shard %d: %s", shard, error)
            outcomes[OUTCOME_ERROR] = outcomes.get(OUTCOME_ERROR, 0) + 1
            return outcomes
        finally:
            try:
                await release_scoped_lock(self.redis, lock_key, token)
            except Exception as error:  # pylint: disable=broad-exception-caught
                logger.debug(
                    "Wait sweep lock release failed (shard %d): %s", shard, error
                )

    async def _prune_shard(self, shard: int) -> int:
        """Delete entries old enough that no interrogation could succeed.

        Runs whether or not compensation does, and that is the point. Nothing
        else ever removes a wait-index entry except the reply it was waiting
        for, so with compensation off every call whose reply never arrives —
        precisely the failures this subsystem exists for — leaves an entry
        behind forever, in a structure that has no TTL of its own (the shard
        ZSET is shared by every session, so it cannot carry one).

        Unlike the triage, this reads nothing: no member is decoded, no
        execution record is fetched, no reply is produced. It is one
        ``ZREMRANGEBYSCORE`` over a range whose upper bound is a *proof*
        rather than a guess. Every writer of an entry sets its score to its
        own clock plus a non-negative offset — registration adds the caller's
        timeout, ``_extend_deadline`` adds ``WAIT_RENEW_INCREMENT_MS`` — and
        only ever writes while the caller's execution record exists. So a
        score more than ``WAIT_PRUNE_AFTER_SECONDS`` in the past means the
        entry was last touched longer than a session TTL ago, hence its
        session registry is gone and the only outcome triage could ever reach
        for it is "caller missing", which deletes it anyway. That holds for
        renewed entries too: a renewal *raises* the score, so an old score is
        evidence about the most recent renewal, not about registration.

        Because it decides nothing, it needs no opt-in — and because the
        threshold sits a day beyond ``DEFAULT_SESSION_TTL``, the longest
        deadline anything registers (``DEFAULT_ASK_USER_TIMEOUT_MS``, which
        equals the session TTL exactly) is never near it.
        """
        cutoff_ms = int(time.time() * 1000) - WAIT_PRUNE_AFTER_SECONDS * 1000
        removed = int(
            await self.redis.zremrangebyscore(  # type: ignore
                RedisKeys.wait_index(shard), 0, cutoff_ms
            )
            or 0
        )
        if removed:
            logger.info(
                "Wait sweep pruned %d abandoned wait-index entr%s from shard %d "
                "(older than %ds)",
                removed,
                "y" if removed == 1 else "ies",
                shard,
                WAIT_PRUNE_AFTER_SECONDS,
            )
        return removed

    async def _resolve_due_entries(self, shard: int) -> dict[str, int]:
        index_key = RedisKeys.wait_index(shard)
        now_ms = int(time.time() * 1000)
        # Scores come back with the members because the score *is* the
        # evidence for how long this wait has already run: it is the caller's
        # original deadline until the first renewal replaces it, and that
        # original is what the renewal budget is measured from.
        due = await self.redis.zrangebyscore(  # type: ignore
            index_key, 0, now_ms, start=0, num=WAIT_SWEEP_BATCH_LIMIT, withscores=True
        )
        outcomes: dict[str, int] = {}
        for raw_member, raw_score in due or []:
            member = _text(raw_member)
            try:
                outcome = await self._resolve_entry(index_key, member, raw_score)
            except Exception as error:  # pylint: disable=broad-exception-caught
                # One poisonous entry must not stop the rest of the shard.
                logger.warning(
                    "Wait sweep could not resolve entry %r: %s", member, error
                )
                outcome = OUTCOME_ERROR
            outcomes[outcome] = outcomes.get(outcome, 0) + 1
        return outcomes

    async def _resolve_entry(
        self, index_key: str, member: str, deadline_ms: Any = 0
    ) -> str:
        """Triage one due entry against evidence that already exists.

        Ordering is deliberate: everything that establishes *there is still
        somebody waiting* is checked before anything that could produce a
        reply. A reply nobody is waiting for is not free — it re-enters a
        finished execution.
        """
        try:
            entry = decode_member(member)
        except ValueError as error:
            logger.warning("Dropping malformed wait-index member %r: %s", member, error)
            await self.redis.zrem(index_key, member)  # type: ignore
            return OUTCOME_MALFORMED

        due = DueEntry(
            index_key=index_key,
            member=member,
            deadline_ms=_int(deadline_ms),
            entry=entry,
        )
        caller = await self.registry.get_execution_by_message_id(
            entry.parent_message_id, session_id=entry.session_id
        )
        if caller is None:
            # The session registry entry expired out from under it: no reply,
            # synthesized or real, can reattach this caller any more.
            logger.info(
                "Wait sweep dropping entry for a caller with no execution record "
                "(session=%s, caller=%s)",
                entry.session_id,
                entry.parent_message_id,
            )
            await self.redis.zrem(index_key, member)  # type: ignore
            return OUTCOME_CALLER_MISSING

        caller_status = _base_status(caller.get("status"))
        if is_terminal_state(caller_status):
            # Reachable through a narrow but real window: the entry is
            # registered before the dispatch xadd, so an xadd that raises
            # fails the caller and leaves the entry behind. Waking a finished
            # execution is exactly what the idempotency work exists to
            # prevent, so clean up and synthesize nothing.
            logger.info(
                "Wait sweep dropping entry for an already-%s caller "
                "(session=%s, caller=%s)",
                caller_status,
                entry.session_id,
                entry.parent_message_id,
            )
            await self.redis.zrem(index_key, member)  # type: ignore
            return OUTCOME_CALLER_TERMINAL

        if caller_status not in SUSPENDED_STATES:
            return await self._resolve_unsuspended_caller(due, caller)

        if not entry.child_message_id:
            # ask_user. "The human hasn't answered yet" is not a fault
            # and has no compensation; the entry stays purely so a repeated
            # answer is recognized as a duplicate.
            return OUTCOME_ASK_USER_SKIPPED

        if entry.task_group_id:
            blocked = await self._group_blocks_compensation(due)
            if blocked is not None:
                return blocked

        return await self._triage_child(due, caller)

    async def _group_blocks_compensation(self, due: DueEntry) -> Optional[str]:
        """Reasons a Task Group orphan must be cleaned up, not compensated.

        A group orphan is otherwise compensated exactly like any other: the
        synthesized reply carries the group id, so ``GatewayWorker``'s join
        stores it, increments ``completed``, and aggregates only if it was the
        last sibling outstanding. That is the whole point of routing it as a
        reply — writing the result and the counter from here would be a second
        implementation of the group's accounting, and when *that* copy is the
        increment reaching ``total`` there is no reply left to trigger the
        join and the caller hangs forever. (Same shape as the dispatch-time
        double-accounting bug this repo has already shipped once.)

        Returns an outcome when the entry was resolved here, or None to let
        the normal triage run.
        """
        entry = due.entry
        group_key = RedisKeys.task_group(entry.task_group_id)
        total = await self.redis.hget(group_key, TASK_GROUP_FIELD_TOTAL)  # type: ignore
        if total is None:
            # The group tracker expired (TASK_GROUP_TTL_SECONDS) or was never
            # written. A reply then finds no group to join, so it would fall
            # through as a lone result and resume the caller with one sibling's
            # payload where the aggregate belongs.
            logger.info(
                "Wait sweep dropping entry whose task group no longer exists "
                "(session=%s, caller=%s, group=%s)",
                entry.session_id,
                entry.parent_message_id,
                entry.task_group_id,
            )
            await self.redis.zrem(due.index_key, due.member)  # type: ignore
            return OUTCOME_GROUP_GONE

        if await self.redis.hget(group_key, TASK_GROUP_FIELD_ABORTED):  # type: ignore
            # Dispatch failed partway through the fan-out, so the caller was
            # already failed and every reply for this group is discarded on
            # arrival. Synthesizing one more changes nothing and re-enters a
            # terminated execution on the way to being discarded.
            logger.info(
                "Wait sweep dropping entry for aborted task group "
                "(session=%s, caller=%s, group=%s)",
                entry.session_id,
                entry.parent_message_id,
                entry.task_group_id,
            )
            await self.redis.zrem(due.index_key, due.member)  # type: ignore
            return OUTCOME_GROUP_ABORTED

        recorded = await self.redis.hget(  # type: ignore
            RedisKeys.task_group_results(entry.task_group_id),
            entry.child_message_id,
        )
        if recorded is not None:
            # A result under this sub-task's id can only have been written by
            # the join, which means its reply already arrived and was counted.
            # The entry outliving that is a gate ZREM that did not land; a
            # second synthesized reply would be counted a second time.
            logger.info(
                "Wait sweep dropping entry already joined by its reply "
                "(session=%s, caller=%s, child=%s, group=%s)",
                entry.session_id,
                entry.parent_message_id,
                entry.child_message_id,
                entry.task_group_id,
            )
            await self.redis.zrem(due.index_key, due.member)  # type: ignore
            return OUTCOME_GROUP_ALREADY_JOINED

        return None

    async def _resolve_unsuspended_caller(
        self, due: DueEntry, caller: dict[str, Any]
    ) -> str:
        """Handle an entry whose caller is not (yet, or ever) suspended.

        Two very different situations share this shape. The caller may still
        be inside the handler that registered the wait, in which case a reply
        now would run alongside it — so back off and look again. Or the
        caller's own worker died before it could record its suspension, in
        which case no reply can ever reattach it and the entry is garbage;
        that chain gets rescued one level up, by the wait its own caller
        registered.
        """
        caller_worker_id = _text(caller.get("worker_id"))
        if caller_worker_id and not await self.registry.is_worker_online(
            caller_worker_id
        ):
            logger.info(
                "Wait sweep dropping entry whose caller was lost with worker %s "
                "(caller=%s, status=%s)",
                caller_worker_id,
                _text(caller.get("message_id")),
                _base_status(caller.get("status")),
            )
            await self.redis.zrem(due.index_key, due.member)  # type: ignore
            return OUTCOME_CALLER_LOST
        await self._extend_deadline(due)
        return OUTCOME_CALLER_NOT_SUSPENDED

    async def _triage_child(self, due: DueEntry, caller: dict[str, Any]) -> str:
        """Decide the callee's fate from its execution record and its lease."""
        entry = due.entry
        child = await self.registry.get_execution_by_message_id(
            entry.child_message_id, session_id=entry.session_id
        )
        if child is None:
            return await self._synthesize_failure(
                due,
                caller,
                child={},
                error_code=LivenessErrorCode.CHILD_NEVER_STARTED,
                message=(
                    f"No execution was ever recorded for sub-task "
                    f"{entry.child_message_id}"
                ),
                outcome=OUTCOME_NEVER_STARTED,
            )

        child_status = _base_status(child.get("status"))
        if is_terminal_state(child_status):
            return await self._recover_finished_child(due, caller, child)

        if child_status in SUSPENDED_STATES:
            # The callee is itself waiting on someone. Its own entry has a
            # deadline of its own and will fail first if that wait breaks;
            # killing this one now would collapse the whole chain at once and
            # report the wrong cause at every level.
            #
            # Deliberately exempt from the renewal ceiling below: this wait was
            # registered *before* the deeper one it is blocked on, so its
            # ceiling would be reached first and the chain would fail from the
            # top down — inverting the propagation order the whole design rests
            # on. The chain still terminates, because the deepest wait is
            # blocked on real work and is subject to the ceiling.
            await self._extend_deadline(due)
            return OUTCOME_CHILD_WAITING

        child_worker_id = _text(child.get("worker_id"))
        if not child_worker_id:
            return await self._synthesize_failure(
                due,
                caller,
                child=child,
                error_code=LivenessErrorCode.CHILD_NEVER_STARTED,
                message=(
                    f"Sub-task {entry.child_message_id} was never picked up by a "
                    f"worker (status={child_status})"
                ),
                outcome=OUTCOME_NEVER_STARTED,
            )

        if await self.registry.is_worker_online(child_worker_id):
            # Running long is not the same as being dead, and the lease is the
            # only signal that tells them apart — so a live lease buys more
            # time, which is what keeps slow work from being killed.
            #
            # But only up to a ceiling. Renewing on a live lease alone answers
            # "is the process up", not "is the work progressing", so a callee
            # deadlocked or stuck in a call that never returns would be renewed
            # forever and its caller would never be resolved — precisely the
            # hang this subsystem exists to bound. There is no signal that
            # separates that from a genuinely long call (both sit still), so
            # the ceiling is deliberately crude: generous, absolute, and
            # therefore predictable.
            limit_ms = await self._renewal_ceiling_ms(due, child)
            if int(time.time() * 1000) < limit_ms:
                await self._extend_deadline(due)
                return OUTCOME_CHILD_ALIVE
            outcome = await self._synthesize_failure(
                due,
                caller,
                child=child,
                error_code=LivenessErrorCode.CHILD_TIMEOUT,
                message=(
                    f"Sub-task {entry.child_message_id} is still {child_status} on "
                    f"live worker {child_worker_id} but produced no reply within "
                    f"{self.renew_max_multiple}x its reply timeout"
                ),
                outcome=OUTCOME_TIMED_OUT,
            )
            # Strictly after the caller has been resolved, and strictly
            # best-effort: this is the one branch with a live process on the
            # other end, so it is the only one where stopping the work is
            # even meaningful — and the caller's wake-up must not depend on
            # whether it lands.
            await self._cancel_timed_out_child(due, child)
            return outcome

        return await self._synthesize_failure(
            due,
            caller,
            child=child,
            error_code=LivenessErrorCode.CHILD_WORKER_LOST,
            message=(
                f"Worker {child_worker_id} running sub-task "
                f"{entry.child_message_id} is no longer alive "
                f"(status={child_status})"
            ),
            outcome=OUTCOME_WORKER_LOST,
        )

    async def _cancel_timed_out_child(
        self, due: DueEntry, child: dict[str, Any]
    ) -> bool:
        """Stop a callee that ran past its ceiling. Best-effort, by nature.

        Only ``CHILD_TIMEOUT`` gets here. ``CHILD_WORKER_LOST`` and
        ``CHILD_NEVER_STARTED`` have nothing on the other end to cancel, and
        a callee that already finished has nothing left to stop.

        The whole path is delegated to ``GatewayClient.cancel_task``, which
        already gets the two hard parts right: it delivers to
        ``RedisKeys.worker_ctrl_stream(worker_id)`` — ``handle_cancel_task``
        looks the execution up in its *worker's own in-memory* dict, so a
        cancel put on the agent type's competitive stream is claimed by an
        arbitrary worker, finds nothing, and cancels nothing while still
        recording that it did — and it walks the sub-tree, so a callee's own
        callees stop too.

        Two things it deliberately does not do:

        * **It does not silence the callee.** ``GatewayWorker``'s
          ``CancelledError`` branch still sends a ``CANCELLED`` reply. That
          copy is dropped by the idempotency gate, which is why cancellation
          can never be a substitute for it.
        * **It does not affect the caller.** Cancellation is cooperative —
          ``task.cancel()`` needs the target to yield the event loop — and
          the archetypal ``CHILD_TIMEOUT`` is a callee wedged in a blocking
          call, i.e. exactly the case where it cannot land. The synthesized
          reply has already gone out above; every failure here is swallowed.

        The opt-out (``BY_FRAMEWORK_WAIT_CANCEL_ON_TIMEOUT``) is per
        deployment rather than per ``call_agent``. A per-call flag has to
        reach a sweeper that only sees the wait-index member, and the member
        is a cross-SDK wire format that must stay rebuildable from a reply
        alone — so it would need a side key written on *every* dispatch to
        serve a decision taken only after a timeout, which is the periodic
        happy-path cost this whole design avoids. A knob whose blast radius
        is "a timed-out callee keeps burning CPU" does not earn that.
        """
        if not self.cancel_on_timeout:
            return False
        child_message_id = due.entry.child_message_id
        if child.get("cancel_requested"):
            # A previous sweep already asked. Repeating it every renewal
            # window adds messages, not cancellation.
            return False
        try:
            from by_framework.client.client import GatewayClient

            response = await GatewayClient(
                registry=self.registry, redis_client=self.redis
            ).cancel_task(
                message_id=child_message_id,
                session_id=due.entry.session_id,
                reason=f"{LivenessErrorCode.CHILD_TIMEOUT}: no reply before deadline",
                requested_by=f"wait_sweeper:{self.worker_id}",
            )
            logger.info(
                "Wait sweep requested cancellation of timed-out sub-task %s "
                "(session=%s): %s",
                child_message_id,
                due.entry.session_id,
                getattr(response, "status", ""),
            )
            return bool(getattr(response, "success", False))
        except Exception as error:  # pylint: disable=broad-exception-caught
            logger.warning(
                "Wait sweep could not cancel timed-out sub-task %s (session=%s): "
                "%s. The caller was resolved regardless.",
                child_message_id,
                due.entry.session_id,
                error,
            )
            return False

    async def _renewal_ceiling_ms(self, due: DueEntry, child: dict[str, Any]) -> int:
        """Absolute instant past which this wait stops being renewed.

        Measured from the wait's *original* deadline, not from a renewal
        count: renewals happen at a fixed increment that is a tunable, so
        counting them would let a config change silently move the bound. The
        original deadline survives renewals in
        ``RedisKeys.wait_renew_origin`` (written by the first renewal, see
        ``_extend_deadline``); before the first renewal the score still is it.

        The caller's own timeout is recovered as the span between the
        sub-task's ``created_at`` — written by ``initialize_execution``
        immediately before the wait is registered — and that original
        deadline, so a caller that asked for ten minutes is not held to the
        same budget as one that asked for four hours. When that span is
        unusable the default timeout is assumed, erring toward waiting longer:
        killing a healthy callee is worse than resolving a dead one late.

        Progress is deliberately *not* used as evidence. A callee's
        ``updated_at`` stands just as still during a legitimate 20-minute
        model call as during a deadlock, so a ceiling keyed on it would kill
        exactly the work it is meant to protect.
        """
        origin_ms = await self._renewal_origin_ms(due)
        registered_ms = _int(child.get("created_at"))
        timeout_ms = origin_ms - registered_ms
        if registered_ms <= 0 or timeout_ms <= 0:
            timeout_ms = DEFAULT_REPLY_TIMEOUT_MS
        # One renewal increment is the floor so a degenerate (zero, or
        # very short) timeout still buys the callee one look.
        grace_ms = max(
            timeout_ms * (self.renew_max_multiple - 1), WAIT_RENEW_INCREMENT_MS
        )
        return origin_ms + grace_ms

    async def _renewal_origin_ms(self, due: DueEntry) -> int:
        """The deadline this wait's renewal budget is measured from."""
        try:
            stored = await self.redis.get(  # type: ignore
                RedisKeys.wait_renew_origin(
                    due.entry.session_id, member_digest(due.member)
                )
            )
        except Exception as error:  # pylint: disable=broad-exception-caught
            logger.debug("Wait sweep could not read renewal origin: %s", error)
            stored = None
        # Nothing recorded yet means nothing has renewed this entry yet, so
        # the score it came due with still is the original deadline.
        return _int(stored) or due.deadline_ms

    async def _recover_finished_child(
        self,
        due: DueEntry,
        caller: dict[str, Any],
        child: dict[str, Any],
    ) -> str:
        """Rebuild the reply of a callee that finished but never got heard.

        The callee stores its result before it sends the reply (see
        ``GatewayWorker._persist_single_call_result``), precisely so this case
        loses a *message* rather than an *answer*. When the stored result is
        there the caller gets the real thing and never learns a message was
        lost, beyond a marker on the metadata.

        When it is not there, the honest outcome is a failure. Reporting an
        empty COMPLETED would hand the caller a fabricated answer, which is
        the one outcome worse than a reported failure.
        """
        entry = due.entry
        stored = await self._load_stored_result(entry.child_message_id)
        child_status = _base_status(child.get("status"))
        if stored is None:
            return await self._synthesize_failure(
                due,
                caller,
                child=child,
                error_code=LivenessErrorCode.REPLY_LOST_RECOVERED,
                message=(
                    f"Sub-task {entry.child_message_id} finished with "
                    f"{child_status} but neither its reply nor its stored result "
                    f"is available"
                ),
                outcome=OUTCOME_RECOVERED,
                status=(
                    child_status
                    if child_status
                    in (AgentState.FAILED.value, AgentState.CANCELLED.value)
                    else AgentState.FAILED.value
                ),
            )

        await self._emit_reply(
            due,
            caller,
            child=child,
            status=_text(stored.get("status")) or child_status,
            content=stored.get("content") or "",
            reply_data=stored.get("reply_data"),
            extra_payload=dict(stored.get("extra_payload") or {}),
            error_code=LivenessErrorCode.REPLY_LOST_RECOVERED,
            metadata=dict(stored.get("metadata") or {}),
        )
        return OUTCOME_RECOVERED

    async def _load_stored_result(self, child_message_id: str) -> Optional[dict]:
        """Read the callee's persisted result, or None if there is none."""
        results_key = RedisKeys.task_group_results(
            single_call_task_group_id(child_message_id)
        )
        raw = await self.redis.hget(results_key, child_message_id)  # type: ignore
        if not raw:
            return None
        try:
            decoded = json.loads(_text(raw))
        except (TypeError, ValueError) as error:
            logger.warning(
                "Stored result for sub-task %s is unreadable: %s",
                child_message_id,
                error,
            )
            return None
        return decoded if isinstance(decoded, dict) else None

    async def _synthesize_failure(
        self,
        due: DueEntry,
        caller: dict[str, Any],
        *,
        child: dict[str, Any],
        error_code: str,
        message: str,
        outcome: str,
        status: str = AgentState.FAILED.value,
    ) -> str:
        """Emit the failure reply the callee would have sent, had it lived.

        ``reply_data`` carries ``error``/``error_code`` because that is where
        a dispatch-time failure already puts them: a caller must not be able
        to tell a callee that failed from one that never got to fail, or the
        two shapes drift apart and callers grow a second error path.
        """
        entry = due.entry
        logger.warning(
            "Wait sweep synthesizing %s reply for caller=%s (child=%s, "
            "session=%s): %s",
            error_code,
            entry.parent_message_id,
            entry.child_message_id,
            entry.session_id,
            message,
        )
        emitted = await self._emit_reply(
            due,
            caller,
            child=child,
            status=status,
            content="",
            reply_data=failure_reply_data(
                error=message,
                error_code=error_code,
                child_message_id=entry.child_message_id,
            ),
            extra_payload={},
            error_code=error_code,
            metadata={},
        )
        return outcome if emitted else OUTCOME_UNROUTABLE

    async def _emit_reply(
        self,
        due: DueEntry,
        caller: dict[str, Any],
        *,
        child: dict[str, Any],
        status: str,
        content: Any,
        reply_data: Any,
        extra_payload: dict,
        error_code: str,
        metadata: dict,
    ) -> bool:
        """Put a stand-in reply on the caller's control stream.

        Field-for-field it is what ``GatewayWorker._enqueue_agent_return``
        produces — same id reversal (``header.message_id`` is the caller's,
        ``header.parent_message_id`` the sub-task's), same stream. That is
        not tidiness: the runner reattaches the suspended execution by
        ``header.message_id``, and the gate rebuilds the wait-index member
        from the header, so anything else either fails to resume the caller
        or fails to clear its entry.

        The wait-index entry is left in place on purpose. Removing it here
        would leave the synthesized reply as the only copy that must not be
        gated — and a reply that bypasses the gate is a second wake-up path,
        which is what makes double-resumes possible in the first place.
        Instead the deadline is pushed out, so a caller whose control stream
        is not being consumed gets at most one stand-in per renewal window
        instead of one per sweep.

        A reply for a Task Group member carries the group id like any other,
        so it is stored and counted by the group's existing join and resolves
        the caller only when it is the last sibling outstanding. Booking the
        result here instead would be a second writer of that accounting, and
        the copy that reaches ``total`` would leave no reply to run the join.
        """
        entry = due.entry
        caller_agent_type = _text(child.get("source_agent_type")) or _text(
            caller.get("target_agent_type")
        )
        if not caller_agent_type:
            logger.warning(
                "Wait sweep cannot route a reply for caller=%s (session=%s): the "
                "execution records name no caller agent type",
                entry.parent_message_id,
                entry.session_id,
            )
            await self._extend_deadline(due)
            return False

        command = stand_in_reply(
            session_id=entry.session_id,
            caller_message_id=entry.parent_message_id,
            caller_agent_type=caller_agent_type,
            child_message_id=entry.child_message_id,
            child_agent_type=_text(child.get("target_agent_type")),
            task_group_id=entry.task_group_id,
            trace_id=(_text(child.get("trace_id")) or _text(caller.get("trace_id"))),
            status=status,
            content=content,
            reply_data=reply_data,
            extra_payload=extra_payload,
            metadata={**metadata, "sweeper_worker_id": self.worker_id},
            error_code=error_code,
            synthesized_by=SYNTHESIZED_BY_SWEEPER,
        )
        await self.redis.xadd(  # type: ignore
            RedisKeys.ctrl_stream(caller_agent_type),
            command.to_redis_payload(),
        )
        await self._extend_deadline(due)
        return True

    async def _extend_deadline(self, due: DueEntry) -> None:
        """Push an entry's deadline out by a fixed increment.

        Fixed rather than the caller's original timeout: the member has to
        stay rebuildable from a reply alone, and a reply cannot know what
        timeout its caller chose, so the timeout is not encoded in it.

        Overwriting the score destroys the only record of the original
        deadline, so the first renewal saves it first (SET NX, so later
        renewals leave it alone). Without that, ``_renewal_ceiling_ms`` would
        re-measure from the deadline it just pushed out and no ceiling could
        ever be reached. Fail-soft: if the write is lost the budget merely
        restarts from the current deadline — bounded, just more generous.
        """
        try:
            await self.redis.set(  # type: ignore
                RedisKeys.wait_renew_origin(
                    due.entry.session_id, member_digest(due.member)
                ),
                str(due.deadline_ms),
                nx=True,
                ex=WAIT_RENEW_ORIGIN_TTL_SECONDS,
            )
        except Exception as error:  # pylint: disable=broad-exception-caught
            logger.debug("Wait sweep could not record renewal origin: %s", error)
        deadline_ms = int(time.time() * 1000) + WAIT_RENEW_INCREMENT_MS
        await self.redis.zadd(due.index_key, {due.member: deadline_ms})  # type: ignore

    def snapshot(self) -> dict[str, Any]:
        """Synchronous diagnostic snapshot for health checks."""
        return {
            "worker_id": self.worker_id,
            "enabled": self.enabled,
            "interval_seconds": self.interval_seconds,
            "shards": WAIT_INDEX_SHARDS,
            "lock_ttl_seconds": self._lock_ttl_seconds,
            "renew_max_multiple": self.renew_max_multiple,
            "cancel_on_timeout": self.cancel_on_timeout,
            "prune_enabled": self.prune_enabled,
            "prune_interval_seconds": self.prune_interval_seconds,
            "prune_after_seconds": WAIT_PRUNE_AFTER_SECONDS,
        }


def _stable_offset(worker_id: str) -> int:
    """Spread workers' starting shard deterministically (FNV-1a, no salt)."""
    return fnv1a32(worker_id) % WAIT_INDEX_SHARDS
