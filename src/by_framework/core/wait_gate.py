"""Idempotency gate for replies that resume a suspended caller.

A suspended caller is woken by exactly one ``ResumeCommand``. Once a sweep
can *synthesize* that reply (a callee whose worker died will never send
one), two copies can exist for the same wait: the synthesized one and the
real one that shows up late. Waking the caller twice re-runs a finished
execution and, in a Task Group, pushes ``completed`` past ``total`` and
aggregates a second time.

The gate is the single place that decides which copy wins. It runs at the
one point every reply passes through — right after a reply is parsed,
before any registry lookup and before Task Group join accounting — and
claims the caller's wait-index entry with a ``ZREM``. Exactly one claimant
can win, because ``ZREM`` is atomic.

The hard part is what ``ZREM`` returning 0 means, since it conflates two
opposite situations:

* the entry existed and someone else already claimed it — a true duplicate,
  drop it;
* the entry never existed — the reply belongs to a dispatch made before
  this version shipped, or to a wait whose entry expired. Dropping it
  silently loses a real reply, and during a rolling upgrade *every*
  in-flight reply looks like this.

A short-lived "consumed" marker written by the winner separates them: a 0
with a marker is a duplicate, a 0 without one is unregistered and must be
let through. When in doubt the gate lets the message through — a spurious
extra wake-up is recoverable, a dropped reply is permanent silence. The
same rule makes the gate fail *open*: any Redis error here allows the
message.
"""

from typing import Any, NamedTuple

from by_framework.common.constants import WAIT_CONSUMED_TTL_SECONDS, RedisKeys
from by_framework.common.logger import logger
from by_framework.core.protocol.commands import GatewayCommand
from by_framework.core.wait_index import (
    encode_member,
    member_digest,
    member_from_resume,
    wait_index_key,
)

# Why a reply was allowed through / dropped. Carried on the decision so the
# caller can log it and put it on the orphaned_reply event.
ALLOW_CLAIMED = "claimed"
ALLOW_UNREGISTERED = "unregistered"
ALLOW_GATE_ERROR = "gate_error"
DENY_ALREADY_CONSUMED = "already_consumed"


class WaitGateDecision(NamedTuple):
    """Outcome of the gate.

    Attributes:
        allow: Whether the reply may be processed. False only when the wait
            it targets is provably already resolved.
        reason: One of the ALLOW_*/DENY_* constants above.
        member: The wait-index member the decision was made about ("" when
            no candidate matched).
    """

    allow: bool
    reason: str
    member: str = ""


def consumed_marker_key(session_id: str, member: str) -> str:
    """Redis key of the "already consumed" marker for one wait-index member.

    Part of the cross-SDK contract, since any SDK's worker may gate another
    SDK's reply — see ``wait_index.member_digest`` for why the member is
    hashed rather than embedded.
    """
    return RedisKeys.wait_consumed(session_id, member_digest(member))


def candidate_members(command: GatewayCommand) -> list[str]:
    """Wait-index members this reply could be clearing, most-specific first.

    Normally there is exactly one: the member rebuilt from the reply's own
    header. The second candidate covers ``ask_user``, which registers with
    an empty ``child_message_id`` because it has no sub-task — while the
    matching reply comes from a client that is free to put anything in
    ``header.parent_message_id`` (existing callers put the caller's own
    parent there, not an empty string), so it cannot be rebuilt exactly.

    Order matters, and so does the fact that each candidate is fully
    resolved (claim, then check its marker) before the next is tried: a
    duplicate sub-agent reply must be caught by *its own* marker rather than
    fall through and clear a live ask_user wait that happens to belong to
    the same caller.

    A reply carrying a ``task_group_id`` is a sub-agent reply by
    construction, so the ask_user variant is not even considered for it.
    """
    header = command.header
    members = [member_from_resume(command)]
    if not header.task_group_id:
        ask_user_member = encode_member(
            session_id=header.session_id,
            parent_message_id=header.message_id,
            child_message_id="",
            task_group_id="",
        )
        if ask_user_member not in members:
            members.append(ask_user_member)
    return members


async def _mark_consumed(redis: Any, session_id: str, member: str) -> None:
    """Record that this wait was resolved, so a late twin can be recognized.

    Fail-soft: losing the marker only means a much later duplicate would be
    allowed through (one extra wake-up), which is the direction this whole
    module errs in anyway.
    """
    try:
        await redis.set(
            consumed_marker_key(session_id, member),
            "1",
            ex=WAIT_CONSUMED_TTL_SECONDS,
        )
    except Exception as error:  # pylint: disable=broad-exception-caught
        logger.warning(
            "Failed to mark wait entry consumed (session=%s): %s", session_id, error
        )


async def consume_wait_entry(redis: Any, command: GatewayCommand) -> WaitGateDecision:
    """Claim the wait a reply resolves; report whether it may be processed.

    Call once per ``ResumeCommand``, before the execution lookup and before
    Task Group join accounting.
    """
    session_id = command.header.session_id
    try:
        index_key = wait_index_key(session_id)
        for member in candidate_members(command):
            removed = int(await redis.zrem(index_key, member) or 0)
            if removed > 0:
                await _mark_consumed(redis, session_id, member)
                return WaitGateDecision(True, ALLOW_CLAIMED, member)
            if await redis.exists(consumed_marker_key(session_id, member)):
                return WaitGateDecision(False, DENY_ALREADY_CONSUMED, member)
        # No entry, no marker: nobody ever registered this wait (a dispatch
        # from before this version, or an entry that outlived its index).
        # Unknown is not the same as duplicate — let it through.
        return WaitGateDecision(True, ALLOW_UNREGISTERED)
    except Exception as error:  # pylint: disable=broad-exception-caught
        # Fail open. A gate that drops messages when Redis hiccups is worse
        # than the duplicate it was built to prevent.
        logger.warning(
            "Wait-index gate unavailable for session=%s, allowing reply: %s",
            session_id,
            error,
        )
        return WaitGateDecision(True, ALLOW_GATE_ERROR)


async def emit_orphaned_reply(
    redis: Any,
    command: GatewayCommand,
    *,
    worker_id: str = "",
    reason: str = DENY_ALREADY_CONSUMED,
) -> None:
    """Announce on the session data stream that a reply was dropped.

    A dropped reply is not noise: the sub-agent ran, produced a result, and
    may have had side effects that nobody will now account for. Emitting it
    on the existing data plane keeps it visible without inventing a second
    reporting mechanism.

    Fail-soft by construction — the drop/allow decision has already been
    made, and reporting it must never change or block it.
    """
    header = command.header
    try:
        from by_framework.common.emitter import GatewayDataEmitter
        from by_framework.core.protocol.event_type import EventType

        await GatewayDataEmitter(redis_client=redis).emit_event(
            session_id=header.session_id,
            trace_id=header.trace_id,
            event_type=EventType.ORPHANED_REPLY.value,
            source_agent_type=header.source_agent_type,
            message_id=header.message_id,
            parent_message_id=header.parent_message_id,
            data={
                "reason": reason,
                # The suspended caller this reply was addressed to...
                "caller_message_id": header.message_id,
                # ...and the sub-task that produced it (empty for ask_user).
                "child_message_id": header.parent_message_id,
                "task_group_id": header.task_group_id,
                "status": str(getattr(command, "status", "") or ""),
                "worker_id": worker_id,
            },
        )
    except Exception as error:  # pylint: disable=broad-exception-caught
        logger.warning(
            "Failed to emit orphaned_reply event (session=%s, message_id=%s): %s",
            header.session_id,
            header.message_id,
            error,
        )
