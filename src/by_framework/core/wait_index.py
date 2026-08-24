"""Wait-index member encoding for suspended-caller liveness.

A caller that suspends on ``call_agent(wait_for_reply=True)`` (or
``call_agents``/``ask_user``) registers one entry in the sharded ZSET
``RedisKeys.wait_index(shard)``; the entry is removed again when the reply
lands. This module owns the *pure* half of that contract: how the ZSET
member is spelled, and how a member is reconstructed from a reply.

The load-bearing property is :func:`member_from_resume`: every field of a
member must be derivable from a single ``ResumeCommand``, with no Redis
lookup. That is what lets the idempotency gate ``ZREM`` the entry the
moment a reply is parsed, before any registry access. Adding a field that
a reply does not carry breaks the gate.

Wire contract — Python/TS/Java must encode members byte-for-byte
identically, since any SDK's sweeper may resolve another SDK's entry.
"""

import hashlib
from typing import NamedTuple

from by_framework.common.constants import WAIT_INDEX_SHARDS, RedisKeys
from by_framework.core.protocol.commands import GatewayCommand

_SEPARATOR = "|"
_ESCAPE = "\\"


class WaitIndexMember(NamedTuple):
    """Decoded wait-index member.

    Attributes:
        session_id: Session the waiting caller belongs to; also picks the shard.
        parent_message_id: The suspended caller's own message_id — what the
            resume reattaches to.
        child_message_id: The dispatched sub-task's message_id — the only
            per-sibling-unique field, so it is what makes a Task Group's
            entries distinct.
        task_group_id: The sub-task's task group id, or "" for a single
            ``call_agent``. Present so a sweep can route an orphan through
            the group's join accounting instead of waking the caller
            directly.
    """

    session_id: str
    parent_message_id: str
    child_message_id: str
    task_group_id: str


def _escape(value: str) -> str:
    return value.replace(_ESCAPE, _ESCAPE * 2).replace(_SEPARATOR, _ESCAPE + _SEPARATOR)


def _split_escaped(encoded: str) -> list[str]:
    fields: list[str] = []
    current: list[str] = []
    escaped = False
    for char in encoded:
        if escaped:
            current.append(char)
            escaped = False
        elif char == _ESCAPE:
            escaped = True
        elif char == _SEPARATOR:
            fields.append("".join(current))
            current = []
        else:
            current.append(char)
    if escaped:
        raise ValueError(f"Invalid wait-index member (dangling escape): {encoded!r}")
    fields.append("".join(current))
    return fields


def encode_member(
    session_id: str,
    parent_message_id: str,
    child_message_id: str,
    task_group_id: str = "",
) -> str:
    """Encode the four identity fields into a wait-index ZSET member.

    Format: ``{session_id}|{parent_message_id}|{child_message_id}|{task_group_id}``.

    Framework-minted ids (``msg-``/``tg-`` + hex) never contain the
    separator, but ``session_id`` and a caller-supplied ``message_id`` are
    arbitrary caller-controlled strings, so ``|`` and ``\\`` are escaped
    (``\\`` -> ``\\\\``, ``|`` -> ``\\|``) rather than assumed absent.
    """
    return _SEPARATOR.join(
        _escape(str(field or ""))
        for field in (
            session_id,
            parent_message_id,
            child_message_id,
            task_group_id,
        )
    )


def decode_member(member: str) -> WaitIndexMember:
    """Inverse of :func:`encode_member`. Raises ValueError on a malformed member."""
    fields = _split_escaped(member)
    if len(fields) != 4:
        raise ValueError(
            f"Invalid wait-index member (expected 4 fields, got {len(fields)}): "
            f"{member!r}"
        )
    return WaitIndexMember(*fields)


def member_from_resume(command: GatewayCommand) -> str:
    """Rebuild the wait-index member a reply is meant to clear.

    Mapping (see ``GatewayWorker._enqueue_agent_return``, which builds every
    reply): a reply's ``header.message_id`` is the *caller's* message_id,
    its ``header.parent_message_id`` is the *sub-task's* dispatch-time
    message_id, and ``header.task_group_id`` is passed through unchanged.
    The direction reversal is the whole point — keying by the reply's own
    ``message_id`` would make every sibling in a Task Group collide.
    """
    header = command.header
    return encode_member(
        session_id=header.session_id,
        parent_message_id=header.message_id,
        child_message_id=header.parent_message_id,
        task_group_id=header.task_group_id,
    )


def member_digest(member: str) -> str:
    """Stable short id for a member, for keys that are named after one.

    The member is hashed rather than embedded because it is built from
    caller-controlled ids of unbounded length. SHA-1 hex is used for the same
    reason as FNV-1a below: it is trivially reproducible in the TS/Java
    ports, and every key derived from it is part of the cross-SDK contract.
    """
    return hashlib.sha1(member.encode("utf-8")).hexdigest()  # nosec B324


def fnv1a32(text: str) -> int:
    """FNV-1a 32-bit over UTF-8.

    Deliberately not Python's ``hash()``, which is salted per process and
    differs across SDKs — a TS/Java sweeper deriving a shard from it would
    look in the wrong one. Exported so everything that needs a stable,
    language-portable bucket (shard selection here, a sweeper's starting
    offset) derives it from one implementation.
    """
    digest = 0x811C9DC5
    for byte in str(text or "").encode("utf-8"):
        digest = ((digest ^ byte) * 0x01000193) & 0xFFFFFFFF
    return digest


def wait_index_shard(session_id: str) -> int:
    """Shard owning a session's wait entries."""
    return fnv1a32(session_id) % WAIT_INDEX_SHARDS


def wait_index_key(session_id: str) -> str:
    """Redis key of the wait-index shard holding this session's entries."""
    return RedisKeys.wait_index(wait_index_shard(session_id))
