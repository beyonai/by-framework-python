"""Wait-index member encoding: the contract the ZREM gate depends on."""

import pytest

from by_framework.common.constants import WAIT_INDEX_SHARDS, RedisKeys
from by_framework.core.protocol.commands import AskAgentCommand, ResumeCommand
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.core.wait_index import (
    WaitIndexMember,
    decode_member,
    encode_member,
    member_from_resume,
    wait_index_key,
    wait_index_shard,
)


def test_encode_member_uses_documented_field_order():
    assert (
        encode_member("sess-1", "parent-msg", "child-msg", "tg-abc")
        == "sess-1|parent-msg|child-msg|tg-abc"
    )


def test_encode_member_keeps_empty_task_group_positional():
    """A single call_agent has no group id, but the field must still be
    present — decode() splits on a fixed field count."""
    encoded = encode_member("sess-1", "parent-msg", "child-msg")
    assert encoded == "sess-1|parent-msg|child-msg|"
    assert decode_member(encoded).task_group_id == ""


@pytest.mark.parametrize(
    "session_id",
    [
        "sess-1",
        "a|b",  # session ids are caller-supplied, so the separator can occur
        "back\\slash",
        "both\\|weird",
        "|",
        "",
    ],
)
def test_encode_decode_round_trips_through_separator_characters(session_id):
    member = WaitIndexMember(session_id, "parent|msg", "child\\msg", "tg-1")
    assert decode_member(encode_member(*member)) == member


def test_decode_member_rejects_wrong_field_count():
    with pytest.raises(ValueError):
        decode_member("only|three|fields")


def test_decode_member_rejects_dangling_escape():
    with pytest.raises(ValueError):
        decode_member("a|b|c|d\\")


def test_member_from_resume_matches_the_member_written_at_dispatch():
    """The gate rebuilds the member from the reply alone — no Redis lookup.

    The reply reverses the direction of the header ids: its message_id is
    the caller's, its parent_message_id is the sub-task's. Getting this
    backwards would make every Task Group sibling collide on one member.
    """
    dispatch = AskAgentCommand(
        header=MessageHeader(
            message_id="child-msg",
            session_id="sess-1",
            trace_id="trace-1",
            source_agent_type="agent-a",
            target_agent_type="agent-b",
            parent_message_id="parent-msg",
            task_group_id="tg-abc",
        ),
        content="hi",
    )
    registered = encode_member(
        session_id=dispatch.header.session_id,
        parent_message_id=dispatch.header.parent_message_id,
        child_message_id=dispatch.header.message_id,
        task_group_id=dispatch.header.task_group_id,
    )

    # Shaped exactly like GatewayWorker._enqueue_agent_return's reply.
    reply = ResumeCommand(
        header=MessageHeader(
            message_id=dispatch.header.parent_message_id,
            session_id=dispatch.header.session_id,
            trace_id="trace-1",
            source_agent_type=dispatch.header.target_agent_type,
            target_agent_type=dispatch.header.source_agent_type,
            parent_message_id=dispatch.header.message_id,
            task_group_id=dispatch.header.task_group_id,
        ),
        status="COMPLETED",
    )

    assert member_from_resume(reply) == registered
    assert decode_member(member_from_resume(reply)) == WaitIndexMember(
        "sess-1", "parent-msg", "child-msg", "tg-abc"
    )


def test_member_from_resume_distinguishes_task_group_siblings():
    """Siblings share the caller's message_id; only child_message_id differs."""

    def reply(child_message_id):
        return ResumeCommand(
            header=MessageHeader(
                message_id="parent-msg",
                session_id="sess-1",
                trace_id="trace-1",
                parent_message_id=child_message_id,
                task_group_id="tg-abc",
            ),
            status="COMPLETED",
        )

    assert member_from_resume(reply("child-1")) != member_from_resume(reply("child-2"))


def test_wait_index_shard_is_stable_and_bounded():
    """Sweepers in another SDK must land on the same shard, so the hash
    cannot be Python's per-process-salted hash()."""
    assert wait_index_shard("sess-1") == wait_index_shard("sess-1")
    assert wait_index_shard("sess-1") == 1  # FNV-1a/16, pinned cross-SDK
    assert all(0 <= wait_index_shard(f"s-{i}") < WAIT_INDEX_SHARDS for i in range(200))
    assert len({wait_index_shard(f"s-{i}") for i in range(200)}) > 1


def test_wait_index_key_routes_through_redis_keys():
    assert wait_index_key("sess-1") == RedisKeys.wait_index(wait_index_shard("sess-1"))
