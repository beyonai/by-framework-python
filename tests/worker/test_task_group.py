"""Unit tests for TaskGroupStore's local invariants and result views."""

import json
from unittest.mock import AsyncMock

import pytest

from by_framework.common.constants import (
    TASK_GROUP_FIELD_TASK_ORDER,
    TASK_GROUP_FIELD_TOTAL,
    RedisKeys,
)
from by_framework.worker.task_group import TaskGroupStore


def test_resolve_message_ids_preserves_legacy_shared_id_without_collisions():
    generated = iter(["generated-a", "generated-b"])

    assert TaskGroupStore.resolve_message_ids(
        [None],
        shared_message_id="shared",
        generate_message_id=lambda: next(generated),
    ) == ["shared"]
    assert TaskGroupStore.resolve_message_ids(
        [None, "explicit"],
        shared_message_id="shared",
        generate_message_id=lambda: next(generated),
    ) == ["shared:0", "explicit"]


def test_resolve_message_ids_rejects_duplicate_explicit_and_derived_ids():
    with pytest.raises(ValueError, match="must be unique"):
        TaskGroupStore.resolve_message_ids(
            [None, "shared:0"],
            shared_message_id="shared",
            generate_message_id=lambda: "unused",
        )


@pytest.mark.asyncio
async def test_create_rejects_invalid_ids_before_redis_writes():
    redis = AsyncMock()
    store = TaskGroupStore(redis)

    with pytest.raises(ValueError, match="at least one"):
        await store.create("tg-empty", message_ids=[], source_agent_type="caller")
    with pytest.raises(ValueError, match="must be unique"):
        await store.create(
            "tg-duplicate",
            message_ids=["same", "same"],
            source_agent_type="caller",
        )

    redis.hset.assert_not_awaited()
    redis.expire.assert_not_awaited()


def test_build_result_repeats_failed_error_fields_at_top_level():
    result = TaskGroupStore.build_result(
        status="FAILED",
        reply_data={"error": "boom", "error_code": "E_BOOM"},
        content="",
        target_agent_type="agent-b",
        metadata={},
        extra_payload={},
    )

    assert result["error"] == "boom"
    assert result["error_code"] == "E_BOOM"
    assert result["reply_data"] == {"error": "boom", "error_code": "E_BOOM"}


@pytest.mark.asyncio
async def test_aggregate_uses_dispatch_order_and_appends_unknown_results():
    redis = AsyncMock()
    task_group_id = "tg-order"
    group_key = RedisKeys.task_group(task_group_id)
    results_key = RedisKeys.task_group_results(task_group_id)
    redis.hgetall.return_value = {
        "msg-b": json.dumps({"status": "COMPLETED"}),
        "unexpected": json.dumps({"status": "COMPLETED"}),
        "msg-a": json.dumps({"status": "COMPLETED"}),
    }

    async def hget(name, field):
        if name == group_key and field == TASK_GROUP_FIELD_TASK_ORDER:
            return json.dumps(["msg-a", "msg-b"])
        if name == group_key and field == TASK_GROUP_FIELD_TOTAL:
            return "3"
        return None

    redis.hget.side_effect = hget

    aggregate = await TaskGroupStore(redis).aggregate(task_group_id)

    assert [item["message_id"] for item in aggregate] == [
        "msg-a",
        "msg-b",
        "unexpected",
    ]
    redis.hgetall.assert_awaited_once_with(results_key)
