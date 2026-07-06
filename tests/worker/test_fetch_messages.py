"""Tests for WorkerRunner.fetch_messages's two-phase XREADGROUP split.

Phase one: concurrent non-blocking XREADGROUP per active agent_type stream.
Phase two: only if every stream came back empty, one blocking XREADGROUP
against a single round-robin "primary" stream.

Every call in both phases targets exactly one stream — this is the
code-level property that eliminates CROSSSLOT under Cluster (verified at
the unit level here; a live 3-master Cluster integration test is a
separate, slow/optional CI job per the issue, not covered in this file).
"""

from typing import Any

import pytest

from by_framework import GatewayWorker, RedisKeys, WorkerRunner


class _FixedAgentTypesWorker(GatewayWorker):
    """Minimal worker declaring a fixed, ordered list of agent_types."""

    def __init__(self, agent_types: list[str]):
        super().__init__("worker-1", None, None, None)
        self._agent_types = agent_types

    def get_agent_types(self) -> list[str]:
        return self._agent_types

    async def process_command(self, command: Any, context: Any) -> None:
        pass


class ScriptedXreadgroupRedis:
    """Redis fake that scripts per-stream XREADGROUP responses and records
    every call's stream set/block value, regardless of concurrent ordering.
    """

    def __init__(self, responses_by_stream: dict[str, list[Any]] | None = None):
        self._responses = {k: list(v) for k, v in (responses_by_stream or {}).items()}
        self.calls: list[dict[str, Any]] = []

    async def xgroup_create(self, name, groupname, id="0", mkstream=False):
        pass

    async def xreadgroup(self, groupname, consumername, streams, count=1, block=None):
        self.calls.append({"streams": dict(streams), "count": count, "block": block})
        queue = None
        for stream_name in streams:
            queue = self._responses.get(stream_name)
            if queue:
                return queue.pop(0)
        return []

    async def xack(self, name, groupname, *ids):
        pass


@pytest.mark.asyncio
async def test_fetch_messages_phase_one_returns_immediately_without_blocking():
    """If any active stream has a message, fetch_messages returns it via the
    non-blocking phase-one scan — every call is single-stream, no `block`."""
    redis_mock = ScriptedXreadgroupRedis(
        {
            RedisKeys.ctrl_stream("agent-b"): [
                [
                    [
                        RedisKeys.ctrl_stream("agent-b").encode(),
                        [(b"1-0", {b"data": b'{"foo": "bar"}'})],
                    ]
                ]
            ]
        }
    )
    worker = _FixedAgentTypesWorker(["agent-a", "agent-b"])
    runner = WorkerRunner(
        redis_client=redis_mock, worker=worker, group_name="test_group"
    )

    messages = await runner.fetch_messages()

    assert len(messages) == 1
    assert messages[0][0] == RedisKeys.ctrl_stream("agent-b")
    assert len(redis_mock.calls) == 2  # one non-blocking call per active stream
    for call in redis_mock.calls:
        assert len(call["streams"]) == 1
        assert call["block"] is None


@pytest.mark.asyncio
async def test_fetch_messages_falls_back_to_single_blocking_primary_when_all_empty():
    """When every stream comes back empty in phase one, exactly one more
    call happens: a single-stream blocking read against the primary."""
    redis_mock = ScriptedXreadgroupRedis()
    worker = _FixedAgentTypesWorker(["agent-a", "agent-b"])
    runner = WorkerRunner(
        redis_client=redis_mock, worker=worker, group_name="test_group"
    )

    messages = await runner.fetch_messages(block=5000)

    assert messages == []
    assert len(redis_mock.calls) == 3  # 2 non-blocking + 1 blocking
    non_blocking_calls = [c for c in redis_mock.calls if c["block"] is None]
    blocking_calls = [c for c in redis_mock.calls if c["block"] is not None]
    assert len(non_blocking_calls) == 2
    assert len(blocking_calls) == 1
    assert blocking_calls[0]["block"] == 5000
    assert len(blocking_calls[0]["streams"]) == 1


@pytest.mark.asyncio
async def test_fetch_messages_round_robins_the_blocking_primary():
    """Across successive empty rounds, the primary used for the phase-two
    blocking read rotates through every declared agent_type before any one
    repeats, so no agent_type is starved of the blocking slot."""
    redis_mock = ScriptedXreadgroupRedis()
    worker = _FixedAgentTypesWorker(["agent-a", "agent-b", "agent-c"])
    runner = WorkerRunner(
        redis_client=redis_mock, worker=worker, group_name="test_group"
    )

    primaries = []
    for _ in range(3):
        await runner.fetch_messages()
        blocking_calls = [c for c in redis_mock.calls if c["block"] is not None]
        primaries.append(next(iter(blocking_calls[-1]["streams"])))

    assert set(primaries) == {
        RedisKeys.ctrl_stream("agent-a"),
        RedisKeys.ctrl_stream("agent-b"),
        RedisKeys.ctrl_stream("agent-c"),
    }
