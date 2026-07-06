"""Tests for TraceWriteClient's best-effort Redis writes."""

import pytest

from by_framework.common.constants import RedisKeys
from by_framework.trace.trace_schema import TraceRecord
from by_framework.trace.trace_writer import TraceWriteClient


class _MockPipeline:
    """Small Redis pipeline fake matching TraceWriteClient's call shapes."""

    def __init__(self, redis):
        self.redis = redis
        self.commands = []

    def hset(self, name, key, value):
        self.commands.append(("hset", name, key, value))
        return self

    def expire(self, name, ttl):
        self.commands.append(("expire", name, ttl))
        return self

    async def execute(self):
        for command in self.commands:
            if command[0] == "hset":
                await self.redis.hset(command[1], command[2], command[3])
            elif command[0] == "expire":
                await self.redis.expire(command[1], command[2])
        return []


class _TraceWriterRedis:
    """Minimal Redis fake for TraceWriteClient.record_trace."""

    def __init__(self):
        self.data = {}
        self.expires = {}

    async def hset(self, name, key, value):
        self.data.setdefault(name, {})[key] = value

    async def zadd(self, name, mapping):
        self.data.setdefault(name, {}).update(mapping)

    async def expire(self, name, ttl):
        self.expires[name] = ttl
        return 1

    def pipeline(self):
        return _MockPipeline(self)


class _AgentIndexFailingRedis(_TraceWriterRedis):
    """Fails only the trace_index_agent write, to verify trace_meta is
    independent of it."""

    async def zadd(self, name, mapping):
        if name == RedisKeys.trace_index_agent("planner"):
            raise ConnectionError("simulated trace_index_agent write failure")
        return await super().zadd(name, mapping)


@pytest.mark.asyncio
async def test_record_trace_survives_trace_index_agent_failure():
    """trace_index_agent is a cross-entity index write, split apart from the
    trace_meta write. If it fails, trace_meta must still be correctly
    readable by trace_id, and the unrelated session index must still land."""
    redis = _AgentIndexFailingRedis()
    client = TraceWriteClient(redis, ttl_seconds=321)

    await client.record_trace(
        TraceRecord(
            trace_id="trace-safe",
            session_id="sess-safe",
            root_agent_type="planner",
            status="OK",
            start_ts=100,
            end_ts=150,
        )
    )

    assert redis.data[RedisKeys.trace_meta("trace-safe")]["trace_id"] == "trace-safe"
    assert redis.data[RedisKeys.trace_meta("trace-safe")]["status"] == "OK"
    assert "trace-safe" in redis.data[RedisKeys.trace_index_session("sess-safe")]
    assert RedisKeys.trace_index_agent("planner") not in redis.data
