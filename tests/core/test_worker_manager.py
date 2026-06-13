"""
Tests for WorkerManager and related WorkerRegistry admin methods.
"""

import unittest
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from by_framework.core.registry import WorkerRegistry
from by_framework.core.worker_manager import WorkerManager


class FakeRedis:
    """Minimal Redis fake for WorkerManager tests."""

    def __init__(self):
        self.hashes: dict = {}
        self.sets: dict = {}
        self.streams: dict = {}
        self.kv: dict = {}

    def pipeline(self):
        return _FakePipeline(self)

    async def hset(self, name, field, value):
        self.hashes.setdefault(name, {})[field] = value

    async def hgetall(self, name):
        return {
            k.encode() if isinstance(k, str) else k: str(v).encode()
            for k, v in self.hashes.get(name, {}).items()
        }

    async def delete(self, name):
        self.hashes.pop(name, None)
        self.sets.pop(name, None)
        self.kv.pop(name, None)

    async def sadd(self, name, *values):
        self.sets.setdefault(name, set()).update(values)

    async def srem(self, name, *values):
        bucket = self.sets.get(name, set())
        for v in values:
            bucket.discard(v)

    async def smembers(self, name):
        return {v.encode() if isinstance(v, str) else v for v in self.sets.get(name, set())}

    async def sismember(self, name, value):
        return value in self.sets.get(name, set())

    async def xadd(self, stream, fields, **kwargs):
        self.streams.setdefault(stream, []).append(fields)

    async def get(self, name):
        return self.kv.get(name)

    async def set(self, name, value, nx=False, ex=None):
        if nx and name in self.kv:
            return False
        self.kv[name] = value
        return True

    async def eval(self, *args, **kwargs):
        return 1


class _FakePipeline:
    def __init__(self, redis: FakeRedis):
        self._redis = redis
        self._calls: list = []

    def hset(self, name, field, value):
        self._calls.append(("hset", name, field, value))
        return self

    def sadd(self, name, *values):
        self._calls.append(("sadd", name, *values))
        return self

    def srem(self, name, *values):
        self._calls.append(("srem", name, *values))
        return self

    async def execute(self):
        for op, *args in self._calls:
            if op == "hset":
                name, field, value = args
                self._redis.hashes.setdefault(name, {})[field] = value
            elif op == "sadd":
                name, *values = args
                self._redis.sets.setdefault(name, set()).update(values)
            elif op == "srem":
                name, *values = args
                bucket = self._redis.sets.get(name, set())
                for v in values:
                    bucket.discard(v)


# ---------------------------------------------------------------------------
# WorkerRegistry admin state tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_and_get_worker_admin_state():
    redis = FakeRedis()
    registry = WorkerRegistry(redis)

    await registry.set_worker_admin_state("w1", "suspended", "maintenance")
    state = await registry.get_worker_admin_state("w1")

    assert state["lifecycle"] == "suspended"
    assert state["reason"] == "maintenance"
    assert int(state["updated_at"]) > 0


@pytest.mark.asyncio
async def test_get_worker_admin_state_returns_empty_when_not_set():
    redis = FakeRedis()
    registry = WorkerRegistry(redis)

    state = await registry.get_worker_admin_state("unknown-worker")
    assert state == {}


@pytest.mark.asyncio
async def test_clear_worker_admin_state():
    redis = FakeRedis()
    registry = WorkerRegistry(redis)

    await registry.set_worker_admin_state("w1", "evicted", "test")
    await registry.clear_worker_admin_state("w1")

    state = await registry.get_worker_admin_state("w1")
    assert state == {}


# ---------------------------------------------------------------------------
# Agent-type denylist tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deny_and_allow_worker_for_type():
    redis = FakeRedis()
    registry = WorkerRegistry(redis)

    # Seed membership
    redis.sets["byai_gateway:registry:agent_type:workers:llm_agent"] = {"w1", "w2"}

    await registry.deny_worker_for_type("llm_agent", "w1")

    assert await registry.is_worker_denied_for_type("llm_agent", "w1")
    assert not await registry.is_worker_denied_for_type("llm_agent", "w2")
    # w1 should be removed from members
    assert "w1" not in redis.sets.get(
        "byai_gateway:registry:agent_type:workers:llm_agent", set()
    )

    await registry.allow_worker_for_type("llm_agent", "w1")
    assert not await registry.is_worker_denied_for_type("llm_agent", "w1")


@pytest.mark.asyncio
async def test_get_agent_type_denylist():
    redis = FakeRedis()
    registry = WorkerRegistry(redis)

    await registry.deny_worker_for_type("llm_agent", "w1")
    await registry.deny_worker_for_type("llm_agent", "w2")

    denylist = await registry.get_agent_type_denylist("llm_agent")
    assert set(denylist) == {"w1", "w2"}


@pytest.mark.asyncio
async def test_register_worker_membership_skips_denied_types():
    redis = FakeRedis()
    registry = WorkerRegistry(redis)

    # Pre-deny w1 for llm_agent
    redis.sets["byai_gateway:registry:agent_type:denied:llm_agent"] = {"w1"}

    await registry.register_worker_membership("w1", ["llm_agent", "search_agent"])

    members_llm = redis.sets.get("byai_gateway:registry:agent_type:workers:llm_agent", set())
    members_search = redis.sets.get("byai_gateway:registry:agent_type:workers:search_agent", set())

    assert "w1" not in members_llm, "denied worker should not be added to llm_agent members"
    assert "w1" in members_search, "non-denied type should still be registered"


# ---------------------------------------------------------------------------
# WorkerManager tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_worker_manager_suspend_worker():
    redis = FakeRedis()
    manager = WorkerManager(redis_client=redis)

    await manager.suspend_worker("w1", reason="scheduled maintenance")

    state = await manager.get_worker_admin_state("w1")
    assert state["lifecycle"] == "suspended"
    assert state["reason"] == "scheduled maintenance"

    stream_key = "byai_gateway:ctrl:worker:w1"
    assert stream_key in redis.streams
    payload = redis.streams[stream_key][0]
    assert "data" in payload
    import json
    data = json.loads(payload["data"])
    assert data["action_type"] == "SUSPEND_WORKER"


@pytest.mark.asyncio
async def test_worker_manager_resume_worker():
    redis = FakeRedis()
    manager = WorkerManager(redis_client=redis)

    await manager.suspend_worker("w1", reason="test")
    await manager.resume_worker("w1")

    state = await manager.get_worker_admin_state("w1")
    assert state["lifecycle"] == "active"

    import json
    stream = redis.streams["byai_gateway:ctrl:worker:w1"]
    last_cmd = json.loads(stream[-1]["data"])
    assert last_cmd["action_type"] == "RESUME_WORKER"


@pytest.mark.asyncio
async def test_worker_manager_evict_worker_graceful():
    redis = FakeRedis()
    manager = WorkerManager(redis_client=redis)

    await manager.evict_worker("w1", reason="decommission")

    state = await manager.get_worker_admin_state("w1")
    assert state["lifecycle"] == "evicted"

    import json
    cmd = json.loads(redis.streams["byai_gateway:ctrl:worker:w1"][-1]["data"])
    assert cmd["action_type"] == "EVICT_WORKER"
    assert cmd["body"]["force"] is False


@pytest.mark.asyncio
async def test_worker_manager_evict_worker_force():
    redis = FakeRedis()
    manager = WorkerManager(redis_client=redis)

    await manager.evict_worker("w1", force=True, reason="emergency")

    import json
    cmd = json.loads(redis.streams["byai_gateway:ctrl:worker:w1"][-1]["data"])
    assert cmd["body"]["force"] is True


@pytest.mark.asyncio
async def test_worker_manager_deny_and_allow():
    redis = FakeRedis()
    manager = WorkerManager(redis_client=redis)

    await manager.deny_worker_for_type("llm_agent", "w1")
    denylist = await manager.get_type_denylist("llm_agent")
    assert "w1" in denylist

    await manager.allow_worker_for_type("llm_agent", "w1")
    denylist = await manager.get_type_denylist("llm_agent")
    assert "w1" not in denylist


@pytest.mark.asyncio
async def test_worker_manager_clear_admin_state():
    redis = FakeRedis()
    manager = WorkerManager(redis_client=redis)

    await manager.evict_worker("w1")
    await manager.clear_worker_admin_state("w1")

    state = await manager.get_worker_admin_state("w1")
    assert state == {}
