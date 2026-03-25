import pytest

from byclaw_gateway_sdk import RedisKeys, WorkerRegistry


class MockPipeline:

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
        for cmd in self.commands:
            if cmd[0] == "hset":
                await self.redis.hset(cmd[1], {cmd[2]: cmd[3]})
            elif cmd[0] == "expire":
                await self.redis.expire(cmd[1], cmd[2])
        return []


class MockRedis:

    def __init__(self):
        self.data = {}
        self.kv = {}
        self.expires = {}

    async def zadd(self, name, mapping):
        self.data[name] = mapping

    async def sadd(self, name, value):
        if name not in self.data:
            self.data[name] = set()
        self.data[name].add(value)

    async def smembers(self, name):
        if name not in self.data:
            return set()
        return self.data[name]

    async def set(self, name, value, nx=False, ex=None):
        if nx and name in self.kv:
            return False
        self.kv[name] = value
        return True

    async def get(self, name):
        return self.kv.get(name)

    async def delete(self, name):
        self.kv.pop(name, None)

    async def expire(self, name, ttl):
        self.expires[name] = ttl
        return 1

    async def hset(self, name, mapping=None, key=None, value=None):
        if name not in self.data:
            self.data[name] = {}
        if mapping:
            self.data[name].update(mapping)
        else:
            self.data[name][key] = value

    async def hget(self, name, key):
        return self.data.get(name, {}).get(key)

    async def hgetall(self, name):
        return self.data.get(name, {})

    def pipeline(self):
        return MockPipeline(self)


@pytest.mark.asyncio
async def test_register_worker():
    """Test that WorkerRegistry correctly registers a worker in Redis."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)
    await registry.register_worker("worker-1", ["super_assistant"])
    assert "worker-1" in redis_mock.data[RedisKeys.ACTIVE_WORKERS]


@pytest.mark.asyncio
async def test_get_target_worker():
    """Test that WorkerRegistry can find workers by agent type."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)
    await registry.register_worker("worker-1", ["super_assistant"])
    await registry.register_worker("worker-2", ["my-agent"])

    # Test finding worker by agent type
    worker = await registry.get_target_worker("super_assistant")
    assert worker == "worker-1"

    worker = await registry.get_target_worker("my-agent")
    assert worker == "worker-2"

    # Test not found case
    worker = await registry.get_target_worker("unknown-agent")
    assert worker is None


@pytest.mark.asyncio
async def test_claim_worker_id_duplicate_should_fail():
    """Test that claiming a duplicate worker_id raises ValueError."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)

    token1 = await registry.claim_worker_id("worker-1")
    assert token1

    with pytest.raises(ValueError):
        await registry.claim_worker_id("worker-1")


@pytest.mark.asyncio
async def test_registry_tracks_execution_lifecycle():
    """Test that WorkerRegistry tracks execution lifecycle (save, query, cancel, finish)."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)

    execution = {
        "execution_id": "exec-1",
        "message_id": "msg-1",
        "session_id": "sess-1",
        "worker_id": "worker-1",
        "target_agent_type": "langgraph_agent",
        "status": "RUNNING",
        "cancel_requested": False,
    }

    await registry.save_execution(execution)

    # 验证是否存入了 session registry
    reg_key = RedisKeys.session_registry("sess-1")
    assert "exec:exec-1" in redis_mock.data[reg_key]
    assert "msg_map:msg-1" in redis_mock.data[reg_key]
    assert redis_mock.expires[reg_key] == RedisKeys.DEFAULT_SESSION_TTL

    found = await registry.get_execution_by_message_id("msg-1", session_id="sess-1")
    assert found["execution_id"] == "exec-1"

    await registry.mark_execution_cancelling("exec-1", "sess-1", "user aborted")
    updated = await registry.get_execution("exec-1", "sess-1")
    assert updated["status"] == "CANCELLING"
    assert updated["cancel_requested"] is True
    assert updated["cancel_reason"] == "user aborted"

    await registry.mark_execution_finished("exec-1", "sess-1", "CANCELLED")
    finished = await registry.get_execution("exec-1", "sess-1")
    assert finished["status"] == "CANCELLED"
