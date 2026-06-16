"""Tests for WorkerRegistry functionality."""

import asyncio
import json
from unittest.mock import patch

import pytest

from by_framework import AgentConfig, PluginRegistry, RedisKeys, WorkerRegistry
from by_framework.core import registry as registry_module

OLD_ACTIVE_WORKERS = "byai_gateway:registry:active_workers"


def test_worker_execution_key_builders():
    """Test worker execution key builders."""
    assert RedisKeys.worker_status("worker-1") == (
        "byai_gateway:registry:worker:status:worker-1"
    )
    assert RedisKeys.worker_executions("worker-1") == (
        "byai_gateway:registry:worker:executions:worker-1"
    )
    assert RedisKeys.worker_active_executions("worker-1") == (
        "byai_gateway:registry:worker:active_executions:worker-1"
    )
    assert RedisKeys.worker_active_execution_index("worker-1") == (
        "byai_gateway:registry:worker:active_execution_index:worker-1"
    )
    assert RedisKeys.worker_active_snapshots("worker-1") == (
        "byai_gateway:registry:worker:active_snapshots:worker-1"
    )


def test_local_ip_prefers_hostname_mapping_when_non_loopback():
    with (
        patch.object(registry_module.socket, "gethostname", return_value="worker-host"),
        patch.object(registry_module.socket, "gethostbyname", return_value="10.0.0.12"),
        patch.object(
            registry_module, "_get_default_route_ip_address", return_value="10.0.0.99"
        ),
    ):
        assert registry_module._get_local_ip_address() == "10.0.0.12"


def test_local_ip_falls_back_to_route_when_hostname_is_loopback():
    with (
        patch.object(registry_module.socket, "gethostname", return_value="localhost"),
        patch.object(registry_module.socket, "gethostbyname", return_value="127.0.0.1"),
        patch.object(
            registry_module, "_get_default_route_ip_address", return_value="10.0.0.99"
        ),
    ):
        assert registry_module._get_local_ip_address() == "10.0.0.99"
    assert RedisKeys.worker_history_snapshots("worker-1") == (
        "byai_gateway:registry:worker:history_snapshots:worker-1"
    )


def test_worker_execution_snapshot_preserves_agent_config_audit_projection():
    """Worker execution history keeps redacted agent config lookup metadata."""
    registry = WorkerRegistry(object())
    snapshot = registry._build_worker_execution_snapshot(  # pylint: disable=protected-access
        {
            "execution_id": "exec-audit",
            "message_id": "msg-audit",
            "session_id": "sess-audit",
            "worker_id": "worker-audit",
            "target_agent_type": "weather-agent",
            "status": "COMPLETED",
            "agent_configs_version": 5,
            "agent_configs_snapshot_key": "exec-audit",
            "agent_config_audit": {
                "target_agent_type": "weather-agent",
                "snapshot_hash": "sha256:audit",
            },
        }
    )

    assert snapshot["agent_configs_version"] == 5
    assert snapshot["agent_configs_snapshot_key"] == "exec-audit"
    assert snapshot["agent_config_audit"]["snapshot_hash"] == "sha256:audit"


class MockPipeline:
    """Mock Redis pipeline for testing."""

    def __init__(self, redis):
        self.redis = redis
        self.commands = []

    def hset(self, name, key, value):
        self.commands.append(("hset", name, key, value))
        return self

    def hdel(self, name, *keys):
        self.commands.append(("hdel", name, keys))
        return self

    def sadd(self, name, value):
        self.commands.append(("sadd", name, value))
        return self

    def srem(self, name, value):
        self.commands.append(("srem", name, value))
        return self

    def zadd(self, name, mapping):
        self.commands.append(("zadd", name, mapping))
        return self

    def zrem(self, name, *values):
        self.commands.append(("zrem", name, values))
        return self

    def hincrby(self, name, key, amount=1):
        self.commands.append(("hincrby", name, key, amount))
        return self

    def expire(self, name, ttl):
        self.commands.append(("expire", name, ttl))
        return self

    def delete(self, name):
        self.commands.append(("delete", name))
        return self

    async def execute(self):
        for cmd in self.commands:
            if cmd[0] == "hset":
                await self.redis.hset(cmd[1], {cmd[2]: cmd[3]})
            elif cmd[0] == "hdel":
                await self.redis.hdel(cmd[1], *cmd[2])
            elif cmd[0] == "sadd":
                await self.redis.sadd(cmd[1], cmd[2])
            elif cmd[0] == "srem":
                await self.redis.srem(cmd[1], cmd[2])
            elif cmd[0] == "zadd":
                await self.redis.zadd(cmd[1], cmd[2])
            elif cmd[0] == "zrem":
                await self.redis.zrem(cmd[1], *cmd[2])
            elif cmd[0] == "hincrby":
                await self.redis.hincrby(cmd[1], cmd[2], cmd[3])
            elif cmd[0] == "expire":
                await self.redis.expire(cmd[1], cmd[2])
            elif cmd[0] == "delete":
                await self.redis.delete(cmd[1])
        return []


class MockRedis:
    """Mock Redis client for testing."""

    def __init__(self):
        self.data = {}
        self.kv = {}
        self.expires = {}

    async def zadd(self, name, mapping):
        if name not in self.data:
            self.data[name] = {}
        for k, v in mapping.items():
            self.data[name][k] = v

    async def zrem(self, name, *values):
        bucket = self.data.get(name)
        if not isinstance(bucket, dict):
            return 0
        removed = 0
        for value in values:
            if value in bucket:
                del bucket[value]
                removed += 1
        return removed

    async def zrangebyscore(self, name, min_score, max_score, withscores=False):
        if name not in self.data:
            return []
        result = []
        # Handle '+inf' as no upper bound
        max_val = float("inf") if max_score == "+inf" else max_score
        for k, v in self.data[name].items():
            if min_score <= v <= max_val:
                result.append((k, v) if withscores else k)
        return result

    async def zrevrange(self, name, start, end):
        if name not in self.data:
            return []
        items = sorted(self.data[name].items(), key=lambda item: item[1], reverse=True)
        if end == -1:
            selected = items[start:]
        else:
            selected = items[start : end + 1]
        return [item[0] for item in selected]

    async def zcard(self, name):
        return len(self.data.get(name, {}))

    async def sadd(self, name, value):
        if name not in self.data:
            self.data[name] = set()
        self.data[name].add(value)

    async def srem(self, name, value):
        bucket = self.data.get(name)
        if not isinstance(bucket, set):
            return 0
        if value in bucket:
            bucket.remove(value)
            return 1
        return 0

    async def smembers(self, name):
        if name not in self.data:
            return set()
        return self.data[name]

    async def sismember(self, name, value):
        bucket = self.data.get(name)
        if not isinstance(bucket, set):
            return False
        return value in bucket

    async def set(self, name, value, nx=False, ex=None):
        if nx and name in self.kv:
            return False
        self.kv[name] = value
        if ex is not None:
            self.expires[name] = ex
        return True

    async def get(self, name):
        return self.kv.get(name)

    async def delete(self, name):
        self.kv.pop(name, None)
        self.data.pop(name, None)

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

    async def hmget(self, name, keys):
        bucket = self.data.get(name, {})
        return [bucket.get(key) for key in keys]

    async def hincrby(self, name, key, amount=1):
        if name not in self.data:
            self.data[name] = {}
        value = int(self.data[name].get(key, 0)) + amount
        self.data[name][key] = value
        return value

    async def hdel(self, name, *keys):
        bucket = self.data.get(name)
        if not isinstance(bucket, dict):
            return 0
        removed = 0
        for key in keys:
            if key in bucket:
                del bucket[key]
                removed += 1
        return removed

    async def eval(self, script, numkeys, *keys_and_args):
        """Simulate Lua registry scripts in Python for unit tests.

        Dispatches by argv count:
          3 args → _HEARTBEAT_CAS_SCRIPT  (token, new_value, ttl)
          2 args → _REFRESH_LOCK_SCRIPT   (token, ttl)
          1 arg  → _RELEASE_LOCK_SCRIPT   (token_or_empty)
        """
        keys = list(keys_and_args[:numkeys])
        args = list(keys_and_args[numkeys:])
        lease_key = keys[0]

        def _parse_token(raw):
            if raw is None:
                return None
            try:
                data = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
                if not isinstance(data, dict):
                    return None if data == 1 else str(data)
                return data.get("token")
            except Exception:  # pylint: disable=broad-exception-caught
                return "__unparseable__"

        if len(args) == 3:
            # _HEARTBEAT_CAS_SCRIPT
            token_arg, new_value, ttl = str(args[0]), args[1], int(args[2])
            raw = self.kv.get(lease_key)
            if token_arg != "":
                if raw is None:
                    self.kv[lease_key] = new_value
                    self.expires[lease_key] = ttl
                    return 1
                stored = _parse_token(raw)
                if stored == "__unparseable__":
                    return -1
                if stored is None or str(stored) != token_arg:
                    return 0
                self.kv[lease_key] = new_value
                self.expires[lease_key] = ttl
                return 1
            if raw is not None:
                stored = _parse_token(raw)
                if stored == "__unparseable__" or stored is not None:
                    return 0
            self.kv[lease_key] = new_value
            self.expires[lease_key] = ttl
            return 1

        if len(args) == 2:
            # _REFRESH_LOCK_SCRIPT
            token, ttl = str(args[0]), int(args[1])
            raw = self.kv.get(lease_key)
            stored = _parse_token(raw)
            if stored is None or stored == "__unparseable__" or str(stored) != token:
                return 0
            self.expires[lease_key] = ttl
            return 1

        # len(args) == 1: _RELEASE_LOCK_SCRIPT
        token_arg = str(args[0])
        raw = self.kv.get(lease_key)
        if raw is None:
            return 1 if token_arg == "" else 0
        if token_arg == "":
            self.kv.pop(lease_key, None)
            return 1
        stored = _parse_token(raw)
        if stored == "__unparseable__" or stored is None or str(stored) != token_arg:
            return 0
        self.kv.pop(lease_key, None)
        return 1

    def pipeline(self):
        return MockPipeline(self)


@pytest.mark.asyncio
async def test_register_worker_compatibility_wrapper():
    """Test legacy register_worker wrapper writes membership and liveness."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)
    with pytest.warns(DeprecationWarning, match="register_worker"):
        await registry.register_worker("worker-1", ["super_assistant"])
    assert OLD_ACTIVE_WORKERS not in redis_mock.data
    assert "worker-1" in redis_mock.data[RedisKeys.KNOWN_WORKERS]
    assert redis_mock.data[RedisKeys.worker_declared_agent_types("worker-1")] == {
        "super_assistant"
    }


@pytest.mark.asyncio
async def test_unregister_worker_compatibility_wrapper_warns():
    """Test that the legacy unregister_worker wrapper emits a deprecation warning."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)
    await registry.register_worker_membership("worker-1", ["super_assistant"])
    await registry.heartbeat_worker("worker-1")

    with pytest.warns(DeprecationWarning, match="unregister_worker"):
        await registry.unregister_worker("worker-1")

    assert OLD_ACTIVE_WORKERS not in redis_mock.data
    assert "worker-1" not in redis_mock.data[RedisKeys.KNOWN_WORKERS]
    assert RedisKeys.worker_declared_agent_types("worker-1") not in redis_mock.data


@pytest.mark.asyncio
async def test_register_worker_membership_only_updates_membership_sets():
    """Test static membership registration does not mark worker online."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)

    await registry.register_worker_membership(
        "worker-1", ["super_assistant", "code_agent"]
    )

    assert OLD_ACTIVE_WORKERS not in redis_mock.data
    assert redis_mock.data[RedisKeys.KNOWN_WORKERS] == {"worker-1"}
    assert redis_mock.data[RedisKeys.worker_declared_agent_types("worker-1")] == {
        "super_assistant",
        "code_agent",
    }
    assert redis_mock.data[RedisKeys.agent_type_members("super_assistant")] == {
        "worker-1"
    }
    assert redis_mock.data[RedisKeys.agent_type_members("code_agent")] == {"worker-1"}


@pytest.mark.asyncio
async def test_register_worker_membership_replaces_stale_agent_type_indexes():
    """Test worker_id reuse removes stale agent-type reverse indexes."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)

    await registry.register_worker_membership("worker-1", ["agent-a", "agent-stale"])
    await registry.register_worker_membership("worker-1", ["agent-a", "agent-b"])

    assert redis_mock.data[RedisKeys.worker_declared_agent_types("worker-1")] == {
        "agent-a",
        "agent-b",
    }
    assert redis_mock.data[RedisKeys.agent_type_members("agent-a")] == {"worker-1"}
    assert redis_mock.data[RedisKeys.agent_type_members("agent-b")] == {"worker-1"}
    assert redis_mock.data[RedisKeys.agent_type_members("agent-stale")] == set()


@pytest.mark.asyncio
async def test_heartbeat_worker_only_updates_presence():
    """Test that heartbeat marks liveness without mutating agent-type membership."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)
    registry._ip_address = "10.0.0.7"

    await registry.heartbeat_worker("worker-1")

    presence = json.loads(redis_mock.kv[RedisKeys.worker_online_lease("worker-1")])
    assert presence["token"] is None
    assert presence["last_seen"] > 0
    assert presence["ip_address"] == "10.0.0.7"
    assert OLD_ACTIVE_WORKERS not in redis_mock.data
    assert redis_mock.data[RedisKeys.KNOWN_WORKERS] == {"worker-1"}
    assert redis_mock.expires[RedisKeys.worker_online_lease("worker-1")] == (
        RedisKeys.WORKER_DEFAULT_LEASE_TTL_SECONDS
    )
    assert RedisKeys.worker_declared_agent_types("worker-1") not in redis_mock.data


@pytest.mark.asyncio
async def test_heartbeat_without_token_does_not_overwrite_claimed_lease():
    """Test legacy/no-token heartbeat cannot steal an owned worker ID lease."""
    redis_mock = MockRedis()
    owner = WorkerRegistry(redis_mock)
    stale = WorkerRegistry(redis_mock)

    token = await owner.claim_worker_id("worker-1")

    assert await stale.heartbeat_worker("worker-1") is False
    presence = json.loads(redis_mock.kv[RedisKeys.worker_online_lease("worker-1")])
    assert presence["token"] == token


@pytest.mark.asyncio
async def test_worker_online_lease_uses_claim_token_as_owner():
    """Test online lease ownership prevents stale workers mutating new owners."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)
    registry._ip_address = "10.0.0.8"

    token1 = await registry.claim_worker_id("worker-1")
    presence1 = json.loads(redis_mock.kv[RedisKeys.worker_online_lease("worker-1")])
    assert presence1["token"] == token1
    assert presence1["ip_address"] == "10.0.0.8"

    await registry.heartbeat_worker("worker-1")
    presence1 = json.loads(redis_mock.kv[RedisKeys.worker_online_lease("worker-1")])
    assert presence1["token"] == token1
    assert presence1["last_seen"] > 0
    assert presence1["ip_address"] == "10.0.0.8"

    redis_mock.kv.pop(RedisKeys.worker_online_lease("worker-1"))
    registry2 = WorkerRegistry(redis_mock)
    token2 = await registry2.claim_worker_id("worker-1")

    assert token2 != token1
    assert await registry.refresh_worker_id_lock("worker-1") is False
    presence2 = json.loads(redis_mock.kv[RedisKeys.worker_online_lease("worker-1")])
    assert presence2["token"] == token2
    assert await registry.release_worker_id("worker-1", token1) is False
    presence2 = json.loads(redis_mock.kv[RedisKeys.worker_online_lease("worker-1")])
    assert presence2["token"] == token2


@pytest.mark.asyncio
async def test_unregister_worker_membership_only_removes_membership_sets():
    """Test that membership unregister does not mutate worker liveness."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)

    await registry.register_worker_membership("worker-1", ["super_assistant"])
    await registry.heartbeat_worker("worker-1")
    online_snapshot = redis_mock.kv[RedisKeys.worker_online_lease("worker-1")]
    await registry.unregister_worker_membership("worker-1")

    assert redis_mock.kv[RedisKeys.worker_online_lease("worker-1")] == online_snapshot
    assert RedisKeys.worker_declared_agent_types("worker-1") not in redis_mock.data
    assert redis_mock.data[RedisKeys.agent_type_members("super_assistant")] == set()
    assert "worker-1" not in redis_mock.data[RedisKeys.KNOWN_WORKERS]


@pytest.mark.asyncio
async def test_mark_worker_inactive_only_removes_online_state():
    """Test that mark_worker_inactive only removes the online record."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)

    await registry.register_worker_membership("worker-1", ["super_assistant"])
    await registry.heartbeat_worker("worker-1")
    await registry.mark_worker_inactive("worker-1")

    assert OLD_ACTIVE_WORKERS not in redis_mock.data
    assert RedisKeys.worker_online_lease("worker-1") not in redis_mock.kv
    assert redis_mock.data[RedisKeys.worker_declared_agent_types("worker-1")] == {
        "super_assistant"
    }
    assert "worker-1" in redis_mock.data[RedisKeys.KNOWN_WORKERS]


@pytest.mark.asyncio
async def test_get_target_worker():
    """Test that WorkerRegistry can find workers by agent type."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)
    await registry.register_worker_membership("worker-1", ["super_assistant"])
    await registry.heartbeat_worker("worker-1")
    await registry.register_worker_membership("worker-2", ["my-agent"])
    await registry.heartbeat_worker("worker-2")

    # Test finding worker by agent type
    worker = await registry.get_target_worker("super_assistant")
    assert worker == "worker-1"

    worker = await registry.get_target_worker("my-agent")
    assert worker == "worker-2"

    # Test not found case
    worker = await registry.get_target_worker("unknown-agent")
    assert worker is None


@pytest.mark.asyncio
async def test_get_target_worker_filters_out_inactive_workers():
    """Test that target worker selection only returns online workers."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)

    await registry.register_worker_membership("dead-worker", ["super_assistant"])
    await registry.register_worker_membership("online-worker", ["super_assistant"])
    await registry.heartbeat_worker("online-worker")

    workers = await registry.get_online_workers("super_assistant")

    assert workers == ["online-worker"]
    assert await registry.get_target_worker("super_assistant") == "online-worker"


@pytest.mark.asyncio
async def test_check_worker_online_only_uses_lease_presence():
    """Test that stale zset entries without lease are not considered online."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)

    await redis_mock.zadd(OLD_ACTIVE_WORKERS, {"worker-1": 123456789})

    assert await registry.is_worker_online("worker-1") is False


@pytest.mark.asyncio
async def test_get_all_workers_uses_presence_last_seen_for_alive_workers():
    """Test worker listing uses presence JSON instead of active worker zset."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)
    registry._ip_address = "10.0.0.9"

    await registry.register_worker_membership("worker-1", ["super_assistant"])
    await registry.heartbeat_worker("worker-1")
    await registry.register_worker_membership("worker-2", ["code_agent"])

    workers = await registry.get_all_workers()

    assert set(workers) == {"worker-1"}
    assert workers["worker-1"]["agent_types"] == ["super_assistant"]
    assert workers["worker-1"]["last_seen"] > 0
    assert workers["worker-1"]["ip_address"] == "10.0.0.9"
    assert OLD_ACTIVE_WORKERS not in redis_mock.data


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
    """Test WorkerRegistry tracks execution lifecycle."""
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

    # Verify that it was saved to session registry
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


@pytest.mark.asyncio
async def test_worker_execution_indexes_track_active_and_history():
    """Test worker stats track active and historical state incrementally."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)

    await registry.initialize_execution(
        {
            "execution_id": "exec-1",
            "message_id": "msg-1",
            "session_id": "sess-1",
            "target_agent_type": "dummy_agent",
            "status": "QUEUED",
        }
    )

    await registry.update_execution_status(
        "exec-1", "sess-1", "RUNNING", worker_id="worker-1"
    )

    active_key = RedisKeys.worker_active_execution_index("worker-1")
    history_key = RedisKeys.worker_executions("worker-1")
    active_snapshots_key = RedisKeys.worker_active_snapshots("worker-1")
    history_snapshots_key = RedisKeys.worker_history_snapshots("worker-1")
    status_key = RedisKeys.worker_status("worker-1")

    assert "exec-1" in redis_mock.data[active_key]
    assert "exec-1" in redis_mock.data[history_key]
    assert "exec-1" in redis_mock.data[active_snapshots_key]
    assert "exec-1" in redis_mock.data[history_snapshots_key]
    assert redis_mock.data[status_key]["total_count"] == 1
    assert redis_mock.data[status_key]["active_count"] == 1
    assert redis_mock.data[status_key]["running_count"] == 1

    await registry.mark_execution_finished("exec-1", "sess-1", "COMPLETED")

    assert "exec-1" not in redis_mock.data[active_key]
    assert "exec-1" not in redis_mock.data[active_snapshots_key]
    assert "exec-1" in redis_mock.data[history_key]
    assert "exec-1" in redis_mock.data[history_snapshots_key]
    assert redis_mock.data[status_key]["active_count"] == 0
    assert redis_mock.data[status_key]["running_count"] == 0
    assert redis_mock.data[status_key]["completed_count"] == 1
    assert redis_mock.expires[active_key] == RedisKeys.DEFAULT_SESSION_TTL
    assert redis_mock.expires[history_key] == RedisKeys.DEFAULT_SESSION_TTL
    assert redis_mock.expires[active_snapshots_key] == RedisKeys.DEFAULT_SESSION_TTL
    assert redis_mock.expires[history_snapshots_key] == RedisKeys.DEFAULT_SESSION_TTL


@pytest.mark.asyncio
async def test_get_worker_executions_returns_recent_execution_records():
    """Test worker execution lookup resolves records through session registries."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)

    with patch("by_framework.core.registry.time.time", side_effect=[1, 2, 3]):
        await registry.save_execution(
            {
                "execution_id": "exec-old",
                "message_id": "msg-old",
                "session_id": "sess-1",
                "worker_id": "worker-1",
                "target_agent_type": "dummy_agent",
                "status": "RUNNING",
            }
        )
        await registry.mark_execution_finished("exec-old", "sess-1", "COMPLETED")
        await registry.save_execution(
            {
                "execution_id": "exec-new",
                "message_id": "msg-new",
                "session_id": "sess-2",
                "worker_id": "worker-1",
                "target_agent_type": "dummy_agent",
                "status": "RUNNING",
            }
        )

    executions = await registry.get_worker_executions("worker-1")

    assert [execution["execution_id"] for execution in executions] == [
        "exec-new",
        "exec-old",
    ]


@pytest.mark.asyncio
async def test_get_worker_executions_can_filter_terminal_records():
    """Test worker execution lookup can return only active records."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)

    await registry.save_execution(
        {
            "execution_id": "exec-finished",
            "message_id": "msg-finished",
            "session_id": "sess-1",
            "worker_id": "worker-1",
            "target_agent_type": "dummy_agent",
            "status": "RUNNING",
        }
    )
    await registry.mark_execution_finished("exec-finished", "sess-1", "COMPLETED")
    await registry.save_execution(
        {
            "execution_id": "exec-running",
            "message_id": "msg-running",
            "session_id": "sess-2",
            "worker_id": "worker-1",
            "target_agent_type": "dummy_agent",
            "status": "RUNNING",
        }
    )

    executions = await registry.get_worker_executions(
        "worker-1", include_terminal=False
    )

    assert [execution["execution_id"] for execution in executions] == ["exec-running"]


@pytest.mark.asyncio
async def test_get_worker_execution_summary_counts_state_and_liveness():
    """Test worker summary reads aggregate counts and snapshots."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)

    await registry.register_worker_membership("worker-1", ["dummy_agent"])
    await registry.heartbeat_worker("worker-1")
    with patch("by_framework.core.registry.time.time", side_effect=[1, 2, 3]):
        await registry.save_execution(
            {
                "execution_id": "exec-completed",
                "message_id": "msg-completed",
                "session_id": "sess-1",
                "worker_id": "worker-1",
                "target_agent_type": "dummy_agent",
                "status": "RUNNING",
            }
        )
        await registry.mark_execution_finished("exec-completed", "sess-1", "COMPLETED")
        await registry.save_execution(
            {
                "execution_id": "exec-running",
                "message_id": "msg-running",
                "session_id": "sess-2",
                "worker_id": "worker-1",
                "target_agent_type": "dummy_agent",
                "status": "RUNNING",
            }
        )

    summary = await registry.get_worker_execution_summary("worker-1")

    assert summary["worker_id"] == "worker-1"
    assert summary["online"] is True
    assert summary["agent_types"] == ["dummy_agent"]
    assert summary["active_count"] == 1
    assert summary["total_tracked"] == 2
    assert summary["counts"] == {
        "total": 2,
        "active": 1,
        "queued": 0,
        "running": 1,
        "cancelling": 0,
        "completed": 1,
        "failed": 0,
        "cancelled": 0,
    }
    assert summary["status_counts"] == {"RUNNING": 1, "COMPLETED": 1}
    assert [item["execution_id"] for item in summary["active_executions"]] == [
        "exec-running"
    ]
    assert [item["execution_id"] for item in summary["recent_executions"]] == [
        "exec-running",
        "exec-completed",
    ]


@pytest.mark.asyncio
async def test_worker_execution_summary_does_not_get_session_executions():
    """Test worker summary does not perform per-execution session lookups."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)

    await registry.save_execution(
        {
            "execution_id": "exec-running",
            "message_id": "msg-running",
            "session_id": "sess-1",
            "worker_id": "worker-1",
            "target_agent_type": "dummy_agent",
            "status": "RUNNING",
        }
    )

    async def fail_get_execution(*args, **kwargs):  # pylint: disable=unused-argument
        raise AssertionError("get_execution should not be used by worker summary")

    registry.get_execution = fail_get_execution

    summary = await registry.get_worker_execution_summary("worker-1")

    assert summary["counts"]["running"] == 1
    assert summary["active_executions"][0]["execution_id"] == "exec-running"


@pytest.mark.asyncio
async def test_worker_execution_summary_can_return_counts_only():
    """Test worker summary can skip active and recent snapshot reads."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)

    await registry.save_execution(
        {
            "execution_id": "exec-running",
            "message_id": "msg-running",
            "session_id": "sess-1",
            "worker_id": "worker-1",
            "target_agent_type": "dummy_agent",
            "status": "RUNNING",
        }
    )

    summary = await registry.get_worker_execution_summary(
        "worker-1", active_limit=0, history_limit=0
    )

    assert summary["counts"]["total"] == 1
    assert summary["counts"]["active"] == 1
    assert summary["active_executions"] == []
    assert summary["recent_executions"] == []


@pytest.mark.asyncio
async def test_update_execution_fields_updates_metadata_without_timeline_noise():
    """Test ad-hoc execution field updates do not append duplicate timeline events."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)

    await registry.initialize_execution(
        {
            "execution_id": "exec-2",
            "message_id": "msg-2",
            "session_id": "sess-2",
            "status": "QUEUED",
        }
    )

    before = await registry.get_execution("exec-2", "sess-2")
    await registry.update_execution_fields(
        "exec-2",
        "sess-2",
        langfuse_observation_id="obs-2",
        trace_url="https://langfuse.local/trace/trace-2",
    )
    after = await registry.get_execution("exec-2", "sess-2")

    assert after["langfuse_observation_id"] == "obs-2"
    assert after["trace_url"] == "https://langfuse.local/trace/trace-2"
    assert after["timeline"] == before["timeline"]
    assert after["status"] == "QUEUED"


def test_update_execution_fields_refreshes_worker_execution_snapshot():
    """Dashboard worker history must see metadata added after save_execution."""

    async def scenario():
        redis_mock = MockRedis()
        registry = WorkerRegistry(redis_mock)

        await registry.save_execution(
            {
                "execution_id": "exec-audit-refresh",
                "message_id": "msg-audit-refresh",
                "session_id": "sess-audit-refresh",
                "worker_id": "worker-audit-refresh",
                "target_agent_type": "weather-agent",
                "status": "RUNNING",
            }
        )
        await registry.update_execution_fields(
            "exec-audit-refresh",
            "sess-audit-refresh",
            agent_config_audit={
                "target_agent_type": "weather-agent",
                "snapshot_hash": "sha256:after-save",
            },
        )

        worker_executions = await registry.get_worker_executions("worker-audit-refresh")
        assert worker_executions[0]["agent_config_audit"]["snapshot_hash"] == (
            "sha256:after-save"
        )

    asyncio.run(scenario())


@pytest.mark.asyncio
async def test_persist_and_load_agent_configs_snapshot_round_trip():
    """Test persisted config snapshots can be loaded back intact."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)
    plugin_registry = PluginRegistry()
    plugin_registry._set_agent_configs([AgentConfig(agent_id="agent_v1")])  # pylint: disable=protected-access
    snapshot = plugin_registry.get_agent_configs_snapshot()

    snapshot_key = await registry.persist_agent_configs_snapshot("exec-3", snapshot)
    restored = await registry.load_agent_configs_snapshot(snapshot_key)

    assert snapshot_key == "exec-3"
    stored_payload = redis_mock.kv[RedisKeys.agent_configs_snapshot("exec-3")]
    assert isinstance(stored_payload, str)
    assert stored_payload.startswith("dill-base64:")
    assert restored is not None
    assert restored.version == snapshot.version
    assert [config.agent_id for config in restored.configs] == ["agent_v1"]
    assert redis_mock.expires[RedisKeys.agent_configs_snapshot("exec-3")] == (
        RedisKeys.AGENT_CONFIGS_SNAPSHOT_TTL_SECONDS
    )


@pytest.mark.asyncio
async def test_mark_execution_finished_cleans_up_persisted_agent_configs_snapshot():
    """Test terminal execution cleanup removes the persisted snapshot blob."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)
    plugin_registry = PluginRegistry()
    plugin_registry._set_agent_configs([AgentConfig(agent_id="agent_v1")])  # pylint: disable=protected-access
    snapshot = plugin_registry.get_agent_configs_snapshot()

    await registry.persist_agent_configs_snapshot("exec-4", snapshot)
    await registry.save_execution(
        {
            "execution_id": "exec-4",
            "message_id": "msg-4",
            "session_id": "sess-4",
            "worker_id": "worker-4",
            "target_agent_type": "agent_v1",
            "status": "RUNNING",
            "cancel_requested": False,
            "agent_configs_snapshot_key": "exec-4",
        }
    )

    await registry.mark_execution_finished("exec-4", "sess-4", "COMPLETED")

    assert RedisKeys.agent_configs_snapshot("exec-4") not in redis_mock.kv
    finished = await registry.get_execution("exec-4", "sess-4")
    assert finished is not None
    assert finished["agent_configs_snapshot_cleanup_status"] == "deleted"
    assert finished["agent_configs_snapshot_cleaned_at"] > 0
    assert finished["agent_configs_snapshot_cleanup_error"] == ""


@pytest.mark.asyncio
async def test_non_terminal_execution_status_keeps_persisted_agent_configs_snapshot():
    """Test suspended executions keep snapshots for later resume."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)
    plugin_registry = PluginRegistry()
    plugin_registry._set_agent_configs([AgentConfig(agent_id="agent_v1")])  # pylint: disable=protected-access
    snapshot = plugin_registry.get_agent_configs_snapshot()

    await registry.persist_agent_configs_snapshot("exec-waiting", snapshot)
    await registry.save_execution(
        {
            "execution_id": "exec-waiting",
            "message_id": "msg-waiting",
            "session_id": "sess-waiting",
            "worker_id": "worker-waiting",
            "target_agent_type": "agent_v1",
            "status": "RUNNING",
            "cancel_requested": False,
            "agent_configs_snapshot_key": "exec-waiting",
        }
    )

    await registry.mark_execution_finished(
        "exec-waiting",
        "sess-waiting",
        "WAITING_USER",
    )

    assert RedisKeys.agent_configs_snapshot("exec-waiting") in redis_mock.kv
    waiting = await registry.get_execution("exec-waiting", "sess-waiting")
    assert waiting is not None
    assert waiting["status"] == "WAITING_USER"
    assert "agent_configs_snapshot_cleanup_status" not in waiting


@pytest.mark.asyncio
async def test_mark_finished_preserves_completion_when_snapshot_cleanup_fails():
    """Test cleanup failures are logged but do not block execution completion."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)
    await registry.save_execution(
        {
            "execution_id": "exec-5",
            "message_id": "msg-5",
            "session_id": "sess-5",
            "worker_id": "worker-5",
            "target_agent_type": "agent_v1",
            "status": "RUNNING",
            "cancel_requested": False,
            "agent_configs_snapshot_key": "exec-5",
        }
    )

    with patch.object(
        registry,
        "delete_agent_configs_snapshot",
        side_effect=RuntimeError("delete failed"),
    ):
        await registry.mark_execution_finished("exec-5", "sess-5", "FAILED")

    finished = await registry.get_execution("exec-5", "sess-5")
    assert finished is not None
    assert finished["status"] == "FAILED"
    assert finished["agent_configs_snapshot_cleanup_status"] == "delete_failed"
    assert finished["agent_configs_snapshot_cleaned_at"] == 0
    assert "delete failed" in finished["agent_configs_snapshot_cleanup_error"]


@pytest.mark.asyncio
async def test_has_online_agent_type():
    """Test has_online_agent_type returns correct status and worker list."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)
    await registry.register_worker_membership(
        "worker-1", ["super_assistant", "code_agent"]
    )
    await registry.heartbeat_worker("worker-1")
    await registry.register_worker_membership("worker-2", ["super_assistant"])
    await registry.heartbeat_worker("worker-2")

    # Test with workers registered and online
    exists, workers = await registry.has_online_agent_type("super_assistant")
    assert exists is True
    assert set(workers) == {"worker-1", "worker-2"}

    # Test with single worker
    exists, workers = await registry.has_online_agent_type("code_agent")
    assert exists is True
    assert set(workers) == {"worker-1"}

    # Test with no workers
    exists, workers = await registry.has_online_agent_type("unknown_agent")
    assert exists is False
    assert workers == []


@pytest.mark.asyncio
async def test_has_online_agent_type_filters_offline_workers():
    """Test has_online_agent_type filters offline workers with check_active=True."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)

    # Manually register agent types WITHOUT active heartbeat
    await redis_mock.sadd(RedisKeys.agent_type_members("inactive_agent"), "dead_worker")
    await redis_mock.sadd(
        RedisKeys.worker_declared_agent_types("dead_worker"), "inactive_agent"
    )

    # With check_active=True (default), dead worker should not be found
    exists, workers = await registry.has_online_agent_type(
        "inactive_agent", check_active=True
    )
    assert exists is False
    assert workers == []

    # With check_active=False, dead worker should be found
    exists, workers = await registry.has_online_agent_type(
        "inactive_agent", check_active=False
    )
    assert exists is True
    assert workers == ["dead_worker"]


@pytest.mark.asyncio
async def test_is_worker_online():
    """Test that is_worker_online correctly checks worker lease state."""
    redis_mock = MockRedis()
    registry = WorkerRegistry(redis_mock)
    await registry.register_worker_membership("online_worker", ["test_agent"])
    await registry.heartbeat_worker("online_worker")

    # Worker with an active lease should be online
    is_online = await registry.is_worker_online("online_worker")
    assert is_online is True

    # Unknown worker should not be online
    is_online = await registry.is_worker_online("unknown_worker")
    assert is_online is False
