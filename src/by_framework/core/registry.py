"""
Worker registry module.

Provides worker registration, discovery, and execution tracking
through Redis-backed storage.
"""

import base64
import ipaddress
import json
import logging
import random
import socket
import time
import uuid
import warnings
from typing import Any, List, Optional, TypedDict

from by_framework.common.constants import (EXEC_FIELD_PREFIX, MSG_MAP_PREFIX, RedisKeys)
from by_framework.common.exceptions import ExecutionDataError
from by_framework.common.logger import observability_log_extra
from by_framework.common.redis_client import Redis, get_redis
from by_framework.core.extensions import AgentConfigsSnapshot, PluginRegistry
from by_framework.core.protocol.agent_state import is_terminal_state

logger = logging.getLogger("by_framework.registry")
SNAPSHOT_PAYLOAD_PREFIX = "dill-base64:"
PRESENCE_PAYLOAD_VERSION = 1

# Atomic compare-and-swap for heartbeat renewal.
# Token-mode: verifies the stored token matches before overwriting.
# No-token (legacy) mode: only updates absent/unowned legacy presence.
# ARGV: [1]=token ('' for legacy), [2]=new_value, [3]=ttl_seconds
# Returns: 1 = success, 0 = lock owned by another instance, -1 = unparseable legacy key
_HEARTBEAT_CAS_SCRIPT = """
local function decode_token(raw)
    local ok, data = pcall(cjson.decode, raw)
    if not ok then return nil, false end
    if type(data) == 'table' then
        local stored = data['token']
        if stored == nil or stored == cjson.null then return nil, true end
        return tostring(stored), true
    end
    if data == 1 then return nil, true end
    return tostring(data), true
end

local raw = redis.call('GET', KEYS[1])
if ARGV[1] ~= '' then
    if raw == false then
        local ok = redis.call('SET', KEYS[1], ARGV[2], 'NX', 'EX', tonumber(ARGV[3]))
        if ok then return 1 else return 0 end
    end
    local stored, parsed = decode_token(raw)
    if not parsed then return -1 end
    if stored == nil then return 0 end
    if stored ~= ARGV[1] then return 0 end
    redis.call('SET', KEYS[1], ARGV[2], 'EX', tonumber(ARGV[3]))
    return 1
end

if raw ~= false then
    local stored, parsed = decode_token(raw)
    if not parsed then return 0 end
    if stored ~= nil then return 0 end
end
redis.call('SET', KEYS[1], ARGV[2], 'EX', tonumber(ARGV[3]))
return 1
"""

# Atomic token-verified TTL refresh (GET+EXPIRE in one step).
# ARGV: [1]=expected_token, [2]=ttl_seconds
# Returns: 1 = refreshed, 0 = token mismatch / key absent / unparseable
_REFRESH_LOCK_SCRIPT = """
local function decode_token(raw)
    local ok, data = pcall(cjson.decode, raw)
    if not ok then return nil end
    if type(data) ~= 'table' then
        if data == 1 then return nil end
        return tostring(data)
    end
    local stored = data['token']
    if stored == nil or stored == cjson.null then return nil end
    return tostring(stored)
end

local raw = redis.call('GET', KEYS[1])
if raw == false then return 0 end
local stored = decode_token(raw)
if stored == nil then return 0 end
if stored ~= ARGV[1] then return 0 end
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return 1
"""

# Atomic token-verified key deletion (Redlock release pattern).
# ARGV: [1]=expected_token
#   Empty string deletes unconditionally for cases with no token ownership.
# Returns: 1 = deleted (or key already absent with no-token mode), 0 = token mismatch
_RELEASE_LOCK_SCRIPT = """
local function decode_token(raw)
    local ok, data = pcall(cjson.decode, raw)
    if not ok then return nil end
    if type(data) ~= 'table' then
        if data == 1 then return nil end
        return tostring(data)
    end
    local stored = data['token']
    if stored == nil or stored == cjson.null then return nil end
    return tostring(stored)
end

local raw = redis.call('GET', KEYS[1])
if raw == false then
    if ARGV[1] == '' then return 1 end
    return 0
end
if ARGV[1] == '' then
    redis.call('DEL', KEYS[1])
    return 1
end
local stored = decode_token(raw)
if stored == nil then return 0 end
if stored ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
return 1
"""


class ExecutionCompletionFields(TypedDict, total=False):
    """Structured terminal metadata attached for observability."""

    error_type: str
    error_message: str
    error_code: str
    failed_stage: str
    retryable: bool


def _is_useful_presence_ip(value: str) -> bool:
    try:
        ip = ipaddress.ip_address(value)
    except ValueError:
        return False
    return not (ip.is_loopback or ip.is_unspecified)


def _get_default_route_ip_address() -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            return str(sock.getsockname()[0])
    except OSError:
        return ""


def _get_local_ip_address() -> str:
    """Best-effort non-loopback IP address for worker presence diagnostics."""
    hostname_ip = ""
    try:
        hostname_ip = socket.gethostbyname(socket.gethostname())
        if _is_useful_presence_ip(hostname_ip):
            return hostname_ip
    except OSError:
        pass

    route_ip = _get_default_route_ip_address()
    if _is_useful_presence_ip(route_ip):
        return route_ip
    return hostname_ip or route_ip


def _decode_worker_presence(raw: Any) -> tuple[Optional[str], int, bool, str]:
    """Decode worker presence payload.

    Returns:
        (owner token, last_seen timestamp in ms, whether the payload is legacy, ip)
    """
    if raw is None:
        return (None, 0, False, "")
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")

    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return (str(raw), 0, True, "")

    if isinstance(payload, dict):
        token = payload.get("token")
        last_seen = payload.get("last_seen", 0)
        ip_address = str(payload.get("ip_address") or "")
        if token is not None:
            token = str(token)
        return (token, int(last_seen or 0), False, ip_address)

    if payload == 1:
        return (None, 0, True, "")
    return (str(payload), 0, True, "")


def _encode_worker_presence(
    token: Optional[str], last_seen: int, ip_address: str = ""
) -> str:
    return json.dumps(
        {
            "version": PRESENCE_PAYLOAD_VERSION,
            "token": token,
            "last_seen": last_seen,
            "ip_address": ip_address,
        },
        separators=(",", ":"),
    )


# --- Standalone agent type probing functions (usable without WorkerRegistry) ---


async def check_worker_online(
    redis: Redis,
    worker_id: str,
) -> bool:
    """Check if the specified worker is active.

    Args:
        redis: Redis client instance.
        worker_id: Worker ID.
    Returns:
        Whether the worker is active.
    """
    lease_value = await redis.get(RedisKeys.worker_online_lease(worker_id))
    if lease_value is None:
        return False
    decoded_token, last_seen, is_legacy, ip_address = _decode_worker_presence(
        lease_value
    )
    del decoded_token
    del ip_address
    return is_legacy or last_seen > 0


async def check_agent_type_online(
    redis: Redis,
    agent_type: str,
    check_active: bool = True,
) -> tuple[bool, List[str]]:
    """Check if there are registered and active workers for the agent type.

    Args:
        redis: Redis client instance.
        agent_type: Agent type identifier.
        check_active: Whether to check worker active status (default True).
    Returns:
        (Whether there are active workers, list of active worker IDs)
    """
    workers = await redis.smembers(RedisKeys.agent_type_members(agent_type))
    if not workers:
        return (False, [])

    worker_ids = [w.decode() if isinstance(w, bytes) else w for w in workers]

    if check_active:
        online_worker_ids = []
        for worker_id in worker_ids:
            if await check_worker_online(redis, worker_id):
                online_worker_ids.append(worker_id)
        worker_ids = online_worker_ids

    return (len(worker_ids) > 0, worker_ids)


class WorkerRegistry:
    """Worker registry responsible for worker registration, discovery, and execution.

    Stores worker information and execution state through Redis sorted sets
    and Hash structures.
    """

    def __init__(self, redis_client: Optional[Redis] = None):
        self.redis = redis_client or get_redis()
        self._lock_tokens: dict[str, str] = {}
        self._ip_address = _get_local_ip_address()

    async def register_worker_membership(
        self, worker_id: str, agent_types: List[str]
    ) -> None:
        declared_key = RedisKeys.worker_declared_agent_types(worker_id)
        new_agent_types = set(agent_types)
        old_agent_types_raw = await self.redis.smembers(declared_key)
        old_agent_types = {
            item.decode("utf-8") if isinstance(item, bytes) else item
            for item in old_agent_types_raw
        }

        await self.redis.sadd(RedisKeys.KNOWN_WORKERS, worker_id)
        for stale_agent_type in old_agent_types - new_agent_types:
            await self.redis.srem(
                RedisKeys.agent_type_members(stale_agent_type), worker_id
            )
            await self.redis.srem(declared_key, stale_agent_type)

        for agent_type in new_agent_types:
            await self.redis.sadd(declared_key, agent_type)
            denied = await self.redis.sismember(
                RedisKeys.agent_type_denied(agent_type), worker_id
            )
            if not denied:
                await self.redis.sadd(
                    RedisKeys.agent_type_members(agent_type), worker_id
                )

    async def heartbeat_worker(
        self,
        worker_id: str,
        lease_ttl_seconds: int = RedisKeys.WORKER_DEFAULT_LEASE_TTL_SECONDS,
    ) -> bool:
        now = int(time.time() * 1000)
        lease_key = RedisKeys.worker_online_lease(worker_id)
        token = self._lock_tokens.get(worker_id)
        token_arg = token or ""
        new_value = _encode_worker_presence(
            token if token else None, now, self._ip_address
        )

        result = await self.redis.eval(
            _HEARTBEAT_CAS_SCRIPT,
            1,
            lease_key,
            token_arg,
            new_value,
            str(lease_ttl_seconds),
        )

        if result == -1:
            logger.warning(
                "[%s] Unparseable legacy presence key detected; heartbeat rejected",
                worker_id,
            )
            return False

        if not result:
            return False

        await self.redis.sadd(RedisKeys.KNOWN_WORKERS, worker_id)
        return True

    async def register_worker(self, worker_id: str, agent_types: List[str]):
        """Compatibility wrapper for callers that couple registration and heartbeat."""
        warnings.warn(
            "WorkerRegistry.register_worker() is deprecated; use "
            "register_worker_membership() plus heartbeat_worker() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        await self.register_worker_membership(worker_id, agent_types)
        await self.heartbeat_worker(worker_id)

    async def unregister_worker_membership(self, worker_id: str) -> None:
        """Remove static worker-agent-type membership without mutating liveness."""
        agent_types_raw = await self.redis.smembers(
            RedisKeys.worker_declared_agent_types(worker_id)
        )
        await self.redis.delete(RedisKeys.worker_declared_agent_types(worker_id))
        await self.redis.srem(RedisKeys.KNOWN_WORKERS, worker_id)
        for agent_type_raw in agent_types_raw:
            agent_type = (
                agent_type_raw.decode()
                if isinstance(agent_type_raw, bytes)
                else agent_type_raw
            )
            await self.redis.srem(RedisKeys.agent_type_members(agent_type), worker_id)

    async def mark_worker_inactive(
        self, worker_id: str, token: Optional[str] = None
    ) -> bool:
        expected = token or self._lock_tokens.get(worker_id)
        lease_key = RedisKeys.worker_online_lease(worker_id)
        result = await self.redis.eval(
            _RELEASE_LOCK_SCRIPT,
            1,
            lease_key,
            expected or "",
        )
        return bool(result)

    async def unregister_worker(self, worker_id: str):
        """Compatibility wrapper for callers that couple deregistration and liveness."""
        warnings.warn(
            "WorkerRegistry.unregister_worker() is deprecated; use "
            "mark_worker_inactive() plus unregister_worker_membership() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        await self.mark_worker_inactive(worker_id)
        await self.unregister_worker_membership(worker_id)

    async def get_online_workers(
        self,
        agent_type: str,
    ) -> List[str]:
        _, worker_ids = await check_agent_type_online(
            self.redis,
            agent_type,
            check_active=True,
        )
        return worker_ids

    async def get_random_online_worker(
        self,
        agent_type: str,
    ) -> Optional[str]:
        workers = await self.get_online_workers(agent_type)
        if not workers:
            return None
        return random.choice(workers)

    async def get_target_worker(self, agent_id: str) -> Optional[str]:
        return await self.get_random_online_worker(agent_id)

    async def is_worker_online(
        self,
        worker_id: str,
    ) -> bool:
        """Check if the specified worker is active.

        Args:
            worker_id: Worker ID.

        Returns:
            Whether the worker is active.
        """
        return await check_worker_online(self.redis, worker_id)

    async def has_online_agent_type(
        self,
        agent_type: str,
        check_active: bool = True,
    ) -> tuple[bool, List[str]]:
        """Check if there are registered and active workers for the agent type.

        Args:
            agent_type: Agent type identifier.
            check_active: Whether to check worker active status (default True).
        Returns:
            (Whether there are active workers, list of active worker IDs)
        """
        return await check_agent_type_online(self.redis, agent_type, check_active)

    async def get_all_workers(self) -> dict[str, Any]:
        """Get all active Worker information.

        Returns:
            Dictionary containing active worker IDs with their capabilities
            and last active time.
        """
        redis_inst = self.redis
        worker_ids_raw = await redis_inst.smembers(RedisKeys.KNOWN_WORKERS)
        worker_ids = [w.decode() if isinstance(w, bytes) else w for w in worker_ids_raw]

        result = {}
        for worker_id in sorted(worker_ids):
            presence = await redis_inst.get(RedisKeys.worker_online_lease(worker_id))
            decoded_token, last_seen, is_legacy, ip_address = _decode_worker_presence(
                presence
            )
            del decoded_token
            if presence is None or (not is_legacy and last_seen <= 0):
                continue

            agent_types_raw = await redis_inst.smembers(
                RedisKeys.worker_declared_agent_types(worker_id)
            )
            agent_types = [
                c.decode() if isinstance(c, bytes) else c for c in agent_types_raw
            ]
            admin_state = {}
            if hasattr(self, "get_worker_admin_state"):
                admin_state = await self.get_worker_admin_state(worker_id)
            result[worker_id] = {
                "agent_types": agent_types,
                "last_seen": int(time.time() * 1000) if is_legacy else last_seen,
                "ip_address": ip_address,
                "lifecycle": admin_state.get("lifecycle", "active") or "active",
                "lifecycle_reason": admin_state.get("reason", ""),
            }
        return result

    async def claim_worker_id(
        self,
        worker_id: str,
        ttl_seconds: int = RedisKeys.WORKER_DEFAULT_LEASE_TTL_SECONDS,
    ) -> str:
        """Attempt to acquire an exclusive lock for Worker ID.

        Args:
            worker_id: Worker ID to acquire lock for
            ttl_seconds: Lock TTL in seconds

        Returns:
            Lock token

        Raises:
            ValueError: If worker_id is already in use
        """
        token = uuid.uuid4().hex
        lease_key = RedisKeys.worker_online_lease(worker_id)
        ok = await self.redis.set(
            lease_key,
            _encode_worker_presence(token, 0, self._ip_address),
            nx=True,
            ex=ttl_seconds,
        )
        if not ok:
            raise ValueError(f"worker_id already in use: {worker_id}")
        self._lock_tokens[worker_id] = token
        await self.redis.sadd(RedisKeys.KNOWN_WORKERS, worker_id)
        return token

    async def refresh_worker_id_lock(
        self, worker_id: str, ttl_seconds: int = 60
    ) -> bool:
        """Refresh the TTL of the Worker ID lock.

        Args:
            worker_id: Worker ID
            ttl_seconds: New TTL in seconds

        Returns:
            True if refresh succeeded, otherwise False
        """
        token = self._lock_tokens.get(worker_id)
        if not token:
            return False

        lease_key = RedisKeys.worker_online_lease(worker_id)
        result = await self.redis.eval(
            _REFRESH_LOCK_SCRIPT,
            1,
            lease_key,
            token,
            str(ttl_seconds),
        )
        return bool(result)

    async def release_worker_id(
        self, worker_id: str, token: Optional[str] = None
    ) -> bool:
        """Release the exclusive lock for Worker ID.

        Args:
            worker_id: Worker ID
            token: Optional lock token

        Returns:
            True if release succeeded, otherwise False
        """
        expected = token or self._lock_tokens.get(worker_id)
        if not expected:
            return False

        key = RedisKeys.worker_online_lease(worker_id)
        result = await self.redis.eval(
            _RELEASE_LOCK_SCRIPT,
            1,
            key,
            expected,
        )
        if result:
            self._lock_tokens.pop(worker_id, None)
        return bool(result)

    async def initialize_execution(self, execution: dict[str, Any]):
        """Initialize Execution on sender side (status QUEUED) with first timeline
        record.

        Args:
            execution: Execution info dict containing execution_id, message_id,
                session_id, etc.
        """
        now = int(time.time() * 1000)
        execution["created_at"] = now
        execution["updated_at"] = now
        execution.setdefault("started_at", 0)
        execution.setdefault("finished_at", 0)
        if "timeline" not in execution:
            execution["timeline"] = [
                {"status": execution.get("status", "QUEUED"), "timestamp": now}
            ]

        execution_id = execution["execution_id"]
        message_id = execution["message_id"]
        session_id = execution["session_id"]

        reg_key = RedisKeys.session_registry(session_id)
        encoded_data = json.dumps(execution, ensure_ascii=False)

        # Use Pipeline to ensure atomicity and set TTL
        pipe = self.redis.pipeline()
        pipe.hset(reg_key, f"{EXEC_FIELD_PREFIX}{execution_id}", encoded_data)
        pipe.hset(reg_key, f"{MSG_MAP_PREFIX}{message_id}", execution_id)
        pipe.expire(reg_key, RedisKeys.DEFAULT_SESSION_TTL)
        await pipe.execute()

    async def persist_agent_configs_snapshot(
        self,
        snapshot_key: str,
        snapshot: AgentConfigsSnapshot,
    ) -> str:
        """Persist an AgentConfigsSnapshot blob for later execution recovery."""
        payload = SNAPSHOT_PAYLOAD_PREFIX + base64.b64encode(
            PluginRegistry.serialize_agent_configs_snapshot(snapshot)
        ).decode("ascii")
        redis_key = RedisKeys.agent_configs_snapshot(snapshot_key)
        await self.redis.set(
            redis_key,
            payload,
            ex=RedisKeys.AGENT_CONFIGS_SNAPSHOT_TTL_SECONDS,
        )
        return snapshot_key

    async def load_agent_configs_snapshot(
        self,
        snapshot_key: str,
    ) -> Optional[AgentConfigsSnapshot]:
        """Load a previously persisted AgentConfigsSnapshot by key."""
        payload = await self.redis.get(RedisKeys.agent_configs_snapshot(snapshot_key))
        if payload is None:
            return None
        if isinstance(payload, str):
            if not payload.startswith(SNAPSHOT_PAYLOAD_PREFIX):
                raise ValueError(
                    "Unsupported persisted agent configs snapshot payload format"
                )
            payload = base64.b64decode(payload.removeprefix(SNAPSHOT_PAYLOAD_PREFIX))
        elif isinstance(payload, bytes) and payload.startswith(
            SNAPSHOT_PAYLOAD_PREFIX.encode("ascii")
        ):
            payload = base64.b64decode(
                payload.removeprefix(SNAPSHOT_PAYLOAD_PREFIX.encode("ascii"))
            )
        return PluginRegistry.deserialize_agent_configs_snapshot(payload)

    async def delete_agent_configs_snapshot(self, snapshot_key: str) -> None:
        """Delete a persisted AgentConfigsSnapshot blob."""
        await self.redis.delete(RedisKeys.agent_configs_snapshot(snapshot_key))

    def _build_worker_execution_snapshot(
        self, execution: dict[str, Any]
    ) -> dict[str, Any]:
        """Build a compact worker-facing execution snapshot."""
        return {
            "execution_id": execution.get("execution_id", ""),
            "message_id": execution.get("message_id", ""),
            "session_id": execution.get("session_id", ""),
            "trace_id": execution.get("trace_id", ""),
            "worker_id": execution.get("worker_id", ""),
            "source_agent_type": execution.get("source_agent_type", ""),
            "target_agent_type": execution.get("target_agent_type", ""),
            "stream_name": execution.get("stream_name", ""),
            "redis_message_id": execution.get("redis_message_id", ""),
            "status": execution.get("status", ""),
            "created_at": int(execution.get("created_at", 0) or 0),
            "started_at": int(execution.get("started_at", 0) or 0),
            "finished_at": int(execution.get("finished_at", 0) or 0),
            "updated_at": int(execution.get("updated_at", 0) or 0),
            "parent_message_id": execution.get("parent_message_id", ""),
            "cancel_requested": bool(execution.get("cancel_requested", False)),
            "cancel_reason": execution.get("cancel_reason", ""),
            "route_policy": execution.get("route_policy", ""),
            "route_status": execution.get("route_status", ""),
            "selected_agent_type": execution.get("selected_agent_type", ""),
            "availability_error_code": execution.get("availability_error_code", ""),
            "availability_error": execution.get("availability_error", ""),
            "error_type": execution.get("error_type", ""),
            "error_message": execution.get("error_message", ""),
            "error_code": execution.get("error_code", ""),
            "failed_stage": execution.get("failed_stage", ""),
            "retryable": bool(execution.get("retryable", False)),
            "agent_configs_version": execution.get("agent_configs_version", 0),
            "agent_configs_snapshot_key": execution.get(
                "agent_configs_snapshot_key", ""
            ),
            "agent_config_audit": execution.get("agent_config_audit"),
        }

    @staticmethod
    def _status_count_field(status: str) -> str:
        return f"{status.lower()}_count"

    async def _update_worker_execution_stats(
        self,
        old_execution: Optional[dict[str, Any]],
        new_execution: dict[str, Any],
        now: int,
    ) -> None:
        """Incrementally update worker-level aggregate stats and snapshots."""
        worker_id = str(new_execution.get("worker_id") or "")
        if not worker_id:
            return

        old_worker_id = str((old_execution or {}).get("worker_id") or "")
        old_status = str((old_execution or {}).get("status") or "")
        new_status = str(new_execution.get("status") or "")
        execution_id = str(new_execution["execution_id"])

        first_seen_by_worker = old_worker_id != worker_id
        old_active = bool(
            old_worker_id == worker_id and not is_terminal_state(old_status)
        )
        new_active = not is_terminal_state(new_status)

        status_key = RedisKeys.worker_status(worker_id)
        history_key = RedisKeys.worker_executions(worker_id)
        active_key = RedisKeys.worker_active_execution_index(worker_id)
        active_snapshots_key = RedisKeys.worker_active_snapshots(worker_id)
        history_snapshots_key = RedisKeys.worker_history_snapshots(worker_id)
        snapshot = json.dumps(
            self._build_worker_execution_snapshot(new_execution), ensure_ascii=False
        )

        pipe = self.redis.pipeline()
        if first_seen_by_worker:
            pipe.hincrby(status_key, "total_count", 1)
        if first_seen_by_worker or old_status != new_status:
            if old_status and old_worker_id == worker_id:
                pipe.hincrby(status_key, self._status_count_field(old_status), -1)
            if new_status:
                pipe.hincrby(status_key, self._status_count_field(new_status), 1)
        if not old_active and new_active:
            pipe.hincrby(status_key, "active_count", 1)
        elif old_active and not new_active:
            pipe.hincrby(status_key, "active_count", -1)

        pipe.hset(status_key, "last_updated_at", now)
        if new_status == "RUNNING":
            pipe.hset(
                status_key,
                "last_started_at",
                int(new_execution.get("started_at", 0) or 0),
            )
        if is_terminal_state(new_status):
            pipe.hset(
                status_key,
                "last_finished_at",
                int(new_execution.get("finished_at", 0) or 0),
            )

        pipe.zadd(history_key, {execution_id: now})
        pipe.hset(history_snapshots_key, execution_id, snapshot)
        if new_active:
            pipe.zadd(active_key, {execution_id: now})
            pipe.hset(active_snapshots_key, execution_id, snapshot)
        else:
            pipe.zrem(active_key, execution_id)
            pipe.hdel(active_snapshots_key, execution_id)
        pipe.expire(status_key, RedisKeys.DEFAULT_SESSION_TTL)
        pipe.expire(history_key, RedisKeys.DEFAULT_SESSION_TTL)
        pipe.expire(active_key, RedisKeys.DEFAULT_SESSION_TTL)
        pipe.expire(active_snapshots_key, RedisKeys.DEFAULT_SESSION_TTL)
        pipe.expire(history_snapshots_key, RedisKeys.DEFAULT_SESSION_TTL)
        await pipe.execute()

    async def update_execution_status(
        self, execution_id: str, session_id: str, status: str, **kwargs
    ):
        """Update existing execution record's status and append to timeline."""
        current = await self.get_execution(execution_id, session_id)
        if current is None:
            return

        old_execution = dict(current)
        now = int(time.time() * 1000)
        current["status"] = status
        current["updated_at"] = now
        if status == "RUNNING" and current.get("started_at", 0) == 0:
            current["started_at"] = now

        for key, value in kwargs.items():
            current[key] = value

        timeline = current.get("timeline", [])
        timeline.append({"status": status, "timestamp": now})
        current["timeline"] = timeline

        reg_key = RedisKeys.session_registry(session_id)
        pipe = self.redis.pipeline()
        pipe.hset(
            reg_key,
            f"{EXEC_FIELD_PREFIX}{execution_id}",
            json.dumps(current, ensure_ascii=False),
        )
        pipe.expire(reg_key, RedisKeys.DEFAULT_SESSION_TTL)
        await pipe.execute()
        await self._update_worker_execution_stats(old_execution, current, now)

    async def update_execution_status_by_message(
        self, message_id: str, session_id: str, status: str
    ):
        """Update specific execution status by message_id and append to timeline"""
        execution = await self.get_execution_by_message_id(message_id, session_id)
        if not execution:
            return
        await self.update_execution_status(
            execution["execution_id"], session_id, status
        )

    async def update_execution_fields(
        self, execution_id: str, session_id: str, **kwargs: Any
    ) -> None:
        """Update execution metadata fields without changing status or timeline."""
        current = await self.get_execution(execution_id, session_id)
        if current is None:
            return

        old_execution = dict(current)
        current.update(kwargs)
        now = int(time.time() * 1000)
        current["updated_at"] = now

        reg_key = RedisKeys.session_registry(session_id)
        pipe = self.redis.pipeline()
        pipe.hset(
            reg_key,
            f"{EXEC_FIELD_PREFIX}{execution_id}",
            json.dumps(current, ensure_ascii=False),
        )
        pipe.expire(reg_key, RedisKeys.DEFAULT_SESSION_TTL)
        await pipe.execute()
        await self._update_worker_execution_stats(old_execution, current, now)

    async def save_execution(self, execution: dict[str, Any]):
        """(Compatibility) Save execution data to Redis.

        Recommend using initialize_execution first.

        Args:
            execution: Execution info dict containing execution_id,
                message_id, session_id, etc.
        """
        now = int(time.time() * 1000)
        if "created_at" not in execution or execution["created_at"] == 0:
            execution["created_at"] = now
        if "updated_at" not in execution or execution["updated_at"] == 0:
            execution["updated_at"] = now

        if "timeline" not in execution:
            execution["timeline"] = [
                {"status": execution.get("status", "RUNNING"), "timestamp": now}
            ]

        execution_id = execution["execution_id"]
        message_id = execution["message_id"]
        session_id = execution["session_id"]
        old_execution = await self.get_execution(execution_id, session_id)

        reg_key = RedisKeys.session_registry(session_id)
        encoded_data = json.dumps(execution, ensure_ascii=False)

        # Use Pipeline to ensure atomicity and set TTL
        pipe = self.redis.pipeline()
        pipe.hset(reg_key, f"{EXEC_FIELD_PREFIX}{execution_id}", encoded_data)
        pipe.hset(reg_key, f"{MSG_MAP_PREFIX}{message_id}", execution_id)
        pipe.expire(reg_key, RedisKeys.DEFAULT_SESSION_TTL)
        await pipe.execute()
        await self._update_worker_execution_stats(old_execution, execution, now)

    async def get_execution(
        self, execution_id: str, session_id: str = ""
    ) -> Optional[dict[str, Any]]:
        """
        Get execution details.

        Note: In the new architecture, callers should provide session_id to
        optimize query performance. If session_id is not provided, global
        search is needed (which is not recommended).
        """
        if not session_id:
            logger.warning(
                "get_execution called without session_id, this is inefficient in the "
                "new registry architecture."
            )
            # Compatibility logic: if session_id is truly unavailable, may need to
            # scan all or return error
            return None

        reg_key = RedisKeys.session_registry(session_id)
        data = await self.redis.hget(reg_key, f"{EXEC_FIELD_PREFIX}{execution_id}")
        if not data:
            return None

        if isinstance(data, bytes):
            data = data.decode("utf-8")

        try:
            return json.loads(data)
        except json.JSONDecodeError as err:
            raise ExecutionDataError(execution_id, cause=err) from err

    async def get_execution_by_message_id(
        self, message_id: str, session_id: str = ""
    ) -> Optional[dict[str, Any]]:
        """
        Get execution details by message_id.
        """
        if not session_id:
            # In some flows (like cancellation), only message_id may be available.
            # To support this, we maintain session_id passing on the GatewayClient side.
            return None

        reg_key = RedisKeys.session_registry(session_id)
        execution_id = await self.redis.hget(reg_key, f"{MSG_MAP_PREFIX}{message_id}")
        if isinstance(execution_id, bytes):
            execution_id = execution_id.decode("utf-8")

        if not execution_id:
            return None
        return await self.get_execution(execution_id, session_id)

    async def mark_execution_cancelling(
        self, execution_id: str, session_id: str, reason: str
    ):
        """Mark execution status as CANCELLING.

        Args:
            execution_id: Execution ID
            session_id: Session ID
            reason: Cancellation reason
        """
        current = await self.get_execution(execution_id, session_id)
        if current is None:
            return

        old_execution = dict(current)
        current["status"] = "CANCELLING"
        current["cancel_requested"] = True
        current["cancel_reason"] = reason
        now = int(time.time() * 1000)
        current["updated_at"] = now

        timeline = current.get("timeline", [])
        timeline.append({"status": "CANCELLING", "timestamp": now})
        current["timeline"] = timeline

        reg_key = RedisKeys.session_registry(session_id)
        pipe = self.redis.pipeline()
        pipe.hset(
            reg_key,
            f"{EXEC_FIELD_PREFIX}{execution_id}",
            json.dumps(current, ensure_ascii=False),
        )
        pipe.expire(reg_key, RedisKeys.DEFAULT_SESSION_TTL)
        await pipe.execute()
        await self._update_worker_execution_stats(old_execution, current, now)

    async def mark_cancel_requested(
        self, execution_id: str, session_id: str, reason: str = ""
    ):
        """Only mark the cancel_requested flag, do not change execution status.

        Used in cascade cancellation scenarios to mark completed (COMPLETED)
        parent nodes, so that cancelled child agents know they don't need to
        callback and wake up the parent.

        Args:
            execution_id: Execution ID
            session_id: Session ID
            reason: Cancellation reason
        """
        current = await self.get_execution(execution_id, session_id)
        if current is None:
            return

        old_execution = dict(current)
        current["cancel_requested"] = True
        if reason:
            current["cancel_reason"] = reason
        current["updated_at"] = int(time.time() * 1000)

        reg_key = RedisKeys.session_registry(session_id)
        pipe = self.redis.pipeline()
        pipe.hset(
            reg_key,
            f"{EXEC_FIELD_PREFIX}{execution_id}",
            json.dumps(current, ensure_ascii=False),
        )
        pipe.expire(reg_key, RedisKeys.DEFAULT_SESSION_TTL)
        await pipe.execute()
        await self._update_worker_execution_stats(
            old_execution, current, current["updated_at"]
        )

    async def mark_execution_finished(
        self,
        execution_id: str,
        session_id: str,
        status: str,
        completion: Optional[ExecutionCompletionFields] = None,
    ):
        """Mark execution as finished status.

        Args:
            execution_id: Execution ID
            session_id: Session ID
            status: Final status
            completion: Optional structured terminal metadata for observability.
        """
        current = await self.get_execution(execution_id, session_id)
        if current is None:
            return

        old_execution = dict(current)
        current["status"] = status
        now = int(time.time() * 1000)
        current["finished_at"] = now
        current["updated_at"] = now
        if completion:
            for key, value in completion.items():
                current[key] = value

        timeline = current.get("timeline", [])
        timeline.append({"status": status, "timestamp": now})
        current["timeline"] = timeline

        reg_key = RedisKeys.session_registry(session_id)
        pipe = self.redis.pipeline()
        pipe.hset(
            reg_key,
            f"{EXEC_FIELD_PREFIX}{execution_id}",
            json.dumps(current, ensure_ascii=False),
        )
        pipe.expire(reg_key, RedisKeys.DEFAULT_SESSION_TTL)
        await pipe.execute()
        await self._update_worker_execution_stats(old_execution, current, now)

        snapshot_key = current.get("agent_configs_snapshot_key", "")
        if snapshot_key and is_terminal_state(status):
            try:
                await self.delete_agent_configs_snapshot(snapshot_key)
                current["agent_configs_snapshot_cleanup_status"] = "deleted"
                current["agent_configs_snapshot_cleaned_at"] = int(time.time() * 1000)
                current["agent_configs_snapshot_cleanup_error"] = ""
            except Exception as err:  # pylint: disable=broad-exception-caught
                current["agent_configs_snapshot_cleanup_status"] = "delete_failed"
                current["agent_configs_snapshot_cleaned_at"] = 0
                current["agent_configs_snapshot_cleanup_error"] = str(err)
                logger.warning(
                    "Failed to delete persisted agent config snapshot: "
                    "execution_id=%s session_id=%s snapshot_key=%s error=%s",
                    execution_id,
                    session_id,
                    snapshot_key,
                    err,
                    **observability_log_extra(
                        execution_id=execution_id,
                        session_id=session_id,
                    ),
                )
            finally:
                current["updated_at"] = int(time.time() * 1000)
                cleanup_pipe = self.redis.pipeline()
                cleanup_pipe.hset(
                    reg_key,
                    f"{EXEC_FIELD_PREFIX}{execution_id}",
                    json.dumps(current, ensure_ascii=False),
                )
                cleanup_pipe.expire(reg_key, RedisKeys.DEFAULT_SESSION_TTL)
                await cleanup_pipe.execute()

    async def record_failed_route_decision(
        self,
        *,
        execution_id: str,
        message_id: str,
        session_id: str,
        trace_id: str,
        parent_message_id: str,
        source_agent_type: str,
        target_agent_type: str,
        route_policy: str,
        route_status: str,
        stream_name: str,
        selected_agent_type: str,
        availability_error_code: str,
        availability_error: str,
    ) -> None:
        """Persist a failed availability routing decision for the dashboard."""
        try:
            await self.initialize_execution(
                {
                    "execution_id": execution_id,
                    "message_id": message_id,
                    "session_id": session_id,
                    "trace_id": trace_id,
                    "parent_message_id": parent_message_id,
                    "source_agent_type": source_agent_type,
                    "target_agent_type": target_agent_type,
                    "stream_name": stream_name,
                    "status": "FAILED",
                    "route_policy": route_policy,
                    "route_status": route_status,
                    "selected_agent_type": selected_agent_type,
                    "availability_error_code": availability_error_code,
                    "availability_error": availability_error,
                    "error_code": availability_error_code,
                    "error_message": availability_error,
                    "failed_stage": "availability",
                }
            )
        except Exception:  # pylint: disable=broad-exception-caught
            pass

    async def get_all_session_executions(self, session_id: str) -> list[dict[str, Any]]:
        """Get all execution records under the specified Session.

        Used in cascade cancellation scenarios, fetches the entire Session's
        registry data through HGETALL at once, filters out all entries with
        exec: prefix and deserializes them.

        Args:
            session_id: Session ID

        Returns:
            List of all execution records under this session
        """
        reg_key = RedisKeys.session_registry(session_id)
        all_data = await self.redis.hgetall(reg_key)
        executions = []
        for field, value in all_data.items():
            field_str = field.decode() if isinstance(field, bytes) else field
            if not field_str.startswith(EXEC_FIELD_PREFIX):
                continue
            value_str = value.decode("utf-8") if isinstance(value, bytes) else value
            try:
                executions.append(json.loads(value_str))
            except json.JSONDecodeError:
                continue
        return executions

    async def get_worker_executions(
        self,
        worker_id: str,
        *,
        include_terminal: bool = True,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Get recent lightweight execution snapshots assigned to a Worker."""
        if limit <= 0:
            return []

        execution_ids = await self.redis.zrevrange(
            RedisKeys.worker_executions(worker_id), 0, limit - 1
        )
        return await self._get_worker_snapshots(
            RedisKeys.worker_history_snapshots(worker_id),
            execution_ids,
            include_terminal=include_terminal,
        )

    async def _get_worker_snapshots(
        self,
        snapshots_key: str,
        raw_execution_ids: list[Any],
        *,
        include_terminal: bool = True,
    ) -> list[dict[str, Any]]:
        """Fetch worker execution snapshots in batch and preserve ID order."""
        execution_ids = [
            raw_execution_id.decode("utf-8")
            if isinstance(raw_execution_id, bytes)
            else str(raw_execution_id)
            for raw_execution_id in raw_execution_ids
        ]
        if not execution_ids:
            return []

        raw_snapshots = await self.redis.hmget(snapshots_key, execution_ids)
        snapshots: list[dict[str, Any]] = []
        for raw_snapshot in raw_snapshots:
            if not raw_snapshot:
                continue
            snapshot_data = (
                raw_snapshot.decode("utf-8")
                if isinstance(raw_snapshot, bytes)
                else str(raw_snapshot)
            )
            try:
                snapshot = json.loads(snapshot_data)
            except json.JSONDecodeError:
                continue
            status = str(snapshot.get("status") or "")
            if not include_terminal and is_terminal_state(status):
                continue
            snapshots.append(snapshot)

        return snapshots

    @staticmethod
    def _get_int_hash_value(data: dict[Any, Any], key: str) -> int:
        value = data.get(key)
        if value is None:
            value = data.get(key.encode("utf-8"))
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        return int(value or 0)

    async def get_worker_execution_summary(
        self,
        worker_id: str,
        *,
        active_limit: int = 100,
        history_limit: int = 20,
        limit: Optional[int] = None,
        workers: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Get worker liveness metadata and recent execution state summary."""
        if limit is not None:
            history_limit = limit

        workers = workers if workers is not None else await self.get_all_workers()
        worker_info = workers.get(worker_id, {})
        raw_counts = await self.redis.hgetall(RedisKeys.worker_status(worker_id))
        counts = {
            "total": self._get_int_hash_value(raw_counts, "total_count"),
            "active": self._get_int_hash_value(raw_counts, "active_count"),
            "queued": self._get_int_hash_value(raw_counts, "queued_count"),
            "running": self._get_int_hash_value(raw_counts, "running_count"),
            "cancelling": self._get_int_hash_value(raw_counts, "cancelling_count"),
            "completed": self._get_int_hash_value(raw_counts, "completed_count"),
            "failed": self._get_int_hash_value(raw_counts, "failed_count"),
            "cancelled": self._get_int_hash_value(raw_counts, "cancelled_count"),
        }

        active_executions = []
        if active_limit > 0:
            active_ids = await self.redis.zrevrange(
                RedisKeys.worker_active_execution_index(worker_id), 0, active_limit - 1
            )
            active_executions = await self._get_worker_snapshots(
                RedisKeys.worker_active_snapshots(worker_id), active_ids
            )
        recent_executions = await self.get_worker_executions(
            worker_id, limit=history_limit
        )
        status_counts = {
            "QUEUED": counts["queued"],
            "RUNNING": counts["running"],
            "CANCELLING": counts["cancelling"],
            "COMPLETED": counts["completed"],
            "FAILED": counts["failed"],
            "CANCELLED": counts["cancelled"],
        }
        status_counts = {key: value for key, value in status_counts.items() if value}

        return {
            "worker_id": worker_id,
            "online": worker_id in workers,
            "agent_types": sorted(worker_info.get("agent_types", [])),
            "last_seen": int(worker_info.get("last_seen", 0)),
            "ip_address": worker_info.get("ip_address", ""),
            "counts": counts,
            "active_count": counts["active"],
            "total_tracked": counts["total"],
            "last_updated_at": self._get_int_hash_value(raw_counts, "last_updated_at"),
            "last_started_at": self._get_int_hash_value(raw_counts, "last_started_at"),
            "last_finished_at": self._get_int_hash_value(
                raw_counts, "last_finished_at"
            ),
            "status_counts": status_counts,
            "active_executions": active_executions,
            "recent_executions": recent_executions,
        }

    def _encode_execution(self, execution: dict[str, Any]) -> dict[str, str]:  # pylint: disable=unused-argument
        # Deprecated, since we switched to JSON storage
        return {}

    def _decode_execution(self, execution: dict[str, Any]) -> dict[str, Any]:  # pylint: disable=unused-argument
        # Deprecated, since we switched to JSON storage
        return {}

    # --- Admin lifecycle state ---

    async def set_worker_admin_state(
        self,
        worker_id: str,
        lifecycle: str,
        reason: str = "",
    ) -> None:
        """Persist admin-controlled lifecycle state for a worker.

        Args:
            worker_id: Target worker ID.
            lifecycle: One of "active", "suspended", "evicted".
            reason: Human-readable reason for the state change.
        """
        now = int(time.time() * 1000)
        key = RedisKeys.worker_admin(worker_id)
        pipe = self.redis.pipeline()
        pipe.hset(key, "lifecycle", lifecycle)
        pipe.hset(key, "reason", reason)
        pipe.hset(key, "updated_at", now)
        pipe.sadd(RedisKeys.ADMIN_WORKERS, worker_id)
        await pipe.execute()

    async def get_worker_admin_state(self, worker_id: str) -> dict[str, Any]:
        """Return the admin-controlled state for a worker.

        Returns an empty dict when no admin state has been set (implying
        the worker is in the default "active" state).
        """
        raw = await self.redis.hgetall(RedisKeys.worker_admin(worker_id))
        if not raw:
            return {}
        result: dict[str, Any] = {}
        for field, value in raw.items():
            field_str = field.decode() if isinstance(field, bytes) else str(field)
            value_str = value.decode() if isinstance(value, bytes) else str(value)
            result[field_str] = value_str
        if "updated_at" in result:
            try:
                result["updated_at"] = int(result["updated_at"])
            except (ValueError, TypeError):
                pass
        return result

    async def clear_worker_admin_state(self, worker_id: str) -> None:
        """Remove the admin lifecycle key, restoring default-active behaviour."""
        pipe = self.redis.pipeline()
        pipe.delete(RedisKeys.worker_admin(worker_id))
        pipe.srem(RedisKeys.ADMIN_WORKERS, worker_id)
        await pipe.execute()

    async def remove_worker_from_type_members(self, worker_id: str) -> None:
        """SREM worker_id from every agent_type:members set it currently belongs to.

        Preserves the declared-agent-types key so membership can be restored later.
        Used by suspend and evict to make the worker immediately invisible to routing.
        """
        agent_types_raw = await self.redis.smembers(
            RedisKeys.worker_declared_agent_types(worker_id)
        )
        for raw in agent_types_raw:
            agent_type = raw.decode() if isinstance(raw, bytes) else raw
            await self.redis.srem(RedisKeys.agent_type_members(agent_type), worker_id)

    async def restore_worker_to_type_members(self, worker_id: str) -> None:
        """SADD worker_id back to every agent_type:members set it declared.

        Used by resume to make the worker immediately visible to routing again.
        Denylist is still respected by register_worker_membership on the next
        heartbeat cycle, so denied types are re-excluded automatically.
        """
        agent_types_raw = await self.redis.smembers(
            RedisKeys.worker_declared_agent_types(worker_id)
        )
        for raw in agent_types_raw:
            agent_type = raw.decode() if isinstance(raw, bytes) else raw
            denied = await self.is_worker_denied_for_type(agent_type, worker_id)
            if not denied:
                await self.redis.sadd(
                    RedisKeys.agent_type_members(agent_type), worker_id
                )

    # --- Agent-type denylist ---

    async def deny_worker_for_type(self, agent_type: str, worker_id: str) -> None:
        """Add worker_id to the denylist for agent_type.

        The worker will stop being added to agent_type:workers on its next
        membership refresh and will skip XREADGROUP for that stream.
        """
        pipe = self.redis.pipeline()
        pipe.sadd(RedisKeys.agent_type_denied(agent_type), worker_id)
        pipe.srem(RedisKeys.agent_type_members(agent_type), worker_id)
        await pipe.execute()

    async def allow_worker_for_type(self, agent_type: str, worker_id: str) -> None:
        """Remove worker_id from the denylist for agent_type."""
        await self.redis.srem(RedisKeys.agent_type_denied(agent_type), worker_id)

    async def is_worker_denied_for_type(self, agent_type: str, worker_id: str) -> bool:
        """Return True if worker_id is on the denylist for agent_type."""
        return bool(
            await self.redis.sismember(
                RedisKeys.agent_type_denied(agent_type), worker_id
            )
        )

    async def get_agent_type_denylist(self, agent_type: str) -> list[str]:
        """Return all worker_ids on the denylist for agent_type."""
        raw = await self.redis.smembers(RedisKeys.agent_type_denied(agent_type))
        return [m.decode() if isinstance(m, bytes) else m for m in raw]
