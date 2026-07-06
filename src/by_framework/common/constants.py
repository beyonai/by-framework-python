"""
Gateway SDK Redis Key constants definition.

All Redis Stream names, Hash Keys, Set Keys and other configuration items
are centrally managed in this file. Do not hardcode literal strings in business code.
"""

import os


def get_key_schema_version() -> str:
    """Return the configured Redis key schema version ("v1" or "v2").

    Controlled by REDIS_KEY_SCHEMA_VERSION, defaulting to "v1" (the current
    unprefixed key format). Cluster mode requires "v2" (see redis_client.init_redis).
    """
    version = os.environ.get("REDIS_KEY_SCHEMA_VERSION", "v1")
    if version not in ("v1", "v2"):
        raise ValueError(
            f"Invalid REDIS_KEY_SCHEMA_VERSION: {version!r} (must be 'v1' or 'v2')"
        )
    return version


class RedisKeys:
    """Gateway SDK global Redis Key naming conventions and constants."""

    CONTROL_PLANE_PREFIX = "byai_gateway:control_plane"
    CONTROL_PLANE_SUFFIX = "control_plane"
    V2_PREFIX = "byai_gateway:v2:"

    @classmethod
    def _versioned(cls, v1_key: str, v2_suffix: str) -> str:
        """Resolve a key according to REDIS_KEY_SCHEMA_VERSION.

        v1 (default): returns v1_key unchanged, byte-for-byte.
        v2: returns V2_PREFIX + v2_suffix, where v2_suffix already encodes
        any Cluster hash tag needed for same-entity key groups.

        Every RedisKeys factory method routes through this one function so
        the v1/v2 decision lives in exactly one place.
        """
        if get_key_schema_version() == "v2":
            return f"{cls.V2_PREFIX}{v2_suffix}"
        return v1_key

    # --- Queues and Streams ---
    @classmethod
    def ctrl_stream(cls, agent_type: str) -> str:
        """Control stream queue for dispatching tasks to Workers.

        Single-key: agent_type is the only variable dimension. Multiple
        ctrl_stream calls are fanned out at the application layer (see
        Phase 4's two-phase XREADGROUP split), not combined into one slot.
        """
        return cls._versioned(
            v1_key=f"byai_gateway:ctrl:agent_type:{agent_type}",
            v2_suffix=f"ctrl:agent_type:{agent_type}",
        )

    @classmethod
    def worker_ctrl_stream(cls, worker_id: str) -> str:
        """Worker-specific control queue for directing control commands to worker."""
        return cls._versioned(
            v1_key=f"byai_gateway:ctrl:worker:{worker_id}",
            v2_suffix=f"ctrl:worker:{{{worker_id}}}",
        )

    @classmethod
    def plugin_reload_ack_stream(cls, reload_id: str) -> str:
        """Stream for worker ACKs emitted after handling a plugin reload command."""
        return cls._versioned(
            v1_key=f"byai_gateway:plugin_reload:{reload_id}:ack",
            v2_suffix=f"plugin_reload:{reload_id}:ack",
        )

    @classmethod
    def control_plane_wakeup_stream(cls) -> str:
        """Management stream for agent availability wakeup requests."""
        return cls._versioned(
            v1_key=f"{cls.CONTROL_PLANE_PREFIX}:mgmt:wakeup",
            v2_suffix=f"{cls.CONTROL_PLANE_SUFFIX}:mgmt:wakeup",
        )

    @classmethod
    def control_plane_wakeup_result_stream(cls, execution_id: str) -> str:
        """Management stream for wakeup controller decisions."""
        return cls._versioned(
            v1_key=f"{cls.CONTROL_PLANE_PREFIX}:mgmt:wakeup:result:{execution_id}",
            v2_suffix=f"{cls.CONTROL_PLANE_SUFFIX}:mgmt:wakeup:result:{execution_id}",
        )

    @classmethod
    def control_plane_delivery_pending_stream(cls) -> str:
        """Management stream for pending control-message delivery."""
        return cls._versioned(
            v1_key=f"{cls.CONTROL_PLANE_PREFIX}:mgmt:delivery:pending",
            v2_suffix=f"{cls.CONTROL_PLANE_SUFFIX}:mgmt:delivery:pending",
        )

    @classmethod
    def control_plane_deadletter_stream(cls) -> str:
        """Management stream for failed control-plane work."""
        return cls._versioned(
            v1_key=f"{cls.CONTROL_PLANE_PREFIX}:mgmt:deadletter",
            v2_suffix=f"{cls.CONTROL_PLANE_SUFFIX}:mgmt:deadletter",
        )

    @classmethod
    def control_plane_agent_availability(cls, agent_type: str) -> str:
        """Availability state key for an agent type."""
        return cls._versioned(
            v1_key=f"{cls.CONTROL_PLANE_PREFIX}:availability:agent_type:{agent_type}",
            v2_suffix=(
                f"{cls.CONTROL_PLANE_SUFFIX}:availability:agent_type:{agent_type}"
            ),
        )

    @classmethod
    def control_plane_agent_circuit(cls, agent_type: str) -> str:
        """Circuit-breaker state key for an agent type."""
        return cls._versioned(
            v1_key=f"{cls.CONTROL_PLANE_PREFIX}:circuit:agent_type:{agent_type}",
            v2_suffix=f"{cls.CONTROL_PLANE_SUFFIX}:circuit:agent_type:{agent_type}",
        )

    @classmethod
    def control_plane_agent_fallback(cls, agent_type: str) -> str:
        """Fallback routing state key for an agent type."""
        return cls._versioned(
            v1_key=f"{cls.CONTROL_PLANE_PREFIX}:fallback:agent_type:{agent_type}",
            v2_suffix=f"{cls.CONTROL_PLANE_SUFFIX}:fallback:agent_type:{agent_type}",
        )

    @classmethod
    def control_plane_user_quota(cls, user_code: str) -> str:
        """User quota state key for control-plane scheduling."""
        return cls._versioned(
            v1_key=f"{cls.CONTROL_PLANE_PREFIX}:quota:user:{user_code}",
            v2_suffix=f"{cls.CONTROL_PLANE_SUFFIX}:quota:user:{user_code}",
        )

    @classmethod
    def control_plane_tenant_quota(cls, tenant_id: str) -> str:
        """Backward-compatible alias for user-code based quota state."""
        return cls.control_plane_user_quota(tenant_id)

    @classmethod
    def control_plane_wakeup_dedupe(
        cls, agent_type: str, user_code: str, region: str
    ) -> str:
        """Dedupe key for concurrent wakeup requests."""
        return cls._versioned(
            v1_key=(
                f"{cls.CONTROL_PLANE_PREFIX}:wakeup:dedupe:"
                f"{agent_type}:{user_code}:{region}"
            ),
            v2_suffix=(
                f"{cls.CONTROL_PLANE_SUFFIX}:wakeup:dedupe:"
                f"{agent_type}:{user_code}:{region}"
            ),
        )

    @classmethod
    def agent_configs_snapshot(cls, snapshot_key: str) -> str:
        """Blob key for a persisted AgentConfigsSnapshot payload."""
        return cls._versioned(
            v1_key=f"byai_gateway:agent_configs_snapshot:{snapshot_key}",
            v2_suffix=f"agent_configs_snapshot:{snapshot_key}",
        )

    @classmethod
    def session_data_stream(cls, session_id: str) -> str:
        """Session-level data stream. Workers push streaming content here."""
        return cls._versioned(
            v1_key=f"byai_gateway:session:{session_id}:data_stream",
            v2_suffix=f"session:{{{session_id}}}:data_stream",
        )

    @classmethod
    def session_data_checkpoint(cls, session_id: str, consumer_name: str) -> str:
        """Checkpoint key storing a consumer's last processed data stream ID."""
        return cls._versioned(
            v1_key=(
                f"byai_gateway:session:{session_id}:consumer:"
                f"{consumer_name}:checkpoint"
            ),
            v2_suffix=(f"session:{{{session_id}}}:consumer:{consumer_name}:checkpoint"),
        )

    @classmethod
    def trace_meta(cls, trace_id: str) -> str:
        """Hash storing trace-level metadata for observability.

        v1 keeps Python's historical by_framework:trace:* namespace. v2
        unifies onto the shared byai_gateway:v2:trace:{id} format used by
        all three language SDKs (Python/Java previously shared
        by_framework:trace:*, TS used a different byai_gateway:trace:*
        layout — v2 replaces both with one namespace).
        """
        return cls._versioned(
            v1_key=f"by_framework:trace:{trace_id}",
            v2_suffix=f"trace:{{{trace_id}}}",
        )

    @classmethod
    def trace_spans(cls, trace_id: str) -> str:
        """List storing trace span JSON payloads ordered by write time."""
        return cls._versioned(
            v1_key=f"by_framework:trace:spans:{trace_id}",
            v2_suffix=f"trace:spans:{{{trace_id}}}",
        )

    @classmethod
    def trace_index_session(cls, session_id: str) -> str:
        """Sorted Set index from session_id to trace IDs.

        Cross-entity relative to the trace group (meta/spans) — deliberately
        untagged, see _versioned/module docs on cross-entity splitting.
        """
        return cls._versioned(
            v1_key=f"by_framework:trace:idx:session:{session_id}",
            v2_suffix=f"trace:idx:session:{session_id}",
        )

    @classmethod
    def trace_index_worker(cls, worker_id: str) -> str:
        """Sorted Set index from worker_id to trace IDs. Cross-entity, untagged."""
        return cls._versioned(
            v1_key=f"by_framework:trace:idx:worker:{worker_id}",
            v2_suffix=f"trace:idx:worker:{worker_id}",
        )

    @classmethod
    def trace_index_agent(cls, agent_type: str) -> str:
        """Sorted Set index from agent type to trace IDs. Cross-entity, untagged."""
        return cls._versioned(
            v1_key=f"by_framework:trace:idx:agent:{agent_type}",
            v2_suffix=f"trace:idx:agent:{agent_type}",
        )

    @classmethod
    def task_group(cls, group_id: str) -> str:
        """Task group progress tracking Hash Key."""
        return cls._versioned(
            v1_key=f"byai_gateway:task_group:{group_id}",
            v2_suffix=f"task_group:{{{group_id}}}",
        )

    @classmethod
    def task_group_results(cls, group_id: str) -> str:
        """All subtask results Hash Key for a task group."""
        return cls._versioned(
            v1_key=f"byai_gateway:task_group:{group_id}:results",
            v2_suffix=f"task_group:{{{group_id}}}:results",
        )

    # --- Registry ---
    @classmethod
    def known_workers(cls) -> str:
        """Set of known workers used for registry enumeration. Global index,
        untagged (spans every worker entity), still version-prefixed."""
        return cls._versioned(
            v1_key="byai_gateway:registry:workers",
            v2_suffix="registry:workers",
        )

    @classmethod
    def admin_workers(cls) -> str:
        """Set of workers with explicit admin lifecycle state. Used by
        dashboard management views to include offline suspended/evicted
        workers. Global index, untagged, still version-prefixed."""
        return cls._versioned(
            v1_key="byai_gateway:registry:worker:admin_workers",
            v2_suffix="registry:worker:admin_workers",
        )

    WORKER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 5
    # 6× the heartbeat interval gives enough margin even when the main event
    # loop is briefly occupied by an LLM call.  The dedicated heartbeat thread
    # makes starvation practically impossible, but the larger TTL is a second
    # line of defence and also gives Worker 2 time to claim after a crash.
    WORKER_DEFAULT_LEASE_TTL_SECONDS = 30

    # Default TTL (7 days) for cleaning up session-related aggregate Keys
    DEFAULT_SESSION_TTL = 7 * 24 * 3600
    AGENT_CONFIGS_SNAPSHOT_TTL_SECONDS = DEFAULT_SESSION_TTL

    @classmethod
    def worker_declared_agent_types(cls, worker_id: str) -> str:
        """Set Key storing all agent type identifiers supported by a Worker."""
        return cls._versioned(
            v1_key=f"byai_gateway:registry:worker:agent_types:{worker_id}",
            v2_suffix=f"registry:worker:{{{worker_id}}}:agent_types",
        )

    @classmethod
    def agent_type_members(cls, agent_type: str) -> str:
        """Set Key storing all Worker IDs with a specific agent type."""
        return cls._versioned(
            v1_key=f"byai_gateway:registry:agent_type:workers:{agent_type}",
            v2_suffix=f"registry:agent_type:{{{agent_type}}}:workers",
        )

    @classmethod
    def worker_lock(cls, worker_id: str) -> str:
        """Worker startup mutex lock to prevent duplicate worker_id startup."""
        return cls._versioned(
            v1_key=f"byai_gateway:registry:worker:lock:{worker_id}",
            v2_suffix=f"registry:worker:{{{worker_id}}}:lock",
        )

    @classmethod
    def worker_online_lease(cls, worker_id: str) -> str:
        """Worker online lease Key. Presence means the worker is considered online."""
        return cls._versioned(
            v1_key=f"byai_gateway:registry:worker:online:{worker_id}",
            v2_suffix=f"registry:worker:{{{worker_id}}}:online",
        )

    @classmethod
    def worker_status(cls, worker_id: str) -> str:
        """HASH storing aggregate execution counters for a Worker."""
        return cls._versioned(
            v1_key=f"byai_gateway:registry:worker:status:{worker_id}",
            v2_suffix=f"registry:worker:{{{worker_id}}}:status",
        )

    @classmethod
    def worker_executions(cls, worker_id: str) -> str:
        """ZSET of execution IDs handled by a Worker, scored by update time."""
        return cls._versioned(
            v1_key=f"byai_gateway:registry:worker:executions:{worker_id}",
            v2_suffix=f"registry:worker:{{{worker_id}}}:executions",
        )

    @classmethod
    def worker_active_executions(cls, worker_id: str) -> str:
        """Legacy SET of non-terminal execution IDs assigned to a Worker."""
        return cls._versioned(
            v1_key=f"byai_gateway:registry:worker:active_executions:{worker_id}",
            v2_suffix=f"registry:worker:{{{worker_id}}}:active_executions",
        )

    @classmethod
    def worker_active_execution_index(cls, worker_id: str) -> str:
        """ZSET of active execution IDs assigned to a Worker, scored by update time."""
        return cls._versioned(
            v1_key=f"byai_gateway:registry:worker:active_execution_index:{worker_id}",
            v2_suffix=f"registry:worker:{{{worker_id}}}:active_execution_index",
        )

    @classmethod
    def worker_active_snapshots(cls, worker_id: str) -> str:
        """HASH mapping active execution IDs to lightweight snapshots."""
        return cls._versioned(
            v1_key=f"byai_gateway:registry:worker:active_snapshots:{worker_id}",
            v2_suffix=f"registry:worker:{{{worker_id}}}:active_snapshots",
        )

    @classmethod
    def worker_history_snapshots(cls, worker_id: str) -> str:
        """HASH mapping worker execution IDs to lightweight history snapshots."""
        return cls._versioned(
            v1_key=f"byai_gateway:registry:worker:history_snapshots:{worker_id}",
            v2_suffix=f"registry:worker:{{{worker_id}}}:history_snapshots",
        )

    @classmethod
    def worker_admin(cls, worker_id: str) -> str:
        """HASH storing admin-controlled state for a Worker.

        Fields: lifecycle (active|suspended|evicted), reason, updated_at.
        Written by WorkerManager; read by the worker on heartbeat and startup.
        No TTL — persists until explicitly cleared by an admin action.
        """
        return cls._versioned(
            v1_key=f"byai_gateway:registry:worker:admin:{worker_id}",
            v2_suffix=f"registry:worker:{{{worker_id}}}:admin",
        )

    @classmethod
    def agent_type_denied(cls, agent_type: str) -> str:
        """SET of worker_ids explicitly denied from consuming an agent_type stream.

        Key absent or empty SET means the agent_type is open to all workers.
        Written by WorkerManager; checked by workers before XREADGROUP and
        inside register_worker_membership().
        """
        return cls._versioned(
            v1_key=f"byai_gateway:registry:agent_type:denied:{agent_type}",
            v2_suffix=f"registry:agent_type:{{{agent_type}}}:denied",
        )

    @classmethod
    def session_registry(cls, session_id: str) -> str:
        """Session-level aggregate registry (Hash).

        Internally divided into the following Field categories:
        - exec:{execution_id} -> Stores specific execution details JSON
        - msg_map:{message_id} -> Stores message ID to execution ID mapping
        """
        return cls._versioned(
            v1_key=f"byai_gateway:session:{session_id}:registry",
            v2_suffix=f"session:{{{session_id}}}:registry",
        )

    # --- Service Discovery ---
    @classmethod
    def sd_active_instances(cls, service_name: str) -> str:
        """ZSET Key for active service instances (sorted by heartbeat timestamp)."""
        return cls._versioned(
            v1_key=f"byai_gateway:sd:active:{service_name}",
            v2_suffix=f"sd:{{{service_name}}}:active",
        )

    @classmethod
    def sd_instance_details(cls, service_name: str) -> str:
        """HASH Key for service instance metadata."""
        return cls._versioned(
            v1_key=f"byai_gateway:sd:instances:{service_name}",
            v2_suffix=f"sd:{{{service_name}}}:instances",
        )

    @classmethod
    def sd_services(cls) -> str:
        """Set of all known service names. Global index, untagged, still
        version-prefixed."""
        return cls._versioned(
            v1_key="byai_gateway:sd:services", v2_suffix="sd:services"
        )

    # Default heartbeat send interval (10 seconds)
    SD_DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 10
    # Default service heartbeat threshold (30 seconds)
    SD_DEFAULT_HEALTH_THRESHOLD_MS = 30 * 1000
    # Disable heartbeat-based filtering in discovery.
    SD_NO_HEALTH_CHECK = -1
    # Register a visible service instance without starting recurring heartbeats.
    SD_NO_HEARTBEAT = 0

    # --- Consumer Groups ---
    # Consumer Group used by Gateway Worker to consume control streams
    CG_AGENT_ENGINES = "byai_gateway:consumer_group:agent_engines"


# --- ID Prefix Constants ---
# Used for generating unique IDs, avoid hardcoding in business code
MESSAGE_ID_PREFIX = "msg-"
EXECUTION_ID_PREFIX = "exec-"
TASK_GROUP_ID_PREFIX = "tg-"
CANCEL_MESSAGE_ID_PREFIX = "msg-cancel-"

# --- Redis Hash Field Prefixes ---
# Field prefixes in Session Registry Hash
EXEC_FIELD_PREFIX = "exec:"
MSG_MAP_PREFIX = "msg_map:"


# --- Task Group Hash Fields ---
TASK_GROUP_FIELD_TOTAL = "total"
TASK_GROUP_FIELD_COMPLETED = "completed"
TASK_GROUP_FIELD_SOURCE_AGENT = "source_agent_type"


# --- Timing and Sleep Constants ---
# Control loop sleep interval (seconds)
CONTROL_LOOP_SLEEP_SECONDS = 0.01
# Wait for task completion timeout (seconds)
WAIT_FOR_TASKS_TIMEOUT_SECONDS = 5.0
# Task group Key TTL (seconds), default 1 day
TASK_GROUP_TTL_SECONDS = 86400
# First retry wait time (seconds)
FIRST_RETRY_WAIT_SECONDS = 1.0
# Maximum retry count
MAX_RETRY_COUNT = 3


# --- Filesystem Constants ---
DEFAULT_WORKSPACE_DIR = "/workspace"

# --- Stream Read Markers ---
# Redis XREAD/XREADGROUP uses ">" to read only new messages
STREAM_READ_LAST_ID = ">"
