"""
Gateway SDK Redis Key constants definition.

All Redis Stream names, Hash Keys, Set Keys and other configuration items
are centrally managed in this file. Do not hardcode literal strings in business code.
"""


class RedisKeys:
    """Gateway SDK global Redis Key naming conventions and constants."""

    CONTROL_PLANE_PREFIX = "byai_gateway:control_plane"

    # --- Queues and Streams ---
    @staticmethod
    def ctrl_stream(agent_type: str) -> str:
        """Control stream queue for dispatching tasks to Workers."""
        return f"byai_gateway:ctrl:agent_type:{agent_type}"

    @staticmethod
    def worker_ctrl_stream(worker_id: str) -> str:
        """Worker-specific control queue for directing control commands to worker."""
        return f"byai_gateway:ctrl:worker:{worker_id}"

    @staticmethod
    def plugin_reload_ack_stream(reload_id: str) -> str:
        """Stream for worker ACKs emitted after handling a plugin reload command."""
        return f"byai_gateway:plugin_reload:{reload_id}:ack"

    @classmethod
    def control_plane_wakeup_stream(cls) -> str:
        """Management stream for agent availability wakeup requests."""
        return f"{cls.CONTROL_PLANE_PREFIX}:mgmt:wakeup"

    @classmethod
    def control_plane_wakeup_result_stream(cls, execution_id: str) -> str:
        """Management stream for wakeup controller decisions."""
        return f"{cls.CONTROL_PLANE_PREFIX}:mgmt:wakeup:result:{execution_id}"

    @classmethod
    def control_plane_delivery_pending_stream(cls) -> str:
        """Management stream for pending control-message delivery."""
        return f"{cls.CONTROL_PLANE_PREFIX}:mgmt:delivery:pending"

    @classmethod
    def control_plane_deadletter_stream(cls) -> str:
        """Management stream for failed control-plane work."""
        return f"{cls.CONTROL_PLANE_PREFIX}:mgmt:deadletter"

    @classmethod
    def control_plane_agent_availability(cls, agent_type: str) -> str:
        """Availability state key for an agent type."""
        return f"{cls.CONTROL_PLANE_PREFIX}:availability:agent_type:{agent_type}"

    @classmethod
    def control_plane_agent_circuit(cls, agent_type: str) -> str:
        """Circuit-breaker state key for an agent type."""
        return f"{cls.CONTROL_PLANE_PREFIX}:circuit:agent_type:{agent_type}"

    @classmethod
    def control_plane_agent_fallback(cls, agent_type: str) -> str:
        """Fallback routing state key for an agent type."""
        return f"{cls.CONTROL_PLANE_PREFIX}:fallback:agent_type:{agent_type}"

    @classmethod
    def control_plane_user_quota(cls, user_code: str) -> str:
        """User quota state key for control-plane scheduling."""
        return f"{cls.CONTROL_PLANE_PREFIX}:quota:user:{user_code}"

    @classmethod
    def control_plane_tenant_quota(cls, tenant_id: str) -> str:
        """Backward-compatible alias for user-code based quota state."""
        return cls.control_plane_user_quota(tenant_id)

    @classmethod
    def control_plane_wakeup_dedupe(
        cls, agent_type: str, user_code: str, region: str
    ) -> str:
        """Dedupe key for concurrent wakeup requests."""
        return (
            f"{cls.CONTROL_PLANE_PREFIX}:wakeup:dedupe:"
            f"{agent_type}:{user_code}:{region}"
        )

    @staticmethod
    def agent_configs_snapshot(snapshot_key: str) -> str:
        """Blob key for a persisted AgentConfigsSnapshot payload."""
        return f"byai_gateway:agent_configs_snapshot:{snapshot_key}"

    @staticmethod
    def session_data_stream(session_id: str) -> str:
        """Session-level data stream. Workers push streaming content here."""
        return f"byai_gateway:session:{session_id}:data_stream"

    @staticmethod
    def session_data_checkpoint(session_id: str, consumer_name: str) -> str:
        """Checkpoint key storing a consumer's last processed data stream ID."""
        return f"byai_gateway:session:{session_id}:consumer:{consumer_name}:checkpoint"

    @staticmethod
    def trace_meta(trace_id: str) -> str:
        """Hash storing trace-level metadata for observability."""
        return f"by_framework:trace:{trace_id}"

    @staticmethod
    def trace_spans(trace_id: str) -> str:
        """List storing trace span JSON payloads ordered by write time."""
        return f"by_framework:trace:spans:{trace_id}"

    @staticmethod
    def trace_index_session(session_id: str) -> str:
        """Sorted Set index from session_id to trace IDs."""
        return f"by_framework:trace:idx:session:{session_id}"

    @staticmethod
    def trace_index_worker(worker_id: str) -> str:
        """Sorted Set index from worker_id to trace IDs."""
        return f"by_framework:trace:idx:worker:{worker_id}"

    @staticmethod
    def trace_index_agent(agent_type: str) -> str:
        """Sorted Set index from agent type to trace IDs."""
        return f"by_framework:trace:idx:agent:{agent_type}"

    @staticmethod
    def task_group(group_id: str) -> str:
        """Task group progress tracking Hash Key."""
        return f"byai_gateway:task_group:{group_id}"

    @staticmethod
    def task_group_results(group_id: str) -> str:
        """All subtask results Hash Key for a task group."""
        return f"byai_gateway:task_group:{group_id}:results"

    # --- Registry ---
    # Set of known workers used for registry enumeration.
    KNOWN_WORKERS = "byai_gateway:registry:workers"
    WORKER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 5
    WORKER_DEFAULT_LEASE_TTL_SECONDS = 15

    # Default TTL (7 days) for cleaning up session-related aggregate Keys
    DEFAULT_SESSION_TTL = 7 * 24 * 3600
    AGENT_CONFIGS_SNAPSHOT_TTL_SECONDS = DEFAULT_SESSION_TTL

    @staticmethod
    def worker_declared_agent_types(worker_id: str) -> str:
        """Set Key storing all agent type identifiers supported by a Worker."""
        return f"byai_gateway:registry:worker:agent_types:{worker_id}"

    @staticmethod
    def agent_type_members(agent_type: str) -> str:
        """Set Key storing all Worker IDs with a specific agent type."""
        return f"byai_gateway:registry:agent_type:workers:{agent_type}"

    @staticmethod
    def worker_lock(worker_id: str) -> str:
        """Worker startup mutex lock to prevent duplicate worker_id startup."""
        return f"byai_gateway:registry:worker:lock:{worker_id}"

    @staticmethod
    def worker_online_lease(worker_id: str) -> str:
        """Worker online lease Key. Presence means the worker is considered online."""
        return f"byai_gateway:registry:worker:online:{worker_id}"

    @staticmethod
    def worker_status(worker_id: str) -> str:
        """HASH storing aggregate execution counters for a Worker."""
        return f"byai_gateway:registry:worker:status:{worker_id}"

    @staticmethod
    def worker_executions(worker_id: str) -> str:
        """ZSET of execution IDs handled by a Worker, scored by update time."""
        return f"byai_gateway:registry:worker:executions:{worker_id}"

    @staticmethod
    def worker_active_executions(worker_id: str) -> str:
        """Legacy SET of non-terminal execution IDs assigned to a Worker."""
        return f"byai_gateway:registry:worker:active_executions:{worker_id}"

    @staticmethod
    def worker_active_execution_index(worker_id: str) -> str:
        """ZSET of active execution IDs assigned to a Worker, scored by update time."""
        return f"byai_gateway:registry:worker:active_execution_index:{worker_id}"

    @staticmethod
    def worker_active_snapshots(worker_id: str) -> str:
        """HASH mapping active execution IDs to lightweight snapshots."""
        return f"byai_gateway:registry:worker:active_snapshots:{worker_id}"

    @staticmethod
    def worker_history_snapshots(worker_id: str) -> str:
        """HASH mapping worker execution IDs to lightweight history snapshots."""
        return f"byai_gateway:registry:worker:history_snapshots:{worker_id}"

    @staticmethod
    def session_registry(session_id: str) -> str:
        """Session-level aggregate registry (Hash).

        Internally divided into the following Field categories:
        - exec:{execution_id} -> Stores specific execution details JSON
        - msg_map:{message_id} -> Stores message ID to execution ID mapping
        """
        return f"byai_gateway:session:{session_id}:registry"

    # --- Service Discovery ---
    @staticmethod
    def sd_active_instances(service_name: str) -> str:
        """ZSET Key for active service instances (sorted by heartbeat timestamp)."""
        return f"byai_gateway:sd:active:{service_name}"

    @staticmethod
    def sd_instance_details(service_name: str) -> str:
        """HASH Key for service instance metadata."""
        return f"byai_gateway:sd:instances:{service_name}"

    # Set of all known service names (SET)
    SD_SERVICES = "byai_gateway:sd:services"
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
