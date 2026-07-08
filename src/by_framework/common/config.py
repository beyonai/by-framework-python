"""
By-Framework Configuration.

Provides typed configuration loaded from environment variables.
"""

import os
from dataclasses import dataclass, field
from typing import Literal, Optional

from by_framework.common.constants import RedisKeys


@dataclass(frozen=True)
class RedisConfig:
    """Redis connection configuration."""

    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: str = ""
    username: Optional[str] = None
    decode_responses: bool = True
    max_connections: Optional[int] = None
    mode: Literal["standalone", "cluster"] = "standalone"
    cluster_nodes: Optional[list[tuple[str, int]]] = None
    socket_connect_timeout: int = 5
    socket_timeout: int = 10

    @classmethod
    def from_env(cls) -> "RedisConfig":
        """Load Redis configuration from environment variables.

        REDIS_CLUSTER_HOST (comma-separated "host:port" list) is the
        preferred way to configure Cluster mode: just setting it is enough
        to switch to cluster mode, no separate REDIS_MODE=cluster needed.
        An explicit REDIS_MODE still takes precedence when set, so existing
        explicit REDIS_MODE=cluster + REDIS_CLUSTER_NODES setups keep working.
        """
        password = os.environ.get("REDIS_PASSWORD", "")
        username = os.environ.get("REDIS_USERNAME") or None
        max_connections = os.environ.get("REDIS_MAX_CONNECTIONS")
        cluster_host_str = os.environ.get("REDIS_CLUSTER_HOST")
        cluster_nodes_str = cluster_host_str or os.environ.get("REDIS_CLUSTER_NODES")
        mode = os.environ.get("REDIS_MODE") or ("cluster" if cluster_host_str else "standalone")
        cluster_nodes = None
        if cluster_nodes_str:
            cluster_nodes = []
            for node in cluster_nodes_str.split(","):
                node_host, node_port = node.rsplit(":", 1)
                cluster_nodes.append((node_host, int(node_port)))
        return cls(
            host=os.environ.get("REDIS_HOST", "localhost"),
            port=int(os.environ.get("REDIS_PORT", "6379")),
            db=int(os.environ.get("REDIS_DB", "0")),
            password=password,
            username=username,
            max_connections=int(max_connections) if max_connections else None,
            mode=mode,
            cluster_nodes=cluster_nodes,
        )


@dataclass(frozen=True)
class WorkerConfig:
    """Worker runner configuration."""

    max_concurrency: int = 50
    fetch_count: int = 10
    heartbeat_interval_seconds: int = (
        RedisKeys.WORKER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    )
    heartbeat_lease_ttl_seconds: int = RedisKeys.WORKER_DEFAULT_LEASE_TTL_SECONDS
    lock_ttl_seconds: int = 60
    worker_id_claim_max_wait_seconds: int = 90
    worker_id_claim_retry_interval_seconds: float = 3.0
    stream_block_ms: int = 2000


@dataclass(frozen=True)
class LoggingConfig:
    """Logging configuration."""

    level: str = "INFO"
    use_json: bool = False
    log_file: Optional[str] = "by-framework.log"

    @classmethod
    def from_env(cls) -> "LoggingConfig":
        """Load logging configuration from environment variables."""
        level_str = os.environ.get("LOG_LEVEL", "INFO").upper()

        use_json_str = os.environ.get("LOG_USE_JSON", "false").lower()
        use_json = use_json_str in ("true", "1", "yes")

        log_file = os.environ.get("LOG_FILE")
        if log_file == "":
            log_file = None

        return cls(level=level_str, use_json=use_json, log_file=log_file)


@dataclass(frozen=True)
class FrameworkConfig:
    """Main Framework configuration combining all sub-configs."""

    redis: RedisConfig = field(default_factory=RedisConfig)
    worker: WorkerConfig = field(default_factory=WorkerConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    @classmethod
    def from_env(cls) -> "FrameworkConfig":
        """Load complete Framework configuration from environment variables."""
        return cls(
            redis=RedisConfig.from_env(),
            worker=WorkerConfig(),
            logging=LoggingConfig.from_env(),
        )


# Global config instance
_config: Optional[FrameworkConfig] = None


def get_config() -> FrameworkConfig:
    """Get the global Framework configuration, loading from environment if needed."""
    global _config
    if _config is None:
        _config = FrameworkConfig.from_env()
    return _config


def init_config(config: FrameworkConfig) -> None:
    """Initialize the global Framework configuration (for testing or custom config)."""
    global _config
    _config = config
