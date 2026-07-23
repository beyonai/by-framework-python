# pylint: disable=C0103
"""
By-Framework CLI entry point.

Allows running a Worker class from the command line with Redis configuration.
"""

#!/usr/bin/env python3
import argparse
import importlib

from .worker.app import run_worker


def _parse_cluster_nodes(value: str):
    """Parse a comma-separated "host:port,host:port" string, mirroring
    RedisConfig.from_env()'s REDIS_CLUSTER_HOST/REDIS_CLUSTER_NODES parsing
    so the CLI and env-var paths accept the same format."""
    nodes = []
    for node in value.split(","):
        node_host, node_port = node.rsplit(":", 1)
        nodes.append((node_host, int(node_port)))
    return nodes


def parse_args():
    """Parse command line arguments for the CLI runner."""
    parser = argparse.ArgumentParser(description="By-Framework CLI Runner")
    parser.add_argument(
        "--worker-class",
        type=str,
        required=True,
        help=(
            "Full import path of the Worker class to run "
            "(e.g., 'examples.workers.MyAgent')"
        ),
    )
    parser.add_argument(
        "--redis-host",
        type=str,
        default=None,
        help="Redis server hostname (default: REDIS_HOST env var, then 'localhost')",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=None,
        help="Redis server port (default: REDIS_PORT env var, then 6379)",
    )
    parser.add_argument(
        "--redis-db",
        type=int,
        default=None,
        help="Redis database number (default: REDIS_DATABASE env var, then 0)",
    )
    parser.add_argument(
        "--redis-password",
        type=str,
        default=None,
        help="Redis password (default: REDIS_PASSWORD env var)",
    )
    parser.add_argument(
        "--redis-username",
        type=str,
        default=None,
        help="Redis username, for ACL-enabled Redis (default: REDIS_USERNAME env var)",
    )
    parser.add_argument(
        "--redis-mode",
        type=str,
        choices=["standalone", "cluster"],
        default=None,
        help=(
            "Redis deployment mode (default: REDIS_MODE env var; implied by "
            "--redis-cluster-nodes/REDIS_CLUSTER_HOST alone if neither is set)"
        ),
    )
    parser.add_argument(
        "--redis-cluster-nodes",
        type=str,
        default=None,
        help=(
            "Comma-separated Redis Cluster seed nodes, 'host:port,host:port' "
            "(default: REDIS_CLUSTER_HOST/REDIS_CLUSTER_NODES env var). "
            "Passing this alone implies --redis-mode cluster."
        ),
    )
    parser.add_argument(
        "--worker-id", type=str, default="worker-1", help="Unique worker identifier"
    )
    parser.add_argument(
        "--workspace",
        type=str,
        default="/tmp/gateway-workspace",
        help="Worker workspace directory",
    )
    parser.add_argument("--max-concurrency", type=int, help="Maximum concurrent tasks")
    parser.add_argument(
        "--fetch-count", type=int, help="Number of messages to fetch per batch"
    )
    parser.add_argument(
        "--redis-max-connections", type=int, help="Max Redis connections allowed"
    )
    parser.add_argument(
        "--health-port",
        type=int,
        default=None,
        help=(
            "Port for the local /readyz readiness endpoint (Docker/Kubernetes "
            "health checks). Opt-in only - omit to leave it disabled, no "
            "default port is provided. Also settable via BYAI_WORKER_HEALTH_PORT. "
            "See docs/architecture/worker-readiness-endpoint.md."
        ),
    )

    return parser.parse_args()


def get_class_from_string(class_path: str):
    try:
        module_path, class_name = class_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, class_name)
    except Exception as e:
        raise ValueError(f"Could not load worker class '{class_path}': {str(e)}") from e


def main():
    args = parse_args()

    # Dynamically load the provided Worker class
    worker_class = get_class_from_string(args.worker_class)

    print(f"Starting worker class: {worker_class.__name__} (ID: {args.worker_id})")

    redis_cluster_nodes = (
        _parse_cluster_nodes(args.redis_cluster_nodes)
        if args.redis_cluster_nodes
        else None
    )

    # Delegate to the common launcher. Every redis_* arg defaults to None
    # here (not a literal like "localhost"), same as run_worker()'s own
    # defaults, so an unset CLI flag actually falls through to the
    # corresponding REDIS_* env var instead of silently shadowing it.
    run_worker(
        worker_class=worker_class,
        worker_id=args.worker_id,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db,
        redis_password=args.redis_password,
        redis_username=args.redis_username,
        redis_mode=args.redis_mode,
        redis_cluster_nodes=redis_cluster_nodes,
        workspace_dir=args.workspace,
        max_concurrency=args.max_concurrency,
        fetch_count=args.fetch_count,
        redis_max_connections=args.redis_max_connections,
        health_port=args.health_port,
    )


if __name__ == "__main__":
    main()
