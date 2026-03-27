"""
Gateway SDK CLI entry point.

Allows running a Worker class from the command line with Redis configuration.
"""

#!/usr/bin/env python3
import argparse
import importlib

from .worker.app import run_worker


def parse_args():
    """Parse command line arguments for the CLI runner."""
    parser = argparse.ArgumentParser(description="Gateway SDK CLI Runner")
    parser.add_argument(
        "--worker-class",
        type=str,
        required=True,
        help="Full import path of the Worker class to run (e.g., 'examples.workers.MyAgent')",
    )
    parser.add_argument(
        "--redis-host", type=str, default="localhost", help="Redis server hostname"
    )
    parser.add_argument(
        "--redis-port", type=int, default=6379, help="Redis server port"
    )
    parser.add_argument("--redis-db", type=int, default=0, help="Redis database number")
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

    return parser.parse_args()


def get_class_from_string(class_path: str):
    try:
        module_path, class_name = class_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, class_name)
    except Exception as e:
        raise ValueError(f"Could not load worker class '{class_path}': {str(e)}")


def main():
    args = parse_args()

    # 动态加载传入的 Worker 类
    worker_class = get_class_from_string(args.worker_class)

    print(f"Starting worker class: {worker_class.__name__} (ID: {args.worker_id})")

    # 交给公共启动器
    run_worker(
        worker_class=worker_class,
        worker_id=args.worker_id,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db,
        workspace_dir=args.workspace,
        max_concurrency=args.max_concurrency,
        fetch_count=args.fetch_count,
        redis_max_connections=args.redis_max_connections,
    )


if __name__ == "__main__":
    main()
