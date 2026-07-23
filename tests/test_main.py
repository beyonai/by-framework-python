"""Unit tests for the `python -m by_framework` CLI entry point."""

from unittest.mock import MagicMock, patch

from by_framework.__main__ import _parse_cluster_nodes, main, parse_args


def test_parse_args_redis_fields_default_to_none():
    """Every Redis-related flag must default to None (not a literal like
    "localhost"), otherwise an unset flag silently shadows the
    corresponding REDIS_* env var instead of falling through to it - see
    docs/architecture/production-deployment.md gap 2."""
    with patch(
        "sys.argv",
        ["by_framework", "--worker-class", "my_agent.MyAgent"],
    ):
        args = parse_args()

    assert args.redis_host is None
    assert args.redis_port is None
    assert args.redis_db is None
    assert args.redis_password is None
    assert args.redis_username is None
    assert args.redis_mode is None
    assert args.redis_cluster_nodes is None


def test_parse_args_accepts_all_redis_flags():
    with patch(
        "sys.argv",
        [
            "by_framework",
            "--worker-class",
            "my_agent.MyAgent",
            "--redis-host",
            "redis.internal",
            "--redis-port",
            "6380",
            "--redis-db",
            "2",
            "--redis-password",
            "secret",
            "--redis-username",
            "svc-worker",
            "--redis-mode",
            "cluster",
            "--redis-cluster-nodes",
            "h1:6379,h2:6380",
        ],
    ):
        args = parse_args()

    assert args.redis_host == "redis.internal"
    assert args.redis_port == 6380
    assert args.redis_db == 2
    assert args.redis_password == "secret"
    assert args.redis_username == "svc-worker"
    assert args.redis_mode == "cluster"
    assert args.redis_cluster_nodes == "h1:6379,h2:6380"


def test_parse_cluster_nodes_parses_host_port_pairs():
    assert _parse_cluster_nodes("h1:6379,h2:6380") == [("h1", 6379), ("h2", 6380)]


def test_main_passes_parsed_redis_args_through_to_run_worker():
    with (
        patch(
            "sys.argv",
            [
                "by_framework",
                "--worker-class",
                "my_agent.MyAgent",
                "--redis-password",
                "secret",
                "--redis-cluster-nodes",
                "h1:6379,h2:6380",
            ],
        ),
        patch("by_framework.__main__.get_class_from_string") as mock_get_class,
        patch("by_framework.__main__.run_worker") as mock_run_worker,
    ):
        mock_worker_class = MagicMock()
        mock_worker_class.__name__ = "MyAgent"
        mock_get_class.return_value = mock_worker_class

        main()

    mock_run_worker.assert_called_once()
    _, kwargs = mock_run_worker.call_args
    assert kwargs["redis_host"] is None
    assert kwargs["redis_password"] == "secret"
    assert kwargs["redis_cluster_nodes"] == [("h1", 6379), ("h2", 6380)]
    # redis_mode wasn't passed - cluster mode must still be inferable from
    # redis_cluster_nodes alone downstream in run_worker(), so main() must
    # not default this to "standalone" itself.
    assert kwargs["redis_mode"] is None


def test_main_leaves_cluster_nodes_none_when_flag_not_passed():
    with (
        patch("sys.argv", ["by_framework", "--worker-class", "my_agent.MyAgent"]),
        patch("by_framework.__main__.get_class_from_string") as mock_get_class,
        patch("by_framework.__main__.run_worker") as mock_run_worker,
    ):
        mock_worker_class = MagicMock()
        mock_worker_class.__name__ = "MyAgent"
        mock_get_class.return_value = mock_worker_class

        main()

    _, kwargs = mock_run_worker.call_args
    assert kwargs["redis_cluster_nodes"] is None
