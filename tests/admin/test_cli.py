"""Unit tests for the by-admin CLI.

All SDK calls are mocked with AsyncMock — no live Redis required.
"""

# pylint: disable=invalid-name  # _mock_init is intentionally unused in many tests

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

from typer.testing import CliRunner

from by_framework.admin import cli
from by_framework.admin.cli import app
from by_framework.common.config import RedisConfig

runner = CliRunner()

# --------------------------------------------------------------------------- #
#  Fixtures / helpers
# --------------------------------------------------------------------------- #

_WORKER_A = {
    "agent_types": ["chat", "embed"],
    "last_seen": 1718432100000,
    "ip_address": "192.168.1.5",
    "lifecycle": "active",
    "lifecycle_reason": "",
}
_WORKER_B = {
    "agent_types": ["chat"],
    "last_seen": 1718432090000,
    "ip_address": "192.168.1.6",
    "lifecycle": "suspended",
    "lifecycle_reason": "maintenance",
}

_ALL_WORKERS = {"worker-a": _WORKER_A, "worker-b": _WORKER_B}


def _mock_redis():
    """Return a MagicMock that satisfies init_redis_from_url."""
    return MagicMock()


def _cluster_env(monkeypatch):
    cli._redis_url = None  # pylint: disable=protected-access
    monkeypatch.setenv("REDIS_MODE", "cluster")
    monkeypatch.setenv("REDIS_CLUSTER_NODES", "h1:6379,h2:6380")
    monkeypatch.setenv("REDIS_USERNAME", "cluster-user")
    monkeypatch.setenv("REDIS_PASSWORD", "cluster-secret")
    monkeypatch.setenv("REDIS_KEY_SCHEMA_VERSION", "v2")
    monkeypatch.delenv("BYAI_REDIS_URL", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)


# --------------------------------------------------------------------------- #
#  worker list
# --------------------------------------------------------------------------- #


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.WorkerRegistry")
def test_worker_list_table(mock_registry_cls, _mock_init):
    mock_registry = MagicMock()
    mock_registry.get_all_workers = AsyncMock(return_value=_ALL_WORKERS)
    mock_registry_cls.return_value = mock_registry

    result = runner.invoke(app, ["worker", "list"])
    assert result.exit_code == 0
    assert "worker-a" in result.output
    assert "worker-b" in result.output
    assert "active" in result.output
    assert "suspended" in result.output


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.WorkerRegistry")
def test_worker_list_json(mock_registry_cls, _mock_init):
    mock_registry = MagicMock()
    mock_registry.get_all_workers = AsyncMock(return_value=_ALL_WORKERS)
    mock_registry_cls.return_value = mock_registry

    result = runner.invoke(app, ["worker", "list", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert len(data) == 2
    worker_ids = {w["worker_id"] for w in data}
    assert worker_ids == {"worker-a", "worker-b"}


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.WorkerRegistry")
def test_worker_list_filter_by_type(mock_registry_cls, _mock_init):
    mock_registry = MagicMock()
    mock_registry.get_all_workers = AsyncMock(return_value=_ALL_WORKERS)
    mock_registry_cls.return_value = mock_registry

    result = runner.invoke(app, ["worker", "list", "--type", "embed"])
    assert result.exit_code == 0
    assert "worker-a" in result.output
    assert "worker-b" not in result.output


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.WorkerRegistry")
def test_worker_list_empty(mock_registry_cls, _mock_init):
    mock_registry = MagicMock()
    mock_registry.get_all_workers = AsyncMock(return_value={})
    mock_registry_cls.return_value = mock_registry

    result = runner.invoke(app, ["worker", "list"])
    assert result.exit_code == 0
    assert "No workers found" in result.output


# --------------------------------------------------------------------------- #
#  worker info
# --------------------------------------------------------------------------- #


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.WorkerRegistry")
def test_worker_info_found(mock_registry_cls, _mock_init):
    mock_registry = MagicMock()
    mock_registry.get_all_workers = AsyncMock(return_value=_ALL_WORKERS)
    mock_registry_cls.return_value = mock_registry

    result = runner.invoke(app, ["worker", "info", "worker-a"])
    assert result.exit_code == 0
    assert "worker-a" in result.output
    assert "active" in result.output


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.WorkerRegistry")
def test_worker_info_not_found(mock_registry_cls, _mock_init):
    mock_registry = MagicMock()
    mock_registry.get_all_workers = AsyncMock(return_value={})
    mock_registry_cls.return_value = mock_registry

    result = runner.invoke(app, ["worker", "info", "nonexistent"])
    assert result.exit_code == 1


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.WorkerRegistry")
def test_worker_info_json(mock_registry_cls, _mock_init):
    mock_registry = MagicMock()
    mock_registry.get_all_workers = AsyncMock(return_value=_ALL_WORKERS)
    mock_registry_cls.return_value = mock_registry

    result = runner.invoke(app, ["worker", "info", "worker-b", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["worker_id"] == "worker-b"
    assert data["lifecycle"] == "suspended"


# --------------------------------------------------------------------------- #
#  worker suspend / resume / evict
# --------------------------------------------------------------------------- #


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.WorkerManager")
def test_worker_suspend(mock_mgr_cls, _mock_init):
    mock_mgr = MagicMock()
    mock_mgr.suspend_worker = AsyncMock()
    mock_mgr_cls.return_value = mock_mgr

    result = runner.invoke(app, ["worker", "suspend", "worker-a"])
    assert result.exit_code == 0
    mock_mgr.suspend_worker.assert_awaited_once_with("worker-a", reason="")
    assert "Suspended" in result.output


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.WorkerManager")
def test_worker_suspend_with_reason(mock_mgr_cls, _mock_init):
    mock_mgr = MagicMock()
    mock_mgr.suspend_worker = AsyncMock()
    mock_mgr_cls.return_value = mock_mgr

    result = runner.invoke(app, ["worker", "suspend", "worker-a", "--reason", "maint"])
    assert result.exit_code == 0
    mock_mgr.suspend_worker.assert_awaited_once_with("worker-a", reason="maint")


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.WorkerManager")
def test_worker_resume(mock_mgr_cls, _mock_init):
    mock_mgr = MagicMock()
    mock_mgr.resume_worker = AsyncMock()
    mock_mgr_cls.return_value = mock_mgr

    result = runner.invoke(app, ["worker", "resume", "worker-a"])
    assert result.exit_code == 0
    mock_mgr.resume_worker.assert_awaited_once_with("worker-a")
    assert "Resumed" in result.output


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.WorkerManager")
def test_worker_evict(mock_mgr_cls, _mock_init):
    mock_mgr = MagicMock()
    mock_mgr.evict_worker = AsyncMock()
    mock_mgr_cls.return_value = mock_mgr

    result = runner.invoke(app, ["worker", "evict", "worker-a"])
    assert result.exit_code == 0
    mock_mgr.evict_worker.assert_awaited_once_with("worker-a", force=False)
    assert "Evicted" in result.output


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.WorkerManager")
def test_worker_evict_force(mock_mgr_cls, _mock_init):
    mock_mgr = MagicMock()
    mock_mgr.evict_worker = AsyncMock()
    mock_mgr_cls.return_value = mock_mgr

    result = runner.invoke(app, ["worker", "evict", "worker-a", "--force"])
    assert result.exit_code == 0
    mock_mgr.evict_worker.assert_awaited_once_with("worker-a", force=True)


# --------------------------------------------------------------------------- #
#  type subcommands
# --------------------------------------------------------------------------- #


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.WorkerManager")
def test_type_denylist_empty(mock_mgr_cls, _mock_init):
    mock_mgr = MagicMock()
    mock_mgr.get_type_denylist = AsyncMock(return_value=[])
    mock_mgr_cls.return_value = mock_mgr

    result = runner.invoke(app, ["type", "denylist", "chat"])
    assert result.exit_code == 0
    assert "No denied workers" in result.output


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.WorkerManager")
def test_type_denylist_non_empty(mock_mgr_cls, _mock_init):
    mock_mgr = MagicMock()
    mock_mgr.get_type_denylist = AsyncMock(return_value=["worker-a", "worker-b"])
    mock_mgr_cls.return_value = mock_mgr

    result = runner.invoke(app, ["type", "denylist", "chat", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "worker-a" in data


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.WorkerManager")
def test_type_deny(mock_mgr_cls, _mock_init):
    mock_mgr = MagicMock()
    mock_mgr.deny_worker_for_type = AsyncMock()
    mock_mgr_cls.return_value = mock_mgr

    result = runner.invoke(app, ["type", "deny", "chat", "worker-a"])
    assert result.exit_code == 0
    mock_mgr.deny_worker_for_type.assert_awaited_once_with("chat", "worker-a")
    assert "Denied" in result.output


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.WorkerManager")
def test_type_allow(mock_mgr_cls, _mock_init):
    mock_mgr = MagicMock()
    mock_mgr.allow_worker_for_type = AsyncMock()
    mock_mgr_cls.return_value = mock_mgr

    result = runner.invoke(app, ["type", "allow", "chat", "worker-a"])
    assert result.exit_code == 0
    mock_mgr.allow_worker_for_type.assert_awaited_once_with("chat", "worker-a")
    assert "Allowed" in result.output


# --------------------------------------------------------------------------- #
#  metrics subcommands
# --------------------------------------------------------------------------- #

_SNAPSHOT = {
    "generated_at": 1718432100000,
    "totals": {"workers_online": 2, "agent_types": 1, "active_executions": 3},
    "status_counts": {},
    "queue_depth_total": 7,
}

_HISTORY_POINTS = [
    {
        "generated_at": 1718432000000,
        "workers_online": 2,
        "active_executions": 1,
        "queue_depth_total": 4,
    },
    {
        "generated_at": 1718432100000,
        "workers_online": 2,
        "active_executions": 3,
        "queue_depth_total": 7,
    },
]


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.build_observability_snapshot", new_callable=AsyncMock)
def test_metrics_snapshot_table(mock_snapshot, _mock_init):
    mock_snapshot.return_value = _SNAPSHOT
    result = runner.invoke(app, ["metrics", "snapshot"])
    assert result.exit_code == 0
    assert "Workers online" in result.output
    assert "2" in result.output


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.build_observability_snapshot", new_callable=AsyncMock)
def test_metrics_snapshot_json(mock_snapshot, _mock_init):
    mock_snapshot.return_value = _SNAPSHOT
    result = runner.invoke(app, ["metrics", "snapshot", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["queue_depth_total"] == 7


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.load_history_from_redis", new_callable=AsyncMock)
def test_metrics_history_table(mock_history, _mock_init):
    mock_history.return_value = _HISTORY_POINTS
    result = runner.invoke(app, ["metrics", "history"])
    assert result.exit_code == 0
    assert "1718432000000" in result.output


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.load_history_from_redis", new_callable=AsyncMock)
def test_metrics_history_empty(mock_history, _mock_init):
    mock_history.return_value = []
    result = runner.invoke(app, ["metrics", "history"])
    assert result.exit_code == 0
    assert "No history points found" in result.output


@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
@patch("by_framework.admin.cli.load_history_from_redis", new_callable=AsyncMock)
def test_metrics_history_json(mock_history, _mock_init):
    mock_history.return_value = _HISTORY_POINTS
    result = runner.invoke(app, ["metrics", "history", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert len(data) == 2


# --------------------------------------------------------------------------- #
#  Global --redis-url and env var
# --------------------------------------------------------------------------- #


@patch("by_framework.admin.cli.WorkerRegistry")
@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
def test_redis_url_flag(mock_init, mock_registry_cls):
    mock_registry = MagicMock()
    mock_registry.get_all_workers = AsyncMock(return_value={})
    mock_registry_cls.return_value = mock_registry

    runner.invoke(app, ["--redis-url", "redis://myhost:6380/1", "worker", "list"])
    mock_init.assert_called_with("redis://myhost:6380/1")


@patch("by_framework.admin.cli.WorkerRegistry")
@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
def test_redis_url_env_var(mock_init, mock_registry_cls, monkeypatch):
    monkeypatch.setenv("BYAI_REDIS_URL", "redis://envhost:6379/2")
    mock_registry = MagicMock()
    mock_registry.get_all_workers = AsyncMock(return_value={})
    mock_registry_cls.return_value = mock_registry

    runner.invoke(app, ["worker", "list"])
    mock_init.assert_called_with("redis://envhost:6379/2")


def test_help_mentions_cluster_env_configuration():
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "REDIS_MODE=cluster" in result.output
    assert "REDIS_CLUSTER_NODES" in result.output
    assert "REDIS_KEY_SCHEMA_VERSION=v2" in result.output


@patch("by_framework.admin.cli.init_redis", return_value=_mock_redis())
@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
def test_get_redis_uses_cluster_config_from_env(
    mock_init_from_url, mock_init, monkeypatch
):
    _cluster_env(monkeypatch)

    redis = cli._get_redis()  # pylint: disable=protected-access

    assert redis is mock_init.return_value
    mock_init_from_url.assert_not_called()
    mock_init.assert_called_once()
    config = mock_init.call_args.kwargs["config"]
    assert isinstance(config, RedisConfig)
    assert config.mode == "cluster"
    assert config.cluster_nodes == [("h1", 6379), ("h2", 6380)]
    assert config.username == "cluster-user"
    assert config.password == "cluster-secret"


@patch("by_framework.admin.cli.init_redis", return_value=_mock_redis())
@patch("by_framework.admin.cli.init_redis_from_url", return_value=_mock_redis())
def test_get_redis_prefers_explicit_url_over_cluster_env(
    mock_init_from_url, mock_init, monkeypatch
):
    _cluster_env(monkeypatch)

    redis = cli._get_redis("redis://operator-host:6379/0")  # pylint: disable=protected-access

    assert redis is mock_init_from_url.return_value
    mock_init_from_url.assert_called_once_with("redis://operator-host:6379/0")
    mock_init.assert_not_called()


@patch("by_framework.admin.cli.init_redis", side_effect=RuntimeError("v2 required"))
def test_get_redis_cluster_mode_surfaces_schema_error(mock_init, monkeypatch):
    monkeypatch.setenv("REDIS_MODE", "cluster")
    monkeypatch.setenv("REDIS_CLUSTER_NODES", "h1:6379,h2:6380")
    monkeypatch.setenv("REDIS_KEY_SCHEMA_VERSION", "v1")

    try:
        cli._get_redis()  # pylint: disable=protected-access
    except RuntimeError as err:
        assert "v2 required" in str(err)
    else:
        raise AssertionError("cluster schema error was not surfaced")

    mock_init.assert_called_once()


@patch("by_framework.admin.cli.WorkerRegistry")
@patch("by_framework.admin.cli.init_redis", return_value=_mock_redis())
def test_worker_list_uses_cluster_config(mock_init, mock_registry_cls, monkeypatch):
    _cluster_env(monkeypatch)
    mock_registry = MagicMock()
    mock_registry.get_all_workers = AsyncMock(return_value={})
    mock_registry_cls.return_value = mock_registry

    result = runner.invoke(app, ["worker", "list"])

    assert result.exit_code == 0
    assert mock_init.call_args.kwargs["config"].mode == "cluster"


@patch("by_framework.admin.cli.WorkerManager")
@patch("by_framework.admin.cli.init_redis", return_value=_mock_redis())
def test_type_deny_uses_cluster_config(mock_init, mock_mgr_cls, monkeypatch):
    _cluster_env(monkeypatch)
    mock_mgr = MagicMock()
    mock_mgr.deny_worker_for_type = AsyncMock()
    mock_mgr_cls.return_value = mock_mgr

    result = runner.invoke(app, ["type", "deny", "chat", "worker-a"])

    assert result.exit_code == 0
    assert mock_init.call_args.kwargs["config"].mode == "cluster"


@patch("by_framework.admin.cli.build_observability_snapshot", new_callable=AsyncMock)
@patch("by_framework.admin.cli.init_redis", return_value=_mock_redis())
def test_metrics_snapshot_uses_cluster_config(mock_init, mock_snapshot, monkeypatch):
    _cluster_env(monkeypatch)
    mock_snapshot.return_value = _SNAPSHOT

    result = runner.invoke(app, ["metrics", "snapshot"])

    assert result.exit_code == 0
    assert mock_init.call_args.kwargs["config"].mode == "cluster"
