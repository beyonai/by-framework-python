# pylint: disable=redefined-outer-name
from unittest.mock import AsyncMock, MagicMock

import pytest
from by_framework.core.runtime.history.history_manager import HistoryManager
from by_framework.core.runtime.history.in_memory import InMemoryHistoryBackend


@pytest.fixture
def mock_redis():
    mock = MagicMock()
    mock.xadd = AsyncMock()
    mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    mock.hset = AsyncMock()
    mock.hget = AsyncMock(return_value=None)
    mock.hgetall = AsyncMock(return_value={})
    mock.hincrby = AsyncMock(return_value=1)
    mock.expire = AsyncMock()
    mock.delete = AsyncMock()
    return mock


@pytest.fixture
def workspace_manager():
    manager = MagicMock()
    manager.setup_workspace = AsyncMock(
        return_value={"private": "/tmp", "public": "/tmp"}
    )
    manager.cleanup_task = AsyncMock()
    return manager


@pytest.fixture(autouse=True)
def _isolated_history_backend():
    """Each test gets a fresh in-memory history store (default backend is
    process-global, so cross-test bleed is possible otherwise)."""
    HistoryManager.set_default_backend(InMemoryHistoryBackend())
    yield
