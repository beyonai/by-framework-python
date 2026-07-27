# pylint: disable=redefined-outer-name
from unittest.mock import AsyncMock, MagicMock

import pytest
from by_framework.core.runtime.history.history_manager import HistoryManager
from by_framework.core.runtime.history.in_memory import InMemoryHistoryBackend


class FakeRedisStore:
    """Minimal real in-memory backing for the Redis calls the harness makes.

    Shared across multiple worker instances within a test (they're all
    handed the same ``mock_redis``), so a value written by one "worker
    process" is readable by another — proving loop state genuinely
    round-trips through Redis rather than living in Python memory.
    """

    def __init__(self):
        self.strings: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}

    async def set(self, key, value, ex=None):  # pylint: disable=unused-argument
        self.strings[key] = value

    async def get(self, key):
        return self.strings.get(key)

    async def delete(self, key):
        self.strings.pop(key, None)
        self.hashes.pop(key, None)

    async def hset(self, key, mapping=None, **kwargs):
        self.hashes.setdefault(key, {}).update(mapping or kwargs)

    async def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    async def hincrby(self, key, field, amount=1):
        current = int(self.hashes.get(key, {}).get(field, 0)) + amount
        self.hashes.setdefault(key, {})[field] = str(current)
        return current

    async def expire(self, key, seconds):  # pylint: disable=unused-argument
        return True


@pytest.fixture
def mock_redis():
    store = FakeRedisStore()
    mock = MagicMock()
    mock.xadd = AsyncMock()
    mock.pipeline = MagicMock(
        return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
    )
    mock.set = store.set
    mock.get = store.get
    mock.delete = store.delete
    mock.hset = store.hset
    mock.hget = store.hget
    mock.hgetall = store.hgetall
    mock.hincrby = store.hincrby
    mock.expire = store.expire
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
