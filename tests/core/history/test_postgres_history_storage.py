from unittest.mock import AsyncMock, MagicMock

import pytest

from byclaw_gateway_sdk.core.history.storage.postgres import PostgresHistoryStorage


@pytest.mark.asyncio
async def test_save_message_executes_insert_with_default_metadata():
    """Test that save_message executes an INSERT statement with correct parameters."""
    conn = AsyncMock()

    class _AcquireCtx:

        async def __aenter__(self):
            return conn

        async def __aexit__(self, exc_type, exc, tb):
            return False

    pool = MagicMock()
    pool.acquire.return_value = _AcquireCtx()

    storage = PostgresHistoryStorage(connection_pool=pool)

    await storage.save_message("s1", "assistant", "hello", None)

    # schema ensure + insert
    assert conn.execute.await_count == 3
    insert_call = conn.execute.await_args_list[-1]
    args = insert_call.args

    assert "INSERT INTO gateway_session_messages" in args[0]
    assert args[1] == "s1"
    assert args[2] == "assistant"
    assert args[3] == "hello"
    assert args[4] == {}


@pytest.mark.asyncio
async def test_get_history_returns_latest_messages_in_chronological_order():
    """Test that get_history returns messages in chronological order (oldest first)."""
    conn = AsyncMock()
    conn.fetch.return_value = [
        {
            "role": "assistant",
            "content": "second",
            "metadata": {"n": 2},
        },
        {
            "role": "user",
            "content": "first",
            "metadata": {"n": 1},
        },
    ]

    class _AcquireCtx:

        async def __aenter__(self):
            return conn

        async def __aexit__(self, exc_type, exc, tb):
            return False

    pool = MagicMock()
    pool.acquire.return_value = _AcquireCtx()

    storage = PostgresHistoryStorage(connection_pool=pool)

    history = await storage.get_history("session-x", limit=2)

    # schema ensure + select query
    assert conn.execute.await_count == 2
    conn.fetch.assert_awaited_once()
    fetch_args = conn.fetch.await_args.args
    assert "FROM gateway_session_messages" in fetch_args[0]
    assert fetch_args[1] == "session-x"
    assert fetch_args[2] == 2

    assert history == [
        {"role": "user", "content": "first", "metadata": {"n": 1}},
        {"role": "assistant", "content": "second", "metadata": {"n": 2}},
    ]


@pytest.mark.asyncio
async def test_no_pool_behaves_as_noop_storage():
    """Test that PostgresHistoryStorage with None pool is a no-op storage."""
    storage = PostgresHistoryStorage(connection_pool=None)

    history = await storage.get_history("no-pool", limit=5)
    await storage.save_message("no-pool", "user", "hello")

    assert history == []


@pytest.mark.asyncio
async def test_dsn_auto_initializes_connection_pool_once():
    """Test that DSN constructor auto-initializes connection pool only once."""
    conn = AsyncMock()

    class _AcquireCtx:

        async def __aenter__(self):
            return conn

        async def __aexit__(self, exc_type, exc, tb):
            return False

    pool = MagicMock()
    pool.acquire.return_value = _AcquireCtx()

    pool_factory = AsyncMock(return_value=pool)
    storage = PostgresHistoryStorage(
        dsn="postgresql://u:p@localhost:5432/db",
        pool_factory=pool_factory,
    )

    await storage.save_message("s2", "user", "hello")
    await storage.get_history("s2", limit=10)

    pool_factory.assert_awaited_once()
