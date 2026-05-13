# pylint: disable=C0114,C0301,R0902,R0913,R0917,C0415,C0116,E1102
"""
PostgreSQL history storage implementation.

Provides a PostgreSQL-based storage backend for session history
with proper schema initialization and connection pooling.
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Awaitable, Callable, Optional

from by_framework.common.logger import logger
from by_framework.core.runtime.history.base import BaseHistoryBackend


class PostgresHistoryBackend(BaseHistoryBackend):
    """PostgreSQL-based storage backend."""

    _CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS gateway_session_messages (
        id BIGSERIAL PRIMARY KEY,
        session_id VARCHAR(128) NOT NULL,
        role VARCHAR(32) NOT NULL CHECK (role IN ('user', 'assistant', 'system', 'tool')),
        content TEXT NOT NULL,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    _CREATE_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_gateway_session_messages_session_created_at
    ON gateway_session_messages (session_id, created_at DESC, id DESC);
    """

    _INSERT_SQL = """
    INSERT INTO gateway_session_messages (session_id, role, content, metadata)
    VALUES ($1, $2, $3, $4);
    """

    _SELECT_SQL = """
    SELECT role, content, metadata
    FROM gateway_session_messages
    WHERE session_id = $1
    ORDER BY created_at DESC, id DESC
    LIMIT $2;
    """

    _LIST_SESSIONS_SQL = """
    SELECT session_id, MAX(created_at) as last_active_at, COUNT(*) as message_count
    FROM gateway_session_messages
    GROUP BY session_id
    ORDER BY last_active_at DESC;
    """

    def __init__(
        self,
        connection_pool: Any = None,
        dsn: Optional[str] = None,
        min_size: int = 1,
        max_size: int = 10,
        command_timeout: float = 30.0,
        pool_factory: Optional[Callable[..., Awaitable[Any]]] = None,
    ):
        self.pool = connection_pool
        self._external_pool = connection_pool is not None
        self._dsn = dsn or os.environ.get("BYAI_HISTORY_PG_DSN", "")
        self._pool_min_size = min_size
        self._pool_max_size = max_size
        self._pool_command_timeout = command_timeout
        self._pool_factory = pool_factory or self._default_pool_factory

        self._pool_lock = asyncio.Lock()
        self._schema_ready = False
        self._schema_lock = asyncio.Lock()

    async def _default_pool_factory(
        self,
        dsn: str,
        min_size: int,
        max_size: int,
        command_timeout: float,
    ) -> Any:
        try:
            import asyncpg
        except ImportError as err:
            raise RuntimeError(
                "PostgresHistoryBackend requires `asyncpg`. "
                "Please install it or pass a pre-built connection pool."
            ) from err
        return await asyncpg.create_pool(
            dsn=dsn,
            min_size=min_size,
            max_size=max_size,
            command_timeout=command_timeout,
        )

    async def _ensure_pool(self) -> bool:
        if self.pool is not None:
            return True
        if not self._dsn:
            logger.warning(
                "PostgresHistoryBackend disabled: missing connection_pool and BYAI_HISTORY_PG_DSN/dsn"
            )
            return False

        async with self._pool_lock:
            if self.pool is not None:
                return True
            self.pool = await self._pool_factory(
                self._dsn,
                self._pool_min_size,
                self._pool_max_size,
                self._pool_command_timeout,
            )
            logger.info("PostgresHistoryBackend connection pool initialized")
        return True

    async def _ensure_schema(self) -> None:
        if self._schema_ready:
            return
        if not await self._ensure_pool():
            return

        async with self._schema_lock:
            if self._schema_ready:
                return
            async with self.pool.acquire() as conn:
                await conn.execute(self._CREATE_TABLE_SQL)
                await conn.execute(self._CREATE_INDEX_SQL)
            self._schema_ready = True

    async def get_history(
        self, session_id: str, limit: int = 10
    ) -> list[dict[str, Any]]:
        logger.info("Fetching history from Postgres for session: %s", session_id)
        if limit <= 0:
            return []
        if not await self._ensure_pool():
            return []

        await self._ensure_schema()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(self._SELECT_SQL, session_id, limit)

        history: list[dict[str, Any]] = []
        for row in reversed(rows):
            metadata = row["metadata"]
            if isinstance(metadata, str):
                metadata = json.loads(metadata)
            history.append(
                {
                    "role": row["role"],
                    "content": row["content"],
                    "metadata": metadata or {},
                }
            )
        return history

    async def save_message(
        self,
        session_id: str,
        role: str,
        content: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        logger.info("Saving message to Postgres for session %s", session_id)
        if not await self._ensure_pool():
            return

        await self._ensure_schema()
        async with self.pool.acquire() as conn:
            await conn.execute(
                self._INSERT_SQL,
                session_id,
                role,
                content,
                json.dumps(metadata or {}),
            )

    async def list_sessions(self) -> list[dict[str, Any]]:
        """Get all session list."""
        logger.info("Listing all sessions from Postgres")
        if not await self._ensure_pool():
            return []

        await self._ensure_schema()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(self._LIST_SESSIONS_SQL)

        return [
            {
                "session_id": row["session_id"],
                "last_active_at": row["last_active_at"].isoformat()
                if row["last_active_at"]
                else None,
                "message_count": row["message_count"],
            }
            for row in rows
        ]

    async def close(self) -> None:
        if self._external_pool:
            return
        if self.pool is None:
            return
        close = getattr(self.pool, "close", None)
        if close is None:
            self.pool = None
            return
        result = close()
        if asyncio.iscoroutine(result):
            await result
        self.pool = None
