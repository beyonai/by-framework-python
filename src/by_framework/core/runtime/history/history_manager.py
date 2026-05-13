"""History manager for session message persistence."""

from typing import Any, Dict, List, Optional

from by_framework.common.logger import logger

from .base import BaseHistoryBackend
from .in_memory import InMemoryHistoryBackend


class HistoryManager:
    """
    Provides session history message management.
    As a sub-capability of SessionManager.
    """

    # Global default backend
    _default_backend: BaseHistoryBackend = InMemoryHistoryBackend()

    @classmethod
    def set_default_backend(cls, backend: BaseHistoryBackend):
        """Configure global default backend"""
        logger.info(
            "Default history backend switched to: %s", backend.__class__.__name__
        )
        cls._default_backend = backend

    def __init__(self, session_id: str, backend: Optional[BaseHistoryBackend] = None):
        """Initialize HistoryManager.

        Args:
            session_id: Session ID
            backend: Optional backend, uses global default backend if not provided
        """
        self._session_id = session_id
        self._backend = backend or self._default_backend

    async def get_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get historical messages for the current session"""
        return await self._backend.get_history(self._session_id, limit)

    async def save_message(
        self,
        role: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Persist a message to the current session history."""
        if not content:
            return
        await self._backend.save_message(self._session_id, role, content, metadata)

    async def list_sessions(self) -> List[Dict[str, Any]]:
        """Get all session list."""
        return await self._backend.list_sessions()
