"""
In-memory history storage implementation.

Provides a simple in-memory storage backend for session history.
Suitable for development and testing.
"""

from typing import Any, Dict, List, Optional

from .base import BaseHistoryBackend


class InMemoryHistoryBackend(BaseHistoryBackend):
    """In-memory storage backend.

    Suitable for development and testing. Data is stored in an in-memory
    dictionary and will be lost after process restart.
    """

    def __init__(self):
        # Structure: {session_id: [message1, message2, ...]}
        self._storage: Dict[str, List[Dict[str, Any]]] = {}

    async def get_history(
        self, session_id: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        messages = self._storage.get(session_id, [])
        return messages[-limit:]

    async def save_message(
        self,
        session_id: str,
        role: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if session_id not in self._storage:
            self._storage[session_id] = []

        self._storage[session_id].append(
            {"role": role, "content": content, "metadata": metadata or {}}
        )

    async def list_sessions(self) -> List[Dict[str, Any]]:
        sessions = []
        for session_id, messages in self._storage.items():
            sessions.append(
                {
                    "session_id": session_id,
                    "message_count": len(messages),
                    # InMemory doesn't track timestamps easily
                    "last_active_at": "unknown",
                }
            )
        return sessions

    async def close(self) -> None:
        pass
