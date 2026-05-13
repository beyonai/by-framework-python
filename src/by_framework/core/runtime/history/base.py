"""
Base history storage interface.

Defines the abstract base class for all history storage backends.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BaseHistoryBackend(ABC):
    """Abstract base class for history storage backend.

    All history message storage backends must implement this interface.
    """

    @abstractmethod
    async def get_history(
        self, session_id: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get historical messages for the specified session.

        Args:
            session_id: Session ID
            limit: Maximum number of messages to return

        Returns:
            List of message dictionaries, sorted in chronological order
        """
        pass

    @abstractmethod
    async def save_message(
        self,
        session_id: str,
        role: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Persist a message to session history.

        Args:
            session_id: Session ID
            role: Message role (user/assistant/system/tool)
            content: Message content
            metadata: Extra metadata
        """
        pass

    @abstractmethod
    async def list_sessions(self) -> List[Dict[str, Any]]:
        """Get all session list.

        Returns:
            List of session information, including session_id and last active time, etc.
        """
        pass
