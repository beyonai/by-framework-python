"""
Session management for agent runtime.

Provides simple session-level management.
"""

from typing import Optional

from by_framework.core.runtime.file_manager import FileManager
from by_framework.core.runtime.history import HistoryManager
from by_framework.core.runtime.filestore.base import FileStorage


class SessionManager:
    """Session management for agent runtime.

    Provides basic session-level state access and sub-capabilities:
    - file_manager: File operations within this session
    - history: Message history for this session
    """

    def __init__(
        self,
        session_id: str,
        tenant_id: Optional[str] = None,
        storage: Optional[FileStorage] = None,
        workspace_dir: Optional[str] = None,
    ):
        """Initialize the session manager.

        Args:
            session_id: Unique session identifier
            tenant_id: Optional tenant identifier
            storage: Optional storage backend for file operations
            workspace_dir: Custom workspace directory (only used when storage is not provided)
        """
        self._session_id = session_id
        self._tenant_id = tenant_id
        self._file_manager = FileManager(session_id, storage, workspace_dir)
        self._history = HistoryManager(session_id)
        self._message_count = 0

    @property
    def session_id(self) -> str:
        """Get the session ID."""
        return self._session_id

    @property
    def tenant_id(self) -> Optional[str]:
        """Get the tenant ID."""
        return self._tenant_id

    @property
    def file_manager(self) -> FileManager:
        """Get the file manager for this session."""
        return self._file_manager

    @property
    def history(self) -> HistoryManager:
        """Get the history manager for this session."""
        return self._history

    @property
    def message_count(self) -> int:
        """Get the message count."""
        return self._message_count
