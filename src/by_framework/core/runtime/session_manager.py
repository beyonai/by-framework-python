"""
Session management for agent runtime.

Provides simple session-level management.
"""

from typing import Optional

from by_framework.core.runtime.file_manager import FileManager
from by_framework.core.runtime.file_paths import FileAccessContext
from by_framework.core.runtime.file_permissions import FilePermissionPolicy
from by_framework.core.runtime.filestore.base import FileStorage
from by_framework.core.runtime.history import HistoryManager


class SessionManager:
    """Session management for agent runtime.

    Provides basic session-level state access and sub-agent-type helpers:
    - file_manager: File operations within this session
    - history: Message history for this session
    """

    def __init__(
        self,
        session_id: str,
        user_code: Optional[str] = None,
        user_name: Optional[str] = None,
        storage: Optional[FileStorage] = None,
        workspace_dir: Optional[str] = None,
        permission_policy: FilePermissionPolicy | None = None,
        agent_id: str = "",
    ):
        """Initialize the session manager.

        Args:
            session_id: Unique session identifier
            user_code: Optional user identifier
            user_name: Optional user name
            storage: Optional storage backend for file operations
            workspace_dir: Custom workspace directory (only used when storage is None).
        """
        self._session_id = session_id
        self._user_code = user_code or "default"
        self._user_name = user_name or ""
        self._agent_id = agent_id

        private_access_context = FileAccessContext(
            session_id=session_id,
            user_code=self._user_code,
            workspace_scope="agent_private",
            agent_id=agent_id,
        )
        shared_access_context = FileAccessContext(
            session_id=session_id,
            user_code=self._user_code,
            workspace_scope="shared_public",
            agent_id=agent_id,
        )

        self._private_file_manager = FileManager(
            session_id=session_id,
            storage=storage,
            workspace_dir=workspace_dir,
            permission_policy=permission_policy,
            access_context=private_access_context,
            agent_id=agent_id,
        )
        self._shared_file_manager = FileManager(
            session_id=session_id,
            storage=storage,
            workspace_dir=workspace_dir,
            permission_policy=permission_policy,
            access_context=shared_access_context,
            agent_id=agent_id,
        )
        self._history = HistoryManager(session_id)
        self._message_count = 0

    @property
    def session_id(self) -> str:
        """Get the session ID."""
        return self._session_id

    @property
    def user_code(self) -> Optional[str]:
        """Get the user code."""
        return self._user_code

    @property
    def user_name(self) -> Optional[str]:
        """Get the user name."""
        return self._user_name

    @property
    def file_manager(self) -> FileManager:
        """Backward-compatible alias for the private file manager."""
        return self._private_file_manager

    @property
    def private_file_manager(self) -> FileManager:
        """Get the agent-private file manager for this session."""
        return self._private_file_manager

    @property
    def shared_file_manager(self) -> FileManager:
        """Get the cross-agent shared file manager for this session."""
        return self._shared_file_manager

    @property
    def history(self) -> HistoryManager:
        """Get the history manager for this session."""
        return self._history

    @property
    def message_count(self) -> int:
        """Get the message count."""
        return self._message_count
