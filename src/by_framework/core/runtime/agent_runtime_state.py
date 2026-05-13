"""
Agent runtime state container.

Provides a unified state container for agent execution including:
- SessionManager: Session and file management
- AgentConfigManager: Agent configuration management
"""

from typing import List, Optional

from by_framework.core.extensions import AgentConfig
from by_framework.core.runtime.agent_config_manager import AgentConfigManager
from by_framework.core.runtime.file_permissions import FilePermissionPolicy
from by_framework.core.runtime.filestore.base import FileStorage
from by_framework.core.runtime.session_manager import SessionManager


class AgentRuntimeState:
    """Unified agent runtime state container.

    Provides a single entry point for accessing:
    - session_manager: Session metadata and file management
    - config_manager: Agent configuration management
    """

    def __init__(
        self,
        session_id: str,
        user_code: Optional[str] = None,
        user_name: Optional[str] = None,
        storage: Optional[FileStorage] = None,
        workspace_dir: Optional[str] = None,
        agent_configs: Optional[List[AgentConfig]] = None,
        permission_policy: FilePermissionPolicy | None = None,
        agent_id: str = "",
    ):
        """Initialize the agent runtime state.

        Args:
            session_id: Unique session identifier
            user_code: Optional user identifier
            user_name: Optional user name
            storage: Optional storage backend for file operations
            workspace_dir: Custom workspace directory
                (only used when storage is not provided)
            agent_configs: Initial list of AgentConfig instances.
        """
        self._session_manager = SessionManager(
            session_id,
            user_code=user_code,
            user_name=user_name,
            storage=storage,
            workspace_dir=workspace_dir,
            permission_policy=permission_policy,
            agent_id=agent_id,
        )
        self._config_manager = AgentConfigManager(agent_configs)

    @property
    def session_manager(self) -> SessionManager:
        """Get the session manager.

        Returns:
            SessionManager instance for the current session
        """
        return self._session_manager

    @property
    def config_manager(self) -> AgentConfigManager:
        """Get the agent configuration manager.

        Returns:
            AgentConfigManager instance for config management
        """
        return self._config_manager
