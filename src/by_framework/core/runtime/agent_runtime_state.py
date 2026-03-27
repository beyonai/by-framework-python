"""
Agent runtime state container.

Provides a unified state container for agent execution including:
- SessionManager: Session and file management
- AgentConfigManager: Agent configuration management
"""

from typing import List, Optional

from by_framework.core.extensions import AgentConfig
from by_framework.core.runtime.agent_config_manager import AgentConfigManager
from by_framework.core.runtime.session_manager import SessionManager
from by_framework.core.runtime.filestore.base import FileStorage


class AgentRuntimeState:
    """Unified agent runtime state container.

    Provides a single entry point for accessing:
    - session_manager: Session metadata and file management
    - config_manager: Agent configuration management
    """

    def __init__(
        self,
        session_id: str,
        tenant_id: Optional[str] = None,
        storage: Optional[FileStorage] = None,
        workspace_dir: Optional[str] = None,
        agent_configs: Optional[List[AgentConfig]] = None,
    ):
        """Initialize the agent runtime state.

        Args:
            session_id: Unique session identifier
            tenant_id: Optional tenant identifier
            storage: Optional storage backend for file operations
            workspace_dir: Custom workspace directory (only used when storage is not provided)
            agent_configs: Initial list of AgentConfig instances
        """
        self._session_manager = SessionManager(
            session_id, tenant_id, storage, workspace_dir
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
