"""
Workspace management module.

Provides workspace setup and cleanup functionality for agent tasks,
managing both session-level and task-level directories.
"""

import asyncio
import shutil
from pathlib import Path
from typing import Dict


class WorkspaceManager:
    """Manages workspace setup and cleanup for agent tasks.

    Workspace is divided into two levels:
    - Public directory: Session-level shared resources
    - Private directory: Request-level temporary files

    Args:
        base_dir: Workspace root directory, defaults to /tmp/workspace
    """

    def __init__(self, base_dir: str = "/tmp/workspace"):
        self.base_dir = Path(base_dir)

    async def setup_workspace(
        self,
        session_id: str,
        task_id: str,
        user_code: str = "default",
        agent_id: str = "",
    ) -> Dict[str, str]:
        return await asyncio.to_thread(
            self._setup_workspace_sync,
            session_id,
            task_id,
            user_code,
            agent_id,
        )

    def _setup_workspace_sync(
        self,
        session_id: str,
        task_id: str,
        user_code: str = "default",
        agent_id: str = "",
    ) -> Dict[str, str]:
        """Create workspace directories synchronously.

        Sets up shared and agent-private directories for session and task.
        """
        agent_namespace = agent_id or "default-agent"
        shared_root = self.base_dir / "public" / user_code
        shared_public_dir = shared_root / "public"
        shared_session_dir = shared_root / "sessions" / session_id

        agent_root = self.base_dir / agent_namespace / user_code
        agent_public_dir = agent_root / "public"
        agent_session_dir = agent_root / "sessions" / session_id
        private_task_dir = agent_session_dir / "private" / task_id

        # Shared public directory: shared across agents
        for dir_path in [shared_public_dir, shared_session_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)

        # Agent-private public directory: same lifecycle as agent+user
        for sub_dir in ["session", "memory/local_db", "agent_skills"]:
            (agent_public_dir / sub_dir).mkdir(parents=True, exist_ok=True)

        # Agent-private session directory: same lifecycle as session
        agent_session_dir.mkdir(parents=True, exist_ok=True)

        # Private directory: request-level temporary files
        for sub_dir in ["input", "temp", "output", "system"]:
            (private_task_dir / sub_dir).mkdir(parents=True, exist_ok=True)

        return {
            "root": str(agent_root),
            "public": str(agent_public_dir),
            "private": str(private_task_dir),
            "history_db": str(agent_public_dir / "session" / "history.json"),
            "shared_root": str(shared_root),
            "shared_public": str(shared_public_dir),
            "shared_session": str(shared_session_dir),
            "agent_root": str(agent_root),
            "agent_public": str(agent_public_dir),
            "agent_session": str(agent_session_dir),
        }

    async def cleanup_task(
        self,
        session_id: str,
        task_id: str,
        user_code: str = "default",
        agent_id: str = "",
    ):
        await asyncio.to_thread(
            self._cleanup_task_sync,
            session_id,
            task_id,
            user_code,
            agent_id,
        )

    def _cleanup_task_sync(
        self,
        session_id: str,
        task_id: str,
        user_code: str = "default",
        agent_id: str = "",
    ):
        """Remove task-specific private directory synchronously."""
        agent_namespace = agent_id or "default-agent"
        task_dir = (
            self.base_dir
            / agent_namespace
            / user_code
            / "sessions"
            / session_id
            / "private"
            / task_id
        )
        if task_dir.exists():
            shutil.rmtree(task_dir)
