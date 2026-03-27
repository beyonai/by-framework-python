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
    """工作区管理器，负责为智能体任务设置和清理工作目录。

    工作区分为两个层级：
    - 公共目录 (public): 与 session 同生命周期，包含 session 级共享资源
    - 私有目录 (private): 请求级别临时文件，任务完成后清理

    Args:
        base_dir: 工作区根目录，默认为 /tmp/workspace
    """

    def __init__(self, base_dir: str = "/tmp/workspace"):
        self.base_dir = Path(base_dir)

    async def setup_workspace(self, session_id: str, task_id: str) -> Dict[str, str]:
        return await asyncio.to_thread(self._setup_workspace_sync, session_id, task_id)

    def _setup_workspace_sync(self, session_id: str, task_id: str) -> Dict[str, str]:
        session_dir = self.base_dir / session_id
        public_dir = session_dir / "public"
        private_task_dir = session_dir / "private" / task_id

        # Public 目录：生命周期同 session
        for sub_dir in ["session", "memory/local_db", "agent_skills"]:
            (public_dir / sub_dir).mkdir(parents=True, exist_ok=True)

        # Private 目录：请求级别临时文件
        for sub_dir in ["input", "temp", "output", "system"]:
            (private_task_dir / sub_dir).mkdir(parents=True, exist_ok=True)

        return {
            "root": str(session_dir),
            "public": str(public_dir),
            "private": str(private_task_dir),
            "history_db": str(public_dir / "session" / "history.json"),
        }

    async def cleanup_task(self, session_id: str, task_id: str):
        await asyncio.to_thread(self._cleanup_task_sync, session_id, task_id)

    def _cleanup_task_sync(self, session_id: str, task_id: str):
        task_dir = self.base_dir / session_id / "private" / task_id
        if task_dir.exists():
            shutil.rmtree(task_dir)
