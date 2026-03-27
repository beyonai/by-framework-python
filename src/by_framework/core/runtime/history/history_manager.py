from typing import Any, Dict, List, Optional

from by_framework.common.logger import logger

from .base import BaseHistoryBackend
from .backends.in_memory import InMemoryHistoryBackend


class HistoryManager:
    """
    提供会话历史消息的管理。
    作为 SessionManager 的一个子能力。
    """

    # 全局默认后端
    _default_backend: BaseHistoryBackend = InMemoryHistoryBackend()

    @classmethod
    def set_default_backend(cls, backend: BaseHistoryBackend):
        """配置全局默认后端"""
        logger.info(
            "Default history backend switched to: %s", backend.__class__.__name__
        )
        cls._default_backend = backend

    def __init__(self, session_id: str, backend: Optional[BaseHistoryBackend] = None):
        """初始化 HistoryManager。

        Args:
            session_id: 会话 ID
            backend: 可选的后端，若不提供则使用全局默认后端
        """
        self._session_id = session_id
        self._backend = backend or self._default_backend

    async def get_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """获取当前会话的历史消息"""
        return await self._backend.get_history(self._session_id, limit)

    async def save_message(
        self,
        role: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """持久化一条消息到当前会话历史。"""
        if not content:
            return
        await self._backend.save_message(self._session_id, role, content, metadata)
