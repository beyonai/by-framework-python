from typing import Any, Dict, List, Optional

from by_framework.common.logger import logger

from .base import BaseHistoryStorage
from .backends.in_memory import InMemoryHistoryStorage


class HistoryManager:
    """
    提供会话历史消息的管理。
    作为 SessionManager 的一个子能力。
    """

    # 全局默认存储后端
    _default_storage: BaseHistoryStorage = InMemoryHistoryStorage()

    @classmethod
    def set_default_storage(cls, storage: BaseHistoryStorage):
        """配置全局默认存储后端"""
        logger.info(
            "Default history storage backend switched to: %s", storage.__class__.__name__
        )
        cls._default_storage = storage

    def __init__(self, session_id: str, storage: Optional[BaseHistoryStorage] = None):
        """初始化 HistoryManager。

        Args:
            session_id: 会话 ID
            storage: 可选的存储后端，若不提供则使用全局默认存储
        """
        self._session_id = session_id
        self._storage = storage or self._default_storage

    async def get_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """获取当前会话的历史消息"""
        return await self._storage.get_history(self._session_id, limit)

    async def save_message(
        self,
        role: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """持久化一条消息到当前会话历史。"""
        if not content:
            return
        await self._storage.save_message(self._session_id, role, content, metadata)
