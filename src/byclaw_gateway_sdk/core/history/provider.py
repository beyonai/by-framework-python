from typing import Any, Dict, List, Optional

from byclaw_gateway_sdk.common.logger import logger

from .base import BaseHistoryStorage
from .storage.in_memory import InMemoryHistoryStorage


class HistoryProvider:
    """
    提供获取和存储会话历史消息的全局入口。
    支持动态切换存储后端（BaseHistoryStorage）。
    """

    _storage: BaseHistoryStorage = InMemoryHistoryStorage()

    @classmethod
    def set_storage(cls, storage: BaseHistoryStorage):
        """配置全局存储后端"""
        logger.info(
            "History storage backend switched to: %s", storage.__class__.__name__
        )
        cls._storage = storage

    @classmethod
    async def get_session_history(
        cls, session_id: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """获取指定会话的历史消息"""
        return await cls._storage.get_history(session_id, limit)

    @classmethod
    async def save_message(
        cls,
        session_id: str,
        role: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """持久化一条消息到会话历史。

        具体行为取决于当前存储后端是否实现 ``save_message`` 方法。
        """
        if not content:
            return
        await cls._storage.save_message(session_id, role, content, metadata)
