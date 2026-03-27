"""
Base history storage interface.

Defines the abstract base class for all history storage backends.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BaseHistoryBackend(ABC):
    """历史记录存储后端抽象基类。

    所有历史消息存储后端必须实现此接口。
    """

    @abstractmethod
    async def get_history(
        self, session_id: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """获取指定会话的历史消息。

        Args:
            session_id: 会话ID
            limit: 返回的最大消息数量

        Returns:
            消息字典列表，按时间正序排列
        """
        pass

    async def save_message(
        self,
        session_id: str,
        role: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """持久化一条消息到会话历史。

        Args:
            session_id: 会话ID
            role: 消息角色 (user/assistant/system/tool)
            content: 消息内容
            metadata: 附加元数据
        """
        pass
