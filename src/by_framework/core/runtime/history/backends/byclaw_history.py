"""
In-memory history storage implementation.

Provides a simple in-memory storage backend for session history.
Suitable for development and testing.
"""

from typing import Any, Dict, List, Optional

from ..base import BaseHistoryStorage


class ByClawHistoryStorage(BaseHistoryStorage):
    """基于内存的存储后端（默认，适用于开发和测试）。

    所有数据存储在内存字典中，进程重启后数据会丢失。
    """

    def __init__(self):
        # 结构: {session_id: [message1, message2, ...]}
        self._storage: Dict[str, List[Dict[str, Any]]] = {}

    async def get_history(
        self, session_id: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        messages = self._storage.get(session_id, [])
        return messages[-limit:]
