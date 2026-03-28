import os
from typing import Any, Dict, List, Optional

import httpx

from by_framework.common.logger import logger
from ..base import BaseHistoryBackend


class ByClawHistoryBackend(BaseHistoryBackend):
    """基于 ByAI 接口的历史记录存储后端。

    从外部 ByAI 服务获取会话历史消息。
    """

    def __init__(self, base_url: Optional[str] = None):
        """初始化存储后端。

        Args:
            base_url: ByAI 服务基础 URL。若不提供则从环境变量 BYAI_BASE_URL 获取。
        """
        self.base_url = base_url or os.environ.get("BYAI_BASE_URL", "")

    async def get_history(
        self, session_id: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """通过接口获取当前会话的历史消息。

        POST /byaiService/open/api/inner/getMessages
        """

        url = f"{self.base_url.rstrip('/')}/byaiService/open/api/inner/getMessages"
        payload = {
            "sessionId": session_id,
            "topK": limit,
        }

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(url, json=payload, timeout=10.0)
                resp.raise_for_status()
                data = resp.json()

                if not isinstance(data, dict):
                    logger.error(
                        "ByClawHistoryBackend: Invalid response format for session %s, expected dict, got %s",
                        session_id,
                        type(data),
                    )
                    return []

                result_code = data.get("code")
                if result_code != 0:
                    logger.error(
                        "ByClawHistoryBackend: Failed to get messages for session %s, code: %s, message: %s",
                        session_id,
                        result_code,
                        data.get("msg", "Unknown error"),
                    )
                    return []

                messages_data = data.get("data")
                if not isinstance(messages_data, list):
                    logger.warning(
                        "ByClawHistoryBackend: 'data' field for session %s is not a list, got %s",
                        session_id,
                        type(messages_data),
                    )
                    return []

                formatted_messages = []
                for item in messages_data:
                    if not isinstance(item, dict):
                        continue

                    usage = item.get("usage")
                    # 映射规则：usage=1 -> user, usage=2 -> assistant
                    role = (
                        "user"
                        if usage == 1
                        else "assistant"
                        if usage == 2
                        else "unknown"
                    )
                    content = item.get("messageContent") or ""

                    formatted_messages.append(
                        {
                            "role": role,
                            "content": content,
                            "metadata": item.get("metadata"),
                        }
                    )

                # 返回的消息通常是按时间正序排列的，符合 SDK 要求
                return formatted_messages
        except Exception as e:
            # 记录错误并返回空，保证 runtime 不崩溃
            logger.error(
                "ByClawHistoryBackend: Unexpected error while getting messages for session %s: %s",
                session_id,
                str(e),
                exc_info=True,
            )
            return []