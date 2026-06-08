# pylint: disable=C0114,C0301,R0914,W0718
from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

import httpx
from by_framework.common.logger import get_logger
from by_framework.core.discovery import DiscoveryClient
from by_framework.core.runtime.history.base import BaseHistoryBackend
from by_framework.util.discovery_http_client import DiscoveryHttpClient
from by_framework.util.http_client import RetryConfig

logger = get_logger("by_framework_history_byclaw.byclaw_history")


# Sentinel value to distinguish between not provided and explicitly None
_DEFAULT_DHC = object()


class ByClawHistoryBackend(BaseHistoryBackend):
    """History backend that fetches messages from ByClaw remote service."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        discovery_http_client: Optional[DiscoveryHttpClient] = _DEFAULT_DHC,  # type: ignore
        service_name: Optional[str] = None,
    ):
        """
        Args:
            base_url: Base URL for ByClaw service (fallback if discovery is not used)
            discovery_http_client: HTTP client with service discovery. If not provided,
                tries to auto-initialize using default DiscoveryClient. Pass None to disable.
            service_name: Service name in discovery (default: "ByaiService")
        """
        self.base_url = base_url or os.environ.get("BYAI_BASE_URL", "")
        self.service_name = service_name or os.environ.get(
            "BE_DOMAINNAME", "ByaiService"
        )
        self._discovery_http_client = discovery_http_client

    def _get_discovery_client(self) -> Optional[DiscoveryHttpClient]:
        """Lazy initialization of the discovery HTTP client."""
        if self._discovery_http_client is _DEFAULT_DHC:
            try:
                # 默认配置：5秒缓存间隔，3次重试（针对 502/503/504 节点切换）
                discovery_client = DiscoveryClient(cache_interval=5)
                retry_config = RetryConfig(
                    max_attempts=3,
                    retry_on_status_codes=frozenset({502, 503, 504}),
                )
                self._discovery_http_client = DiscoveryHttpClient(
                    discovery_client, retry_config=retry_config
                )
                logger.debug(
                    "Lazy-initialized default DiscoveryHttpClient for ByClawHistoryBackend"
                )
            except Exception as e:
                logger.warning(
                    "Failed to lazy-initialize DiscoveryHttpClient: %s. "
                    "Will fallback to base_url if available.",
                    e,
                )
                self._discovery_http_client = None

        return self._discovery_http_client  # type: ignore

    async def get_history(
        self, session_id: str, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Fetch history messages from ByClaw service."""
        payload = {
            "sessionId": session_id,
            "topK": limit,
        }

        discovery_client = self._get_discovery_client()

        try:
            if discovery_client:
                # 使用服务发现能力，并通过上下文管理器确保资源正确管理
                path = "/byaiService/open/api/inner/getMessages"
                logger.debug(
                    "Fetching history via discovery: service=%s, path=%s, session=%s",
                    self.service_name,
                    path,
                    session_id,
                )
                async with discovery_client as client:
                    response = await client.post(self.service_name, path, json=payload)
                if not response.is_success:
                    logger.error(
                        "Failed to fetch history via discovery: status=%d, data=%s",
                        response.status_code,
                        response.data,
                    )
                    return []
                data = response.data
            else:
                # 回退到传统的 base_url 模式
                url = f"{self.base_url.rstrip('/')}/byaiService/open/api/inner/getMessages"
                logger.debug(
                    "Fetching history via URL: %s, session=%s", url, session_id
                )
                async with httpx.AsyncClient() as client:
                    response = await client.post(url, json=payload, timeout=10.0)
                    response.raise_for_status()
                    data = response.json()

            if not isinstance(data, dict) or data.get("code") != 0:
                logger.warning("Unexpected response from history service: %s", data)
                return []

            # ByClaw 返回的数据结构中消息列表通常在 data 字段中
            messages_data = data.get("data", [])
            if not isinstance(messages_data, list):
                logger.warning(
                    "ByClaw 'data' field is not a list for session %s: %s",
                    session_id,
                    type(messages_data),
                )
                return []

            return self._transform_messages(messages_data)

        except Exception as e:
            logger.error(
                "Error fetching history from ByClaw for session %s: %s",
                session_id,
                e,
                exc_info=True,
            )
            return []

    def _transform_messages(
        self, messages: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Transform ByClaw internal message format to standard format."""
        transformed = []
        for item in messages:
            if not isinstance(item, dict):
                continue

            # 兼容 ByClaw 原始的 usage 映射逻辑
            usage = item.get("usage")
            role = (
                "user"
                if usage == 1
                else "assistant"
                if usage == 2
                else item.get("role", "unknown")
            )
            # 兼容多种字段名：messageContent 是 ByClaw 风格，content 是标准风格
            raw_content = item.get("messageContent") or item.get("content") or ""
            related_resources = item.get("relatedResources")
            files = []
            if related_resources:
                try:
                    res_dict = json.loads(related_resources)
                    files = res_dict.get("files") or []
                except Exception:
                    pass

            if role and (raw_content or files):
                content_list = []
                if raw_content:
                    content_list.append({"type": "text", "text": raw_content})

                if files:
                    for f in files:
                        content_list.append({"type": "file", "file": f})

                message = {
                    "role": role,
                    "content": content_list,
                    "metadata": item.get("metadata", {}),
                }
                transformed.append(message)

        return transformed

    async def save_message(
        self,
        session_id: str,
        role: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        ByClawHistoryBackend typically relies on external systems to save messages.
        This implementation is read-only for now.
        """
        logger.warning(
            "ByClawHistoryBackend: save_message is not supported for ByAI backend, session_id=%s",
            session_id,
        )

    async def list_sessions(self) -> List[Dict[str, Any]]:
        """Not implemented for ByClaw backend."""
        logger.warning(
            "ByClawHistoryBackend: list_sessions is not supported for ByAI backend"
        )
        return []
