# pylint: disable=C0114,C0115,C0116,W0613
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from by_framework.util.http_client import HttpResponse

from by_framework_history_byclaw import ByClawHistoryBackend


@pytest.mark.asyncio
async def test_byclaw_history_backend_formats_messages(monkeypatch) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/byaiService/open/api/inner/getMessages"
        assert request.content == b'{"sessionId":"sess-1","topK":2}'
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "data": [
                    {"usage": 1, "messageContent": "hello", "metadata": {"n": 1}},
                    {"usage": 2, "messageContent": "world", "metadata": {"n": 2}},
                ],
            },
        )

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(
        transport=transport, base_url="https://history.example.com"
    )

    class FakeAsyncClient:

        def __init__(self, *args, **kwargs):
            self._client = client

        async def __aenter__(self):
            return self._client

        async def __aexit__(self, exc_type, exc, tb):
            await self._client.aclose()
            return False

        async def aclose(self):
            await self._client.aclose()

    monkeypatch.setattr(
        "by_framework_history_byclaw.byclaw_history.httpx.AsyncClient", FakeAsyncClient
    )

    # 显式传入 None 以确保在旧逻辑测试中不触发自动初始化的 discovery_http_client
    # (尽管 mock 已经修复，但这样测试意图更清晰)
    backend = ByClawHistoryBackend(
        base_url="https://history.example.com", discovery_http_client=None
    )
    history = await backend.get_history("sess-1", limit=2)

    assert history == [
        {"role": "user", "content": "hello", "metadata": {"n": 1}},
        {"role": "assistant", "content": "world", "metadata": {"n": 2}},
    ]


@pytest.mark.asyncio
async def test_byclaw_history_backend_with_discovery() -> None:
    mock_discovery_client = AsyncMock()
    # Support async context manager
    mock_discovery_client.__aenter__.return_value = mock_discovery_client

    # Mock response from DiscoveryHttpClient
    mock_response = HttpResponse(
        status_code=200,
        headers={},
        data={
            "code": 0,
            "data": [
                {"usage": 1, "messageContent": "hello from discovery"},
                {"role": "assistant", "content": "response from discovery"},
            ],
        },
        is_success=True,
    )
    mock_discovery_client.post.return_value = mock_response

    backend = ByClawHistoryBackend(
        discovery_http_client=mock_discovery_client, service_name="MyTestService"
    )

    history = await backend.get_history("sess-discovery", limit=5)

    # Verify discovery client was called correctly
    mock_discovery_client.post.assert_called_once_with(
        "MyTestService",
        "/byaiService/open/api/inner/getMessages",
        json={"sessionId": "sess-discovery", "topK": 5},
    )

    # Ensure __aenter__ was called
    mock_discovery_client.__aenter__.assert_called_once()

    # Verify transformed results
    assert history == [
        {"role": "user", "content": "hello from discovery", "metadata": {}},
        {"role": "assistant", "content": "response from discovery", "metadata": {}},
    ]


@pytest.mark.asyncio
async def test_byclaw_history_backend_lazy_initialization(monkeypatch) -> None:
    # Mock components used in lazy initialization
    mock_dhc_class = MagicMock()
    mock_dhc_instance = AsyncMock()
    mock_dhc_instance.__aenter__.return_value = mock_dhc_instance
    mock_dhc_class.return_value = mock_dhc_instance

    mock_response = HttpResponse(
        status_code=200,
        headers={},
        data={"code": 0, "data": []},
        is_success=True,
    )
    mock_dhc_instance.post.return_value = mock_response

    # Monkeypatch the imports in the module where they are used
    monkeypatch.setattr(
        "by_framework_history_byclaw.byclaw_history.DiscoveryClient", MagicMock()
    )
    monkeypatch.setattr(
        "by_framework_history_byclaw.byclaw_history.DiscoveryHttpClient", mock_dhc_class
    )

    # 1. Instantiate - should NOT trigger discovery client creation
    backend = ByClawHistoryBackend()
    assert mock_dhc_class.call_count == 0

    # 2. Call get_history - should trigger lazy initialization
    await backend.get_history("sess-lazy", limit=1)

    assert mock_dhc_class.call_count == 1
    mock_dhc_instance.post.assert_called_once()
