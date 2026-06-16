from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from by_framework.common.constants import RedisKeys
from by_framework.core.discovery import DiscoveryClient, ServiceInstance
from by_framework.errors import HttpRequestError
from by_framework.util.discovery_http_client import (
    DiscoveryHttpClient,
    DiscoveryHttpClientError,
)
from by_framework.util.http_client import (ByHttpClient, HttpResponse, RetryConfig)


@pytest.fixture
def mock_discovery_client():
    client = MagicMock(spec=DiscoveryClient)
    client.discover = AsyncMock()
    return client


@pytest.fixture
def mock_http_client():
    client = MagicMock(spec=ByHttpClient)
    client._request = AsyncMock()
    return client


@pytest.fixture
def fake_instance_1():
    return ServiceInstance(id="inst1", host="192.168.1.100", port=8080)


@pytest.fixture
def fake_instance_2():
    return ServiceInstance(id="inst2", host="192.168.1.101", port=8080)


@pytest.mark.asyncio
async def test_successful_request(
    mock_discovery_client, mock_http_client, fake_instance_1
):
    # Setup
    mock_discovery_client.discover.return_value = fake_instance_1
    success_response = HttpResponse(
        status_code=200, headers={}, data={"status": "ok"}, is_success=True
    )
    mock_http_client._request.return_value = success_response

    retry_config = RetryConfig(max_attempts=3)
    client = DiscoveryHttpClient(
        discovery_client=mock_discovery_client,
        http_client=mock_http_client,
        retry_config=retry_config,
    )

    # Execute
    result = await client.get("my-service", "/api/data", params={"q": "1"})

    # Verify
    assert result.status_code == 200
    assert result.data == {"status": "ok"}

    mock_discovery_client.discover.assert_called_once_with(
        "my-service",
        health_threshold_ms=RedisKeys.SD_DEFAULT_HEALTH_THRESHOLD_MS,
    )
    mock_http_client._request.assert_called_once_with(
        method="GET",
        url="http://192.168.1.100:8080/api/data",
        headers=None,
        params={"q": "1"},
        json=None,
        data=None,
    )


@pytest.mark.asyncio
async def test_discovery_failure(mock_discovery_client, mock_http_client):
    # Setup: discovery returns None
    mock_discovery_client.discover.return_value = None
    client = DiscoveryHttpClient(mock_discovery_client, http_client=mock_http_client)

    # Execute & Verify
    with pytest.raises(DiscoveryHttpClientError, match="No available instances"):
        await client.get("my-service", "/api/data")

    mock_http_client._request.assert_not_called()


@pytest.mark.asyncio
@patch("by_framework.util.discovery_http_client.sleep_async", new_callable=AsyncMock)
async def test_retry_on_status_code_with_node_switch(
    mock_sleep,
    mock_discovery_client,
    mock_http_client,
    fake_instance_1,
    fake_instance_2,
):
    # Setup: First discover returns instance 1, second returns instance 2
    mock_discovery_client.discover.side_effect = [fake_instance_1, fake_instance_2]

    # Setup: First request returns 502, second request returns 200
    error_response = HttpResponse(
        status_code=502, headers={}, data="", is_success=False
    )
    success_response = HttpResponse(
        status_code=200, headers={}, data="ok", is_success=True
    )
    mock_http_client._request.side_effect = [error_response, success_response]

    client = DiscoveryHttpClient(
        mock_discovery_client,
        http_client=mock_http_client,
        retry_config=RetryConfig(
            max_attempts=2, retry_on_status_codes=frozenset({502})
        ),
    )

    # Execute
    result = await client.post("my-service", "api/submit", json={"key": "value"})

    # Verify
    assert result.status_code == 200

    # Discovery should be called twice (node switch)
    assert mock_discovery_client.discover.call_count == 2

    # Request should be called twice on different URLs
    assert mock_http_client._request.call_count == 2
    mock_http_client._request.assert_has_calls(
        [
            call(
                method="POST",
                url="http://192.168.1.100:8080/api/submit",
                headers=None,
                params=None,
                json={"key": "value"},
                data=None,
            ),
            call(
                method="POST",
                url="http://192.168.1.101:8080/api/submit",
                headers=None,
                params=None,
                json={"key": "value"},
                data=None,
            ),
        ]
    )

    # It should have slept once between retries
    mock_sleep.assert_called_once()


@pytest.mark.asyncio
@patch("by_framework.util.discovery_http_client.sleep_async", new_callable=AsyncMock)
async def test_retry_on_network_error_exhausted(
    mock_sleep, mock_discovery_client, mock_http_client, fake_instance_1
):
    # Setup: Discovery always returns the same node
    mock_discovery_client.discover.return_value = fake_instance_1

    # Setup: Http client always raises network error
    mock_http_client._request.side_effect = HttpRequestError("Connection timeout")

    client = DiscoveryHttpClient(
        mock_discovery_client,
        http_client=mock_http_client,
        retry_config=RetryConfig(max_attempts=3),
    )

    # Execute & Verify
    with pytest.raises(
        DiscoveryHttpClientError, match="Service request failed after 3 attempts"
    ):
        await client.delete("my-service", "/api/item/1")

    assert mock_discovery_client.discover.call_count == 3
    assert mock_http_client._request.call_count == 3
    assert mock_sleep.call_count == 2


@pytest.mark.asyncio
async def test_non_retryable_status_code_returns_immediately(
    mock_discovery_client, mock_http_client, fake_instance_1
):
    """404 is not in retry_on_status_codes, so should return immediately."""
    mock_discovery_client.discover.return_value = fake_instance_1
    response = HttpResponse(
        status_code=404, headers={}, data="Not Found", is_success=False
    )
    mock_http_client._request.return_value = response

    client = DiscoveryHttpClient(
        mock_discovery_client,
        http_client=mock_http_client,
        retry_config=RetryConfig(max_attempts=3),
    )

    result = await client.get("my-service", "/api/missing")

    assert result.status_code == 404
    # Should only be called once (no retries)
    mock_discovery_client.discover.call_count == 1
    mock_http_client._request.call_count == 1


@pytest.mark.asyncio
async def test_put_method(mock_discovery_client, mock_http_client, fake_instance_1):
    mock_discovery_client.discover.return_value = fake_instance_1
    success_response = HttpResponse(
        status_code=200, headers={}, data={"updated": True}, is_success=True
    )
    mock_http_client._request.return_value = success_response

    client = DiscoveryHttpClient(
        discovery_client=mock_discovery_client,
        http_client=mock_http_client,
    )

    result = await client.put("my-service", "/api/data/1", json={"name": "test"})

    assert result.status_code == 200
    mock_http_client._request.assert_called_once_with(
        method="PUT",
        url="http://192.168.1.100:8080/api/data/1",
        headers=None,
        params=None,
        json={"name": "test"},
        data=None,
    )


@pytest.mark.asyncio
async def test_patch_method(mock_discovery_client, mock_http_client, fake_instance_1):
    mock_discovery_client.discover.return_value = fake_instance_1
    success_response = HttpResponse(
        status_code=200, headers={}, data={"patched": True}, is_success=True
    )
    mock_http_client._request.return_value = success_response

    client = DiscoveryHttpClient(
        discovery_client=mock_discovery_client,
        http_client=mock_http_client,
    )

    result = await client.patch("my-service", "/api/data/1", json={"op": "replace"})

    assert result.status_code == 200
    mock_http_client._request.assert_called_once_with(
        method="PATCH",
        url="http://192.168.1.100:8080/api/data/1",
        headers=None,
        params=None,
        json={"op": "replace"},
        data=None,
    )


@pytest.mark.asyncio
async def test_delete_method(mock_discovery_client, mock_http_client, fake_instance_1):
    mock_discovery_client.discover.return_value = fake_instance_1
    success_response = HttpResponse(
        status_code=204, headers={}, data="", is_success=True
    )
    mock_http_client._request.return_value = success_response

    client = DiscoveryHttpClient(
        discovery_client=mock_discovery_client,
        http_client=mock_http_client,
    )

    result = await client.delete("my-service", "/api/data/1")

    assert result.status_code == 204
    mock_http_client._request.assert_called_once_with(
        method="DELETE",
        url="http://192.168.1.100:8080/api/data/1",
        headers=None,
        params=None,
        json=None,
        data=None,
    )


@pytest.mark.asyncio
async def test_path_without_leading_slash(
    mock_discovery_client, mock_http_client, fake_instance_1
):
    """Path without leading slash should still be correctly joined."""
    mock_discovery_client.discover.return_value = fake_instance_1
    success_response = HttpResponse(
        status_code=200, headers={}, data="ok", is_success=True
    )
    mock_http_client._request.return_value = success_response

    client = DiscoveryHttpClient(
        discovery_client=mock_discovery_client,
        http_client=mock_http_client,
    )

    await client.get("my-service", "api/data")

    mock_http_client._request.assert_called_once_with(
        method="GET",
        url="http://192.168.1.100:8080/api/data",
        headers=None,
        params=None,
        json=None,
        data=None,
    )


@pytest.mark.asyncio
async def test_headers_passed_to_request(
    mock_discovery_client, mock_http_client, fake_instance_1
):
    mock_discovery_client.discover.return_value = fake_instance_1
    success_response = HttpResponse(
        status_code=200, headers={}, data="ok", is_success=True
    )
    mock_http_client._request.return_value = success_response

    client = DiscoveryHttpClient(
        discovery_client=mock_discovery_client,
        http_client=mock_http_client,
    )

    headers = {"Authorization": "Bearer token123", "X-Custom": "header"}
    await client.post("my-service", "/api/data", headers=headers, json={"k": "v"})

    mock_http_client._request.assert_called_once_with(
        method="POST",
        url="http://192.168.1.100:8080/api/data",
        headers=headers,
        params=None,
        json={"k": "v"},
        data=None,
    )


@pytest.mark.asyncio
async def test_context_manager_creates_http_client(mock_discovery_client):
    """When http_client is None, should create its own and manage lifecycle."""
    client = DiscoveryHttpClient(
        discovery_client=mock_discovery_client,
        retry_config=RetryConfig(max_attempts=2),
    )
    # http_client should be created internally
    assert client.http_client is not None
    assert client._owns_http_client is True


@pytest.mark.asyncio
async def test_context_manager_enter_exit(mock_discovery_client):
    """Async context manager __aenter__/__aexit__ should delegate to http_client."""
    client = DiscoveryHttpClient(
        discovery_client=mock_discovery_client,
    )

    # Mock the internal http_client's context manager methods
    client.http_client.__aenter__ = AsyncMock(return_value=client.http_client)
    client.http_client.__aexit__ = AsyncMock(return_value=None)

    # Should not raise when entering/exiting context
    async with client as c:
        assert c is client

    # http_client's __aenter__ and __aexit__ should have been called
    client.http_client.__aenter__.assert_called_once()
    client.http_client.__aexit__.assert_called_once()


@pytest.mark.asyncio
async def test_context_manager_enter_exit_with_external_client(mock_http_client):
    """When external http_client is provided, should not manage its lifecycle."""
    mock_dc = MagicMock(spec=DiscoveryClient)
    mock_dc.discover = AsyncMock(return_value=None)

    client = DiscoveryHttpClient(
        discovery_client=mock_dc,
        http_client=mock_http_client,
    )

    assert client._owns_http_client is False

    async with client as c:
        assert c is client

    # External http_client's __aenter__/__aexit__ should NOT be called
    mock_http_client.__aenter__.assert_not_called()
    mock_http_client.__aexit__.assert_not_called()


@pytest.mark.asyncio
async def test_default_retry_config(mock_discovery_client, mock_http_client):
    """When retry_config is None, should use default RetryConfig."""
    mock_discovery_client.discover.return_value = MagicMock(
        spec=ServiceInstance, id="inst", host="127.0.0.1", port=80
    )
    mock_http_client._request.return_value = HttpResponse(
        status_code=200, headers={}, data="ok", is_success=True
    )

    client = DiscoveryHttpClient(
        discovery_client=mock_discovery_client,
        http_client=mock_http_client,
        retry_config=None,
    )

    # Default RetryConfig has max_attempts=3 and proper defaults
    assert client.retry_config.max_attempts == 3
    assert 502 in client.retry_config.retry_on_status_codes


@pytest.mark.asyncio
async def test_request_uses_protocol_and_path_prefix(
    mock_discovery_client, mock_http_client
):
    mock_discovery_client.discover.return_value = ServiceInstance(
        id="inst1",
        protocol="https",
        host="api.service.local",
        port=8443,
        path_prefix="/v1",
    )
    mock_http_client._request.return_value = HttpResponse(
        status_code=200, headers={}, data="ok", is_success=True
    )

    client = DiscoveryHttpClient(
        discovery_client=mock_discovery_client,
        http_client=mock_http_client,
    )

    await client.get("my-service", "/users")

    mock_http_client._request.assert_called_once_with(
        method="GET",
        url="https://api.service.local:8443/v1/users",
        headers=None,
        params=None,
        json=None,
        data=None,
    )


@pytest.mark.asyncio
async def test_request_passes_custom_health_threshold(
    mock_discovery_client, mock_http_client, fake_instance_1
):
    mock_discovery_client.discover.return_value = fake_instance_1
    mock_http_client._request.return_value = HttpResponse(
        status_code=200, headers={}, data="ok", is_success=True
    )

    client = DiscoveryHttpClient(
        discovery_client=mock_discovery_client,
        http_client=mock_http_client,
        health_threshold_ms=RedisKeys.SD_NO_HEALTH_CHECK,
    )

    await client.get("my-service", "/health")

    mock_discovery_client.discover.assert_called_once_with(
        "my-service",
        health_threshold_ms=RedisKeys.SD_NO_HEALTH_CHECK,
    )
