"""Tests for file download support in ByHttpClient."""

from pathlib import Path

import httpx
import pytest

from by_framework.util.http_client import ByHttpClient, RetryConfig


@pytest.mark.asyncio
async def test_download_streams_response_to_file(tmp_path: Path):
    payload = b"binary payload for download"

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/files/archive.bin"
        return httpx.Response(
            status_code=200,
            content=payload,
            headers={"content-type": "application/octet-stream"},
            request=request,
        )

    target_path = tmp_path / "archive.bin"
    transport = httpx.MockTransport(handler)

    async with ByHttpClient(
        base_url="https://example.com",
        http_client=httpx.AsyncClient(
            transport=transport,
            base_url="https://example.com",
        ),
        retry_config=RetryConfig.no_retry(),
    ) as client:
        response = await client.download("/files/archive.bin", target_path)

    assert response.is_success is True
    assert response.status_code == 200
    assert response.data == str(target_path)
    assert target_path.read_bytes() == payload
