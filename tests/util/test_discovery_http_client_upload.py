import io
from unittest.mock import AsyncMock, MagicMock

import pytest

from by_framework.core.discovery import DiscoveryClient, ServiceInstance
from by_framework.util.discovery_http_client import DiscoveryHttpClient
from by_framework.util.http_client import (ByHttpClient, HttpResponse, RetryConfig)


@pytest.fixture
def mock_discovery_client():
    client = MagicMock(spec=DiscoveryClient)
    client.discover = AsyncMock()
    return client


@pytest.fixture
def mock_http_client():
    client = MagicMock(spec=ByHttpClient)
    client._upload = AsyncMock()
    return client


@pytest.fixture
def fake_instance():
    return ServiceInstance(id="inst1", host="192.168.1.100", port=8080)


@pytest.mark.asyncio
async def test_discovery_upload_single_file(
    mock_discovery_client, mock_http_client, fake_instance, tmp_path
):
    mock_discovery_client.discover.return_value = fake_instance
    success_response = HttpResponse(
        status_code=200, headers={}, data={"status": "ok"}, is_success=True
    )
    mock_http_client._upload.return_value = success_response

    client = DiscoveryHttpClient(
        discovery_client=mock_discovery_client,
        http_client=mock_http_client,
    )

    file_path = tmp_path / "test.txt"
    file_path.write_bytes(b"hello")

    response = await client.upload(
        service_name="my-service",
        path="/upload",
        file_path=file_path,
        form_fields={"field1": "value1"},
    )

    assert response.is_success is True
    mock_http_client._upload.assert_called_once()
    called_args, _ = mock_http_client._upload.call_args
    assert called_args[0] == "http://192.168.1.100:8080/upload"
    parts = called_args[1]

    assert ("field1", "value1") in parts
    file_part = [p for p in parts if p[0] == "file"][0]
    assert file_part[1][0] == "test.txt"
    assert not isinstance(file_part[1][1], str)
    assert hasattr(file_part[1][1], "read")


@pytest.mark.asyncio
async def test_discovery_upload_multiple_files(
    mock_discovery_client, mock_http_client, fake_instance, tmp_path
):
    mock_discovery_client.discover.return_value = fake_instance
    success_response = HttpResponse(
        status_code=200, headers={}, data={"status": "ok"}, is_success=True
    )
    mock_http_client._upload.return_value = success_response

    client = DiscoveryHttpClient(
        discovery_client=mock_discovery_client,
        http_client=mock_http_client,
    )

    f1 = tmp_path / "f1.txt"
    f2 = tmp_path / "f2.txt"
    f1.write_bytes(b"1")
    f2.write_bytes(b"2")

    response = await client.upload_multiple(
        service_name="my-service",
        path="/upload",
        file_paths=[f1, f2],
    )

    assert response.is_success is True
    mock_http_client._upload.assert_called_once()
    called_args, _ = mock_http_client._upload.call_args
    parts = called_args[1]

    file_parts = [p for p in parts if p[0] == "file"]
    assert len(file_parts) == 2
    assert file_parts[0][1][0] == "f1.txt"
    assert file_parts[1][1][0] == "f2.txt"


@pytest.mark.asyncio
async def test_discovery_upload_with_stream(
    mock_discovery_client, mock_http_client, fake_instance
):
    mock_discovery_client.discover.return_value = fake_instance
    success_response = HttpResponse(
        status_code=200, headers={}, data={"status": "ok"}, is_success=True
    )
    mock_http_client._upload.return_value = success_response

    client = DiscoveryHttpClient(
        discovery_client=mock_discovery_client,
        http_client=mock_http_client,
    )

    response = await client.upload_with_stream(
        service_name="my-service",
        path="/upload",
        file_name="stream.bin",
        content=b"stream content",
    )

    assert response.is_success is True
    mock_http_client._upload.assert_called_once()
    called_args, _ = mock_http_client._upload.call_args
    parts = called_args[1]
    file_part = [p for p in parts if p[0] == "file"][0]
    assert file_part[1][0] == "stream.bin"
    assert file_part[1][2] == "application/octet-stream"
    assert isinstance(file_part[1][1], io.BytesIO)


@pytest.mark.asyncio
async def test_discovery_upload_retry_resets_stream_seeks(
    mock_discovery_client, mock_http_client, fake_instance
):
    mock_discovery_client.discover.return_value = fake_instance

    fail_response = HttpResponse(
        status_code=502, headers={}, data="Error", is_success=False
    )
    success_response = HttpResponse(
        status_code=200, headers={}, data="Success", is_success=True
    )

    async def mock_upload_handler(url, parts, headers=None):
        file_part = [p for p in parts if p[0] == "file"][0]
        file_stream = file_part[1][1]

        data = file_stream.read()
        assert data == b"test bytes"

        if mock_upload_handler.call_count == 0:
            mock_upload_handler.call_count += 1
            return fail_response
        return success_response

    mock_upload_handler.call_count = 0
    mock_http_client._upload.side_effect = mock_upload_handler

    client = DiscoveryHttpClient(
        discovery_client=mock_discovery_client,
        http_client=mock_http_client,
        retry_config=RetryConfig(
            max_attempts=2, retry_on_status_codes=frozenset({502})
        ),
    )

    response = await client.upload_with_stream(
        service_name="my-service",
        path="/upload",
        file_name="stream.bin",
        content=b"test bytes",
    )

    assert response.is_success is True
    assert mock_http_client._upload.call_count == 2
