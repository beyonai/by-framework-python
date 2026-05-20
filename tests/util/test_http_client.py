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


@pytest.mark.asyncio
async def test_upload_single_file(tmp_path: Path):
    file_content = b"my unique file content"
    file_path = tmp_path / "test_upload.txt"
    file_path.write_bytes(file_content)

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/upload"
        req_body = await request.aread()

        # 验证是否上传了真正的文件内容
        assert file_content in req_body
        assert b'filename="test_upload.txt"' in req_body
        assert b'name="file"' in req_body
        assert b'name="field1"' in req_body
        assert b"value1" in req_body

        return httpx.Response(
            status_code=200,
            json={"status": "uploaded"},
            request=request,
        )

    transport = httpx.MockTransport(handler)

    async with ByHttpClient(
        base_url="https://example.com",
        http_client=httpx.AsyncClient(
            transport=transport,
            base_url="https://example.com",
        ),
        retry_config=RetryConfig.no_retry(),
    ) as client:
        response = await client.upload(
            "/upload",
            file_path,
            file_field="file",
            form_fields={"field1": "value1"},
        )

    assert response.is_success is True
    assert response.data == {"status": "uploaded"}


@pytest.mark.asyncio
async def test_upload_multiple_files(tmp_path: Path):
    file1_content = b"content of file 1"
    file2_content = b"content of file 2"
    file1_path = tmp_path / "file1.txt"
    file2_path = tmp_path / "file2.txt"
    file1_path.write_bytes(file1_content)
    file2_path.write_bytes(file2_content)

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        req_body = await request.aread()

        # 验证两个文件的内容和文件名都被正确发送
        assert file1_content in req_body
        assert file2_content in req_body
        assert b'filename="file1.txt"' in req_body
        assert b'filename="file2.txt"' in req_body
        assert b'name="file"' in req_body

        return httpx.Response(
            status_code=200,
            json={"status": "uploaded_multiple"},
            request=request,
        )

    transport = httpx.MockTransport(handler)

    async with ByHttpClient(
        base_url="https://example.com",
        http_client=httpx.AsyncClient(
            transport=transport,
            base_url="https://example.com",
        ),
        retry_config=RetryConfig.no_retry(),
    ) as client:
        response = await client.upload_multiple(
            "/upload",
            [file1_path, file2_path],
            file_field="file",
        )

    assert response.is_success is True
    assert response.data == {"status": "uploaded_multiple"}


@pytest.mark.asyncio
async def test_upload_with_stream():
    stream_content = b"stream upload data"
    file_name = "stream_file.bin"

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        req_body = await request.aread()

        assert stream_content in req_body
        assert f'filename="{file_name}"'.encode() in req_body
        assert b'name="file"' in req_body

        return httpx.Response(
            status_code=200,
            json={"status": "uploaded_stream"},
            request=request,
        )

    transport = httpx.MockTransport(handler)

    async with ByHttpClient(
        base_url="https://example.com",
        http_client=httpx.AsyncClient(
            transport=transport,
            base_url="https://example.com",
        ),
        retry_config=RetryConfig.no_retry(),
    ) as client:
        response = await client.upload_with_stream(
            "/upload",
            file_name,
            stream_content,
            content_type="application/octet-stream",
        )

    assert response.is_success is True
    assert response.data == {"status": "uploaded_stream"}
