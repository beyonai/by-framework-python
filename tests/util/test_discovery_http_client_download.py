"""Tests for file download support in DiscoveryHttpClient."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from by_framework.common.constants import RedisKeys
from by_framework.core.discovery import DiscoveryClient, ServiceInstance
from by_framework.util.discovery_http_client import DiscoveryHttpClient
from by_framework.util.http_client import ByHttpClient, HttpResponse


@pytest.mark.asyncio
async def test_download_passes_resolved_url_and_target_path(tmp_path: Path):
    discovery_client = MagicMock(spec=DiscoveryClient)
    discovery_client.discover = AsyncMock(
        return_value=ServiceInstance(
            id="inst1",
            protocol="https",
            host="10.0.0.8",
            port=9000,
            path_prefix="/files",
        )
    )

    http_client = MagicMock(spec=ByHttpClient)
    http_client.download = AsyncMock(
        return_value=HttpResponse(
            status_code=200,
            headers={},
            data=str(tmp_path / "report.csv"),
            is_success=True,
        )
    )

    client = DiscoveryHttpClient(discovery_client, http_client=http_client)
    target_path = tmp_path / "report.csv"

    response = await client.download(
        "report-service",
        "/exports/report.csv",
        target_path,
        headers={"Accept": "text/csv"},
        params={"date": "2026-04-11"},
    )

    assert response.is_success is True
    discovery_client.discover.assert_awaited_once_with(
        "report-service",
        health_threshold_ms=RedisKeys.SD_DEFAULT_HEALTH_THRESHOLD_MS,
    )
    http_client.download.assert_awaited_once_with(
        "https://10.0.0.8:9000/files/exports/report.csv",
        target_path,
        headers={"Accept": "text/csv"},
        params={"date": "2026-04-11"},
    )
