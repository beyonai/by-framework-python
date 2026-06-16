"""HTTP client with service discovery and node-switching retries."""

import asyncio
from pathlib import Path
from typing import Any, Dict, Optional

from by_framework.common.constants import RedisKeys
from by_framework.common.logger import get_logger
from by_framework.core.discovery import DiscoveryClient, ServiceInstance
from by_framework.errors import DiscoveryHttpClientError, HttpRequestError
from by_framework.util.http_client import (
    ByHttpClient,
    HttpResponse,
    RetryConfig,
    _calculate_delay,
)

logger = get_logger("by_framework.discovery_http")
sleep_async = asyncio.sleep


class DiscoveryHttpClient:
    """
    HTTP client that integrates with Service Discovery.

    Resolves service names to physical addresses dynamically and handles load balancing.
    Supports automatically switching to a different node upon request failures.
    """

    def __init__(
        self,
        discovery_client: DiscoveryClient,
        *,
        http_client: Optional[ByHttpClient] = None,
        retry_config: Optional[RetryConfig] = None,
        health_threshold_ms: int = RedisKeys.SD_DEFAULT_HEALTH_THRESHOLD_MS,
    ):
        """
        Args:
            discovery_client: Service discovery client instance.
            http_client: Underlying HTTP client. If provided, its internal
                retry mechanism might retry on the same node. It is highly
                recommended to let this class manage retries or pass an
                http_client with no_retry configured.
            retry_config: Configuration for node-switching retries.
            health_threshold_ms: Heartbeat age threshold used during discovery.
        """
        self.discovery_client = discovery_client
        # We enforce RetryConfig.no_retry() on the underlying ByHttpClient
        # if we create it, so retries stay discovery-aware.
        self._owns_http_client = http_client is None
        self.http_client = http_client or ByHttpClient(
            base_url="", retry_config=RetryConfig.no_retry()
        )
        self.retry_config = retry_config or RetryConfig()
        self.health_threshold_ms = (
            health_threshold_ms
            if health_threshold_ms > 0
            or health_threshold_ms == RedisKeys.SD_NO_HEALTH_CHECK
            else RedisKeys.SD_DEFAULT_HEALTH_THRESHOLD_MS
        )

    @staticmethod
    def _build_absolute_url(instance: ServiceInstance, path: str) -> str:
        """Build a request URL from the discovered instance metadata."""
        protocol = instance.protocol or "http"

        path_segments: list[str] = []
        if instance.path_prefix:
            path_segments.append(instance.path_prefix.strip("/"))
        if path:
            path_segments.append(path.strip("/"))

        suffix = "/".join(segment for segment in path_segments if segment)
        if suffix:
            return f"{protocol}://{instance.host}:{instance.port}/{suffix}"
        return f"{protocol}://{instance.host}:{instance.port}"

    async def __aenter__(self) -> "DiscoveryHttpClient":
        """Enter the underlying HTTP client context if owned internally."""
        if self._owns_http_client:
            await self.http_client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._owns_http_client:
            await self.http_client.__aexit__(exc_type, exc_val, exc_tb)

    async def _request_with_discovery(
        self,
        method: str,
        service_name: str,
        path: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        retry_count: int = 0,
        exclude_instances: Optional[set[str]] = None,
    ) -> HttpResponse:
        """Resolve a service instance and perform an HTTP request with retries."""
        exclude_instances = exclude_instances or set()

        # 1. Discover a healthy instance
        instance = await self.discovery_client.discover(
            service_name,
            health_threshold_ms=self.health_threshold_ms,
        )
        if not instance:
            raise DiscoveryHttpClientError(
                f"No available instances for service: {service_name}"
            )

        # Optional: If we want strict node-switching, we could avoid
        # the excluded instances.
        # However, discovery_client.discover doesn't support exclusion yet.
        # If there's only 1 node, we'd still have to retry on it. For now,
        # the balancer likely picks a different node often enough.

        # 2. Construct the absolute URL
        absolute_url = self._build_absolute_url(instance, path)
        attempt = retry_count + 1

        last_error: Optional[Exception] = None

        # 3. Perform the request
        try:
            logger.debug(
                "[%s] %s -> %s (attempt %d)",
                method.upper(),
                service_name,
                absolute_url,
                attempt,
            )
            # Use the underlying ByHttpClient internal _request.
            # We bypass its public wrappers because _request already
            # returns the HttpResponse shape this class expects.
            response = await self.http_client._request(
                method=method,
                url=absolute_url,
                headers=headers,
                params=params,
                json=json,
                data=data,
            )

            # If success or not a retryable status code, return directly
            if (
                response.is_success
                or response.status_code not in self.retry_config.retry_on_status_codes
            ):
                return response

            logger.warning(
                "[%s] %s -> %d, switching node and retrying...",
                method.upper(),
                absolute_url,
                response.status_code,
            )

        except HttpRequestError as e:
            # The internal client runs with no_retry(), so network
            # failures surface immediately and can trigger node switching.
            last_error = e
            logger.warning(
                "[%s] %s network error (attempt %d): %s",
                method.upper(),
                absolute_url,
                attempt,
                e,
            )

        # 4. Handle Retry
        if attempt < self.retry_config.max_attempts:
            exclude_instances.add(instance.id)
            delay = _calculate_delay(attempt, self.retry_config)
            logger.warning(
                "Node-switching retry in %.1fs for service %s", delay, service_name
            )
            await sleep_async(delay)
            return await self._request_with_discovery(
                method,
                service_name,
                path,
                headers=headers,
                params=params,
                json=json,
                data=data,
                retry_count=attempt,
                exclude_instances=exclude_instances,
            )

        if last_error:
            raise DiscoveryHttpClientError(
                "Service request failed after "
                f"{self.retry_config.max_attempts} attempts: {last_error}"
            ) from last_error

        raise DiscoveryHttpClientError(
            f"Service request failed after {self.retry_config.max_attempts} attempts. "
            f"Last status code: {response.status_code}"  # type: ignore
        )

    async def get(
        self,
        service_name: str,
        path: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> HttpResponse:
        return await self._request_with_discovery(
            "GET", service_name, path, headers=headers, params=params
        )

    async def post(
        self,
        service_name: str,
        path: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        json: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> HttpResponse:
        return await self._request_with_discovery(
            "POST", service_name, path, headers=headers, json=json, data=data
        )

    async def put(
        self,
        service_name: str,
        path: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        json: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> HttpResponse:
        return await self._request_with_discovery(
            "PUT", service_name, path, headers=headers, json=json, data=data
        )

    async def patch(
        self,
        service_name: str,
        path: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        json: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> HttpResponse:
        return await self._request_with_discovery(
            "PATCH", service_name, path, headers=headers, json=json, data=data
        )

    async def delete(
        self,
        service_name: str,
        path: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> HttpResponse:
        return await self._request_with_discovery(
            "DELETE", service_name, path, headers=headers, params=params
        )

    async def download(
        self,
        service_name: str,
        path: str,
        destination: str | Path,
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        retry_count: int = 0,
    ) -> HttpResponse:
        """Download a file from a discovered service instance."""
        exclude_instances: set[str] = set()

        instance = await self.discovery_client.discover(
            service_name,
            health_threshold_ms=self.health_threshold_ms,
        )
        if not instance:
            raise DiscoveryHttpClientError(
                f"No available instances for service: {service_name}"
            )

        absolute_url = self._build_absolute_url(instance, path)
        attempt = retry_count + 1
        last_error: Optional[Exception] = None

        try:
            logger.debug(
                "[DOWNLOAD] %s -> %s (attempt %d)",
                service_name,
                absolute_url,
                attempt,
            )
            response = await self.http_client.download(
                absolute_url,
                destination,
                headers=headers,
                params=params,
            )

            if (
                response.is_success
                or response.status_code not in self.retry_config.retry_on_status_codes
            ):
                return response

            logger.warning(
                "[DOWNLOAD] %s -> %d, switching node and retrying...",
                absolute_url,
                response.status_code,
            )
        except HttpRequestError as e:
            last_error = e
            logger.warning(
                "[DOWNLOAD] %s network error (attempt %d): %s",
                absolute_url,
                attempt,
                e,
            )

        if attempt < self.retry_config.max_attempts:
            exclude_instances.add(instance.id)
            delay = _calculate_delay(attempt, self.retry_config)
            logger.warning(
                "Node-switching retry in %.1fs for service %s", delay, service_name
            )
            await sleep_async(delay)
            return await self.download(
                service_name,
                path,
                destination,
                headers=headers,
                params=params,
                retry_count=attempt,
            )

        if last_error:
            raise DiscoveryHttpClientError(
                "Service download failed after "
                f"{self.retry_config.max_attempts} attempts: {last_error}"
            ) from last_error

        raise DiscoveryHttpClientError(
            f"Service download failed after {self.retry_config.max_attempts} attempts. "
            f"Last status code: {response.status_code}"  # type: ignore
        )

    # ─────────────────────────────────────────────────────────────────────────────
    # File Upload Methods (with service discovery)
    # ─────────────────────────────────────────────────────────────────────────────

    async def upload(
        self,
        service_name: str,
        path: str,
        file_path: str | Path,
        *,
        file_field: str = "file",
        headers: Optional[Dict[str, str]] = None,
        form_fields: Optional[Dict[str, str]] = None,
    ) -> HttpResponse:
        """
        Upload a file using multipart/form-data with service discovery.

        Args:
            service_name: Service name for discovery
            path: URL path
            file_path: Path to the file to upload
            file_field: Name of the file field (default: "file")
            headers: Optional headers
            form_fields: Optional form fields

        Returns:
            HttpResponse from the server
        """
        p = Path(file_path)
        parts: list[tuple[str, Any]] = []
        if form_fields:
            for key, value in form_fields.items():
                parts.append((key, value))

        with open(p, "rb") as f:
            parts.append((file_field, (p.name, f)))
            return await self._upload_with_discovery(
                service_name, path, parts, headers=headers
            )

    async def upload_multiple(
        self,
        service_name: str,
        path: str,
        file_paths: list[str | Path],
        *,
        file_field: str = "file",
        headers: Optional[Dict[str, str]] = None,
        form_fields: Optional[Dict[str, str]] = None,
    ) -> HttpResponse:
        """
        Upload multiple files using multipart/form-data with service discovery.

        Args:
            service_name: Service name for discovery
            path: URL path
            file_paths: List of paths to the files to upload
            file_field: Name of the file field (default: "file")
            headers: Optional headers
            form_fields: Optional form fields

        Returns:
            HttpResponse from the server
        """
        from contextlib import ExitStack

        parts: list[tuple[str, Any]] = []
        if form_fields:
            for key, value in form_fields.items():
                parts.append((key, value))

        with ExitStack() as stack:
            for fp in file_paths:
                p = Path(fp)
                f = stack.enter_context(open(p, "rb"))
                parts.append((file_field, (p.name, f)))
            return await self._upload_with_discovery(
                service_name, path, parts, headers=headers
            )

    async def upload_with_stream(
        self,
        service_name: str,
        path: str,
        file_name: str,
        content: bytes,
        *,
        file_field: str = "file",
        content_type: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        form_fields: Optional[Dict[str, str]] = None,
    ) -> HttpResponse:
        """
        Upload a file from bytes using multipart/form-data with service discovery.

        Args:
            service_name: Service name for discovery
            path: URL path
            file_name: Name of the file
            content: File content as bytes
            file_field: Name of the file field (default: "file")
            content_type: MIME type of the file
            headers: Optional headers
            form_fields: Optional form fields

        Returns:
            HttpResponse from the server
        """
        import io

        parts: list[tuple[str, Any]] = []
        if form_fields:
            for key, value in form_fields.items():
                parts.append((key, value))
        parts.append(
            (
                file_field,
                (
                    file_name,
                    io.BytesIO(content),
                    content_type or "application/octet-stream",
                ),
            )
        )
        return await self._upload_with_discovery(
            service_name, path, parts, headers=headers
        )

    async def _upload_with_discovery(
        self,
        service_name: str,
        path: str,
        parts: list[tuple[str, Any]],
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        retry_count: int = 0,
        exclude_instances: Optional[set[str]] = None,
    ) -> HttpResponse:
        """Upload with service discovery and node-switching retries."""
        exclude_instances = exclude_instances or set()

        instance = await self.discovery_client.discover(
            service_name,
            health_threshold_ms=self.health_threshold_ms,
        )
        if not instance:
            raise DiscoveryHttpClientError(
                f"No available instances for service: {service_name}"
            )

        absolute_url = self._build_absolute_url(instance, path)
        attempt = retry_count + 1
        last_error: Optional[Exception] = None

        try:
            logger.debug(
                "[UPLOAD] %s -> %s (attempt %d, parts: %d)",
                service_name,
                absolute_url,
                attempt,
                len(parts),
            )
            response = await self.http_client._upload(
                absolute_url,
                parts,
                headers=headers,
            )

            if (
                response.is_success
                or response.status_code not in self.retry_config.retry_on_status_codes
            ):
                return response

            logger.warning(
                "[UPLOAD] %s -> %d, switching node and retrying...",
                absolute_url,
                response.status_code,
            )
        except HttpRequestError as e:
            last_error = e
            logger.warning(
                "[UPLOAD] %s network error (attempt %d): %s",
                absolute_url,
                attempt,
                e,
            )

        if attempt < self.retry_config.max_attempts:
            exclude_instances.add(instance.id)
            delay = _calculate_delay(attempt, self.retry_config)
            logger.warning(
                "Node-switching retry in %.1fs for service %s", delay, service_name
            )

            # Reset seek position for any file-like objects in parts before retrying
            for _, val in parts:
                if isinstance(val, tuple):
                    for item in val:
                        if hasattr(item, "seek") and callable(item.seek):
                            try:
                                item.seek(0)
                            except Exception:
                                pass
                elif hasattr(val, "seek") and callable(val.seek):
                    try:
                        val.seek(0)
                    except Exception:
                        pass

            await sleep_async(delay)
            return await self._upload_with_discovery(
                service_name,
                path,
                parts,
                headers=headers,
                params=params,
                retry_count=attempt,
                exclude_instances=exclude_instances,
            )

        if last_error:
            raise DiscoveryHttpClientError(
                "Service upload failed after "
                f"{self.retry_config.max_attempts} attempts: {last_error}"
            ) from last_error

        raise DiscoveryHttpClientError(
            f"Service upload failed after {self.retry_config.max_attempts} attempts. "
            f"Last status code: {response.status_code}"  # type: ignore
        )
