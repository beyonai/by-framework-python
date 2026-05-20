"""HTTP client wrapper with retry, timeout, and error handling."""

from __future__ import annotations

import asyncio
import base64
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

from by_framework.errors import HttpClientError, HttpRequestError

if TYPE_CHECKING:
    pass

logger = logging.getLogger("by_framework.http")


# ─────────────────────────────────────────────────────────────────────────────
# Authentication
# ─────────────────────────────────────────────────────────────────────────────


class Auth(ABC):
    """Abstract base class for authentication strategies."""

    @abstractmethod
    def apply(self, request: httpx.Request) -> None:
        """Apply authentication to the outgoing request."""
        raise NotImplementedError


class NoAuth(Auth):
    """No authentication."""

    def apply(self, request: httpx.Request) -> None:
        pass


class ApiKeyAuth(Auth):
    """API key authentication (header or query param)."""

    def __init__(
        self, key: str, value: str, *, in_header: bool = True, prefix: str = ""
    ):
        self.key = key
        self.value = value
        self.in_header = in_header
        self.prefix = prefix

    def apply(self, request: httpx.Request) -> None:
        if self.in_header:
            header_value = f"{self.prefix} {self.value}" if self.prefix else self.value
            request.headers[self.key] = header_value
        else:
            request.url = request.url.copy_merge_params({self.key: self.value})


class BearerAuth(Auth):
    """Bearer token authentication (JWT, OAuth2 tokens)."""

    def __init__(self, token: str):
        self.token = token

    def apply(self, request: httpx.Request) -> None:
        request.headers["Authorization"] = f"Bearer {self.token}"


class BasicAuth(Auth):
    """Basic authentication (username/password)."""

    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password

    def apply(self, request: httpx.Request) -> None:
        credentials = f"{self.username}:{self.password}"
        encoded = base64.b64encode(credentials.encode()).decode()
        request.headers["Authorization"] = f"Basic {encoded}"


def _resolve_auth(auth: Auth | str | None) -> Auth:
    """Resolve auth parameter to an Auth instance."""
    if auth is None or isinstance(auth, Auth):
        return auth or NoAuth()
    if isinstance(auth, str):
        return BearerAuth(auth)
    raise TypeError(f"Unsupported auth type: {type(auth).__name__}")


class HttpRetryExhaustedError(HttpClientError):
    """Raised when all retry attempts are exhausted."""

    pass


@dataclass(frozen=True)
class HttpResponse:
    """Wrapper for HTTP response with typed data."""

    status_code: int
    headers: dict[str, str]
    data: Any
    is_success: bool


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    initial_delay: float = 0.5
    max_delay: float = 30.0
    backoff_multiplier: float = 2.0
    retry_on_status_codes: frozenset[int] = frozenset({429, 500, 502, 503, 504})

    @classmethod
    def no_retry(cls) -> RetryConfig:
        """Create a config that disables retries."""
        return cls(max_attempts=1, retry_on_status_codes=frozenset())


def _calculate_delay(attempt: int, config: RetryConfig) -> float:
    """Calculate delay for given attempt using exponential backoff."""
    delay = config.initial_delay * (config.backoff_multiplier ** (attempt - 1))
    return min(delay, config.max_delay)


class ByHttpClient:
    """
    Async HTTP client wrapper with automatic retry, timeout, and logging.

    Features:
    - Configurable retry with exponential backoff
    - Automatic timeout handling
    - Structured error responses
    - Request/response logging
    - Pluggable authentication (API Key, Bearer, Basic, OAuth2)

    Example:
        async with ByHttpClient(base_url="https://api.example.com") as client:
            response = await client.get("/users/123")
            if response.is_success:
                print(response.data)

        # With authentication
        async with ByHttpClient(
            base_url="https://api.example.com",
            auth=BearerAuth("my-jwt-token"),
        ) as client:
            response = await client.get("/protected")
    """

    def __init__(
        self,
        base_url: str,
        *,
        auth: Auth | str | None = None,
        timeout: float = 30.0,
        headers: dict[str, str] | None = None,
        retry_config: RetryConfig | None = None,
        http_client: httpx.AsyncClient | None = None,
    ):
        self._base_url = base_url.rstrip("/")
        self._default_headers = dict(headers or {})
        self._auth = _resolve_auth(auth)
        self._timeout = httpx.Timeout(timeout, connect=timeout)
        self._retry_config = retry_config or RetryConfig()
        self._client = http_client
        self._owns_client = http_client is None

    async def __aenter__(self) -> ByHttpClient:
        if self._client is None:

            async def _auth_middleware(request: httpx.Request) -> None:
                self._auth.apply(request)

            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=self._timeout,
                headers=self._default_headers,
                event_hooks={"request": [_auth_middleware]},
            )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        retry_count: int = 0,
    ) -> HttpResponse:
        """Execute HTTP request with retry logic."""
        if self._client is None:
            raise HttpClientError(
                "ByHttpClient not initialized. Use async context manager."
            )

        request_headers = dict(self._default_headers)
        if headers:
            request_headers.update(headers)

        last_error: Exception | None = None
        attempt = retry_count + 1

        try:
            logger.debug("[%s] %s (attempt %d)", method.upper(), url, attempt)
            response = await self._client.request(
                method=method,
                url=url,
                headers=request_headers,
                params=params,
                json=json,
                data=data,
            )

            if response.is_success:
                logger.debug("[%s] %s -> %d", method.upper(), url, response.status_code)
                return await self._parse_response(response)

            if (
                response.status_code in self._retry_config.retry_on_status_codes
                and attempt < self._retry_config.max_attempts
            ):
                delay = _calculate_delay(attempt, self._retry_config)
                logger.warning(
                    "[%s] %s -> %d, retrying in %.1fs",
                    method.upper(),
                    url,
                    response.status_code,
                    delay,
                )
                await asyncio.sleep(delay)
                return await self._request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    json=json,
                    data=data,
                    retry_count=attempt,
                )

            logger.error("[%s] %s -> %d", method.upper(), url, response.status_code)
            return await self._parse_response(response)

        except httpx.TimeoutException as e:
            last_error = e
            logger.warning("[%s] %s timeout (attempt %d)", method.upper(), url, attempt)
        except httpx.ConnectError as e:
            last_error = e
            logger.warning(
                "[%s] %s connection error (attempt %d): %s",
                method.upper(),
                url,
                attempt,
                e,
            )
        except httpx.HTTPError as e:
            last_error = e
            logger.warning(
                "[%s] %s HTTP error (attempt %d): %s", method.upper(), url, attempt, e
            )

        if attempt < self._retry_config.max_attempts:
            delay = _calculate_delay(attempt, self._retry_config)
            logger.warning(
                "Retrying in %.1fs after %s", delay, type(last_error).__name__
            )
            await asyncio.sleep(delay)
            return await self._request(
                method,
                url,
                headers=headers,
                params=params,
                json=json,
                data=data,
                retry_count=attempt,
            )

        raise HttpRequestError(
            "Request failed after "
            f"{self._retry_config.max_attempts} attempts: {last_error}",
            status_code=getattr(last_error, "response", None)
            and getattr(last_error.response, "status_code", None),
        )

    async def _download(
        self,
        method: str,
        url: str,
        destination: str | Path,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        retry_count: int = 0,
    ) -> HttpResponse:
        """Stream a response body into a local file with retry support."""
        if self._client is None:
            raise HttpClientError(
                "ByHttpClient not initialized. Use async context manager."
            )

        request_headers = dict(self._default_headers)
        if headers:
            request_headers.update(headers)

        target_path = Path(destination)
        target_path.parent.mkdir(parents=True, exist_ok=True)

        last_error: Exception | None = None
        attempt = retry_count + 1

        try:
            logger.debug(
                "[%s] %s -> %s (attempt %d)",
                method.upper(),
                url,
                target_path,
                attempt,
            )
            async with self._client.stream(
                method=method,
                url=url,
                headers=request_headers,
                params=params,
            ) as response:
                if response.is_success:
                    with target_path.open("wb") as file_obj:
                        async for chunk in response.aiter_bytes():
                            file_obj.write(chunk)
                    logger.debug(
                        "[%s] %s -> %d",
                        method.upper(),
                        url,
                        response.status_code,
                    )
                    return HttpResponse(
                        status_code=response.status_code,
                        headers=dict(response.headers),
                        data=str(target_path),
                        is_success=True,
                    )

                if (
                    response.status_code in self._retry_config.retry_on_status_codes
                    and attempt < self._retry_config.max_attempts
                ):
                    delay = _calculate_delay(attempt, self._retry_config)
                    logger.warning(
                        "[%s] %s -> %d, retrying download in %.1fs",
                        method.upper(),
                        url,
                        response.status_code,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    return await self._download(
                        method,
                        url,
                        destination=target_path,
                        headers=headers,
                        params=params,
                        retry_count=attempt,
                    )

                logger.error("[%s] %s -> %d", method.upper(), url, response.status_code)
                return HttpResponse(
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    data=response.text,
                    is_success=False,
                )

        except httpx.TimeoutException as e:
            last_error = e
            logger.warning("[%s] %s timeout (attempt %d)", method.upper(), url, attempt)
        except httpx.ConnectError as e:
            last_error = e
            logger.warning(
                "[%s] %s connection error (attempt %d): %s",
                method.upper(),
                url,
                attempt,
                e,
            )
        except httpx.HTTPError as e:
            last_error = e
            logger.warning(
                "[%s] %s HTTP error (attempt %d): %s", method.upper(), url, attempt, e
            )

        if attempt < self._retry_config.max_attempts:
            delay = _calculate_delay(attempt, self._retry_config)
            logger.warning(
                "Retrying download in %.1fs after %s",
                delay,
                type(last_error).__name__,
            )
            await asyncio.sleep(delay)
            return await self._download(
                method,
                url,
                destination=target_path,
                headers=headers,
                params=params,
                retry_count=attempt,
            )

        raise HttpRequestError(
            "Download failed after "
            f"{self._retry_config.max_attempts} attempts: {last_error}",
            status_code=getattr(last_error, "response", None)
            and getattr(last_error.response, "status_code", None),
        )

    async def _parse_response(self, response: httpx.Response) -> HttpResponse:
        """Parse httpx response into HttpResponse."""
        content_type = response.headers.get("content-type", "")

        data: Any = None
        if content_type.startswith("application/json"):
            try:
                data = response.json()
            except ValueError:
                data = response.text
        else:
            data = response.text

        return HttpResponse(
            status_code=response.status_code,
            headers=dict(response.headers),
            data=data,
            is_success=200 <= response.status_code < 300,
        )

    async def get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
    ) -> HttpResponse:
        """Send GET request."""
        return await self._request("GET", url, headers=headers, params=params)

    async def post(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> HttpResponse:
        """Send POST request."""
        return await self._request("POST", url, headers=headers, json=json, data=data)

    async def put(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> HttpResponse:
        """Send PUT request."""
        return await self._request("PUT", url, headers=headers, json=json, data=data)

    async def patch(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> HttpResponse:
        """Send PATCH request."""
        return await self._request("PATCH", url, headers=headers, json=json, data=data)

    async def delete(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
    ) -> HttpResponse:
        """Send DELETE request."""
        return await self._request("DELETE", url, headers=headers, params=params)

    async def download(
        self,
        url: str,
        destination: str | Path,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
    ) -> HttpResponse:
        """Download a remote file to a local destination."""
        return await self._download(
            "GET",
            url,
            destination=destination,
            headers=headers,
            params=params,
        )

    # ─────────────────────────────────────────────────────────────────────────────
    # File Upload Methods
    # ─────────────────────────────────────────────────────────────────────────────

    async def upload(
        self,
        url: str,
        file_path: str | Path,
        *,
        file_field: str = "file",
        headers: dict[str, str] | None = None,
        form_fields: dict[str, str] | None = None,
    ) -> HttpResponse:
        """
        Upload a file using multipart/form-data.

        Args:
            url: Upload URL
            file_path: Path to the file to upload
            file_field: Name of the file field (default: "file")
            headers: Optional headers
            form_fields: Optional form fields

        Returns:
            HttpResponse from the server
        """
        path = Path(file_path)
        parts: list[tuple[str, Any]] = []
        if form_fields:
            for key, value in form_fields.items():
                parts.append((key, value))

        with open(path, "rb") as f:
            parts.append((file_field, (path.name, f)))
            return await self._upload(url, parts, headers=headers)

    async def upload_multiple(
        self,
        url: str,
        file_paths: list[str | Path],
        *,
        file_field: str = "file",
        headers: dict[str, str] | None = None,
        form_fields: dict[str, str] | None = None,
    ) -> HttpResponse:
        """
        Upload multiple files using multipart/form-data.

        Args:
            url: Upload URL
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
                path = Path(fp)
                f = stack.enter_context(open(path, "rb"))
                parts.append((file_field, (path.name, f)))
            return await self._upload(url, parts, headers=headers)

    async def upload_with_stream(
        self,
        url: str,
        file_name: str,
        content: bytes,
        *,
        file_field: str = "file",
        content_type: str | None = None,
        headers: dict[str, str] | None = None,
        form_fields: dict[str, str] | None = None,
    ) -> HttpResponse:
        """
        Upload a file from a stream/bytes using multipart/form-data.

        Args:
            url: Upload URL
            file_name: Name of the file
            content: File content as bytes
            file_field: Name of the file field (default: "file")
            content_type: MIME type of the file
            headers: Optional headers
            form_fields: Optional form fields

        Returns:
            HttpResponse from the server
        """
        parts: list[tuple[str, Any]] = []
        if form_fields:
            for key, value in form_fields.items():
                parts.append((key, value))
        import io

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
        return await self._upload(url, parts, headers=headers)

    async def _upload(
        self,
        url: str,
        parts: list[tuple[str, Any]],
        *,
        headers: dict[str, str] | None = None,
    ) -> HttpResponse:
        """Execute multipart/form-data upload request."""
        if self._client is None:
            raise HttpClientError(
                "ByHttpClient not initialized. Use async context manager."
            )

        request_headers = dict(self._default_headers)
        if headers:
            request_headers.update(headers)

        logger.debug("[POST] %s (multipart upload, %d parts)", url, len(parts))

        # Convert non-file parts (e.g. standard form fields) to (None, value)
        # to ensure httpx renders them without filename attribute,
        # and avoid AsyncClient sync/async conflicts.
        formatted_parts: list[tuple[str, Any]] = []
        for key, val in parts:
            if isinstance(val, tuple):
                formatted_parts.append((key, val))
            elif hasattr(val, "read") and callable(val.read):
                formatted_parts.append((key, val))
            else:
                formatted_parts.append((key, (None, str(val))))

        try:
            response = await self._client.request(
                method="POST",
                url=url,
                headers=request_headers,
                files=formatted_parts,
            )

            logger.debug(
                "[POST] %s -> %d",
                url,
                response.status_code,
            )
            return await self._parse_response(response)

        except httpx.TimeoutException as e:
            logger.warning("[POST] %s timeout: %s", url, e)
            raise HttpRequestError(
                f"Upload timeout: {e}",
                status_code=None,
            ) from e
        except httpx.ConnectError as e:
            logger.warning("[POST] %s connection error: %s", url, e)
            raise HttpRequestError(
                f"Upload connection error: {e}",
                status_code=None,
            ) from e
        except httpx.HTTPError as e:
            logger.warning("[POST] %s HTTP error: %s", url, e)
            raise HttpRequestError(
                f"Upload error: {e}",
                status_code=getattr(e, "response", None)
                and getattr(e.response, "status_code", None),
            ) from e
