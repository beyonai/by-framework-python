"""HTTP-related exception definitions."""

from by_framework.errors.base import FrameworkError


class HttpClientError(FrameworkError):
    """Base exception for HTTP client errors."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        response_body: str | None = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class HttpRequestError(HttpClientError):
    """Raised when an HTTP request fails after all retry attempts."""

    pass


class DiscoveryHttpClientError(HttpClientError):
    """Raised when service discovery fails to find a valid instance."""

    pass
