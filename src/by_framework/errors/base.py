"""Base exception class definitions."""


class FrameworkError(Exception):
    """By-Framework base exception class for all framework-level exceptions."""

    def __init__(
        self, message: str, cause: Exception | None = None, code: str | None = None
    ):
        super().__init__(message)
        self._cause = cause
        self.code = code or type(self).__name__

    @property
    def cause(self) -> Exception | None:
        return self._cause
