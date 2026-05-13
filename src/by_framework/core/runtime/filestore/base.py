"""
File storage abstract interface.

Defines the contract for file storage backends.
"""

from __future__ import annotations

from typing import Literal, NotRequired, Protocol, TypedDict, runtime_checkable

FileContentType = Literal["original", "markdown"]
FileReadKind = Literal["text", "binary", "image", "error"]


class FileReadResult(TypedDict):
    """Structured read result returned by storage backends."""

    kind: FileReadKind
    path: str
    absolute_path: NotRequired[str]
    content: str | bytes
    media_type: NotRequired[str]
    error: NotRequired[str]


class FilePathEntry(TypedDict):
    """Structured path entry returned by storage backends."""

    path: str
    absolute_path: NotRequired[str]


class FileSearchMatch(TypedDict):
    """Single grep match returned by storage backends."""

    path: str
    absolute_path: NotRequired[str]
    line_number: int
    content: str


class FileSearchResult(TypedDict):
    """Structured search result returned by storage backends."""

    matches: list[FileSearchMatch]
    error: NotRequired[str]


class FileGlobResult(TypedDict):
    """Structured glob result returned by storage backends."""

    paths: list[FilePathEntry]
    error: NotRequired[str]


class FileWriteResult(TypedDict):
    """Structured write result returned by storage backends."""

    path: str
    absolute_path: NotRequired[str]
    bytes_written: int
    error: NotRequired[str]


class FileEditResult(TypedDict):
    """Structured edit result returned by storage backends."""

    path: str
    absolute_path: NotRequired[str]
    occurrences: int
    error: NotRequired[str]


class FileDeleteResult(TypedDict):
    """Structured delete result returned by storage backends."""

    path: str
    absolute_path: NotRequired[str]
    deleted: bool
    error: NotRequired[str]


class FileListResult(TypedDict):
    """Structured list result returned by storage backends."""

    paths: list[FilePathEntry]
    error: NotRequired[str]


@runtime_checkable
class FileStorage(Protocol):
    """File storage abstract interface.

    Defines the contract for file storage backends.
    Implementations must provide all methods defined in this interface.
    """

    async def initialize(self) -> None:
        ...

    async def shutdown(self) -> None:
        ...

    async def write(
        self, path: str, content: str | bytes, encoding: str = "utf-8"
    ) -> FileWriteResult:
        ...

    async def read(
        self,
        path: str,
        encoding: str = "utf-8",
        *,
        offset: int = 0,
        limit: int | None = None,
        content_type: FileContentType = "markdown",
    ) -> FileReadResult:
        ...

    async def delete(self, path: str) -> FileDeleteResult:
        ...

    async def list(self, path: str = "") -> FileListResult:
        ...

    async def grep(self, pattern: str, glob_pattern: str = "*") -> FileSearchResult:
        ...

    async def edit(
        self,
        path: str,
        old_string: str,
        new_string: str,
        replace_all: bool = False,
        encoding: str = "utf-8",
    ) -> FileEditResult:
        ...

    async def glob(self, pattern: str) -> FileGlobResult:
        ...
