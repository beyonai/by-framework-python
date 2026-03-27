"""
File storage abstract interface.

Defines the contract for file storage backends.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class FileStorage(Protocol):
    """File storage abstract interface.

    Defines the contract for file storage backends.
    Implementations must provide all methods defined in this interface.
    """

    async def initialize(self) -> None: ...

    async def shutdown(self) -> None: ...

    async def write(self, path: str, content: str | bytes, encoding: str = "utf-8") -> None: ...

    async def read(self, path: str, encoding: str = "utf-8") -> str | bytes: ...

    async def delete(self, path: str) -> None: ...

    async def exists(self, path: str) -> bool: ...

    async def is_file(self, path: str) -> bool: ...

    async def is_dir(self, path: str) -> bool: ...

    async def list(self, path: str = "") -> list[str]: ...

    async def get_url(self, path: str, expires: int = 3600) -> str: ...
