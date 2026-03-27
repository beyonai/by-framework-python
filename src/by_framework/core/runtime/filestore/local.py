"""
Local file system storage implementation.

Provides file storage backed by local filesystem.
"""

import os
import shutil
from pathlib import Path

from .base import FileStorage


class LocalFileStorage(FileStorage):
    """Local filesystem storage implementation.

    Provides file storage backed by local filesystem.
    Suitable for single-node deployments.
    """

    def __init__(self, base_dir: str):
        """Initialize local storage.

        Args:
            base_dir: Local storage root directory
        """
        self.base_dir = base_dir

    async def initialize(self) -> None:
        """Initialize the storage backend."""
        os.makedirs(self.base_dir, exist_ok=True)

    async def shutdown(self) -> None:
        """Shutdown the storage backend."""
        pass  # No cleanup needed for local filesystem

    async def write(self, path: str, content: str | bytes, encoding: str = "utf-8") -> None:
        """Write content to a file.

        Args:
            path: File path (relative to base_dir)
            content: File content
            encoding: Text encoding (only effective for str type)
        """
        full_path = self._get_full_path(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(content, str):
            full_path.write_text(content, encoding=encoding)
        else:
            full_path.write_bytes(content)

    async def read(self, path: str, encoding: str = "utf-8") -> str | bytes:
        """Read file content.

        Args:
            path: File path (relative to base_dir)
            encoding: Text encoding

        Returns:
            File content (str or bytes)
        """
        full_path = self._get_full_path(path)
        if encoding:
            return full_path.read_text(encoding=encoding)
        return full_path.read_bytes()

    async def delete(self, path: str) -> None:
        """Delete a file or directory.

        Args:
            path: File or directory path (relative to base_dir)
        """
        full_path = self._get_full_path(path)
        if full_path.is_file():
            full_path.unlink()
        elif full_path.is_dir():
            shutil.rmtree(full_path)

    async def exists(self, path: str) -> bool:
        """Check if path exists.

        Args:
            path: File or directory path (relative to base_dir)

        Returns:
            True if exists, False otherwise
        """
        return self._get_full_path(path).exists()

    async def is_file(self, path: str) -> bool:
        """Check if path is a file.

        Args:
            path: File path (relative to base_dir)

        Returns:
            True if it's a file, False otherwise
        """
        return self._get_full_path(path).is_file()

    async def is_dir(self, path: str) -> bool:
        """Check if path is a directory.

        Args:
            path: Directory path (relative to base_dir)

        Returns:
            True if it's a directory, False otherwise
        """
        return self._get_full_path(path).is_dir()

    async def list(self, path: str = "") -> list[str]:
        """List files and directories under a path.

        Args:
            path: Directory path (relative to base_dir, empty means root)

        Returns:
            List of relative paths
        """
        full_path = self._get_full_path(path)
        if not full_path.is_dir():
            return []

        items = []
        for item in full_path.iterdir():
            relative_path = item.relative_to(full_path)
            items.append(relative_path.as_posix())
        return items

    async def get_url(self, path: str, expires: int = 3600) -> str:
        """Get a URL for accessing the file.

        For local storage, this returns the absolute file path.

        Args:
            path: File path (relative to base_dir)
            expires: Expiration time (unused for local storage)

        Returns:
            Absolute file path as URL
        """
        return str(self._get_full_path(path).resolve())

    def _get_full_path(self, path: str) -> Path:
        """Get the full path for a relative path.

        Args:
            path: Relative path

        Returns:
            Full path
        """
        # Normalize path to handle both forward and backward slashes
        normalized_path = path.replace("/", os.sep).replace("\\", os.sep)
        # Remove leading separators to avoid absolute path issues
        normalized_path = normalized_path.lstrip(os.sep)
        return Path(self.base_dir) / normalized_path
