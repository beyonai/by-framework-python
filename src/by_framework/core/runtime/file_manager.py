"""
File management for agent runtime sessions.

Provides file operations within an agent session using pluggable storage backends.
"""

from typing import Optional

from by_framework.common.constants import DEFAULT_WORKSPACE_DIR
from by_framework.core.runtime.filestore.base import FileStorage
from by_framework.core.runtime.filestore.local import LocalFileStorage


class FileManager:
    """Session-based file management.

    Provides file operations within an agent session with proper isolation.
    Uses a pluggable storage backend for actual file operations.
    """

    def __init__(
        self,
        session_id: str,
        storage: Optional[FileStorage] = None,
        workspace_dir: Optional[str] = None,
    ):
        """Initialize the file manager.

        Args:
            session_id: Unique session identifier
            storage: Storage backend (default: LocalFileStorage)
            workspace_dir: Workspace directory (only used when storage is not provided)
        """
        self.session_id = session_id
        self._storage = storage

        # Use provided storage or create default local storage
        if self._storage is None:
            workspace = workspace_dir or DEFAULT_WORKSPACE_DIR
            self._storage = LocalFileStorage(
                base_dir=f"{workspace}/session_{session_id}"
            )

    @property
    def storage(self) -> FileStorage:
        """Get the storage backend."""
        return self._storage

    @property
    def workspace_dir(self) -> str:
        """Get the workspace identifier (path or bucket prefix)."""
        return f"session_{self.session_id}"

    async def initialize(self) -> None:
        """Initialize the file manager and storage backend."""
        await self._storage.initialize()

    async def shutdown(self) -> None:
        """Shutdown the file manager and storage backend."""
        await self._storage.shutdown()

    async def read_file(self, filename: str, encoding: str = "utf-8") -> str:
        """Read a file from the session workspace.

        Args:
            filename: Relative filename within the session
            encoding: File encoding (default: utf-8)

        Returns:
            File content as string

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        content = await self._storage.read(filename, encoding=encoding)
        return content if isinstance(content, str) else content.decode(encoding)

    async def write_file(
        self, filename: str, content: str, encoding: str = "utf-8", overwrite: bool = True
    ) -> None:
        """Write content to a file in the session workspace.

        Args:
            filename: Relative filename within the session
            content: Content to write
            encoding: File encoding (default: utf-8)
            overwrite: Whether to allow overwriting existing file (default: True)

        Raises:
            FileExistsError: If file exists and overwrite is False
        """
        if not overwrite and await self._storage.exists(filename):
            raise FileExistsError(f"File {filename} already exists")
        await self._storage.write(filename, content, encoding=encoding)

    async def exists(self, filename: str) -> bool:
        """Check if a file or directory exists.

        Args:
            filename: Relative filename or directory within the session

        Returns:
            True if exists, False otherwise
        """
        return await self._storage.exists(filename)

    async def is_file(self, filename: str) -> bool:
        """Check if path is a file.

        Args:
            filename: Relative path within the session

        Returns:
            True if it's a file, False otherwise
        """
        return await self._storage.is_file(filename)

    async def is_dir(self, filename: str) -> bool:
        """Check if path is a directory.

        Args:
            filename: Relative path within the session

        Returns:
            True if it's a directory, False otherwise
        """
        return await self._storage.is_dir(filename)

    async def list_files(self, directory: str = "") -> list[str]:
        """List files in a directory within the session workspace.

        Args:
            directory: Relative directory within the session (default: root)

        Returns:
            List of relative filenames
        """
        items = await self._storage.list(directory)
        result = []
        for item in items:
            full_path = f"{directory}/{item}".lstrip("/") if directory else item
            if await self._storage.is_file(full_path):
                result.append(item)
        return result

    async def get_file_url(self, filename: str, expires: int = 3600) -> str:
        """Get a URL for accessing the file.

        Args:
            filename: File path
            expires: Expiration time for signed URL (seconds)

        Returns:
            File access URL
        """
        return await self._storage.get_url(filename, expires)
