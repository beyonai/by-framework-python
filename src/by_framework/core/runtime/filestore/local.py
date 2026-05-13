"""
Local file system storage implementation.

Provides file storage backed by local filesystem.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from by_framework.common.logger import logger

from .base import (
    FileContentType,
    FileDeleteResult,
    FileEditResult,
    FileGlobResult,
    FileListResult,
    FilePathEntry,
    FileReadResult,
    FileSearchResult,
    FileStorage,
    FileWriteResult,
)

IMAGE_MEDIA_TYPES = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
}


class LocalFileStorage(FileStorage):
    """Local filesystem storage implementation."""

    def __init__(self, base_dir: str):
        self.base_dir = base_dir

    async def initialize(self) -> None:
        os.makedirs(self.base_dir, exist_ok=True)

    async def shutdown(self) -> None:
        pass

    async def write(
        self, path: str, content: str | bytes, encoding: str = "utf-8"
    ) -> FileWriteResult:
        try:
            full_path = self._get_full_path(path)
            full_path.parent.mkdir(parents=True, exist_ok=True)

            if isinstance(content, str):
                full_path.write_text(content, encoding=encoding)
            else:
                full_path.write_bytes(content)

            logger.info(
                "Successfully wrote %s chars/bytes to %s",
                len(content),
                full_path,
            )
            return {
                "path": path,
                "absolute_path": str(full_path),
                "bytes_written": len(content),
            }
        except OSError as err:
            logger.error("Failed to write file %s: %s", path, err)
            return {
                "path": path,
                "bytes_written": 0,
                "error": f"Error writing file: {err}",
            }

    async def read(
        self,
        path: str,
        encoding: str = "utf-8",
        *,
        offset: int = 0,
        limit: int | None = None,
        content_type: FileContentType = "markdown",
    ) -> FileReadResult:
        del content_type
        try:
            full_path = self._get_full_path(path)
            suffix = full_path.suffix.lower()
            if suffix in IMAGE_MEDIA_TYPES:
                if offset > 0 or limit is not None:
                    return {
                        "kind": "error",
                        "path": path,
                        "content": "",
                        "error": (
                            "Error: Image reads do not support "
                            "offset/limit pagination"
                        ),
                    }
                return {
                    "kind": "image",
                    "path": path,
                    "absolute_path": str(full_path),
                    "content": full_path.read_bytes(),
                    "media_type": IMAGE_MEDIA_TYPES[suffix],
                }
            if offset > 0 or limit is not None:
                if not encoding:
                    return {
                        "kind": "error",
                        "path": path,
                        "content": "",
                        "error": "Error: Line-based reads require text encoding",
                    }
                text = full_path.read_text(encoding=encoding)
                return {
                    "kind": "text",
                    "path": path,
                    "absolute_path": str(full_path),
                    "content": self._slice_lines(text, offset=offset, limit=limit),
                }
            if encoding:
                return {
                    "kind": "text",
                    "path": path,
                    "absolute_path": str(full_path),
                    "content": full_path.read_text(encoding=encoding),
                }
            binary_content = full_path.read_bytes()
            return {
                "kind": "binary",
                "path": path,
                "absolute_path": str(full_path),
                "content": binary_content,
            }
        except OSError as err:
            return {
                "kind": "error",
                "path": path,
                "content": "",
                "error": f"Error reading file: {err}",
            }

    async def delete(self, path: str) -> FileDeleteResult:
        try:
            full_path = self._get_full_path(path)
            if full_path.is_file():
                full_path.unlink()
            elif full_path.is_dir():
                shutil.rmtree(full_path)
            return {"path": path, "absolute_path": str(full_path), "deleted": True}
        except OSError as err:
            return {
                "path": path,
                "deleted": False,
                "error": f"Error deleting path: {err}",
            }

    async def list(self, path: str = "") -> FileListResult:
        try:
            full_path = self._get_full_path(path)
            if not full_path.is_dir():
                return {"paths": []}

            return {
                "paths": [
                    self._build_path_entry(
                        item.relative_to(full_path).as_posix(),
                        item,
                    )
                    for item in full_path.iterdir()
                ]
            }
        except OSError as err:
            return {"paths": [], "error": f"Error listing directory: {err}"}

    async def grep(self, pattern: str, glob_pattern: str = "*") -> FileSearchResult:
        try:
            base_path = Path(self.base_dir)
            matches = []
            for item in sorted(base_path.rglob(glob_pattern)):
                if item.is_file():
                    try:
                        content = item.read_text(encoding="utf-8")
                        for i, line in enumerate(content.splitlines(), start=1):
                            if pattern in line:
                                matches.append(
                                    {
                                        "path": item.relative_to(base_path).as_posix(),
                                        "absolute_path": str(item),
                                        "line_number": i,
                                        "content": line,
                                    }
                                )
                    except UnicodeDecodeError:
                        pass
            return {"matches": matches}
        except OSError as err:
            return {"matches": [], "error": f"Error searching files: {err}"}

    async def edit(
        self,
        path: str,
        old_string: str,
        new_string: str,
        replace_all: bool = False,
        encoding: str = "utf-8",
    ) -> FileEditResult:
        try:
            full_path = self._get_full_path(path)
            if not full_path.is_file():
                return {
                    "path": path,
                    "occurrences": 0,
                    "error": f"File {path} does not exist.",
                }
            content = full_path.read_text(encoding=encoding)

            occurrences = content.count(old_string)
            if occurrences == 0:
                return {
                    "path": path,
                    "occurrences": 0,
                    "error": f"String '{old_string}' not found in {path}",
                }
            if not replace_all and occurrences > 1:
                return {
                    "path": path,
                    "occurrences": occurrences,
                    "error": (
                        f"String '{old_string}' is not unique in {path} "
                        f"(found {occurrences} occurrences) and replace_all is False."
                    ),
                }

            if replace_all:
                new_content = content.replace(old_string, new_string)
            else:
                new_content = content.replace(old_string, new_string, 1)

            full_path.write_text(new_content, encoding=encoding)
            return {
                "path": path,
                "absolute_path": str(full_path),
                "occurrences": occurrences,
            }
        except OSError as err:
            return {
                "path": path,
                "occurrences": 0,
                "error": f"Error editing file: {err}",
            }

    async def glob(self, pattern: str) -> FileGlobResult:
        """Find files matching a glob pattern relative to the storage base."""
        try:
            base_path = Path(self.base_dir).resolve()
            # Use rglob to support recursive matching
            return {
                "paths": [
                    self._build_path_entry(
                        item.relative_to(base_path).as_posix(),
                        item,
                    )
                    for item in base_path.rglob(pattern)
                    if item.is_file() or item.is_dir()
                ]
            }
        except OSError as err:
            return {"paths": [], "error": f"Error during glob: {err}"}

    def _get_full_path(self, path: str) -> Path:
        """Build a sandboxed absolute path for a storage-relative path."""

        # Use os.path.normpath and abspath to handle logical paths, avoiding direct
        # use of resolve() which forces path prefix expansion on macOS and other
        # systems (e.g., /home expands to /System/Volumes/Data/home)
        normalized_path = path.replace("/", os.sep).replace("\\", os.sep).lstrip(os.sep)

        # 1. Construct logical absolute path (preserving the user's original path style)
        logical_base = os.path.abspath(self.base_dir)
        logical_full = os.path.normpath(os.path.join(logical_base, normalized_path))

        # 2. Security check: When checking boundaries, must use resolve() to resolve
        # all symbolic links. This is to prevent attackers from escaping the sandbox
        # by constructing symbolic links pointing outside.
        resolved_base = Path(logical_base).resolve()
        resolved_full = Path(logical_full).resolve()

        if not resolved_full.is_relative_to(resolved_base):
            raise PermissionError(
                "Path traversal detected: "
                f"{path} escapes base directory {self.base_dir}"
            )

        return Path(logical_full)

    def _slice_lines(
        self,
        text: str,
        *,
        offset: int = 0,
        limit: int | None = None,
    ) -> str:
        """Return a newline-joined window of lines from text."""

        lines = text.splitlines()
        start_index = max(offset, 0)
        if limit is not None:
            end_index = start_index + max(limit, 0)
            return "\n".join(lines[start_index:end_index])
        return "\n".join(lines[start_index:])

    def _build_path_entry(self, path: str, absolute_path: Path) -> FilePathEntry:
        return {
            "path": path,
            "absolute_path": str(absolute_path),
        }
