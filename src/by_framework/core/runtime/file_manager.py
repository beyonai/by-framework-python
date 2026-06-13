"""
File management for agent runtime sessions.

Provides file operations within an agent session using pluggable storage backends.
"""

from __future__ import annotations

import base64
import json
import uuid
from typing import Any, Literal, Optional, TypedDict

from by_framework.common.constants import DEFAULT_WORKSPACE_DIR
from by_framework.core.runtime.file_paths import (FileAccessContext, RuntimePathMapper)
from by_framework.core.runtime.file_permissions import (
    FilePermissionPolicy,
    WorkspaceScopedPermissionPolicy,
)
from by_framework.core.runtime.filestore.base import (
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
from by_framework.core.runtime.filestore.local import LocalFileStorage


class FileOperationResponse(TypedDict, total=False):
    """Standardized Agent-Ready Response format for file operations"""

    success: bool
    message: str
    error: str
    data: Any


GrepOutputMode = Literal["files_with_matches", "content", "count"]


class FileCountEntry(TypedDict):
    """Counted grep results for a single path."""

    path: str
    absolute_path: str | None
    count: int


class FileManager:
    """Agent file workspace manager."""

    def __init__(
        self,
        session_id: str,
        storage: Optional[FileStorage] = None,
        workspace_dir: Optional[str] = None,
        user_code: Optional[str] = None,
        tool_result_max_chars: int = 8000,
        permission_policy: FilePermissionPolicy | None = None,
        access_context: FileAccessContext | None = None,
        agent_id: str = "",
    ):
        """Initialize the file manager.

        Args:
            session_id: The unique identifier for the current session workspace
            storage: Custom storage implementation. Defaults to LocalFileStorage
            workspace_dir: Base directory for workspace files. Only used if
                storage is None
            user_code: The parent user container identifier
        """
        resolved_access_context = access_context or FileAccessContext(
            session_id=session_id,
            user_code=user_code or "default",
            workspace_scope="agent_private",
            agent_id=agent_id,
        )
        self._access_context = resolved_access_context
        self.session_id = resolved_access_context.session_id
        self.user_code = resolved_access_context.user_code
        self._storage = storage
        self._tool_result_max_chars = tool_result_max_chars
        self._permission_policy = permission_policy or WorkspaceScopedPermissionPolicy()
        self._path_mapper = RuntimePathMapper(resolved_access_context)

        if self._storage is None:
            workspace = workspace_dir or DEFAULT_WORKSPACE_DIR
            self._storage = LocalFileStorage(base_dir=workspace)

    def _get_storage_path(self, filename: str) -> str:
        """Convert agent virtual path to the user-isolated storage path."""
        return self._path_mapper.to_storage_path(filename)

    @property
    def storage(self) -> FileStorage:
        """Get the storage backend."""
        return self._storage

    @property
    def workspace_dir(self) -> str:
        """Get the workspace identifier (path or bucket prefix)."""
        return f"{self.user_code}/session_{self.session_id}"

    def _check_permission(self, path: str) -> str | None:
        """Backward-compatible wrapper for permission checks."""
        return self._check_permission_for("unknown", path)

    def _check_permission_for(self, operation: str, path: str) -> str | None:
        """Validate access through the configured permission policy."""
        try:
            return self._permission_policy.check(
                operation,
                path,
                access_context=self._access_context,
            )
        except TypeError as error:
            if "access_context" not in str(error):
                raise
            return self._permission_policy.check(
                operation,
                path,
                session_id=self.session_id,
                user_code=self.user_code,
            )

    def _strip_user_prefix(self, path: str) -> str:
        """Convert a storage path back into a virtual workspace path."""
        return self._path_mapper.from_storage_path(path)

    def _normalize_path_entry(self, entry: FilePathEntry) -> FilePathEntry:
        return {
            "path": self._strip_user_prefix(entry["path"]),
            "absolute_path": entry.get("absolute_path"),
        }

    async def _build_evicted_response(
        self,
        *,
        operation: str,
        payload: Any,
        success_message: str,
    ) -> FileOperationResponse:
        """Persist oversized tool output and return a lightweight reference."""
        serialized = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        if len(serialized) <= self._tool_result_max_chars:
            return {"success": True, "message": success_message, "data": payload}

        preview = serialized[: min(240, self._tool_result_max_chars)]
        result_path = self._path_mapper.build_large_result_path(
            operation,
            uuid.uuid4().hex[:8],
        )
        write_result = await self.write_file(result_path, serialized)
        if not write_result.get("success"):
            err_msg = write_result.get("error", "unknown error")
            return {
                "success": False,
                "error": f"Failed to persist large tool result: {err_msg}",
            }
        return {
            "success": True,
            "message": (
                f"{success_message} Result was stored in `{result_path}` because it "
                "exceeded the inline size limit. Use read_file with offset/limit to "
                "page through it."
            ),
            "data": {
                "evicted": True,
                "path": result_path,
                "absolute_path": write_result.get("data", {}).get("absolute_path"),
                "preview": preview,
            },
        }

    async def initialize(self) -> None:
        """Initialize the file manager and storage backend."""
        await self._storage.initialize()

    async def shutdown(self) -> None:
        """Shutdown the file manager and storage backend."""
        await self._storage.shutdown()

    async def read_file(
        self,
        filename: str,
        encoding: str = "utf-8",
        offset: int = 0,
        limit: int | None = None,
        content_type: str = "markdown",
    ) -> FileOperationResponse:
        """Read content from a file in the session workspace."""
        err = self._check_permission_for("read", filename)
        if err:
            return {"success": False, "error": err}

        storage_path = self._get_storage_path(filename)
        read_result: FileReadResult = await self._storage.read(
            storage_path,
            encoding=encoding,
            offset=offset,
            limit=limit,
            content_type=content_type,
        )
        if read_result["kind"] == "error":
            return {"success": False, "error": read_result["error"]}
        if read_result["kind"] == "image":
            image_content = read_result["content"]
            if not isinstance(image_content, bytes):
                return {
                    "success": False,
                    "error": f"Invalid image content returned for {filename}",
                }
            return {
                "success": True,
                "message": f"Successfully read image {filename}",
                "data": {
                    "path": filename,
                    "absolute_path": read_result.get("absolute_path"),
                    "type": read_result["media_type"],
                    "base64": base64.standard_b64encode(image_content).decode("utf-8"),
                },
            }
        return {
            "success": True,
            "message": f"Successfully read file {filename}",
            "data": {
                "path": filename,
                "absolute_path": read_result.get("absolute_path"),
                "content": read_result["content"],
            },
        }

    async def edit_file(
        self,
        filename: str,
        old_string: str,
        new_string: str,
        replace_all: bool = False,
        encoding: str = "utf-8",
    ) -> FileOperationResponse:
        """Edit a file in the session workspace by replacing a string."""
        err = self._check_permission_for("edit", filename)
        if err:
            return {"success": False, "error": err}

        storage_path = self._get_storage_path(filename)
        res: FileEditResult = await self._storage.edit(
            storage_path,
            old_string,
            new_string,
            replace_all=replace_all,
            encoding=encoding,
        )
        if res.get("error"):
            return {"success": False, "error": res["error"]}

        return {
            "success": True,
            "message": f"Successfully edited file {filename}",
            "data": {
                "path": filename,
                "absolute_path": res.get("absolute_path"),
                "occurrences": res["occurrences"],
            },
        }

    async def grep_files(
        self,
        pattern: str,
        glob_pattern: str = "*",
        output_mode: GrepOutputMode = "content",
    ) -> FileOperationResponse:
        """Search across files in the session workspace."""
        err = self._check_permission_for("grep", glob_pattern.replace("*", ""))
        if err:
            return {
                "success": False,
                "error": err
                + " (Tip: Use a strict glob pattern starting with allowed paths)",
            }

        storage_glob = self._get_storage_path(glob_pattern)
        search_result: FileSearchResult = await self._storage.grep(
            pattern, storage_glob
        )
        if search_result.get("error"):
            return {"success": False, "error": search_result["error"]}

        results = [
            {
                "path": self._strip_user_prefix(match["path"]),
                "absolute_path": match.get("absolute_path"),
                "line_number": match["line_number"],
                "content": match["content"],
            }
            for match in search_result["matches"]
        ]

        if output_mode == "files_with_matches":
            deduped: dict[str, FilePathEntry] = {}
            for match in results:
                deduped.setdefault(
                    match["path"],
                    {
                        "path": match["path"],
                        "absolute_path": match.get("absolute_path"),
                    },
                )
            return await self._build_evicted_response(
                operation="grep",
                payload=list(deduped.values()),
                success_message=f"Found {len(deduped)} matching files",
            )

        if output_mode == "count":
            counts: dict[str, FileCountEntry] = {}
            for match in results:
                path = match["path"]
                if path not in counts:
                    counts[path] = {
                        "path": path,
                        "absolute_path": match.get("absolute_path"),
                        "count": 0,
                    }
                counts[path]["count"] += 1
            return await self._build_evicted_response(
                operation="grep",
                payload=list(counts.values()),
                success_message=f"Counted matches across {len(counts)} files",
            )

        return await self._build_evicted_response(
            operation="grep",
            payload=results,
            success_message=f"Found {len(results)} matches",
        )

    async def write_file(
        self,
        filename: str,
        content: str,
        encoding: str = "utf-8",
    ) -> FileOperationResponse:
        """Write content to a file in the session workspace."""
        err = self._check_permission_for("write", filename)
        if err:
            return {"success": False, "error": err}

        storage_path = self._get_storage_path(filename)
        res: FileWriteResult = await self._storage.write(
            storage_path,
            content,
            encoding=encoding,
        )
        if res.get("error"):
            return {"success": False, "error": res["error"]}

        return {
            "success": True,
            "message": f"Successfully wrote file {filename}",
            "data": {
                "path": filename,
                "absolute_path": res.get("absolute_path"),
                "bytes_written": res["bytes_written"],
            },
        }

    async def list_files(self, directory: str = "") -> FileOperationResponse:
        """List files and directories in a directory within the session workspace."""
        err = self._check_permission_for("list", directory)
        if err:
            return {"success": False, "error": err}

        storage_dir = self._get_storage_path(directory)
        list_result: FileListResult = await self._storage.list(storage_dir)
        if list_result.get("error"):
            return {"success": False, "error": list_result["error"]}

        cleaned_items = [
            self._normalize_path_entry(item) for item in list_result["paths"]
        ]
        root_dir = directory or "root"
        return {
            "success": True,
            "message": f"Successfully listed {len(cleaned_items)} items in {root_dir}",
            "data": cleaned_items,
        }

    async def delete_file(self, filename: str) -> FileOperationResponse:
        """Delete a file or directory in the session workspace."""
        err = self._check_permission_for("delete", filename)
        if err:
            return {"success": False, "error": err}

        storage_path = self._get_storage_path(filename)
        res: FileDeleteResult = await self._storage.delete(storage_path)
        if res.get("error"):
            return {"success": False, "error": res["error"]}
        return {
            "success": True,
            "message": f"Successfully deleted {filename}",
            "data": {
                "path": filename,
                "absolute_path": res.get("absolute_path"),
                "deleted": res["deleted"],
            },
        }

    async def glob_files(self, pattern: str) -> FileOperationResponse:
        """Find files matching a glob pattern in the session workspace."""
        err = self._check_permission_for(
            "glob", pattern.replace("*", "").replace("?", "")
        )
        if err:
            return {
                "success": False,
                "error": err + " (Tip: Use a glob pattern rooted at an allowed path)",
            }

        storage_pattern = self._get_storage_path(pattern)
        glob_result: FileGlobResult = await self._storage.glob(storage_pattern)
        if glob_result.get("error"):
            return {"success": False, "error": glob_result["error"]}

        cleaned_items = [
            self._normalize_path_entry(item) for item in glob_result["paths"]
        ]
        return await self._build_evicted_response(
            operation="glob",
            payload=cleaned_items,
            success_message=f"Found {len(cleaned_items)} matching paths",
        )
