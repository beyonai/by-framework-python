"""
Helpers for translating between virtual runtime paths and storage paths.
"""

from __future__ import annotations

import posixpath
from dataclasses import dataclass
from typing import Literal

WorkspaceScope = Literal["agent_private", "shared_public"]


@dataclass(frozen=True)
class FileAccessContext:
    """Context used to resolve file paths and permissions."""

    session_id: str
    user_code: str
    workspace_scope: WorkspaceScope = "agent_private"
    agent_id: str = ""


class RuntimePathMapper:
    """Map virtual workspace paths to user-scoped storage paths."""

    def __init__(self, access_context: FileAccessContext):
        self.access_context = access_context

    def normalize_virtual_path(self, path: str) -> str:
        """Normalize a virtual path while keeping it relative."""
        if not path:
            return ""
        normalized = posixpath.normpath(path)
        if normalized == ".":
            return ""
        return normalized.lstrip("/")

    def to_storage_path(self, virtual_path: str) -> str:
        """Translate a virtual path into a user-scoped storage path."""
        normalized = self.normalize_virtual_path(virtual_path)
        prefix = self.storage_root_prefix()
        user_code = self.access_context.user_code
        if prefix:
            return f"{prefix}/{user_code}/{normalized}"
        return f"{user_code}/{normalized}"

    def from_storage_path(self, storage_path: str) -> str:
        """Translate a user-scoped storage path back into a virtual path."""
        prefix = self.storage_prefix()
        return (
            storage_path[len(prefix) :]
            if storage_path.startswith(prefix)
            else storage_path
        )

    def build_large_result_path(self, operation: str, result_id: str) -> str:
        """Build the virtual path used for evicted large tool results."""
        return (
            f"sessions/{self.access_context.session_id}/large_results/"
            f"{operation}_{result_id}.json"
        )

    def storage_root_prefix(self) -> str:
        """Return the physical root prefix for the current workspace scope."""
        if self.access_context.workspace_scope == "shared_public":
            return "public"
        return self.access_context.agent_id

    def storage_prefix(self) -> str:
        """Return the full prefix applied to all storage paths."""
        root_prefix = self.storage_root_prefix()
        user_code = self.access_context.user_code
        if root_prefix:
            return f"{root_prefix}/{user_code}/"
        return f"{user_code}/"
