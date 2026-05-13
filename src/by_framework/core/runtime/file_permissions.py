"""
File permission policies for runtime file access control.
"""

from __future__ import annotations

import posixpath
from typing import Protocol

from by_framework.core.runtime.file_paths import FileAccessContext


class FilePermissionPolicy(Protocol):
    """Policy interface for authorizing file operations."""

    def check(
        self,
        operation: str,
        path: str,
        *,
        access_context: FileAccessContext,
    ) -> str | None:
        """Return an error message when denied, otherwise None."""


class WorkspaceScopedPermissionPolicy(FilePermissionPolicy):
    """Default policy that scopes access to the current session and public area."""

    def check(
        self,
        operation: str,
        path: str,
        *,
        access_context: FileAccessContext,
    ) -> str | None:
        del operation

        normalized = self._normalize_virtual_path(path)
        if not normalized:
            return None
        if normalized.startswith("/") or normalized.startswith(".."):
            return (
                f"PermissionError: Path '{path}' escapes the virtual root constraints."
            )

        parts = normalized.split("/")
        if parts[0] == "public":
            return None
        if (
            len(parts) >= 2
            and parts[0] == "sessions"
            and parts[1] == access_context.session_id
        ):
            return None
        return (
            f"PermissionError: Access to path '{path}' is denied. Must be within "
            f"'sessions/{access_context.session_id}' or 'public'."
        )

    @staticmethod
    def _normalize_virtual_path(path: str) -> str:
        if not path:
            return ""
        normalized = posixpath.normpath(path)
        if normalized == ".":
            return ""
        return normalized.lstrip("/")


# Backward-compatible alias while callers migrate to the workspace-aware name.
SessionScopedPermissionPolicy = WorkspaceScopedPermissionPolicy
