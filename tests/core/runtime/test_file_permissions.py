import pytest

from by_framework.core.runtime import (
    SessionScopedPermissionPolicy,
    WorkspaceScopedPermissionPolicy,
)
from by_framework.core.runtime.file_manager import FileManager
from by_framework.core.runtime.file_paths import FileAccessContext


def test_workspace_scoped_permission_policy_is_exported_with_compat_alias() -> None:
    policy = WorkspaceScopedPermissionPolicy()

    assert isinstance(policy, WorkspaceScopedPermissionPolicy)
    assert SessionScopedPermissionPolicy is WorkspaceScopedPermissionPolicy


def test_workspace_scoped_permission_policy_allows_current_session_and_public() -> None:
    policy = WorkspaceScopedPermissionPolicy()
    access_context = FileAccessContext(
        session_id="s1",
        user_code="user-a",
        workspace_scope="agent_private",
        agent_id="agent-1",
    )

    assert (
        policy.check(
            "read",
            "sessions/s1/docs/guide.md",
            access_context=access_context,
        )
        is None
    )
    assert (
        policy.check("read", "public/readme.md", access_context=access_context) is None
    )
    assert (
        policy.check(
            "read",
            "sessions/s2/docs/guide.md",
            access_context=access_context,
        )
        is not None
    )


class DenyWritePolicy:

    def check(
        self,
        operation: str,
        path: str,
        *,
        session_id: str,
        user_code: str,
    ) -> str | None:
        del path, session_id, user_code
        if operation == "write":
            return "writes are disabled"
        return None


@pytest.mark.asyncio
async def test_file_manager_uses_custom_permission_policy(tmp_path) -> None:
    manager = FileManager(
        session_id="s1",
        workspace_dir=str(tmp_path),
        permission_policy=DenyWritePolicy(),
    )
    await manager.initialize()

    result = await manager.write_file("sessions/s1/docs/guide.md", "# hello\n")

    assert result == {"success": False, "error": "writes are disabled"}


@pytest.mark.asyncio
async def test_file_manager_default_policy_still_allows_session_paths(tmp_path) -> None:
    manager = FileManager(session_id="s1", workspace_dir=str(tmp_path))
    await manager.initialize()

    result = await manager.write_file("sessions/s1/docs/guide.md", "# hello\n")

    assert result["success"] is True
