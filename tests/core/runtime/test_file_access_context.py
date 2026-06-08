from by_framework.core.runtime.file_paths import (FileAccessContext,
                                                  RuntimePathMapper)
from by_framework.core.runtime.session_manager import SessionManager


def test_runtime_path_mapper_uses_agent_private_scope_prefix() -> None:
    mapper = RuntimePathMapper(
        FileAccessContext(
            session_id="s1",
            user_code="user-a",
            workspace_scope="agent_private",
            agent_id="agent-1",
        )
    )

    assert mapper.to_storage_path("sessions/s1/docs/guide.md") == (
        "agent-1/user-a/sessions/s1/docs/guide.md"
    )
    assert (
        mapper.from_storage_path("agent-1/user-a/public/readme.md")
        == "public/readme.md"
    )


def test_runtime_path_mapper_uses_shared_scope_prefix() -> None:
    mapper = RuntimePathMapper(
        FileAccessContext(
            session_id="s1",
            user_code="user-a",
            workspace_scope="shared_public",
            agent_id="agent-1",
        )
    )

    assert mapper.to_storage_path("sessions/s1/docs/guide.md") == (
        "public/user-a/sessions/s1/docs/guide.md"
    )


def test_session_manager_exposes_private_and_shared_file_managers() -> None:
    manager = SessionManager(session_id="s1", user_code="user-a", agent_id="agent-1")

    assert manager.file_manager is manager.private_file_manager
    assert manager.private_file_manager._get_storage_path("public/readme.md") == (
        "agent-1/user-a/public/readme.md"
    )
    assert manager.shared_file_manager._get_storage_path("public/readme.md") == (
        "public/user-a/public/readme.md"
    )
