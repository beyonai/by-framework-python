from pathlib import Path

import pytest

from by_framework import WorkspaceManager


@pytest.mark.asyncio
async def test_workspace_initialization(tmp_path):
    """Test that WorkspaceManager initializes workspace directory structure."""
    wm = WorkspaceManager(base_dir=str(tmp_path))
    workspace_paths = await wm.setup_workspace(
        session_id="sess-1",
        task_id="task-1",
        user_code="user-a",
        agent_id="agent-1",
    )

    # Assert directory structure
    assert Path(workspace_paths["public"]).exists()
    assert Path(workspace_paths["private"]).exists()
    assert Path(workspace_paths["history_db"]).parent.exists()
    assert Path(workspace_paths["shared_public"]).exists()
    assert Path(workspace_paths["shared_session"]).exists()
    assert "agent-1/user-a" in workspace_paths["agent_root"]
    assert "/public/user-a" in workspace_paths["shared_root"]

    # Assert private subfolders
    for folder in ["input", "temp", "output", "system"]:
        assert (Path(workspace_paths["private"]) / folder).exists()

    # Cleanup verification
    await wm.cleanup_task(
        session_id="sess-1",
        task_id="task-1",
        user_code="user-a",
        agent_id="agent-1",
    )
    assert not Path(workspace_paths["private"]).exists()
    assert Path(workspace_paths["public"]).exists()  # Public should remain
