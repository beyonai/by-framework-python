from pathlib import Path

import pytest

from byclaw_gateway_sdk import WorkspaceManager


@pytest.mark.asyncio
async def test_workspace_initialization(tmp_path):
    """Test that WorkspaceManager correctly initializes workspace directory structure."""
    wm = WorkspaceManager(base_dir=str(tmp_path))
    workspace_paths = await wm.setup_workspace(session_id="sess-1", task_id="task-1")

    # Assert directory structure
    assert Path(workspace_paths["public"]).exists()
    assert Path(workspace_paths["private"]).exists()
    assert Path(workspace_paths["history_db"]).parent.exists()

    # Assert private subfolders
    for folder in ["input", "temp", "output", "system"]:
        assert (Path(workspace_paths["private"]) / folder).exists()

    # Cleanup verification
    await wm.cleanup_task(session_id="sess-1", task_id="task-1")
    assert not Path(workspace_paths["private"]).exists()
    assert Path(workspace_paths["public"]).exists()  # Public should remain
