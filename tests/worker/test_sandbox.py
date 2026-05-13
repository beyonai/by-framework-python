"""Tests for HookSandbox file access restriction."""

import pytest

from by_framework.worker.sandbox.hook_sandbox import (HookSandbox, active_workspace)


def test_hook_sandbox_file_access(tmp_path):
    """Test that HookSandbox restricts file access to active workspace directory."""
    sandbox = HookSandbox()

    # Directory allowed to access
    allowed_dir = tmp_path / "allowed"
    allowed_dir.mkdir()
    allowed_file = allowed_dir / "test.txt"
    allowed_file.write_text("hello")

    # Forbidden directory (simulating system root, etc.)
    forbidden_dir = tmp_path / "forbidden"
    forbidden_dir.mkdir()
    forbidden_file = forbidden_dir / "secret.txt"
    forbidden_file.write_text("secret")

    # Install Hook
    sandbox.install()

    # Set current context workspace
    token = active_workspace.set(str(allowed_dir))

    try:
        # 1. Allowed access should succeed
        with open(allowed_file, "r", encoding="utf-8") as f:
            assert f.read() == "hello"

        # 2. Unauthorized access should be denied
        with pytest.raises(PermissionError):
            with open(forbidden_file, "r", encoding="utf-8") as f:
                pass
    finally:
        active_workspace.reset(token)
        sandbox.uninstall()
