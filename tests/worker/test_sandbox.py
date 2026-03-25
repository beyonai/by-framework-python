import pytest

from byclaw_gateway_sdk.worker.sandbox.hook_sandbox import (
    HookSandbox,
    active_workspace,
)


def test_hook_sandbox_file_access(tmp_path):
    """Test that HookSandbox restricts file access to active workspace directory."""
    sandbox = HookSandbox()

    # 允许访问的目录
    allowed_dir = tmp_path / "allowed"
    allowed_dir.mkdir()
    allowed_file = allowed_dir / "test.txt"
    allowed_file.write_text("hello")

    # 禁止访问的目录（模拟系统根目录等）
    forbidden_dir = tmp_path / "forbidden"
    forbidden_dir.mkdir()
    forbidden_file = forbidden_dir / "secret.txt"
    forbidden_file.write_text("secret")

    # 安装 Hook
    sandbox.install()

    # 设置当前上下文工作区
    token = active_workspace.set(str(allowed_dir))

    try:
        # 1. 允许的访问应当成功
        with open(allowed_file, "r", encoding="utf-8") as f:
            assert f.read() == "hello"

        # 2. 越权访问应当被拒绝
        with pytest.raises(PermissionError):
            with open(forbidden_file, "r", encoding="utf-8") as f:
                pass
    finally:
        active_workspace.reset(token)
        sandbox.uninstall()
