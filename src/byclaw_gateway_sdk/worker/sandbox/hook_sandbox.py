"""
Sandbox hook module.

Provides a sandbox mechanism that restricts file system access to the
current workspace directory using context variables.
"""

import builtins
import os
from contextvars import ContextVar

# 当前请求绑定的工作区根目录
active_workspace: ContextVar[str] = ContextVar("active_workspace", default="")


class HookSandbox:
    """沙箱 Hook，用于限制文件访问在工作区目录内。

    通过替换 builtins.open 来拦截文件操作，确保应用代码
    只能访问当前工作区目录下的文件。
    """

    def __init__(self):
        self._original_open = builtins.open

    def _safe_open(
        self,
        file,
        mode="r",
        buffering=-1,
        encoding=None,
        errors=None,
        newline=None,
        closefd=True,
        opener=None,
    ):
        current_ws = active_workspace.get()
        if current_ws:
            # 获取请求文件的绝对路径
            import inspect

            # 简单绕过：只拦截应用代码的文件操作，不拦截系统库的导入
            caller_frame = inspect.currentframe().f_back
            caller_filename = caller_frame.f_code.co_filename if caller_frame else ""

            # 如果是标准库或 site-packages 下的文件引发的 open 操作，可以放行 (MVP 简化版跳过鉴权)
            if (
                "lib/python" not in caller_filename
                and "site-packages" not in caller_filename
            ):
                abs_path = os.path.abspath(file)
                abs_ws = os.path.abspath(current_ws)

                # 安全检查：请求路径必须是以工作区路径为前缀
                if not abs_path.startswith(abs_ws):
                    raise PermissionError(
                        f"[Sandbox] Access denied to path outside workspace: {file}"
                    )

        return self._original_open(
            file, mode, buffering, encoding, errors, newline, closefd, opener
        )

    def install(self):
        builtins.open = self._safe_open

    def uninstall(self):
        builtins.open = self._original_open
