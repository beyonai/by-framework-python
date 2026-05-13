"""
Sandbox hook module.

Provides a sandbox mechanism that restricts file system access to the
current workspace directory using context variables.
"""

import builtins
import os
from contextvars import ContextVar

# Current request-bound workspace root directory
active_workspace: ContextVar[str] = ContextVar("active_workspace", default="")


class HookSandbox:
    """Sandbox Hook for restricting file access within the workspace directory.

    Intercepts file operations by replacing builtins.open, ensuring application code
    can only access files under the current workspace directory.
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
        """Safe open that restricts file access to workspace directory."""
        current_ws = active_workspace.get()
        if current_ws:
            # Get absolute path of the requested file
            import inspect

            # Simple bypass: only intercept file operations in application code,
            # not system library imports
            caller_frame = inspect.currentframe().f_back
            caller_filename = caller_frame.f_code.co_filename if caller_frame else ""

            # If the open operation is triggered by files under standard library or
            # site-packages, allow it (MVP simplified version skips authentication)
            if (
                "lib/python" not in caller_filename
                and "site-packages" not in caller_filename
            ):
                abs_path = os.path.abspath(file)
                abs_ws = os.path.abspath(current_ws)

                # Security check: requested path must be prefixed with workspace path
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
