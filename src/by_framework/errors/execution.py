"""Execution-related exception definitions."""

from by_framework.errors.base import FrameworkError


class ExecutionNotFoundError(FrameworkError):
    """Execution record does not exist."""

    def __init__(self, execution_id: str, session_id: str = ""):
        msg = f"Execution not found: {execution_id}"
        if session_id:
            msg += f" (session: {session_id})"
        super().__init__(msg)
        self.execution_id = execution_id
        self.session_id = session_id


class ExecutionDataError(FrameworkError):
    """Execution data parsing failed."""

    def __init__(self, execution_id: str, cause: Exception | None = None):
        super().__init__(f"Failed to parse execution data for {execution_id}", cause)
        self.execution_id = execution_id


class SessionMismatchError(FrameworkError):
    """Session mismatch."""

    def __init__(self, message_id: str, expected_session: str, actual_session: str):
        super().__init__(
            f"Session mismatch for message {message_id}: "
            f"expected {expected_session}, got {actual_session}"
        )
        self.message_id = message_id
        self.expected_session = expected_session
        self.actual_session = actual_session


class TerminalStateError(FrameworkError):
    """Attempted operation on an already terminated execution."""

    def __init__(self, execution_id: str, current_status: str):
        super().__init__(
            f"Execution {execution_id} is already in terminal state: {current_status}"
        )
        self.execution_id = execution_id
        self.current_status = current_status
