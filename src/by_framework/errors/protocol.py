"""Protocol-related exception definitions."""

from by_framework.errors.base import FrameworkError


class UnsupportedCommandError(FrameworkError):
    """Unsupported command type."""

    def __init__(self, command_type: str):
        super().__init__(f"Unsupported command type: {command_type}")
        self.command_type = command_type


class MessageParseError(FrameworkError):
    """Message parsing failed."""

    def __init__(self, message_id: str = "", cause: Exception | None = None):
        msg = "Failed to parse message"
        if message_id:
            msg += f": {message_id}"
        super().__init__(msg, cause)
        self.message_id = message_id


class MessageDataNotFoundError(FrameworkError):
    """Message data does not exist."""

    def __init__(self, message_id: str = ""):
        msg = "Message data not found"
        if message_id:
            msg += f": {message_id}"
        super().__init__(msg)
        self.message_id = message_id


class CommandValidationError(FrameworkError):
    """Command parameter validation failed."""

    def __init__(self, command_type: str, reason: str):
        super().__init__(f"Validation failed for {command_type}: {reason}")
        self.command_type = command_type
        self.reason = reason
