"""Message ID generation utilities."""

import uuid

from by_framework.common.constants import MESSAGE_ID_PREFIX


def generate_message_id() -> str:
    """Generate a new message ID.

    Uses the predefined MESSAGE_ID_PREFIX and UUID fragment.
    """
    return f"{MESSAGE_ID_PREFIX}{uuid.uuid4().hex[:8]}"
