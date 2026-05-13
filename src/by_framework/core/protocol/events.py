"""
Event structures for Gateway protocol.

Contains immutable dataclasses for all event types that can be emitted
through the Gateway event system.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class StateChangeEvent:
    """State change event.

    Attributes:
        state: New state value
        metadata: Extra metadata
    """

    state: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StreamChunkEvent:
    """Streaming content chunk event.

    Attributes:
        content: Text content chunk
        role: Message role
        function_call: Function call information
        tool_calls: Tool call list
        metadata: Extra metadata
    """

    content: Optional[str] = None
    role: str = "assistant"
    function_call: Optional[Dict[str, Any]] = None
    function_response: Optional[Dict[str, Any]] = None
    tool_calls: Optional[List[Dict[str, Any]]] = None
    tool_responses: Optional[List[Dict[str, Any]]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ArtifactEvent:
    """Artifact event.

    Attributes:
        url: Artifact URL
        metadata: Extra metadata
    """

    url: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AskUserEvent:
    """Event to request input from the user.

    Attributes:
        prompt: Prompt text
        metadata: Extra metadata
    """

    prompt: str
    metadata: Dict[str, Any] = field(default_factory=dict)
