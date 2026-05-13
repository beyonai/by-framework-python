"""Abstract content codec definitions for agent message transport."""

from typing import Any, Protocol, TypeAlias

WireContent: TypeAlias = str | list[dict[str, Any]]


class ContentCodec(Protocol):
    """Bidirectional codec for converting between domain content and wire content."""

    def serialize(self, content: Any) -> WireContent:
        """Convert domain content into a wire-safe payload."""
        ...  # pylint: disable=unnecessary-ellipsis

    def deserialize(self, content: WireContent) -> Any:
        """Convert wire content back into a domain object when applicable."""
        ...  # pylint: disable=unnecessary-ellipsis
