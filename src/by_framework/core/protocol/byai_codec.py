"""Shared codec helpers for BaiYing message content."""

from dataclasses import asdict
from enum import Enum
from typing import Any

from .content_codec import ContentCodec, WireContent
from .message import (
    BaiYingMessage,
    BaiYingMessageRole,
    MessageContent,
    MessageFile,
    Resource,
)


def serialize_byai_content(content: Any) -> Any:
    """Convert BaiYing domain objects into protocol-safe wire payloads."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return [_serialize_list_item(item) for item in content]
    if isinstance(content, BaiYingMessage):
        return [_serialize_message(content)]
    return content


def deserialize_byai_content(content: Any) -> Any:
    """Convert protocol wire payloads into BaiYing domain objects when applicable."""
    if isinstance(content, str):
        return content
    if not isinstance(content, list) or not content:
        return content
    if not all(_is_wire_message(item) for item in content):
        return content

    messages = [_deserialize_message(item) for item in content]
    if len(messages) == 1:
        return messages[0]
    return messages


def _serialize_list_item(item: Any) -> Any:
    if isinstance(item, dict):
        return item
    if isinstance(item, BaiYingMessage):
        return _serialize_message(item)
    return item


def _serialize_message(message: BaiYingMessage) -> dict[str, Any]:
    """Serialize a BaiYingMessage to a dictionary."""
    role = message.role.value if isinstance(message.role, Enum) else message.role
    if isinstance(message.content, MessageContent):
        return {
            "role": role,
            "content": {
                "text": message.content.text,
                "files": [asdict(file) for file in message.content.files],
                "resources": [
                    asdict(resource) for resource in message.content.resources
                ],
            },
        }
    return {
        "role": role,
        "content": message.content,
    }


def _is_wire_message(item: Any) -> bool:
    return isinstance(item, dict) and "role" in item and "content" in item


def _deserialize_message(item: dict[str, Any]) -> BaiYingMessage:
    """Deserialize a dictionary to a BaiYingMessage."""
    role = BaiYingMessageRole(item["role"])
    payload = item["content"]
    if isinstance(payload, dict):
        files = [MessageFile(**file_data) for file_data in payload.get("files", [])]
        resources = [
            Resource(**resource_data) for resource_data in payload.get("resources", [])
        ]
        content: str | MessageContent = MessageContent(
            text=payload.get("text", ""),
            files=files,
            resources=resources,
        )
    else:
        content = payload
    return BaiYingMessage(role=role, content=content)


class ByaiContentCodec(ContentCodec):
    """Content codec implementation for BaiYing domain objects."""

    def serialize(self, content: Any) -> WireContent:
        return serialize_byai_content(content)

    def deserialize(self, content: WireContent) -> Any:
        return deserialize_byai_content(content)
