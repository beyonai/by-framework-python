"""Shared Byai-specific protocol type aliases."""

from typing import TypeAlias

from .message import BaiYingMessage

ByaiContent: TypeAlias = str | BaiYingMessage | list[BaiYingMessage]
