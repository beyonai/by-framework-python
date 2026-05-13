"""
File storage implementations.

Provides different storage backends:
- `FileStorage`: Abstract interface
- `LocalFileStorage`: Local filesystem implementation
"""

from .base import FileStorage
from .local import LocalFileStorage

__all__ = [
    "FileStorage",
    "LocalFileStorage",
]
