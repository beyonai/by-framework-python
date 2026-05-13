"""Contracts for optional trace provider integrations."""

from __future__ import annotations

from abc import ABC, abstractmethod

from .plugin import Plugin


class TraceProviderFactory(ABC):
    """Build a trace plugin when the provider is installed and configured."""

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Return a stable provider identifier used for logs and conflicts."""

    @abstractmethod
    def build_plugin_from_env(self) -> Plugin | None:
        """Return a configured plugin, or ``None`` when the provider is inactive."""
