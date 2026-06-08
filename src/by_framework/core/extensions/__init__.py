"""
Plugin system module - Provides pluggable Worker extension mechanism.

This module implements a standardized plugin registration and management
system, allowing business logic (such as tools, prompts, skills, callbacks)
to be decoupled from Worker infrastructure, and dynamically injected and
managed through the form of plugins.
"""

from .agent_config import AgentConfig, CallbackType
from .plugin import (AgentConfigsSnapshot, Plugin, PluginBuildContext,
                     PluginManifest, PluginReloadContext, PluginReloadResult,
                     PromptTemplate)
from .registry import PluginRegistry
from .trace_provider import TraceProviderFactory

__all__ = [
    "AgentConfig",
    "AgentConfigsSnapshot",
    "CallbackType",
    "PluginManifest",
    "Plugin",
    "PluginBuildContext",
    "PluginReloadContext",
    "PluginReloadResult",
    "PromptTemplate",
    "PluginRegistry",
    "TraceProviderFactory",
]
