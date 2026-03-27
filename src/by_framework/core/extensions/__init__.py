"""
插件系统模块 - 提供可插拔的 Worker 扩展机制

该模块实现了标准化的插件注册和管理系统，允许业务逻辑（如工具、提示词、技能、回调）
与 Worker 基础设施解耦，通过插件的形式进行动态注入和管理。
"""

from .agent_config import AgentConfig, CallbackType
from .plugin import Plugin, PluginBuildContext, PluginManifest, PromptTemplate
from .registry import PluginRegistry

__all__ = [
    "AgentConfig",
    "CallbackType",
    "PluginManifest",
    "Plugin",
    "PluginBuildContext",
    "PromptTemplate",
    "PluginRegistry",
]
