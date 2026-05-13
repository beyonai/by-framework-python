# pylint: disable=C0103
"""
Agent configuration definitions for the plugin system.

Contains AgentConfig dataclass and CallbackType enum used to configure
agent types, tools, prompts, and callbacks.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Literal

ConflictStrategy = Literal["error", "overwrite", "skip"]


class CallbackType(Enum):
    """Callback type enum, defining callback points during agent execution.

    Attributes:
        before_model_callback: Triggered before model call
        after_model_callback: Triggered after model call
        before_tool_callback: Triggered before tool call
        after_tool_callback: Triggered after tool call
        before_agent_callback: Triggered before agent call
        after_agent_callback: Triggered after agent call
    """

    before_model_callback = "before_model_callback"
    after_model_callback = "after_model_callback"
    before_tool_callback = "before_tool_callback"
    after_tool_callback = "after_tool_callback"
    before_agent_callback = "before_agent_callback"
    after_agent_callback = "after_agent_callback"


@dataclass
class AgentConfig:
    """Single agent capability configuration.

    Attributes:
        agent_id: Agent unique identifier
        name: Agent name
        description: Agent description
        prompts: Prompt template dictionary
        tools: Tool configuration dictionary
        skills: Skill configuration dictionary
        callbacks: Callback function dictionary
        knowledge_bases: Knowledge base configuration dictionary
        sub_agents: Sub-agent ID list
        on_conflict: Conflict strategy: error, overwrite, or skip
        extra: Extension information
    """

    agent_id: str
    name: str = ""
    description: str = ""
    prompts: dict[str, Any] = field(default_factory=dict)
    tools: dict[str, Any] = field(default_factory=dict)
    skills: dict[str, Any] = field(default_factory=dict)
    callbacks: dict[CallbackType, list[Callable]] = field(default_factory=dict)
    knowledge_bases: dict[str, Any] = field(default_factory=dict)
    sub_agents: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)
    on_conflict: ConflictStrategy = "error"
