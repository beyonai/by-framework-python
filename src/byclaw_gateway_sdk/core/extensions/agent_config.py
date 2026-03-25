"""
Agent configuration definitions for the plugin system.

Contains AgentConfig dataclass and CallbackType enum used to configure
agent capabilities, tools, prompts, and callbacks.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Literal

ConflictStrategy = Literal["error", "overwrite", "skip"]


class CallbackType(Enum):
    """回调类型枚举，定义智能体执行过程中的回调点。

    Attributes:
        before_model_callback: 模型调用之前触发
        after_model_callback: 模型调用之后触发
        before_tool_callback: 工具调用之前触发
        after_tool_callback: 工具调用之后触发
        before_agent_callback: 智能体调用之前触发
        after_agent_callback: 智能体调用之后触发
    """

    before_model_callback = "before_model_callback"
    after_model_callback = "after_model_callback"
    before_tool_callback = "before_tool_callback"
    after_tool_callback = "after_tool_callback"
    before_agent_callback = "before_agent_callback"
    after_agent_callback = "after_agent_callback"


@dataclass
class AgentConfig:
    """单个智能体的能力配置。

    Attributes:
        agent_id: 智能体唯一标识符
        name: 智能体名称
        description: 智能体描述
        prompts: 提示词模板字典
        tools: 工具配置字典
        skills: 技能配置字典
        callbacks: 回调函数字典
        knowledge_bases: 知识库配置字典
        sub_agents: 子智能体ID列表
        on_conflict: 冲突策略：error、overwrite 或 skip
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
    on_conflict: ConflictStrategy = "error"
