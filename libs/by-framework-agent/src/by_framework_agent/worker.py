"""``NativeAgentWorker`` — the first-party agent harness worker.

Unlike ``LangGraphWorker``/``AdkWorker``, which require a per-subclass
``build_graph``/``build_agent`` hook, the reasoning loop here is entirely
framework-owned. Subclasses only need to declare ``get_agent_types()`` and
make an ``AgentConfig`` resolvable through the existing
``PluginRegistry``/``AgentConfigManager`` plumbing.
"""

from __future__ import annotations

from typing import Any

from by_framework.common.constants import RedisKeys
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.protocol.commands import (CancelTaskCommand, GatewayCommand)
from by_framework.core.protocol.results import AgentTaskResult
from by_framework.worker.byai_worker import ByaiWorker
from by_framework.worker.context import AgentContext

from .litellm_client import LiteLLMModelClient
from .loop import HarnessLoop
from .model_client import ModelClient


class NativeAgentWorker(ByaiWorker):
    """``GatewayWorker`` that runs a native, litellm-backed reasoning loop."""

    def __init__(
        self,
        *args: Any,
        model_client: ModelClient | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._model_client: ModelClient = model_client or LiteLLMModelClient()

    async def process_command(
        self, command: GatewayCommand, context: AgentContext
    ) -> AgentTaskResult:
        agent_config = context.agent_runtime_state.config_manager.get_config(
            context.current_agent_id
        )
        if agent_config is None:
            return AgentTaskResult(
                status=AgentState.FAILED.value,
                reply_data={
                    "error": (
                        "No AgentConfig registered for agent_id="
                        f"{context.current_agent_id!r}"
                    )
                },
            )

        loop = HarnessLoop(self._model_client, agent_config)
        return await loop.run(context)

    async def on_cancelled_resume(
        self, command: GatewayCommand, context: AgentContext, reason: str
    ) -> None:
        """Clean up a suspended loop's harness_state on a stray, post-cancel resume.

        This is the only hook that fires for an execution cancelled while
        fully suspended (awaiting ask_user or a sub-agent reply) — it has no
        live task for CancelTaskCommand's own on_cancel_task hook to catch.
        """
        del command, reason
        if context.execution_id:
            await context.redis.delete(RedisKeys.harness_state(context.execution_id))

    async def on_cancel_task(self, command: CancelTaskCommand) -> None:
        """Clean up harness_state for a live (still-running) cancelled task.

        Covers the narrow window where a loop just wrote harness_state and
        is cancelled before ever reaching a suspended state; harmless no-op
        otherwise since deleting an absent key is a no-op.
        """
        if command.target_execution_id:
            await self.redis.delete(
                RedisKeys.harness_state(command.target_execution_id)
            )
