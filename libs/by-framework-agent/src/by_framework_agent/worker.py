"""``NativeAgentWorker`` — the first-party agent harness worker.

Unlike ``LangGraphWorker``/``AdkWorker``, which require a per-subclass
``build_graph``/``build_agent`` hook, the reasoning loop here is entirely
framework-owned. Subclasses only need to declare ``get_agent_types()`` and
make an ``AgentConfig`` resolvable through the existing
``PluginRegistry``/``AgentConfigManager`` plumbing.
"""

from __future__ import annotations

from typing import Any

from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.protocol.commands import GatewayCommand
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
