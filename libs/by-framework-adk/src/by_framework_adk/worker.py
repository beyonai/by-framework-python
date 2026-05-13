"""ADK worker base class for by-framework."""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from by_framework.common.logger import logger
from by_framework.worker.byai_worker import ByaiWorker

try:
    from google.adk.runners import Runner
    from google.adk.sessions import InMemorySessionService
except ImportError:
    Runner = None
    InMemorySessionService = None

from .adapter import AdkAdapter

if TYPE_CHECKING:
    from by_framework.core.protocol.commands import GatewayCommand
    from by_framework.worker.context import AgentContext
    from google.adk.agents import LlmAgent


class AdkWorker(ByaiWorker):
    """Base Worker class for ADK-powered agents.

    Subclasses only need to implement:
    - ``get_agent_types()`` → list of agent type strings
    - ``build_agent(context, command)`` → an ADK LlmAgent instance

    The base class automatically handles ADK execution via AdkAdapter.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the ADK worker."""
        super().__init__(*args, **kwargs)
        if InMemorySessionService is None:
            raise ImportError(
                "google-adk is not installed. Please install google-adk to use AdkWorker."
            )
        self._session_service = InMemorySessionService()

    @property
    def app_name(self) -> str:
        """Return the ADK app name."""
        return "by_framework_adk_app"

    @abstractmethod
    def build_agent(
        self,
        context: AgentContext,
        command: GatewayCommand,
    ) -> LlmAgent:
        """Build and return an ADK LlmAgent.

        Args:
            context: Current AgentContext.
            command: The incoming command.

        Returns:
            An ADK LlmAgent ready for invocation.
        """

    async def process_command(
        self,
        command: GatewayCommand,
        context: AgentContext,
    ) -> Any:
        """Framework entry point — delegates to AdkAdapter."""
        logger.info(
            "[AdkWorker] Processing command, type=%s, session=%s",
            type(command).__name__,
            context.session_id,
        )

        agent = self.build_agent(context, command)
        user_id = (
            getattr(command.header, "user_code", "default_user")
            if hasattr(command, "header")
            else "default_user"
        )

        # Ensure session exists
        session = await self._session_service.get_session(
            app_name=self.app_name,
            user_id=user_id,
            session_id=context.session_id,
        )

        if not session:
            # Session doesn't exist, create it
            logger.info(
                "[AdkWorker] Session not found, creating session for user=%s, session=%s",
                user_id,
                context.session_id,
            )
            await self._session_service.create_session(
                app_name=self.app_name,
                user_id=user_id,
                session_id=context.session_id,
            )

        runner = Runner(
            agent=agent,
            app_name=self.app_name,
            session_service=self._session_service,
        )

        adapter = AdkAdapter(agent, context, runner)
        return await adapter.run(command)
