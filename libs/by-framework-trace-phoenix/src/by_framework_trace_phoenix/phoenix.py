"""Arize Phoenix integration for by-framework task lifecycle events."""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from typing import Any, Optional

from by_framework.core.extensions import (
    AgentConfig,
    Plugin,
    PluginManifest,
    TraceProviderFactory,
)
from opentelemetry import trace
from opentelemetry.trace import SpanContext, TraceFlags

PHOENIX_SPAN_ATTR = "_phoenix_span"
_QUOTES_TO_STRIP = "\"'“”‘’"
_FALSE_LIKE_VALUES = {"0", "false", "no", "off", "disabled"}


@dataclass(frozen=True)
class PhoenixConfig:
    """Environment-derived config needed to initialize the Phoenix SDK."""

    collector_endpoint: Optional[str] = None
    project_name: str = "by-framework"
    enabled: bool = False

    @classmethod
    def from_env(cls) -> PhoenixConfig:
        """Build config from environment."""
        enabled_val = cls._clean_env_value(os.environ.get("BYAI_PHOENIX_ENABLED", ""))
        enabled = bool(enabled_val) and enabled_val.lower() not in _FALSE_LIKE_VALUES

        endpoint = cls._clean_env_value(
            os.environ.get("PHOENIX_COLLECTOR_ENDPOINT", "")
        )
        project = cls._clean_env_value(
            os.environ.get("PHOENIX_PROJECT_NAME", "by-framework")
        )

        return cls(
            collector_endpoint=endpoint or None,
            project_name=project,
            enabled=enabled,
        )

    @staticmethod
    def _clean_env_value(value: str) -> str:
        return value.strip().strip(_QUOTES_TO_STRIP)


class PhoenixPlugin(Plugin):
    """Emit worker task lifecycle data into Phoenix using OpenTelemetry."""

    def __init__(
        self,
        config: Optional[PhoenixConfig] = None,
        plugin_id: str = "phoenix",
    ):
        self._config = config or PhoenixConfig.from_env()
        super().__init__(
            PluginManifest(plugin_id=plugin_id, enabled=self._config.enabled)
        )
        self._tracer_provider: Any = None
        self._tracer: Optional[trace.Tracer] = None

    async def register_agent_configs(
        self, build_context: Any
    ) -> list[AgentConfig] | None:
        return None

    async def on_worker_startup(self, worker: Any) -> None:
        if not self.manifest.enabled:
            return

        # pylint: disable=import-outside-toplevel
        try:
            from phoenix.otel import register
        except ImportError as err:
            raise RuntimeError(
                "PhoenixPlugin requires 'arize-phoenix-otel' to be installed."
            ) from err

        # Initialize Phoenix OTEL globally with auto-instrumentation
        self._tracer_provider = register(
            project_name=self._config.project_name,
            endpoint=self._config.collector_endpoint,
            auto_instrument=True,
        )
        self._tracer = trace.get_tracer("by-framework")

        # Reinforce instrumentation for LangChain and OpenAI
        # This ensures they are patched even if imported before register()
        try:
            from openinference.instrumentation.langchain import LangChainInstrumentor

            if not LangChainInstrumentor().is_instrumented_by_opentelemetry:
                LangChainInstrumentor().instrument(
                    tracer_provider=self._tracer_provider
                )
        except ImportError:
            pass

        try:
            from openinference.instrumentation.openai import OpenAIInstrumentor

            if not OpenAIInstrumentor().is_instrumented_by_opentelemetry:
                OpenAIInstrumentor().instrument(tracer_provider=self._tracer_provider)
        except ImportError:
            pass

    async def on_worker_shutdown(self, worker: Any) -> None:
        if self._tracer_provider is not None:
            # Shutdown and flush
            self._tracer_provider.shutdown()

    async def on_task_start(self, context: Any) -> None:
        if not self.manifest.enabled or self._tracer is None:
            return

        identity = self._build_task_identity(context)

        # Build deterministic OTEL TraceId and SpanId from framework IDs
        trace_id_int = self._str_to_uint128(identity["trace_id"])

        parent_context = None
        if identity["parent_message_id"]:
            parent_span_id_int = self._str_to_uint64(identity["parent_message_id"])
            parent_span_context = SpanContext(
                trace_id=trace_id_int,
                span_id=parent_span_id_int,
                is_remote=True,
                trace_flags=TraceFlags(TraceFlags.SAMPLED),
            )
            parent_context = trace.set_span_in_context(
                trace.NonRecordingSpan(parent_span_context)
            )

        # Create the new span with deterministic IDs
        span = self._tracer.start_span(
            name=identity["agent_id"],
            context=parent_context,
            attributes=self._build_metadata(identity),
        )
        # Add input as attribute
        content = getattr(context.current_command, "content", None)
        if content:
            span.set_attribute("input.value", str(content))

        setattr(context, PHOENIX_SPAN_ATTR, span)

    async def on_task_complete(self, context: Any, result: Any) -> None:
        """Handle successful task completion and record output."""
        span = self._get_context_span(context)
        if span:
            if result:
                span.set_attribute("output.value", str(result))
            span.end()

    async def on_task_error(self, context: Any, error: Exception) -> None:
        """Handle task error and record exception."""
        span = self._get_context_span(context)
        if span:
            span.record_exception(error)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(error)))
            span.end()

    async def on_task_cancel(self, context: Any, command: Any) -> None:
        """Handle task cancellation and record reason."""
        span = self._get_context_span(context)
        if span:
            reason = getattr(command, "reason", "cancelled")
            span.set_attribute("cancellation_reason", reason)
            span.set_status(trace.Status(trace.StatusCode.ERROR, reason))
            span.end()

    def _get_context_span(self, context: Any) -> Optional[trace.Span]:
        """Retrieve the current span stored in the task context."""
        return getattr(context, PHOENIX_SPAN_ATTR, None)

    @staticmethod
    def _str_to_uint128(s: str) -> int:
        """Convert a string to a deterministic 128-bit integer for OTEL TraceId."""
        return int(hashlib.md5(s.encode()).hexdigest(), 16)

    @staticmethod
    def _str_to_uint64(s: str) -> int:
        """Convert a string to a deterministic 64-bit integer for OTEL SpanId."""
        return int(hashlib.md5(s.encode()).hexdigest()[:16], 16)

    @staticmethod
    def _build_task_identity(context: Any) -> dict[str, Any]:
        """
        Extract task identity information from the context.
        Returns a dictionary containing session, trace, message IDs and user info.
        """
        command = getattr(context, "current_command", None)
        header = getattr(command, "header", None)
        return {
            "session_id": getattr(context, "session_id", ""),
            "trace_id": getattr(context, "trace_id", ""),
            "message_id": getattr(context, "message_id", ""),
            "parent_message_id": getattr(context, "parent_message_id", ""),
            "agent_id": (
                getattr(context, "current_agent_id", "")
                or getattr(header, "target_agent_type", "")
                or "unknown-agent"
            ),
            "user_code": getattr(header, "user_code", ""),
            "user_name": getattr(header, "user_name", ""),
        }

    @staticmethod
    def _build_metadata(identity: dict[str, Any]) -> dict[str, Any]:
        """Build span metadata from the identity dictionary."""
        return {
            "message_id": identity["message_id"],
            "session_id": identity["session_id"],
            "trace_id": identity["trace_id"],
            "user_code": identity["user_code"],
            "user_name": identity["user_name"],
        }


class PhoenixTraceProviderFactory(TraceProviderFactory):
    """Factory that enables Phoenix tracing when the environment is configured."""

    @property
    def provider_name(self) -> str:
        return "phoenix"

    def build_plugin_from_env(self) -> Plugin | None:
        config = PhoenixConfig.from_env()
        if not config.enabled:
            return None
        return PhoenixPlugin(config=config)
