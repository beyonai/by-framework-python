"""Langfuse integration for by-framework task lifecycle events."""

# pylint: disable=protected-access,import-outside-toplevel,too-many-arguments

from __future__ import annotations

import os
from dataclasses import dataclass
from importlib import import_module
from typing import Any, Optional, Protocol, runtime_checkable

from by_framework.core.extensions import (
    AgentConfig,
    Plugin,
    PluginManifest,
    TraceProviderFactory,
)
from by_framework.core.registry import WorkerRegistry
from by_framework.observability.span_recorder import (
    configure_otel_id_generator,
    current_span_id_var,
    current_trace_id_var,
    str_to_uint64,
    str_to_uint128,
)

LANGFUSE_OBSERVATION_ATTR = "_langfuse_observation"
WORKER_EXECUTE_OBSERVATION_ATTR = "_langfuse_worker_execute_observation"
LANGFUSE_PARENT_OBSERVATION_METADATA_KEY = "langfuse_parent_observation_id"
_QUOTES_TO_STRIP = "\"'“”‘’"
_FALSE_LIKE_VALUES = {"0", "false", "no", "off", "disabled"}


@dataclass(frozen=True)
class LangfuseConfig:
    """Environment-derived config needed to initialize the Langfuse SDK."""

    secret_key: str
    public_key: str
    base_url: str

    @classmethod
    def from_env(cls) -> Optional["LangfuseConfig"]:
        """Build config from environment if all required variables are present."""
        enabled = cls._clean_env_value(os.environ.get("BYAI_LANGFUSE_ENABLED", ""))
        if enabled and enabled.lower() in _FALSE_LIKE_VALUES:
            return None

        secret_key = cls._clean_env_value(os.environ.get("LANGFUSE_SECRET_KEY", ""))
        public_key = cls._clean_env_value(os.environ.get("LANGFUSE_PUBLIC_KEY", ""))
        base_url = cls._clean_env_value(os.environ.get("LANGFUSE_BASE_URL", ""))

        if not secret_key or not public_key or not base_url:
            return None

        return cls(
            secret_key=secret_key,
            public_key=public_key,
            base_url=base_url,
        )

    @staticmethod
    def _clean_env_value(value: str) -> str:
        return value.strip().strip(_QUOTES_TO_STRIP)


@runtime_checkable
class ObservationHandle(Protocol):
    """Minimal observation interface needed by the plugin."""

    id: str

    def update(self, **kwargs: Any) -> None:
        """Update observation fields."""

    def end(self, **kwargs: Any) -> None:
        """End the observation."""


@runtime_checkable
class LangfuseTracer(Protocol):
    """Tracer abstraction to avoid hard dependency on the Langfuse SDK."""

    def start_observation(self, request: _ObservationStartRequest) -> ObservationHandle:
        """Start a new observation."""

    def shutdown(self) -> None:
        """Flush and shutdown the tracer."""


@runtime_checkable
class ObservationStore(Protocol):
    """Lookup and persist the Langfuse observation id by framework message id."""

    async def get_observation_id(
        self, session_id: str, message_id: str
    ) -> Optional[str]:
        """Return the mapped observation id if present."""

    async def set_observation_id(
        self, session_id: str, message_id: str, observation_id: str
    ) -> None:
        """Persist the mapping for later child lookups."""


@dataclass(frozen=True)
class _TaskIdentity:
    """Stable task identity fields copied from the framework context."""

    session_id: str
    trace_id: str
    message_id: str
    parent_message_id: str
    agent_id: str
    user_code: str
    user_name: str


@dataclass(frozen=True)
class _ObservationStartRequest:
    """Arguments required to create a Langfuse observation."""

    trace_id: str
    name: str
    observation_input: Any
    metadata: dict[str, Any]
    parent_observation_id: str = ""
    span_id: Optional[int] = None
    as_root: bool = False


class WorkerRegistryObservationStore:
    """Observation store backed by the existing WorkerRegistry session registry."""

    def __init__(self, registry: WorkerRegistry):
        self._registry = registry

    async def get_observation_id(
        self, session_id: str, message_id: str
    ) -> Optional[str]:
        """Load a previously persisted observation id for a message."""
        execution = await self._registry.get_execution_by_message_id(
            message_id, session_id
        )
        if not execution:
            return None
        observation_id = execution.get("langfuse_observation_id")
        if not observation_id:
            return None
        return str(observation_id)

    async def set_observation_id(
        self, session_id: str, message_id: str, observation_id: str
    ) -> None:
        """Store the Langfuse observation id on the existing execution record."""
        execution = await self._registry.get_execution_by_message_id(
            message_id, session_id
        )
        if not execution:
            return

        await self._registry.update_execution_fields(
            execution["execution_id"],
            session_id,
            langfuse_observation_id=observation_id,
        )


class _SdkLangfuseTracer:
    """Langfuse SDK adapter used only when the SDK is installed."""

    def __init__(self, client: Any):
        self._client = client

    def start_observation(self, request: _ObservationStartRequest) -> ObservationHandle:
        """Start a Langfuse observation with the current framework trace context."""
        trace_context = {"trace_id": request.trace_id}
        if request.parent_observation_id:
            trace_context["parent_span_id"] = request.parent_observation_id

        kwargs = {
            "trace_context": trace_context,
            "name": request.name,
            "as_type": "agent",
            "input": request.observation_input,
            "metadata": request.metadata,
        }

        configure_otel_id_generator()

        obs = None
        if request.span_id is not None:
            trace_id_int = str_to_uint128(request.trace_id)
            trace_id_token = current_trace_id_var.set(trace_id_int)
            span_id_token = current_span_id_var.set(request.span_id)
            try:
                obs = self._client.start_observation(**kwargs)
            finally:
                current_trace_id_var.reset(trace_id_token)
                current_span_id_var.reset(span_id_token)
        else:
            obs = self._client.start_observation(**kwargs)

        # Prevent nested observations from being promoted to a trace root.
        # The LangGraph adapter sets the same attribute on its own fallback path
        # (inside _langfuse_callback_manager); this covers the native plugin path.
        # pylint: disable=protected-access
        if (
            not request.as_root
            and obs is not None
            and hasattr(obs, "_otel_span")
            and obs._otel_span is not None
        ):
            try:
                obs._otel_span.set_attribute("langfuse.internal.as_root", False)
            except Exception:  # pylint: disable=broad-exception-caught
                pass
        elif request.as_root:
            if hasattr(self._client, "trace") and callable(self._client.trace):
                try:
                    session_id = request.metadata.get("session_id")
                    user_id = request.metadata.get("user_code") or request.metadata.get(
                        "user_name"
                    )
                    self._client.trace(
                        id=request.trace_id,
                        name=request.name,
                        session_id=str(session_id) if session_id else None,
                        user_id=str(user_id) if user_id else None,
                    )
                except Exception:  # pylint: disable=broad-exception-caught
                    pass
            self._set_root_trace_attributes(obs, request)

        return obs

    @staticmethod
    def _set_root_trace_attributes(
        observation: ObservationHandle | None,
        request: _ObservationStartRequest,
    ) -> None:
        """Set trace-level attributes used by Langfuse's trace list UI."""
        if (
            observation is None
            or not hasattr(observation, "_otel_span")
            or observation._otel_span is None
        ):
            return
        try:
            from langfuse._client.attributes import LangfuseOtelSpanAttributes

            observation._otel_span.set_attribute(
                LangfuseOtelSpanAttributes.TRACE_NAME, request.name
            )
            session_id = request.metadata.get("session_id")
            if session_id:
                observation._otel_span.set_attribute(
                    LangfuseOtelSpanAttributes.TRACE_SESSION_ID, str(session_id)
                )
            user_id = request.metadata.get("user_code") or request.metadata.get(
                "user_name"
            )
            if user_id:
                observation._otel_span.set_attribute(
                    LangfuseOtelSpanAttributes.TRACE_USER_ID, str(user_id)
                )
        except Exception:  # pylint: disable=broad-exception-caught
            pass

    def shutdown(self) -> None:
        """Flush tracing state using whichever shutdown API the SDK exposes."""
        shutdown = getattr(self._client, "shutdown", None)
        if callable(shutdown):
            shutdown()
            return

        flush = getattr(self._client, "flush", None)
        if callable(flush):
            flush()


class LangfusePlugin(Plugin):
    """Emit worker task lifecycle data into Langfuse using existing trace ids."""

    def __init__(
        self,
        tracer: Optional[LangfuseTracer] = None,
        observation_store: Optional[ObservationStore] = None,
        plugin_id: str = "langfuse",
        enabled: bool = True,
    ):
        super().__init__(PluginManifest(plugin_id=plugin_id, enabled=enabled))
        self._tracer = tracer
        self._observation_store = observation_store

    async def register_agent_configs(
        self, build_context: Any
    ) -> list[AgentConfig] | None:
        del build_context
        return None

    async def on_worker_startup(self, worker: Any) -> None:
        if self._observation_store is None:
            registry = getattr(worker, "registry", None)
            if registry is not None:
                self._observation_store = WorkerRegistryObservationStore(registry)

        if self._tracer is None:
            self._tracer = self._build_default_tracer()

    async def on_worker_shutdown(self, worker: Any) -> None:
        del worker
        if self._tracer is not None:
            self._tracer.shutdown()

    async def on_task_start(self, context: Any) -> None:
        tracer = self._get_tracer()
        observation_store = self._get_observation_store(context)
        identity = self._build_task_identity(context)

        trace_id_hex = f"{str_to_uint128(identity.trace_id):032x}"

        # The client process owns client.dispatch. Worker observations consume
        # the propagated Langfuse parent id and continue the chain with
        # worker.execute -> agent -> LLM.
        execution_anchor = (
            context.execution_id
            if getattr(context, "execution_id", None)
            else context.message_id
        )
        command_input = self._serialize_value(
            getattr(context.current_command, "content", None)
        )
        metadata = self._build_metadata(identity, context)

        # Parent worker.execute to client.dispatch for top-level tasks. For child
        # agents, parent to the stored observation id of the calling task.
        is_resume = (
            "ResumeCommand" in context.current_command.__class__.__name__
            if getattr(context, "current_command", None) is not None
            else False
        )
        root_parent_id = metadata.get(LANGFUSE_PARENT_OBSERVATION_METADATA_KEY)
        if not identity.parent_message_id and is_resume:
            # For top-level task resumption, do not parent to any observation id
            # (especially to avoid parenting back to its own first stage which is in metadata).
            # This lets Langfuse automatically root it under the main trace.
            worker_execute_parent = ""
        elif root_parent_id:
            worker_execute_parent = str(root_parent_id)
        elif identity.parent_message_id:
            worker_execute_parent = (
                await observation_store.get_observation_id(
                    identity.session_id, identity.parent_message_id
                )
                or ""
            )
        else:
            worker_execute_parent = ""

        worker_execute_obs = tracer.start_observation(
            _ObservationStartRequest(
                span_id=str_to_uint64(f"{execution_anchor}:worker.execute"),
                trace_id=trace_id_hex,
                parent_observation_id=worker_execute_parent,
                name="worker.execute",
                observation_input=command_input,
                metadata=metadata,
            )
        )

        observation = tracer.start_observation(
            _ObservationStartRequest(
                span_id=str_to_uint64(f"{execution_anchor}:agent.task"),
                trace_id=trace_id_hex,
                parent_observation_id=worker_execute_obs.id
                if worker_execute_obs is not None
                else "",
                name=identity.agent_id,
                observation_input=command_input,
                metadata=metadata,
            )
        )

        setattr(context, LANGFUSE_OBSERVATION_ATTR, observation)
        setattr(context, WORKER_EXECUTE_OBSERVATION_ATTR, worker_execute_obs)
        # Child agents resolve this task through parent_message_id and nest under
        # agent.task.
        await observation_store.set_observation_id(
            identity.session_id, identity.message_id, observation.id
        )

    async def on_task_complete(self, context: Any, result: Any) -> None:
        self._end_observation(context, output=self._serialize_value(result))

    async def on_task_error(self, context: Any, error: Exception) -> None:
        observations = self._iter_context_observations(context)
        if not observations:
            return

        for observation in observations:
            observation.update(level="ERROR", status_message=str(error))
        self._end_observation(
            context,
            output={"error": str(error)},
        )

    async def on_task_cancel(self, context: Any, command: Any) -> None:
        observations = self._iter_context_observations(context)
        if not observations:
            return

        reason = getattr(command, "reason", "") or "cancelled"
        for observation in observations:
            observation.update(level="WARNING", status_message=reason)
        self._end_observation(
            context,
            output={"cancelled": True, "reason": reason},
        )

    def _get_tracer(self) -> LangfuseTracer:
        tracer = self._tracer or self._build_default_tracer()
        self._tracer = tracer
        return tracer

    def _get_observation_store(self, context: Any) -> ObservationStore:
        if self._observation_store is not None:
            return self._observation_store

        self._observation_store = WorkerRegistryObservationStore(
            WorkerRegistry(getattr(context, "redis", None))
        )
        return self._observation_store

    def _build_default_tracer(self) -> LangfuseTracer:
        config = LangfuseConfig.from_env()
        if config is None:
            raise RuntimeError(
                "LangfusePlugin requires LANGFUSE_SECRET_KEY, "
                "LANGFUSE_PUBLIC_KEY, and LANGFUSE_BASE_URL to be set."
            )

        try:
            langfuse_module = import_module("langfuse")
        except ImportError as err:
            raise RuntimeError(
                "LangfusePlugin requires the 'langfuse' package to be installed "
                "or an explicit tracer to be provided."
            ) from err
        langfuse_client_cls = getattr(langfuse_module, "Langfuse")

        try:
            client = langfuse_client_cls(
                public_key=config.public_key,
                secret_key=config.secret_key,
                base_url=config.base_url,
            )
        except TypeError:
            client = langfuse_client_cls(
                public_key=config.public_key,
                secret_key=config.secret_key,
                host=config.base_url,
            )

        return _SdkLangfuseTracer(client)

    @staticmethod
    def _build_task_identity(context: Any) -> _TaskIdentity:
        command = getattr(context, "current_command", None)
        header = getattr(command, "header", None)
        return _TaskIdentity(
            session_id=getattr(context, "session_id", ""),
            trace_id=getattr(context, "trace_id", ""),
            message_id=getattr(context, "message_id", ""),
            parent_message_id=getattr(context, "parent_message_id", ""),
            agent_id=(
                getattr(context, "current_agent_id", "")
                or getattr(header, "target_agent_type", "")
                or "unknown-agent"
            ),
            user_code=getattr(header, "user_code", ""),
            user_name=getattr(header, "user_name", ""),
        )

    @classmethod
    def _build_metadata(cls, identity: _TaskIdentity, context: Any) -> dict[str, Any]:
        command = getattr(context, "current_command", None)
        header = getattr(command, "header", None)
        header_metadata = cls._serialize_value(getattr(header, "metadata", {}))
        metadata = {
            "message_id": identity.message_id,
            "parent_message_id": identity.parent_message_id,
            "session_id": identity.session_id,
            "trace_id": identity.trace_id,
            "agent_id": identity.agent_id,
            "user_code": identity.user_code,
            "user_name": identity.user_name,
            "header_metadata": header_metadata,
        }

        langfuse_parent_observation_id = ""
        if header is not None:
            langfuse_parent_observation_id = getattr(
                header, "langfuse_parent_observation_id", ""
            ) or (
                header_metadata.get(LANGFUSE_PARENT_OBSERVATION_METADATA_KEY, "")
                if isinstance(header_metadata, dict)
                else ""
            )

        if langfuse_parent_observation_id:
            metadata[LANGFUSE_PARENT_OBSERVATION_METADATA_KEY] = str(
                langfuse_parent_observation_id
            )
        return metadata

    @staticmethod
    def _serialize_value(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, dict):
            return {
                str(key): LangfusePlugin._serialize_value(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [LangfusePlugin._serialize_value(item) for item in value]
        if isinstance(value, tuple):
            return [LangfusePlugin._serialize_value(item) for item in value]
        return repr(value)

    @staticmethod
    def _get_context_observation(context: Any) -> Optional[ObservationHandle]:
        observation = getattr(context, LANGFUSE_OBSERVATION_ATTR, None)
        if observation is None:
            return None
        return observation

    @staticmethod
    def _iter_context_observations(context: Any) -> list[ObservationHandle]:
        """Return the agent task and worker.execute observations, innermost first."""
        observations: list[ObservationHandle] = []
        for attr in (
            LANGFUSE_OBSERVATION_ATTR,
            WORKER_EXECUTE_OBSERVATION_ATTR,
        ):
            observation = getattr(context, attr, None)
            if observation is not None:
                observations.append(observation)
        return observations

    def _end_observation(self, context: Any, *, output: Any) -> None:
        serialized_output = self._serialize_value(output)
        for observation in self._iter_context_observations(context):
            try:
                observation.end(output=serialized_output)
            except TypeError:
                observation.update(output=serialized_output)
                observation.end()


class LangfuseTraceProviderFactory(TraceProviderFactory):
    """Factory that enables Langfuse tracing when the environment is configured."""

    @property
    def provider_name(self) -> str:
        """Return the stable provider name used in discovery and conflicts."""
        return "langfuse"

    def build_plugin_from_env(self) -> Plugin | None:
        """Build the Langfuse plugin when all required config is present."""
        if LangfuseConfig.from_env() is None:
            return None
        return LangfusePlugin()


def start_client_dispatch_observation(
    *,
    trace_id: str,
    message_id: str,
    target_agent_type: str,
    session_id: str,
    user_code: str = "",
    user_name: str = "",
    content: Any = None,
    metadata: Optional[dict[str, Any]] = None,
) -> Optional[ObservationHandle]:
    """Start the client-side Langfuse root observation for a gateway dispatch."""
    config = LangfuseConfig.from_env()
    if config is None:
        return None

    try:
        langfuse_module = import_module("langfuse")
    except ImportError:
        return None

    langfuse_client_cls = getattr(langfuse_module, "Langfuse")
    try:
        client = langfuse_client_cls(
            public_key=config.public_key,
            secret_key=config.secret_key,
            base_url=config.base_url,
        )
    except TypeError:
        client = langfuse_client_cls(
            public_key=config.public_key,
            secret_key=config.secret_key,
            host=config.base_url,
        )

    trace_id_hex = f"{str_to_uint128(trace_id):032x}"
    dispatch_metadata = {
        "message_id": message_id,
        "session_id": session_id,
        "trace_id": trace_id,
        "target_agent_type": target_agent_type,
        "user_code": user_code,
        "user_name": user_name,
        "header_metadata": LangfusePlugin._serialize_value(metadata or {}),
    }
    return _SdkLangfuseTracer(client).start_observation(
        _ObservationStartRequest(
            span_id=str_to_uint64(f"{message_id}:client.dispatch"),
            trace_id=trace_id_hex,
            name=f"client.dispatch:{target_agent_type}",
            observation_input=LangfusePlugin._serialize_value(content),
            metadata=dispatch_metadata,
            as_root=True,
        )
    )
