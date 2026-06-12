"""Langfuse integration for by-framework task lifecycle events."""

# pylint: disable=protected-access,import-outside-toplevel,too-many-arguments

from __future__ import annotations

import asyncio
import os
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import import_module
from typing import Any, Optional, Protocol, runtime_checkable

from by_framework.common.logger import get_logger
from by_framework.core.extensions import (
    AgentConfig,
    Plugin,
    PluginManifest,
    TraceProviderFactory,
)
from by_framework.core.registry import WorkerRegistry
from by_framework.trace.span_recorder import (
    configure_otel_id_generator,
    current_span_id_var,
    current_trace_id_var,
    str_to_uint64,
    str_to_uint128,
)

logger = get_logger(__name__)

LANGFUSE_OBSERVATION_ATTR = "_langfuse_observation"
LANGFUSE_CALL_PARENT_OBSERVATION_ATTR = "_langfuse_call_parent_observation"
LANGFUSE_WORKFLOW_OBSERVATION_ATTR = "_langfuse_workflow_observation"
WORKER_EXECUTE_OBSERVATION_ATTR = "_langfuse_worker_execute_observation"
LANGFUSE_PARENT_OBSERVATION_METADATA_KEY = "langfuse_parent_observation_id"
_QUOTES_TO_STRIP = "\"'“”‘’"
_FALSE_LIKE_VALUES = {"0", "false", "no", "off", "disabled"}
_CLIENT_DISPATCH_TRACER_CACHE: dict[LangfuseConfig, "_SdkLangfuseTracer"] = {}


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

    def update_trace_output(self, trace_id: str, output: Any) -> None:
        """Update trace-level output shown in Langfuse trace lists."""

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

    def __init__(self, client: Any, config: LangfuseConfig | None = None):
        self._client = client
        self._config = config

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

    def update_trace_output(self, trace_id: str, output: Any) -> None:
        """Best-effort trace output upsert for the Langfuse trace list."""
        if self._config is None or not trace_id:
            return

        try:
            import httpx

            now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            payload = {
                "batch": [
                    {
                        "id": str(uuid.uuid4()),
                        "type": "trace-create",
                        "timestamp": now,
                        "body": {"id": trace_id, "output": output},
                    }
                ]
            }
            base_url = self._config.base_url.rstrip("/")
            response = httpx.post(
                f"{base_url}/api/public/ingestion",
                json=payload,
                auth=(self._config.public_key, self._config.secret_key),
                headers={
                    "x-langfuse-sdk-name": "by-framework-python",
                    "x-langfuse-public-key": self._config.public_key,
                },
                timeout=5,
            )
            response.raise_for_status()
        except Exception:  # pylint: disable=broad-exception-caught
            pass

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
        max_active_workflows: int = 10000,
    ):
        super().__init__(PluginManifest(plugin_id=plugin_id, enabled=enabled))
        self._tracer = tracer
        self._observation_store = observation_store
        self._active_workflows: OrderedDict[tuple[str, str], ObservationHandle] = (
            OrderedDict()
        )
        self._max_active_workflows = max(1, int(max_active_workflows or 10000))
        self._pending_trace_output_updates: set[asyncio.Future[Any]] = set()

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
        if self._pending_trace_output_updates:
            await asyncio.gather(
                *list(self._pending_trace_output_updates),
                return_exceptions=True,
            )
        if self._tracer is not None:
            self._tracer.shutdown()

    async def on_task_start(self, context: Any) -> None:  # pylint: disable=too-many-locals
        tracer = self._get_tracer()
        if tracer is None:
            return
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

        # Parent worker.execute under a durable workflow node. The workflow spans
        # the logical task across async suspend/resume; worker.execute only spans
        # one concrete worker execution segment.
        is_resume = (
            "ResumeCommand" in context.current_command.__class__.__name__
            if getattr(context, "current_command", None) is not None
            else False
        )
        observation_anchor = execution_anchor
        if is_resume:
            resume_parent = identity.parent_message_id or "root"
            observation_anchor = f"{execution_anchor}:resume:{resume_parent}"

        root_parent_id = metadata.get(LANGFUSE_PARENT_OBSERVATION_METADATA_KEY)
        workflow_root_parent_id = (
            ""
            if is_resume and not identity.parent_message_id
            else str(root_parent_id or "")
        )
        workflow_key = (identity.session_id, identity.message_id)
        workflow_obs = self._active_workflows.get(workflow_key)
        if workflow_obs is not None:
            self._active_workflows.move_to_end(workflow_key)
        workflow_span_id = str_to_uint64(f"{execution_anchor}:agent.workflow")
        workflow_observation_id = f"{workflow_span_id:016x}"

        if workflow_obs is None:
            workflow_parent = await self._resolve_parent_observation_id(
                identity=identity,
                observation_store=observation_store,
                root_parent_id=workflow_root_parent_id,
                excluded_parent_id=workflow_observation_id,
            )
            workflow_obs = tracer.start_observation(
                _ObservationStartRequest(
                    span_id=workflow_span_id,
                    trace_id=trace_id_hex,
                    parent_observation_id=workflow_parent,
                    name=f"agent.workflow:{identity.agent_id}",
                    observation_input=command_input,
                    metadata=metadata,
                )
            )
            if workflow_obs is not None:
                self._active_workflows[workflow_key] = workflow_obs
                self._evict_active_workflows_if_needed()

        header_metadata = metadata.get("header_metadata", {})
        framework_parent_span_id = (
            str(header_metadata.get("framework_parent_span_id", "") or "")
            if isinstance(header_metadata, dict)
            else ""
        )
        is_agent_return_resume = framework_parent_span_id.endswith(":agent.return")
        worker_parent_observation_id = workflow_obs.id if workflow_obs else ""
        if is_resume and is_agent_return_resume and root_parent_id:
            worker_parent_observation_id = str(root_parent_id)

        worker_execute_obs = tracer.start_observation(
            _ObservationStartRequest(
                span_id=str_to_uint64(f"{observation_anchor}:worker.execute"),
                trace_id=trace_id_hex,
                parent_observation_id=worker_parent_observation_id,
                name="worker.execute",
                observation_input=command_input,
                metadata=metadata,
            )
        )

        observation = tracer.start_observation(
            _ObservationStartRequest(
                span_id=str_to_uint64(f"{observation_anchor}:agent.task"),
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
        setattr(context, LANGFUSE_WORKFLOW_OBSERVATION_ATTR, workflow_obs)
        setattr(context, LANGFUSE_CALL_PARENT_OBSERVATION_ATTR, workflow_obs)
        setattr(context, WORKER_EXECUTE_OBSERVATION_ATTR, worker_execute_obs)
        if observation is not None and hasattr(
            context, "set_trace_parent_observation_id"
        ):
            context.set_trace_parent_observation_id(observation.id)
        # Child agents resolve this task through parent_message_id and nest under
        # the durable workflow instead of a worker segment that may end on suspend.
        if workflow_obs is not None:
            await observation_store.set_observation_id(
                identity.session_id, identity.message_id, workflow_obs.id
            )

    async def on_task_complete(self, context: Any, result: Any) -> None:
        serialized_output = self._serialize_value(result)
        self._update_observation_usage(context)
        self._end_observation(context, output=serialized_output)
        if not self._is_non_terminal_result(result):
            self._end_workflow_observation(context, output=serialized_output)
        self._update_trace_output(context, output=serialized_output)

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
        self._end_workflow_observation(context, output={"error": str(error)})
        self._update_trace_output(context, output={"error": str(error)})

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
        self._end_workflow_observation(
            context,
            output={"cancelled": True, "reason": reason},
        )
        self._update_trace_output(
            context,
            output={"cancelled": True, "reason": reason},
        )

    async def on_call_agent_start(self, context: Any, command: Any) -> None:
        """Create a call observation and pass it as the child task parent."""
        tracer = self._get_tracer()
        if tracer is None:
            return
        header = getattr(command, "header", None)
        if header is None:
            return

        trace_id = getattr(header, "trace_id", "") or getattr(context, "trace_id", "")
        message_id = str(getattr(header, "message_id", "") or "")
        if not trace_id or not message_id:
            return

        parent_observation_id = getattr(
            header, "langfuse_parent_observation_id", ""
        ) or getattr(header, "metadata", {}).get(
            LANGFUSE_PARENT_OBSERVATION_METADATA_KEY, ""
        )
        target_agent_type = str(getattr(header, "target_agent_type", "") or "")
        metadata = {
            "message_id": message_id,
            "parent_message_id": str(getattr(header, "parent_message_id", "") or ""),
            "session_id": str(getattr(header, "session_id", "") or ""),
            "trace_id": trace_id,
            "source_agent_type": str(getattr(header, "source_agent_type", "") or ""),
            "target_agent_type": target_agent_type,
            "header_metadata": self._serialize_value(getattr(header, "metadata", {})),
        }
        observation = tracer.start_observation(
            _ObservationStartRequest(
                span_id=str_to_uint64(f"{message_id}:client.dispatch"),
                trace_id=f"{str_to_uint128(trace_id):032x}",
                parent_observation_id=str(parent_observation_id or ""),
                name=f"agent.call_agent:{target_agent_type}",
                observation_input=self._serialize_value(
                    getattr(command, "content", None)
                ),
                metadata=metadata,
            )
        )
        if observation is None:
            return

        setattr(command, "_langfuse_call_observation", observation)
        header.langfuse_parent_observation_id = observation.id
        header.metadata[LANGFUSE_PARENT_OBSERVATION_METADATA_KEY] = observation.id

    async def on_call_agent_complete(
        self,
        context: Any,
        command: Any,
        result: Any,
    ) -> None:
        del context
        observation = getattr(command, "_langfuse_call_observation", None)
        if observation is None:
            return
        output = self._serialize_value(result)
        try:
            observation.end(output=output)
        except TypeError:
            observation.update(output=output)
            observation.end()

    async def on_call_agent_error(
        self,
        context: Any,
        command: Any,
        error: Exception,
    ) -> None:
        del context
        observation = getattr(command, "_langfuse_call_observation", None)
        if observation is None:
            return
        output = {"error": str(error)}
        try:
            observation.update(level="ERROR", status_message=str(error), output=output)
            observation.end()
        except TypeError:
            observation.end(output=output)

    async def on_agent_return_start(
        self,
        context: Any,
        command: Any,
        callback_command: Any,
    ) -> None:
        """Create a return observation and pass it as the resume parent."""
        del context, command
        tracer = self._get_tracer()
        if tracer is None:
            return
        header = getattr(callback_command, "header", None)
        if header is None:
            return

        trace_id = str(getattr(header, "trace_id", "") or "")
        framework_parent_span_id = str(
            getattr(header, "metadata", {}).get("framework_parent_span_id", "") or ""
        )
        if not trace_id or not framework_parent_span_id:
            return

        parent_observation_id = getattr(
            header, "langfuse_parent_observation_id", ""
        ) or getattr(header, "metadata", {}).get(
            LANGFUSE_PARENT_OBSERVATION_METADATA_KEY, ""
        )
        source_agent_type = str(getattr(header, "source_agent_type", "") or "")
        target_agent_type = str(getattr(header, "target_agent_type", "") or "")
        metadata = {
            "message_id": str(getattr(header, "message_id", "") or ""),
            "parent_message_id": str(getattr(header, "parent_message_id", "") or ""),
            "session_id": str(getattr(header, "session_id", "") or ""),
            "trace_id": trace_id,
            "source_agent_type": source_agent_type,
            "target_agent_type": target_agent_type,
            "return_from_agent_type": source_agent_type,
            "return_to_agent_type": target_agent_type,
            "return_route": f"{source_agent_type}->{target_agent_type}",
            "header_metadata": self._serialize_value(getattr(header, "metadata", {})),
        }
        observation = tracer.start_observation(
            _ObservationStartRequest(
                span_id=str_to_uint64(framework_parent_span_id),
                trace_id=f"{str_to_uint128(trace_id):032x}",
                parent_observation_id=str(parent_observation_id or ""),
                name="agent.return",
                observation_input=self._serialize_value(
                    {
                        "status": getattr(callback_command, "status", ""),
                        "content": getattr(callback_command, "content", ""),
                        "reply_data": getattr(callback_command, "reply_data", {}),
                    }
                ),
                metadata=metadata,
            )
        )
        if observation is None:
            return

        setattr(callback_command, "_langfuse_return_observation", observation)
        header.langfuse_parent_observation_id = observation.id
        header.metadata[LANGFUSE_PARENT_OBSERVATION_METADATA_KEY] = observation.id

    async def on_agent_return_complete(
        self,
        context: Any,
        command: Any,
        callback_command: Any,
    ) -> None:
        del context, command
        observation = getattr(callback_command, "_langfuse_return_observation", None)
        if observation is None:
            return
        output = self._serialize_value(
            {
                "status": getattr(callback_command, "status", ""),
                "reply_data": getattr(callback_command, "reply_data", {}),
            }
        )
        try:
            observation.end(output=output)
        except TypeError:
            observation.update(output=output)
            observation.end()

    async def on_agent_return_error(
        self,
        context: Any,
        command: Any,
        callback_command: Any,
        error: Exception,
    ) -> None:
        del context, command
        observation = getattr(callback_command, "_langfuse_return_observation", None)
        if observation is None:
            return
        output = {"error": str(error)}
        try:
            observation.update(level="ERROR", status_message=str(error), output=output)
            observation.end()
        except TypeError:
            observation.end(output=output)

    async def _resolve_parent_observation_id(
        self,
        *,
        identity: _TaskIdentity,
        observation_store: ObservationStore,
        root_parent_id: str,
        excluded_parent_id: str,
    ) -> str:
        """Resolve the parent observation for a durable workflow span."""
        if root_parent_id and root_parent_id != excluded_parent_id:
            return root_parent_id
        if identity.parent_message_id:
            parent_id = await observation_store.get_observation_id(
                identity.session_id, identity.parent_message_id
            )
            if parent_id and parent_id != excluded_parent_id:
                return parent_id
        return ""

    def _get_tracer(self) -> Optional[LangfuseTracer]:
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

    def _build_default_tracer(self) -> Optional[LangfuseTracer]:
        config = LangfuseConfig.from_env()
        if config is None:
            logger.error(
                "LangfusePlugin: LANGFUSE_SECRET_KEY, LANGFUSE_PUBLIC_KEY, and "
                "LANGFUSE_BASE_URL are not set — Langfuse tracing disabled."
            )
            return None

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

        return _SdkLangfuseTracer(client, config)

    @staticmethod
    def _build_task_identity(context: Any) -> _TaskIdentity:
        command = getattr(context, "current_command", None)
        header = getattr(command, "header", None)
        is_resume = (
            "ResumeCommand" in command.__class__.__name__
            if command is not None
            else False
        )
        parent_message_id = getattr(context, "parent_message_id", "")
        if is_resume and header is not None:
            parent_message_id = (
                getattr(header, "parent_message_id", "") or parent_message_id
            )
        return _TaskIdentity(
            session_id=getattr(context, "session_id", ""),
            trace_id=getattr(context, "trace_id", ""),
            message_id=getattr(context, "message_id", ""),
            parent_message_id=parent_message_id,
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
        if isinstance(header_metadata, dict):
            framework_parent_span_id = str(
                header_metadata.get("framework_parent_span_id", "") or ""
            )
            if (
                framework_parent_span_id.endswith(":agent.return")
                and header is not None
            ):
                metadata["resume_via"] = "agent.return"
                metadata["resume_from_agent_type"] = str(
                    getattr(header, "source_agent_type", "") or ""
                )
                metadata["resume_to_agent_type"] = str(
                    getattr(header, "target_agent_type", "") or identity.agent_id
                )
                metadata["resume_parent_message_id"] = identity.parent_message_id
                metadata["resume_return_span_id"] = framework_parent_span_id
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

    def _update_observation_usage(self, context: Any) -> None:
        """Write accumulated token usage onto the worker.execute observation."""
        try:
            get_usage = getattr(context, "get_token_usage", None)
            token_usage = get_usage() if callable(get_usage) else {}
        except Exception:  # pylint: disable=broad-exception-caught
            return
        if not token_usage:
            return
        observation = getattr(context, WORKER_EXECUTE_OBSERVATION_ATTR, None)
        if observation is None:
            return
        try:
            observation.update(
                usage={
                    "input": token_usage.get("prompt_tokens", 0),
                    "output": token_usage.get("completion_tokens", 0),
                    "total": token_usage.get("total_tokens", 0),
                    "unit": "TOKENS",
                }
            )
        except Exception:  # pylint: disable=broad-exception-caught
            pass

    def _end_observation(self, context: Any, *, output: Any) -> None:
        serialized_output = self._serialize_value(output)
        for observation in self._iter_context_observations(context):
            try:
                observation.end(output=serialized_output)
            except TypeError:
                observation.update(output=serialized_output)
                observation.end()

    def _end_workflow_observation(self, context: Any, *, output: Any) -> None:
        observation = getattr(context, LANGFUSE_WORKFLOW_OBSERVATION_ATTR, None)
        if observation is None:
            return

        serialized_output = self._serialize_value(output)
        try:
            observation.end(output=serialized_output)
        except TypeError:
            observation.update(output=serialized_output)
            observation.end()

        key = (
            str(getattr(context, "session_id", "")),
            str(getattr(context, "message_id", "")),
        )
        if self._active_workflows.get(key) is observation:
            self._active_workflows.pop(key, None)

    def _evict_active_workflows_if_needed(self) -> None:
        while len(self._active_workflows) > self._max_active_workflows:
            _, observation = self._active_workflows.popitem(last=False)
            try:
                observation.update(
                    level="WARNING",
                    status_message="workflow evicted from active workflow cache",
                )
                observation.end()
            except Exception:  # pylint: disable=broad-exception-caught
                pass

    @staticmethod
    def _is_non_terminal_result(result: Any) -> bool:
        if not isinstance(result, dict):
            return False
        status = str(result.get("status", ""))
        return status == "QUEUED" or status.startswith("QUEUED:")

    def _update_trace_output(self, context: Any, *, output: Any) -> None:
        tracer = self._tracer
        if tracer is None or not hasattr(tracer, "update_trace_output"):
            return

        trace_id = getattr(context, "trace_id", "")
        if not trace_id:
            return

        try:
            trace_id_hex = f"{str_to_uint128(trace_id):032x}"
            serialized_output = self._serialize_value(output)
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                tracer.update_trace_output(trace_id_hex, serialized_output)
                return
            future = loop.run_in_executor(
                None,
                tracer.update_trace_output,
                trace_id_hex,
                serialized_output,
            )
            self._pending_trace_output_updates.add(future)

            def _discard_done(done: asyncio.Future[Any]) -> None:
                self._pending_trace_output_updates.discard(done)
                try:
                    done.result()
                except Exception:  # pylint: disable=broad-exception-caught
                    pass

            future.add_done_callback(_discard_done)
        except Exception:  # pylint: disable=broad-exception-caught
            pass


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

    tracer = _CLIENT_DISPATCH_TRACER_CACHE.get(config)
    if tracer is None:
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
        tracer = _SdkLangfuseTracer(client, config)
        _CLIENT_DISPATCH_TRACER_CACHE[config] = tracer

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
    return tracer.start_observation(
        _ObservationStartRequest(
            span_id=str_to_uint64(f"{message_id}:client.dispatch"),
            trace_id=trace_id_hex,
            name=f"client.dispatch:{target_agent_type}",
            observation_input=LangfusePlugin._serialize_value(content),
            metadata=dispatch_metadata,
            as_root=True,
        )
    )
