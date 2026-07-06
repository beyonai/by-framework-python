"""Redis-backed trace span recording helpers and generic span exporters."""

from __future__ import annotations

import contextvars
import hashlib
import json
import os
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field, replace
from inspect import isawaitable
from typing import (Any, AsyncIterator, Optional, Protocol, Sequence, runtime_checkable)

from by_framework.common.constants import RedisKeys
from by_framework.common.logger import logger
from by_framework.common.redis_client import Redis, get_redis

# Context variables to temporarily override generated trace/span IDs
current_trace_id_var: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "current_trace_id_var", default=None
)
current_span_id_var: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "current_span_id_var", default=None
)


def str_to_uint128(s: str) -> int:
    """Convert a string to a deterministic 128-bit integer for OTEL TraceId."""
    if len(s) == 32:
        try:
            return int(s, 16)
        except ValueError:
            pass
    val = int(hashlib.md5(s.encode()).hexdigest(), 16)
    return val if val != 0 else 1


def str_to_uint64(s: str) -> int:
    """Convert a string to a deterministic 64-bit integer for OTEL SpanId."""
    if len(s) == 16:
        try:
            return int(s, 16)
        except ValueError:
            pass
    val = int(hashlib.md5(s.encode()).hexdigest()[:16], 16)
    return val if val != 0 else 1


try:
    from opentelemetry.sdk.trace.id_generator import IdGenerator

    class ContextIdGenerator(IdGenerator):
        """Custom OpenTelemetry ID Generator.

        Checks context variables before falling back.
        """

        def generate_trace_id(self) -> int:
            val = current_trace_id_var.get()
            if val is not None:
                return val
            import secrets

            val_rand = secrets.randbits(128)
            return val_rand if val_rand != 0 else 1

        def generate_span_id(self) -> int:
            val = current_span_id_var.get()
            if val is not None:
                return val
            import secrets

            val_rand = secrets.randbits(64)
            return val_rand if val_rand != 0 else 1

except ImportError:

    class ContextIdGenerator:  # type: ignore
        """Fallback ID generator used when OpenTelemetry is not installed."""

        def generate_trace_id(self) -> int:
            """Return a context-provided or random 128-bit trace id."""
            val = current_trace_id_var.get()
            if val is not None:
                return val
            import secrets

            val_rand = secrets.randbits(128)
            return val_rand if val_rand != 0 else 1

        def generate_span_id(self) -> int:
            """Return a context-provided or random 64-bit span id."""
            val = current_span_id_var.get()
            if val is not None:
                return val
            import secrets

            val_rand = secrets.randbits(64)
            return val_rand if val_rand != 0 else 1


def configure_otel_id_generator() -> None:
    """Hot-patch the global OpenTelemetry TracerProvider to use ContextIdGenerator."""
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider

        provider = trace.get_tracer_provider()
        if hasattr(provider, "_delegate") and provider._delegate is not None:
            provider = provider._delegate

        if isinstance(provider, TracerProvider):
            if not isinstance(provider.id_generator, ContextIdGenerator):
                provider.id_generator = ContextIdGenerator()
            # Patch already instantiated tracers
            if hasattr(provider, "_tracers") and isinstance(provider._tracers, dict):
                for t in provider._tracers.values():
                    if hasattr(t, "id_generator") and not isinstance(
                        t.id_generator, ContextIdGenerator
                    ):
                        t.id_generator = provider.id_generator
    except Exception:  # pylint: disable=broad-exception-caught
        pass


TRACE_TTL_SECONDS = 15 * 60  # Default to 15 minutes to avoid unbounded Redis growth.
DEFAULT_METADATA_VALUE_MAX_LENGTH = 256
DEFAULT_IO_VALUE_MAX_LENGTH = 4096
SENSITIVE_KEY_PARTS = (
    "api_key",
    "apikey",
    "authorization",
    "auth_token",
    "credential",
    "password",
    "secret",
    "token",
)
DISABLED_VALUES = {"0", "false", "no", "off", "disabled"}
ENABLED_VALUES = {"1", "true", "yes", "on", "enabled"}

_OBSERVABILITY_DIAGNOSTICS: dict[str, Any] = {
    "dropped_spans_total": 0,
    "dropped_spans_by_reason": {},
    "export_failures_total": 0,
    "export_failures_by_exporter": {},
}
_OBSERVABILITY_DIAGNOSTICS_LOCK = threading.Lock()
_LANGFUSE_PROCESSOR_PROVIDER_IDS: set[int] = set()
_LANGFUSE_PROCESSOR_LOCK = threading.Lock()


@dataclass(frozen=True)
class ObservabilityConfig:
    """Runtime configuration for by-framework observability exporters."""

    enabled: bool = True
    redis_enabled: bool = True
    otel_enabled: bool = False
    langfuse_enabled: bool = True
    ttl_seconds: int = TRACE_TTL_SECONDS
    sample_rate: float = 1.0
    max_spans_per_trace: int = 1000
    metadata_value_max_length: int = DEFAULT_METADATA_VALUE_MAX_LENGTH
    # When True, trace input/output fields are replaced with "[REDACTED]".
    redact_inputs: bool = False
    io_value_max_length: int = DEFAULT_IO_VALUE_MAX_LENGTH


def build_observability_config() -> ObservabilityConfig:
    """Build observability config from environment variables."""
    enabled = _env_bool("BY_FRAMEWORK_OBSERVABILITY_ENABLED", default=True)
    return ObservabilityConfig(
        enabled=enabled,
        redis_enabled=enabled
        and _env_bool("BY_FRAMEWORK_TRACE_REDIS_ENABLED", default=True),
        otel_enabled=enabled and _env_bool("BY_FRAMEWORK_OTEL_ENABLED", default=False),
        langfuse_enabled=enabled
        and _env_bool(
            "BY_FRAMEWORK_LANGFUSE_ENABLED",
            default=_env_bool("BYAI_LANGFUSE_ENABLED", default=True),
        ),
        ttl_seconds=_env_int("BY_FRAMEWORK_TRACE_TTL_SECONDS", TRACE_TTL_SECONDS),
        sample_rate=_env_float("BY_FRAMEWORK_TRACE_SAMPLE_RATE", 1.0, 0.0, 1.0),
        max_spans_per_trace=max(
            1, _env_int("BY_FRAMEWORK_TRACE_MAX_SPANS_PER_TRACE", 1000)
        ),
        metadata_value_max_length=max(
            32,
            _env_int(
                "BY_FRAMEWORK_TRACE_METADATA_VALUE_MAX_LENGTH",
                DEFAULT_METADATA_VALUE_MAX_LENGTH,
            ),
        ),
        redact_inputs=_env_bool("BY_FRAMEWORK_REDACT_INPUTS", default=False),
        io_value_max_length=max(
            64,
            _env_int("BY_FRAMEWORK_TRACE_IO_MAX_LENGTH", DEFAULT_IO_VALUE_MAX_LENGTH),
        ),
    )


def sanitize_io_value(value: Any, config: "ObservabilityConfig") -> Any:
    """Sanitize a trace input/output value according to the observability config.

    Unlike metadata sanitization (key-name-based), this applies a larger
    truncation budget appropriate for user-visible content, and respects the
    global ``redact_inputs`` flag.
    """
    if config.redact_inputs:
        return "[REDACTED]"
    max_len = config.io_value_max_length
    if isinstance(value, str):
        return value[:max_len] + "...[TRUNCATED]" if len(value) > max_len else value
    if isinstance(value, dict):
        return {
            str(k): sanitize_io_value(v, config) for k, v in list(value.items())[:50]
        }
    if isinstance(value, (list, tuple)):
        return [sanitize_io_value(item, config) for item in value[:50]]
    return value


def get_observability_diagnostics() -> dict[str, Any]:
    """Return trace exporter self-diagnostics."""
    with _OBSERVABILITY_DIAGNOSTICS_LOCK:
        return {
            "dropped_spans_total": int(
                _OBSERVABILITY_DIAGNOSTICS["dropped_spans_total"]
            ),
            "dropped_spans_by_reason": dict(
                _OBSERVABILITY_DIAGNOSTICS["dropped_spans_by_reason"]
            ),
            "export_failures_total": int(
                _OBSERVABILITY_DIAGNOSTICS["export_failures_total"]
            ),
            "export_failures_by_exporter": dict(
                _OBSERVABILITY_DIAGNOSTICS["export_failures_by_exporter"]
            ),
        }


def reset_observability_diagnostics() -> None:
    """Reset trace exporter self-diagnostics for tests."""
    with _OBSERVABILITY_DIAGNOSTICS_LOCK:
        _OBSERVABILITY_DIAGNOSTICS["dropped_spans_total"] = 0
        _OBSERVABILITY_DIAGNOSTICS["dropped_spans_by_reason"] = {}
        _OBSERVABILITY_DIAGNOSTICS["export_failures_total"] = 0
        _OBSERVABILITY_DIAGNOSTICS["export_failures_by_exporter"] = {}


def _record_drop(reason: str) -> None:
    with _OBSERVABILITY_DIAGNOSTICS_LOCK:
        _OBSERVABILITY_DIAGNOSTICS["dropped_spans_total"] += 1
        by_reason = _OBSERVABILITY_DIAGNOSTICS["dropped_spans_by_reason"]
        by_reason[reason] = int(by_reason.get(reason, 0)) + 1


def _record_export_failure(exporter_name: str) -> None:
    with _OBSERVABILITY_DIAGNOSTICS_LOCK:
        _OBSERVABILITY_DIAGNOSTICS["export_failures_total"] += 1
        by_exporter = _OBSERVABILITY_DIAGNOSTICS["export_failures_by_exporter"]
        by_exporter[exporter_name] = int(by_exporter.get(exporter_name, 0)) + 1


def _clean_env(value: str | None) -> str:
    return value.strip().strip("'\"“”‘’") if value else ""


def _env_bool(name: str, *, default: bool) -> bool:
    value = _clean_env(os.environ.get(name)).lower()
    if not value:
        return default
    if value in ENABLED_VALUES:
        return True
    if value in DISABLED_VALUES:
        return False
    return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(_clean_env(os.environ.get(name)) or default)
    except ValueError:
        return default


def _env_float(name: str, default: float, minimum: float, maximum: float) -> float:
    try:
        value = float(_clean_env(os.environ.get(name)) or default)
    except ValueError:
        return default
    return min(maximum, max(minimum, value))


def _contains_sensitive_marker(value: str) -> bool:
    lowered = value.lower()
    return any(marker in lowered for marker in SENSITIVE_KEY_PARTS)


def _is_sensitive_key(key: str) -> bool:
    return _contains_sensitive_marker(key.replace("-", "_"))


def _sanitize_value(
    key: str,
    value: Any,
    *,
    max_length: int = DEFAULT_METADATA_VALUE_MAX_LENGTH,
) -> Any:
    if _is_sensitive_key(key):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {
            str(child_key): _sanitize_value(
                str(child_key), child_value, max_length=max_length
            )
            for child_key, child_value in value.items()
        }
    if isinstance(value, list):
        return [
            _sanitize_value(key, item, max_length=max_length) for item in value[:50]
        ]
    if isinstance(value, tuple):
        return [
            _sanitize_value(key, item, max_length=max_length) for item in value[:50]
        ]
    if isinstance(value, str):
        if _contains_sensitive_marker(value):
            return "[REDACTED]"
        if len(value) > max_length:
            return f"{value[:max_length]}...[TRUNCATED]"
    return value


@dataclass(frozen=True)
class TraceSpan:
    """A single distributed trace span."""

    trace_id: str
    span_id: str
    parent_span_id: str
    operation: str
    component: str
    start_ts: int
    end_ts: int
    status: str
    name: str = ""
    kind: str = ""
    source: str = "redis"
    input: Any = None
    output: Any = None
    tokens: dict[str, Any] = field(default_factory=dict)
    cost: dict[str, Any] = field(default_factory=dict)
    session_id: str = ""
    execution_id: str = ""
    message_id: str = ""
    parent_message_id: str = ""
    worker_id: str = ""
    source_agent_type: str = ""
    target_agent_type: str = ""
    error_type: str = ""
    error_message: str = ""
    error_code: str = ""
    failed_stage: str = ""
    retryable: bool = False
    route_policy: str = ""
    route_status: str = ""
    queue_wait_ms: int = 0
    chunk_count: int = 0
    event_type: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        """Return a JSON-serializable span payload."""
        payload = asdict(self)
        payload["start_ts"] = int(self.start_ts or 0)
        payload["end_ts"] = max(payload["start_ts"], int(self.end_ts or 0))
        payload["duration_ms"] = max(0, payload["end_ts"] - payload["start_ts"])
        payload["name"] = self.name or self.operation
        if payload.get("error_message"):
            payload["error_message"] = _sanitize_value(
                "error_message", payload["error_message"]
            )
        if payload.get("metadata"):
            payload["metadata"] = _sanitize_value("metadata", payload["metadata"])
        return {key: value for key, value in payload.items() if value not in ("", None)}


@runtime_checkable
class SpanExporter(Protocol):
    """Protocol for tracing span exporters."""

    async def export_span(self, span: TraceSpan) -> None:
        """Export a single trace span to the backend."""
        pass


class RedisSpanExporter:
    """Export trace spans to Redis with short-lived cache TTL."""

    def __init__(
        self,
        redis_client: Optional[Redis] = None,
        *,
        ttl_seconds: int = TRACE_TTL_SECONDS,
    ) -> None:
        self.redis = redis_client or get_redis()
        self.ttl_seconds = max(1, int(ttl_seconds or TRACE_TTL_SECONDS))

    async def export_span(self, span: TraceSpan) -> None:
        """Persist a span and update trace lookup indexes in Redis."""
        payload = span.to_payload()
        trace_id = str(payload["trace_id"])
        start_ts = int(payload.get("start_ts", 0) or 0)
        end_ts = int(payload.get("end_ts", start_ts) or start_ts)
        meta_key = RedisKeys.trace_meta(trace_id)
        spans_key = RedisKeys.trace_spans(trace_id)
        existing_start_ts = await self._read_hash_int(meta_key, "start_ts")
        existing_updated_at = await self._read_hash_int(meta_key, "updated_at")
        trace_start_ts = (
            min(value for value in (existing_start_ts, start_ts) if value > 0)
            if existing_start_ts or start_ts
            else 0
        )
        updated_at = max(existing_updated_at, end_ts)
        # trace_meta/trace_spans share a Cluster hash tag (trace_id) since
        # Phase 2a, so this group can stay atomic. The session/worker/agent
        # indexes below are cross-entity relative to the trace group and are
        # written as independent, best-effort calls instead.
        pipe = self.redis.pipeline()
        if isawaitable(pipe):
            pipe = await pipe
        await self._call_pipeline(pipe, "hset", meta_key, "trace_id", trace_id)
        await self._call_pipeline(
            pipe, "hset", meta_key, "session_id", str(payload.get("session_id", ""))
        )
        await self._call_pipeline(
            pipe, "hset", meta_key, "status", str(payload.get("status", ""))
        )
        operation = str(payload.get("operation", ""))
        if payload.get("name") and operation.startswith("client.dispatch"):
            await self._call_pipeline(
                pipe, "hset", meta_key, "name", str(payload.get("name", ""))
            )
        if payload.get("target_agent_type") and operation.startswith("client.dispatch"):
            await self._call_pipeline(
                pipe,
                "hset",
                meta_key,
                "root_agent_type",
                str(payload.get("target_agent_type", "")),
            )
        if payload.get("message_id") and operation.startswith("client.dispatch"):
            await self._call_pipeline(
                pipe,
                "hset",
                meta_key,
                "root_message_id",
                str(payload.get("message_id", "")),
            )
        await self._call_pipeline(pipe, "hset", meta_key, "start_ts", trace_start_ts)
        await self._call_pipeline(pipe, "hset", meta_key, "updated_at", updated_at)
        await self._call_pipeline(
            pipe, "rpush", spans_key, json.dumps(payload, ensure_ascii=False)
        )
        await self._call_pipeline(pipe, "expire", meta_key, self.ttl_seconds)
        await self._call_pipeline(pipe, "expire", spans_key, self.ttl_seconds)
        result = pipe.execute()
        if isawaitable(result):
            await result

        if payload.get("session_id"):
            await self._write_trace_index(
                RedisKeys.trace_index_session(str(payload["session_id"])),
                trace_id,
                start_ts,
            )
        if payload.get("worker_id"):
            await self._write_trace_index(
                RedisKeys.trace_index_worker(str(payload["worker_id"])),
                trace_id,
                start_ts,
            )
        if payload.get("target_agent_type"):
            await self._write_trace_index(
                RedisKeys.trace_index_agent(str(payload["target_agent_type"])),
                trace_id,
                start_ts,
            )

    async def _write_trace_index(
        self, index_key: str, trace_id: str, score: int
    ) -> None:
        """Best-effort write to a trace lookup index (session/worker/agent).

        Cross-entity relative to the trace group, so it's written
        independently and its failure must not affect the trace_meta/
        trace_spans write that already landed above.
        """
        try:
            await self.redis.zadd(index_key, {trace_id: int(score or 0)})
            await self.redis.expire(index_key, self.ttl_seconds)
        except Exception:  # pylint: disable=broad-exception-caught
            logger.warning(
                "RedisSpanExporter: trace index write failed for %s",
                index_key,
                exc_info=True,
            )

    @staticmethod
    async def _call_pipeline(pipe: Any, method_name: str, *args: Any) -> None:
        result = getattr(pipe, method_name)(*args)
        if isawaitable(result):
            await result

    async def _read_hash_int(self, name: str, field_name: str) -> int:
        hget = getattr(self.redis, "hget", None)
        if not callable(hget):
            return 0
        try:
            value = hget(name, field_name)  # pylint: disable=not-callable
            if isawaitable(value):
                value = await value
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            return int(value or 0)
        except (TypeError, ValueError):
            return 0


class OTelSpanExporter:
    """Export TraceSpan objects into OpenTelemetry's global tracer."""

    def __init__(self, tracer_name: str = "by-framework") -> None:
        self._tracer = None
        self.trace_mod = None
        try:
            from opentelemetry import trace

            if trace is not None:
                self.trace_mod = trace
                configure_otel_id_generator()
                self._tracer = trace.get_tracer(tracer_name)
        except (ImportError, AttributeError):
            pass

    async def export_span(self, span: TraceSpan) -> None:
        """Export a TraceSpan to OpenTelemetry."""
        if self._tracer is None or self.trace_mod is None:
            return

        configure_otel_id_generator()
        payload = span.to_payload()

        trace_id_int = str_to_uint128(span.trace_id)
        span_id_int = str_to_uint64(span.span_id)

        parent_context = None
        if span.parent_span_id:
            parent_span_id_int = str_to_uint64(span.parent_span_id)
            parent_span_context = self.trace_mod.SpanContext(
                trace_id=trace_id_int,
                span_id=parent_span_id_int,
                is_remote=True,
                trace_flags=self.trace_mod.TraceFlags(
                    self.trace_mod.TraceFlags.SAMPLED
                ),
            )
            parent_context = self.trace_mod.set_span_in_context(
                self.trace_mod.NonRecordingSpan(parent_span_context)
            )

        attributes = {
            "component": payload.get("component", ""),
            "status": payload.get("status", ""),
            "session_id": payload.get("session_id", ""),
            "execution_id": payload.get("execution_id", ""),
            "message_id": payload.get("message_id", ""),
            "parent_message_id": payload.get("parent_message_id", ""),
            "worker_id": payload.get("worker_id", ""),
            "source_agent_type": payload.get("source_agent_type", ""),
            "target_agent_type": payload.get("target_agent_type", ""),
        }
        for optional_key in (
            "error_type",
            "error_message",
            "error_code",
            "failed_stage",
            "route_policy",
            "route_status",
            "queue_wait_ms",
            "chunk_count",
            "event_type",
        ):
            if payload.get(optional_key):
                attributes[optional_key] = payload[optional_key]

        if payload.get("metadata"):
            for k, v in payload["metadata"].items():
                attributes[f"metadata.{k}"] = str(v)

        trace_id_token = current_trace_id_var.set(trace_id_int)
        span_id_token = current_span_id_var.set(span_id_int)
        try:
            logger.debug(
                "OTelSpanExporter triggering OTel span: %s (trace_id: %s, span_id: %s)",
                span.operation,
                span.trace_id,
                span.span_id,
            )
            otel_span = self._tracer.start_span(
                name=span.operation,
                context=parent_context,
                start_time=int(span.start_ts * 1_000_000),
                attributes=attributes,
            )
            if (
                span.status in ("FAILED", "error")
                or span.error_message
                or span.error_type
            ):
                from opentelemetry.trace import Status, StatusCode

                otel_span.set_status(
                    Status(StatusCode.ERROR, span.error_message or "Execution failed")
                )
            otel_span.end(end_time=int(span.end_ts * 1_000_000))
            logger.debug(
                "OTelSpanExporter successfully triggered OTel span: %s",
                span.operation,
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning(
                "OTelSpanExporter failed to export span %s: %s",
                span.operation,
                e,
                exc_info=True,
            )
        finally:
            current_trace_id_var.reset(trace_id_token)
            current_span_id_var.reset(span_id_token)


class LiveSpanHandle:
    """Mutable handle to update the terminal status of a live OTel span."""

    def __init__(self) -> None:
        self.status = "COMPLETED"
        self.error_message = ""

    @property
    def is_error(self) -> bool:
        """Whether the span should be marked as an OTel error span."""
        return self.status in ("FAILED", "error") or bool(self.error_message)

    def set_status(self, status: str, *, error_message: str = "") -> None:
        """Record the terminal status / error for the wrapped execution."""
        if status:
            self.status = status
        if error_message:
            self.error_message = error_message


@asynccontextmanager
async def live_execution_otel_span(
    *,
    trace_id: str,
    span_id: str,
    parent_span_id: str,
    operation: str,
    attributes: dict[str, Any],
    start_ts: int,
    tracer_name: str = "by-framework",
    otel_enabled: bool = True,
) -> AsyncIterator[LiveSpanHandle]:
    """Open a *live* OTel span set as the current context for an execution.

    Unlike :class:`OTelSpanExporter` (which replays a finished span after the
    fact), this keeps the span active for the whole ``async with`` body, so any
    spans produced inside the agent (e.g. LangGraph/Langfuse LLM calls) nest
    under it via normal OTel context propagation.

    The deterministic ``span_id`` is only injected for the brief moment the span
    is created; the context vars are reset immediately afterwards so that nested
    child spans generate their own ids (and merely inherit this span as parent)
    instead of colliding on the same span_id.

    Pass ``otel_enabled=False`` to skip span creation without incurring any OTel
    import overhead — callers should resolve this flag once at startup from
    :func:`build_observability_config` rather than passing it per-call.
    """
    handle = LiveSpanHandle()
    if not otel_enabled:
        yield handle
        return

    span = None
    ctx_token = None
    context_mod = None
    try:
        from opentelemetry import context as context_mod  # type: ignore
        from opentelemetry import trace as trace_mod

        configure_otel_id_generator()
        tracer = trace_mod.get_tracer(tracer_name)

        trace_id_int = str_to_uint128(trace_id)
        span_id_int = str_to_uint64(span_id)

        parent_context = None
        if parent_span_id:
            parent_span_context = trace_mod.SpanContext(
                trace_id=trace_id_int,
                span_id=str_to_uint64(parent_span_id),
                is_remote=True,
                trace_flags=trace_mod.TraceFlags(trace_mod.TraceFlags.SAMPLED),
            )
            parent_context = trace_mod.set_span_in_context(
                trace_mod.NonRecordingSpan(parent_span_context)
            )

        trace_id_token = current_trace_id_var.set(trace_id_int)
        span_id_token = current_span_id_var.set(span_id_int)
        try:
            span = tracer.start_span(
                name=operation,
                context=parent_context,
                start_time=int(start_ts * 1_000_000),
                attributes=attributes,
            )
        finally:
            current_span_id_var.reset(span_id_token)
            current_trace_id_var.reset(trace_id_token)

        ctx_token = context_mod.attach(trace_mod.set_span_in_context(span))
    except Exception as err:  # pylint: disable=broad-exception-caught
        logger.debug("live_execution_otel_span setup skipped: %s", err)
        span = None
        ctx_token = None

    try:
        yield handle
    finally:
        if span is not None:
            try:
                if handle.is_error:
                    from opentelemetry.trace import Status, StatusCode

                    span.set_status(
                        Status(
                            StatusCode.ERROR,
                            handle.error_message or "Execution failed",
                        )
                    )
                span.end(end_time=int(time.time() * 1000) * 1_000_000)
            except Exception as err:  # pylint: disable=broad-exception-caught
                logger.debug("live_execution_otel_span end failed: %s", err)
        if ctx_token is not None and context_mod is not None:
            try:
                context_mod.detach(ctx_token)
            except Exception as err:  # pylint: disable=broad-exception-caught
                logger.debug("live_execution_otel_span detach failed: %s", err)


def register_langfuse_span_processor() -> None:
    """Register LangfuseSpanProcessor on the global OpenTelemetry TracerProvider."""
    try:
        from importlib import import_module

        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider

        config = build_observability_config()
        if not config.langfuse_enabled:
            return

        secret_key = _clean_env(os.environ.get("LANGFUSE_SECRET_KEY"))
        public_key = _clean_env(os.environ.get("LANGFUSE_PUBLIC_KEY"))
        base_url = _clean_env(os.environ.get("LANGFUSE_BASE_URL"))

        if secret_key and public_key and base_url:
            # 1. Ensure the global TracerProvider exists and patch the ID generator.
            provider = trace.get_tracer_provider()
            if not isinstance(provider, TracerProvider):
                provider = TracerProvider()
                trace.set_tracer_provider(provider)
                configure_otel_id_generator()

            # 2. Avoid duplicate registration from this integration without
            # relying on OpenTelemetry SDK private provider internals.
            provider_id = id(provider)
            with _LANGFUSE_PROCESSOR_LOCK:
                if provider_id in _LANGFUSE_PROCESSOR_PROVIDER_IDS:
                    return

                # 3. Dynamically import and attach LangfuseSpanProcessor.
                langfuse_processor_mod = import_module(
                    "langfuse._client.span_processor"
                )
                langfuse_processor_cls = getattr(
                    langfuse_processor_mod, "LangfuseSpanProcessor"
                )

                def should_export_span(span) -> bool:
                    # Allow spans emitted by the by-framework tracer.
                    if (
                        span.instrumentation_scope
                        and span.instrumentation_scope.name == "by-framework"
                    ):
                        return True
                    # Allow custom client/worker spans.
                    if span.attributes:
                        if span.attributes.get("component") in ("client", "worker"):
                            return True
                    # Fall back to Langfuse's default filter.
                    try:
                        from langfuse import is_default_export_span  # type: ignore

                        return is_default_export_span(span)
                    except ImportError:
                        return True

                processor = langfuse_processor_cls(
                    public_key=public_key,
                    secret_key=secret_key,
                    base_url=base_url,
                    should_export_span=should_export_span,
                )
                provider.add_span_processor(processor)
                _LANGFUSE_PROCESSOR_PROVIDER_IDS.add(provider_id)
                logger.info(
                    "LangfuseSpanProcessor registered successfully to global OTel "
                    "TracerProvider. Base URL: %s",
                    base_url,
                )
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.warning("Failed to auto-register LangfuseSpanProcessor: %s", e)


class SpanRecorder:
    """Wrapper that routes trace spans to one or more exporters."""

    def __init__(
        self,
        redis_client: Optional[Redis] = None,
        *,
        exporters: Optional[Sequence[SpanExporter]] = None,
        ttl_seconds: int = TRACE_TTL_SECONDS,
        enable_otel: bool | None = None,
        config: ObservabilityConfig | None = None,
    ) -> None:
        self.config = config or build_observability_config()
        if ttl_seconds != TRACE_TTL_SECONDS:
            self.config = replace(
                self.config, ttl_seconds=max(1, int(ttl_seconds or TRACE_TTL_SECONDS))
            )
        if enable_otel is not None:
            self.config = replace(
                self.config, otel_enabled=bool(enable_otel) and self.config.enabled
            )
        self._spans_by_trace: dict[str, int] = {}
        self._trace_tracking_max_size = min(
            10_000, self.config.max_spans_per_trace * 20
        )

        if not self.config.enabled:
            self.exporters = []
        elif exporters is not None:
            self.exporters = list(exporters)
        elif not self.config.redis_enabled and not self.config.otel_enabled:
            self.exporters = []
        else:
            self.exporters = []
            if self.config.redis_enabled:
                self.exporters.append(
                    RedisSpanExporter(
                        redis_client,
                        ttl_seconds=self.config.ttl_seconds,
                    )
                )
            if self.config.otel_enabled:
                self._append_otel_exporter()

            if self.config.otel_enabled and self.config.langfuse_enabled:
                try:
                    if os.environ.get("LANGFUSE_SECRET_KEY") and os.environ.get(
                        "LANGFUSE_PUBLIC_KEY"
                    ):
                        register_langfuse_span_processor()
                except Exception as e:  # pylint: disable=broad-exception-caught
                    logger.warning(
                        "Failed to auto-register LangfuseSpanProcessor: %s", e
                    )

        logger.info(
            "SpanRecorder initialized with exporters: %s",
            [type(e).__name__ for e in self.exporters],
        )

    def _append_otel_exporter(self) -> None:
        try:
            from opentelemetry import trace  # pylint: disable=unused-import

            if trace is not None:
                self.exporters.append(OTelSpanExporter())
        except (ImportError, AttributeError):
            pass

    def _should_record(self, span: TraceSpan) -> tuple[bool, str]:
        if not self.exporters:
            return False, "disabled"
        if self.config.sample_rate <= 0:
            return False, "sampled"
        if self.config.sample_rate < 1:
            bucket = int(hashlib.md5(span.trace_id.encode()).hexdigest()[:8], 16)
            if (bucket / 0xFFFFFFFF) >= self.config.sample_rate:
                return False, "sampled"
        current_count = self._spans_by_trace.get(span.trace_id, 0)
        if current_count >= self.config.max_spans_per_trace:
            return False, "trace_span_limit"
        if len(self._spans_by_trace) >= self._trace_tracking_max_size:
            evict = len(self._spans_by_trace) // 2
            for key in list(self._spans_by_trace)[:evict]:
                del self._spans_by_trace[key]
        self._spans_by_trace[span.trace_id] = current_count + 1
        return True, ""

    async def record_span(self, span: TraceSpan) -> None:
        """Record a span by forwarding it to all registered exporters."""
        should_record, drop_reason = self._should_record(span)
        if not should_record:
            _record_drop(drop_reason)
            return
        for exporter in self.exporters:
            try:
                await exporter.export_span(span)
            except Exception as err:  # pylint: disable=broad-exception-caught
                _record_export_failure(type(exporter).__name__)
                logger.warning(
                    "Exporter %s failed to export span: %s",
                    type(exporter).__name__,
                    err,
                )
