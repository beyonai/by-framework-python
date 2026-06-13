"""Tests for trace span recording configuration and safety controls."""

import json
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

from by_framework import RedisKeys
from by_framework.trace import span_recorder as span_module
from by_framework.trace.span_recorder import (
    DEFAULT_IO_VALUE_MAX_LENGTH,
    ObservabilityConfig,
    OTelSpanExporter,
    RedisSpanExporter,
    SpanRecorder,
    TraceSpan,
    sanitize_io_value,
)


class MockPipeline:
    """Small Redis pipeline fake for span recorder tests."""

    def __init__(self, redis):
        self.redis = redis
        self.commands = []

    def hset(self, name, key, value):
        self.commands.append(("hset", name, key, value))
        return self

    def zadd(self, name, mapping):
        self.commands.append(("zadd", name, mapping))
        return self

    def rpush(self, name, value):
        self.commands.append(("rpush", name, value))
        return self

    def expire(self, name, ttl):
        self.commands.append(("expire", name, ttl))
        return self

    async def execute(self):
        for command in self.commands:
            if command[0] == "hset":
                await self.redis.hset(command[1], {command[2]: command[3]})
            elif command[0] == "zadd":
                await self.redis.zadd(command[1], command[2])
            elif command[0] == "rpush":
                await self.redis.rpush(command[1], command[2])
            elif command[0] == "expire":
                await self.redis.expire(command[1], command[2])
        return []


class SpanRedis:
    """Minimal Redis fake for trace storage writes."""

    def __init__(self):
        self.data = {}
        self.expires = {}

    async def hset(self, name, mapping):
        self.data.setdefault(name, {}).update(mapping)

    async def zadd(self, name, mapping):
        self.data.setdefault(name, {}).update(mapping)

    async def rpush(self, name, value):
        self.data.setdefault(name, []).append(value)

    async def expire(self, name, ttl):
        self.expires[name] = ttl
        return 1

    def pipeline(self):
        return MockPipeline(self)


@pytest.mark.asyncio
async def test_redis_span_exporter_redacts_sensitive_metadata_and_expires_indexes():
    """Redis trace storage redacts secrets and applies TTL to lookup indexes."""
    redis = SpanRedis()
    exporter = RedisSpanExporter(redis, ttl_seconds=321)

    await exporter.export_span(
        TraceSpan(
            trace_id="trace-safe",
            span_id="span-1",
            parent_span_id="",
            operation="client.dispatch",
            component="client",
            start_ts=100,
            end_ts=150,
            status="FAILED",
            session_id="sess-safe",
            worker_id="worker-safe",
            target_agent_type="planner",
            error_message="api_key=super-secret-token",
            metadata={
                "api_key": "super-secret-token",
                "nested": {"password": "hidden", "safe": "visible"},
                "prompt": "x" * 300,
            },
        )
    )

    payload = json.loads(redis.data[RedisKeys.trace_spans("trace-safe")][0])

    assert payload["error_message"] == "[REDACTED]"
    assert payload["metadata"]["api_key"] == "[REDACTED]"
    assert payload["metadata"]["nested"]["password"] == "[REDACTED]"
    assert payload["metadata"]["nested"]["safe"] == "visible"
    assert payload["metadata"]["prompt"].endswith("...[TRUNCATED]")
    assert redis.expires[RedisKeys.trace_meta("trace-safe")] == 321
    assert redis.expires[RedisKeys.trace_spans("trace-safe")] == 321
    assert redis.expires[RedisKeys.trace_index_session("sess-safe")] == 321
    assert redis.expires[RedisKeys.trace_index_worker("worker-safe")] == 321
    assert redis.expires[RedisKeys.trace_index_agent("planner")] == 321


def test_observability_config_defaults_keep_redis_and_disable_external_exporters(
    monkeypatch,
):
    """Default config keeps local dashboard traces without automatic external export."""
    monkeypatch.delenv("BY_FRAMEWORK_OBSERVABILITY_ENABLED", raising=False)
    monkeypatch.delenv("BY_FRAMEWORK_TRACE_REDIS_ENABLED", raising=False)
    monkeypatch.delenv("BY_FRAMEWORK_OTEL_ENABLED", raising=False)

    config = span_module.build_observability_config()
    recorder = SpanRecorder(redis_client=MagicMock(), config=config)

    assert config.enabled is True
    assert config.redis_enabled is True
    assert config.otel_enabled is False
    assert [type(exporter) for exporter in recorder.exporters] == [RedisSpanExporter]


def test_observability_config_can_disable_all_exporters(monkeypatch):
    """A global off switch creates a no-op SpanRecorder."""
    monkeypatch.setenv("BY_FRAMEWORK_OBSERVABILITY_ENABLED", "false")

    config = span_module.build_observability_config()
    recorder = SpanRecorder(redis_client=MagicMock(), config=config)

    assert config.enabled is False
    assert recorder.exporters == []


def test_observability_config_enables_otel_only_when_requested(monkeypatch):
    """OTel exporter registration is opt-in through env config."""
    monkeypatch.setenv("BY_FRAMEWORK_OTEL_ENABLED", "true")
    monkeypatch.delenv("BY_FRAMEWORK_OBSERVABILITY_ENABLED", raising=False)

    mock_trace = MagicMock()
    mock_trace.get_tracer.return_value = MagicMock()
    mock_otel_module = types.ModuleType("opentelemetry")
    mock_otel_module.trace = mock_trace
    with patch.dict(
        sys.modules,
        {
            "opentelemetry": mock_otel_module,
            "opentelemetry.trace": mock_trace,
        },
    ):
        recorder = SpanRecorder(redis_client=MagicMock())

    assert any(
        isinstance(exporter, OTelSpanExporter) for exporter in recorder.exporters
    )


@pytest.mark.asyncio
async def test_span_recorder_sampling_and_span_limit_drop_spans(monkeypatch):
    """Sampling and per-trace caps prevent unbounded exporter writes."""
    monkeypatch.setenv("BY_FRAMEWORK_TRACE_SAMPLE_RATE", "0")
    monkeypatch.setenv("BY_FRAMEWORK_TRACE_MAX_SPANS_PER_TRACE", "1")
    span_module.reset_observability_diagnostics()

    exporter = MagicMock()
    exporter.export_span = MagicMock()
    recorder = SpanRecorder(exporters=[exporter])

    span = TraceSpan(
        trace_id="trace-drop",
        span_id="span-1",
        parent_span_id="",
        operation="agent.emit_chunk",
        component="agent_context",
        start_ts=100,
        end_ts=110,
        status="COMPLETED",
    )

    await recorder.record_span(span)

    exporter.export_span.assert_not_called()
    diagnostics = span_module.get_observability_diagnostics()
    assert diagnostics["dropped_spans_total"] == 1
    assert diagnostics["dropped_spans_by_reason"]["sampled"] == 1


@pytest.mark.asyncio
async def test_span_recorder_records_export_failures():
    """Exporter failures are counted for observability self-monitoring."""
    span_module.reset_observability_diagnostics()

    class FailingExporter:

        async def export_span(self, span):
            del span
            raise RuntimeError("backend unavailable")

    recorder = SpanRecorder(exporters=[FailingExporter()])

    await recorder.record_span(
        TraceSpan(
            trace_id="trace-fail",
            span_id="span-1",
            parent_span_id="",
            operation="worker.execute",
            component="worker",
            start_ts=100,
            end_ts=110,
            status="COMPLETED",
        )
    )

    diagnostics = span_module.get_observability_diagnostics()
    assert diagnostics["export_failures_total"] == 1
    assert diagnostics["export_failures_by_exporter"]["FailingExporter"] == 1


def test_sanitize_io_value_truncates_long_strings():
    """sanitize_io_value truncates strings exceeding io_value_max_length."""
    config = ObservabilityConfig(io_value_max_length=10)
    result = sanitize_io_value("A" * 20, config)
    assert result == "AAAAAAAAAA...[TRUNCATED]"


def test_sanitize_io_value_passes_short_strings():
    """sanitize_io_value leaves strings within the limit unchanged."""
    config = ObservabilityConfig(io_value_max_length=100)
    result = sanitize_io_value("hello", config)
    assert result == "hello"


def test_sanitize_io_value_redacts_when_flag_set():
    """sanitize_io_value replaces the entire value with [REDACTED] when configured."""
    config = ObservabilityConfig(redact_inputs=True)
    assert sanitize_io_value("secret prompt", config) == "[REDACTED]"
    assert sanitize_io_value({"key": "value"}, config) == "[REDACTED]"


def test_sanitize_io_value_handles_dicts_and_lists():
    """sanitize_io_value recursively sanitizes nested dicts and lists."""
    config = ObservabilityConfig(io_value_max_length=5)
    result = sanitize_io_value({"msg": "hello world"}, config)
    assert result == {"msg": "hello...[TRUNCATED]"}

    result_list = sanitize_io_value(["abcdefghij"], config)
    assert result_list == ["abcde...[TRUNCATED]"]


def test_observability_config_has_redact_inputs_and_io_max_length():
    """ObservabilityConfig exposes redact_inputs and io_value_max_length fields."""
    cfg = ObservabilityConfig(redact_inputs=True, io_value_max_length=1024)
    assert cfg.redact_inputs is True
    assert cfg.io_value_max_length == 1024
    default = ObservabilityConfig()
    assert default.redact_inputs is False
    assert default.io_value_max_length == DEFAULT_IO_VALUE_MAX_LENGTH
