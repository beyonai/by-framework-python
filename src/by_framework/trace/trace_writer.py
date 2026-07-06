"""Best-effort trace write client for by-framework observability."""

from __future__ import annotations

import json
from inspect import isawaitable
from typing import Any

from by_framework.common.constants import RedisKeys
from by_framework.common.logger import logger
from by_framework.common.redis_client import Redis, get_redis
from by_framework.trace.span_recorder import (
    TRACE_TTL_SECONDS,
    RedisSpanExporter,
    SpanRecorder,
)
from by_framework.trace.trace_schema import (
    EventRecord,
    ExecutionRecord,
    SpanRecord,
    TraceRecord,
    encode_redis_value,
)


class TraceWriteClient:
    """Write trace records to by-framework observability backends.

    The client is deliberately best-effort: backend failures are logged and never
    propagated into task execution.
    """

    def __init__(
        self,
        redis_client: Redis | None = None,
        *,
        ttl_seconds: int = TRACE_TTL_SECONDS,
    ) -> None:
        self.redis = redis_client or get_redis()
        self.ttl_seconds = max(1, int(ttl_seconds or TRACE_TTL_SECONDS))
        self._span_recorder = SpanRecorder(
            exporters=[RedisSpanExporter(self.redis, ttl_seconds=self.ttl_seconds)]
        )

    async def record_trace(self, record: TraceRecord) -> None:
        """Persist trace-level metadata and lookup indexes.

        trace_meta is written in its own atomic pipeline. The session/agent
        lookup indexes are cross-entity relative to trace_meta, so they're
        written as independent, best-effort calls afterward — a failure
        there must not affect the trace_meta write that already landed.
        """
        try:
            payload = record.to_dict()
            if not payload.get("trace_id"):
                return
            trace_id = str(payload["trace_id"])
            meta_key = RedisKeys.trace_meta(trace_id)
            start_ts = int(payload.get("start_ts", 0) or payload.get("end_ts", 0) or 0)
            pipe = self.redis.pipeline()
            if isawaitable(pipe):
                pipe = await pipe
            for key, value in payload.items():
                await self._call_pipeline(
                    pipe, "hset", meta_key, key, encode_redis_value(value)
                )
            if "updated_at" not in payload:
                await self._call_pipeline(
                    pipe,
                    "hset",
                    meta_key,
                    "updated_at",
                    int(payload.get("end_ts", start_ts) or start_ts),
                )
            await self._call_pipeline(pipe, "expire", meta_key, self.ttl_seconds)
            result = pipe.execute()
            if isawaitable(result):
                await result
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.warning("TraceWriteClient.record_trace skipped: %s", err)
            return

        if payload.get("session_id"):
            await self._index_trace(
                RedisKeys.trace_index_session(str(payload["session_id"])),
                trace_id,
                start_ts,
            )
        if payload.get("root_agent_type"):
            await self._index_trace(
                RedisKeys.trace_index_agent(str(payload["root_agent_type"])),
                trace_id,
                start_ts,
            )

    async def record_span(self, record: SpanRecord) -> None:
        """Persist a trace-tree span."""
        try:
            await self._span_recorder.record_span(record.to_trace_span())
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.warning("TraceWriteClient.record_span skipped: %s", err)

    async def record_execution(self, record: ExecutionRecord) -> None:
        """Persist execution metadata into the session registry when possible."""
        try:
            if not record.session_id or not record.execution_id:
                return
            key = RedisKeys.session_registry(record.session_id)
            payload = json.dumps(
                record.to_dict(), ensure_ascii=False, separators=(",", ":")
            )
            await self.redis.hset(key, {f"exec:{record.execution_id}": payload})
            await self.redis.expire(key, self.ttl_seconds)
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.warning("TraceWriteClient.record_execution skipped: %s", err)

    async def record_event(self, record: EventRecord) -> None:
        """Persist a session stream event for fallback trace reads."""
        try:
            if not record.session_id:
                return
            event_payload = {
                "event_id": record.event_id,
                "trace_id": record.trace_id,
                "session_id": record.session_id,
                "message_id": record.message_id,
                "event_type": record.event_type,
                "content_type": record.content_type,
                "timestamp": record.timestamp,
                "data": record.payload,
            }
            await self.redis.xadd(
                RedisKeys.session_data_stream(record.session_id),
                {
                    "data": json.dumps(
                        event_payload, ensure_ascii=False, separators=(",", ":")
                    ),
                    "event_id": record.event_id,
                    "trace_id": record.trace_id,
                    "session_id": record.session_id,
                    "message_id": record.message_id,
                    "event_type": record.event_type,
                    "content_type": record.content_type,
                    "timestamp": str(record.timestamp),
                    "payload": json.dumps(
                        record.payload, ensure_ascii=False, separators=(",", ":")
                    ),
                },
            )
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.warning("TraceWriteClient.record_event skipped: %s", err)

    async def _index_trace(self, index_key: str, trace_id: str, score: int) -> None:
        """Best-effort write to a trace lookup index (session/agent).

        Cross-entity relative to trace_meta, so it's written independently;
        failure here is logged and never propagated to the caller.
        """
        try:
            await self.redis.zadd(index_key, {trace_id: int(score or 0)})
            await self.redis.expire(index_key, self.ttl_seconds)
        except Exception as err:  # pylint: disable=broad-exception-caught
            logger.warning(
                "TraceWriteClient: trace index write failed for %s: %s", index_key, err
            )

    @staticmethod
    async def _call_pipeline(pipe: Any, method_name: str, *args: Any) -> None:
        result = getattr(pipe, method_name)(*args)
        if isawaitable(result):
            await result
