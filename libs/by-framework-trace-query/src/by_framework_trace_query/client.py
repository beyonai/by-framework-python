"""Trace read client facade."""

from __future__ import annotations

import asyncio
from typing import Any

from by_framework.metrics import MetricsReadClient
from by_framework.trace import TraceDiagnostic, TraceReadResult

from .merger import TraceMerger
from .redis_source import RedisTraceSource


class TraceReadClient:
    """Read by-framework traces from one or more sources."""

    def __init__(
        self,
        *,
        redis_client=None,
        sources: list[RedisTraceSource] | None = None,
        max_spans: int = 1000,
        metrics_client: MetricsReadClient | None = None,
    ) -> None:
        self.sources = sources or [RedisTraceSource(redis_client)]
        self.merger = TraceMerger(max_spans=max_spans)
        self.metrics_client = metrics_client or MetricsReadClient(redis_client)

    async def get_trace(
        self, trace_id: str, *, session_id: str = ""
    ) -> TraceReadResult:
        diagnostics: list[TraceDiagnostic] = []
        all_spans = []
        trace = None
        source_names: list[str] = []
        for source in self.sources:
            try:
                source_trace, spans, source_diagnostics = await source.get_trace(
                    trace_id, session_id=session_id
                )
                trace = (
                    source_trace
                    if trace is None
                    else self._prefer_trace(trace, source_trace)
                )
                all_spans.extend(spans)
                diagnostics.extend(source_diagnostics)
                source_names.append(source.name)
            except Exception as err:  # pylint: disable=broad-exception-caught
                diagnostics.append(
                    TraceDiagnostic(
                        code="source_timeout",
                        message=f"Trace source {source.name} failed: {err}",
                        severity="error",
                        source=source.name,
                    )
                )
        if trace is None:
            from by_framework.trace import TraceRecord

            trace = TraceRecord(trace_id=trace_id, session_id=session_id)
        return self.merger.merge(
            trace,
            all_spans,
            sources=source_names,
            diagnostics=diagnostics,
        )

    async def list_traces(
        self,
        *,
        session_id: str = "",
        worker_id: str = "",
        agent_type: str = "",
        limit: int = 50,
    ) -> list[TraceReadResult]:
        trace_ids: list[str] = []
        seen = set()
        for source in self.sources:
            try:
                ids = await source.list_trace_ids(
                    session_id=session_id,
                    worker_id=worker_id,
                    agent_type=agent_type,
                    limit=limit,
                )
            except Exception:  # pylint: disable=broad-exception-caught
                continue
            for trace_id in ids:
                if trace_id in seen:
                    continue
                trace_ids.append(trace_id)
                seen.add(trace_id)
                if len(trace_ids) >= limit:
                    break
        return list(
            await asyncio.gather(
                *[
                    self.get_trace(trace_id, session_id=session_id)
                    for trace_id in trace_ids[:limit]
                ]
            )
        )

    async def explain_trace(
        self,
        trace_id: str,
        *,
        session_id: str = "",
        include_metrics: bool = True,
        metrics_buffer_ms: int = 5_000,
    ) -> dict[str, object]:
        result = await self.get_trace(trace_id, session_id=session_id)
        explanation: dict[str, object] = {
            "trace_id": trace_id,
            "status": result.status,
            "sources": result.sources,
            "diagnostics": [diagnostic.to_dict() for diagnostic in result.diagnostics],
            "span_count": len(result.spans),
            "time_window": self._trace_time_window(result),
        }
        if include_metrics:
            explanation["related_metrics"] = await self._explain_metrics_window(
                explanation["time_window"],
                metrics_buffer_ms=metrics_buffer_ms,
            )
        return explanation

    @staticmethod
    def _prefer_trace(current, candidate):
        current_payload = current.to_dict()
        candidate_payload = candidate.to_dict()
        return candidate if len(candidate_payload) > len(current_payload) else current

    @staticmethod
    def _trace_time_window(result: TraceReadResult) -> dict[str, int]:
        starts: list[int] = []
        ends: list[int] = []
        if result.trace.start_ts:
            starts.append(int(result.trace.start_ts))
        if result.trace.end_ts:
            ends.append(int(result.trace.end_ts))
        for span in result.spans:
            if span.start_ts:
                starts.append(int(span.start_ts))
            if span.end_ts:
                ends.append(int(span.end_ts))
        start_ts = min(starts) if starts else 0
        end_ts = max(ends) if ends else start_ts
        return {
            "start_ts": start_ts,
            "end_ts": max(start_ts, end_ts),
            "duration_ms": max(0, end_ts - start_ts),
        }

    async def _explain_metrics_window(
        self,
        time_window: object,
        *,
        metrics_buffer_ms: int,
    ) -> dict[str, Any]:
        if not isinstance(time_window, dict):
            return {
                "status": "partial",
                "diagnostics": [
                    {
                        "code": "trace_time_window_missing",
                        "message": "Trace time window could not be derived.",
                        "severity": "warning",
                    }
                ],
            }
        start_ts = int(time_window.get("start_ts", 0) or 0)
        end_ts = int(time_window.get("end_ts", 0) or 0)
        if not start_ts and not end_ts:
            return {
                "status": "partial",
                "diagnostics": [
                    {
                        "code": "trace_time_window_missing",
                        "message": "Trace has no timestamps to correlate metrics.",
                        "severity": "warning",
                    }
                ],
            }
        try:
            metrics = await self.metrics_client.explain_window(
                start_ts=start_ts,
                end_ts=end_ts,
                buffer_ms=metrics_buffer_ms,
            )
            return metrics.to_dict()
        except Exception as err:  # pylint: disable=broad-exception-caught
            return {
                "status": "partial",
                "diagnostics": [
                    {
                        "code": "metrics_source_failed",
                        "message": f"Metrics read failed: {err}",
                        "severity": "error",
                    }
                ],
            }
