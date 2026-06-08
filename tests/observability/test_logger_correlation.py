import asyncio
import json
import logging

import pytest

from by_framework.common.logger import ContextFilter, JSONFormatter
from by_framework.worker.context import AgentContext


@pytest.mark.asyncio
async def test_logger_correlation_basic():
    """Log records receive context correlation fields and clean up afterward."""
    # Mock AgentContext.
    context = AgentContext(
        session_id="session-111",
        trace_id="trace-222",
        message_id="msg-333",
        current_agent_id="my-test-agent",
        execution_id="exec-444",
    )

    # 1. No current context.
    record_no_ctx = logging.LogRecord(
        name="test-logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="log message without context",
        args=(),
        exc_info=None,
    )

    cf = ContextFilter()
    cf.filter(record_no_ctx)

    assert getattr(record_no_ctx, "trace_id", "") == ""
    assert getattr(record_no_ctx, "session_id", "") == ""

    # 2. Context is bound.
    async with context.use_context():
        record_with_ctx = logging.LogRecord(
            name="test-logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=12,
            msg="log message with context",
            args=(),
            exc_info=None,
        )
        cf.filter(record_with_ctx)

        assert getattr(record_with_ctx, "trace_id") == "trace-222"
        assert getattr(record_with_ctx, "session_id") == "session-111"
        assert getattr(record_with_ctx, "message_id") == "msg-333"
        assert getattr(record_with_ctx, "execution_id") == "exec-444"
        assert getattr(record_with_ctx, "agent_type") == "my-test-agent"

        # Verify JSONFormatter output.
        formatter = JSONFormatter()
        json_output = formatter.format(record_with_ctx)
        log_data = json.loads(json_output)

        assert log_data["trace_id"] == "trace-222"
        assert log_data["session_id"] == "session-111"
        assert log_data["message_id"] == "msg-333"
        assert log_data["execution_id"] == "exec-444"
        assert log_data["agent_type"] == "my-test-agent"

    # 3. Context has been exited.
    record_post_ctx = logging.LogRecord(
        name="test-logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=14,
        msg="log message after context",
        args=(),
        exc_info=None,
    )
    cf.filter(record_post_ctx)
    assert getattr(record_post_ctx, "trace_id") == ""


@pytest.mark.asyncio
async def test_logger_correlation_isolation():
    """Context variables stay isolated across concurrent coroutines."""

    async def worker_task(session_id, trace_id, results):
        context = AgentContext(
            session_id=session_id,
            trace_id=trace_id,
            message_id="msg-xx",
            current_agent_id="test-agent",
        )
        cf = ContextFilter()

        async with context.use_context():
            # Yield control during execution.
            await asyncio.sleep(0.01)

            record = logging.LogRecord(
                name="test-logger",
                level=logging.INFO,
                pathname="test.py",
                lineno=20,
                msg=f"task {session_id}",
                args=(),
                exc_info=None,
            )
            cf.filter(record)

            # Store the trace_id observed by this coroutine.
            results[session_id] = getattr(record, "trace_id")

    results = {}
    await asyncio.gather(
        worker_task("sess-A", "trace-AAA", results),
        worker_task("sess-B", "trace-BBB", results),
    )

    assert results["sess-A"] == "trace-AAA"
    assert results["sess-B"] == "trace-BBB"
