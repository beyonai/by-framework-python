"""Tests for admin lifecycle command handling in _control_handling."""

import pytest

from by_framework.core.protocol.commands import (
    EvictWorkerCommand,
    ResumeWorkerCommand,
    SuspendWorkerCommand,
    command_from_dict,
)
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.worker._control_handling import (
    handle_evict_worker,
    handle_resume_worker,
    handle_suspend_worker,
    parse_control_command,
)


def _header():
    return MessageHeader(session_id="s1", trace_id="t1", message_id="m1")


# ---------------------------------------------------------------------------
# Serialisation round-trip
# ---------------------------------------------------------------------------


def test_suspend_worker_command_roundtrip():
    cmd = SuspendWorkerCommand(header=_header(), reason="maintenance")
    restored = SuspendWorkerCommand.from_dict(cmd.to_dict())
    assert restored.reason == "maintenance"
    assert restored.action_type == "SUSPEND_WORKER"


def test_resume_worker_command_roundtrip():
    cmd = ResumeWorkerCommand(header=_header())
    restored = ResumeWorkerCommand.from_dict(cmd.to_dict())
    assert restored.action_type == "RESUME_WORKER"


def test_evict_worker_command_roundtrip_graceful():
    cmd = EvictWorkerCommand(header=_header(), reason="decommission", force=False)
    restored = EvictWorkerCommand.from_dict(cmd.to_dict())
    assert restored.reason == "decommission"
    assert restored.force is False


def test_evict_worker_command_roundtrip_force():
    cmd = EvictWorkerCommand(header=_header(), force=True)
    restored = EvictWorkerCommand.from_dict(cmd.to_dict())
    assert restored.force is True


# ---------------------------------------------------------------------------
# parse_control_command accepts admin commands
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parse_control_command_accepts_suspend():
    cmd = SuspendWorkerCommand(header=_header(), reason="test")
    result = await parse_control_command(cmd.to_dict())
    assert isinstance(result, SuspendWorkerCommand)


@pytest.mark.asyncio
async def test_parse_control_command_accepts_resume_worker():
    cmd = ResumeWorkerCommand(header=_header())
    result = await parse_control_command(cmd.to_dict())
    assert isinstance(result, ResumeWorkerCommand)


@pytest.mark.asyncio
async def test_parse_control_command_accepts_evict():
    cmd = EvictWorkerCommand(header=_header(), force=True)
    result = await parse_control_command(cmd.to_dict())
    assert isinstance(result, EvictWorkerCommand)
    assert result.force is True


# ---------------------------------------------------------------------------
# Handler behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handle_suspend_worker_sets_lifecycle():
    lifecycle_states = []
    await handle_suspend_worker(
        SuspendWorkerCommand(header=_header(), reason="maint"),
        set_lifecycle=lifecycle_states.append,
    )
    assert lifecycle_states == ["suspended"]


@pytest.mark.asyncio
async def test_handle_resume_worker_sets_lifecycle():
    lifecycle_states = []
    await handle_resume_worker(
        ResumeWorkerCommand(header=_header()),
        set_lifecycle=lifecycle_states.append,
    )
    assert lifecycle_states == ["active"]


@pytest.mark.asyncio
async def test_handle_evict_worker_graceful():
    lifecycle_states = []
    shutdown_calls = []
    await handle_evict_worker(
        EvictWorkerCommand(header=_header(), force=False),
        set_lifecycle=lifecycle_states.append,
        request_shutdown=shutdown_calls.append,
    )
    assert lifecycle_states == ["evicted"]
    assert shutdown_calls == [False]


@pytest.mark.asyncio
async def test_handle_evict_worker_force():
    lifecycle_states = []
    shutdown_calls = []
    await handle_evict_worker(
        EvictWorkerCommand(header=_header(), force=True),
        set_lifecycle=lifecycle_states.append,
        request_shutdown=shutdown_calls.append,
    )
    assert lifecycle_states == ["evicted"]
    assert shutdown_calls == [True]


@pytest.mark.asyncio
async def test_handle_evict_worker_no_shutdown_callback():
    """handle_evict_worker should not raise when request_shutdown is None."""
    lifecycle_states = []
    await handle_evict_worker(
        EvictWorkerCommand(header=_header()),
        set_lifecycle=lifecycle_states.append,
        request_shutdown=None,
    )
    assert lifecycle_states == ["evicted"]
