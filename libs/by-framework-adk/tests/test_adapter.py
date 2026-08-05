"""Tests for AdkAdapter's streaming/final-response text handling."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from by_framework.core.protocol.commands import AskAgentCommand
from by_framework.core.protocol.event_type import EventType

try:
    from by_framework_adk.adapter import AdkAdapter

    HAS_ADK = True
except ImportError:
    HAS_ADK = False


pytestmark = pytest.mark.skipif(not HAS_ADK, reason="google-adk is not installed")


class _FakePart:
    """Duck-types just the attributes AdkAdapter._process_part reads via getattr."""

    def __init__(self, text=None, function_call=None, function_response=None):
        self.text = text
        self.function_call = function_call
        self.function_response = function_response
        self.executable_code = None
        self.code_execution_result = None


class _FakeContent:
    def __init__(self, parts):
        self.parts = parts


class _FakeEvent:
    def __init__(self, parts=None, is_final=False):
        self.content = _FakeContent(parts) if parts else None
        self._is_final = is_final

    def is_final_response(self):
        return self._is_final


class _FakeRunner:
    def __init__(self, events):
        self._events = events

    async def run_async(self, **_kwargs):
        for event in self._events:
            yield event


def _command() -> AskAgentCommand:
    return AskAgentCommand(header=MagicMock(), content="hello")


def _make_adapter(events) -> tuple[AdkAdapter, MagicMock]:
    context = MagicMock()
    context.session_id = "session-1"
    context.emit_chunk = AsyncMock()
    agent = MagicMock()
    agent.name = "test_agent"
    adapter = AdkAdapter(agent=agent, context=context, runner=_FakeRunner(events))
    return adapter, context


@pytest.mark.asyncio
async def test_final_answer_uses_full_accumulated_text_not_first_chunk():
    # Regression: ADK's final-response event's parts[0].text only carries a
    # fragment of what was actually streamed via prior non-final events —
    # the adapter must use the accumulated streamed text, not re-derive a
    # (truncated) final answer from the final event's own content.
    events = [
        _FakeEvent(parts=[_FakePart(text="The")]),
        _FakeEvent(parts=[_FakePart(text=" user")]),
        _FakeEvent(parts=[_FakePart(text=" is happy.")]),
        _FakeEvent(parts=[_FakePart(text="The")], is_final=True),
    ]
    adapter, context = _make_adapter(events)

    result = await adapter.run(_command())

    assert result == "The user is happy."

    final_answer_calls = [
        call
        for call in context.emit_chunk.await_args_list
        if call.kwargs.get("event_type") == EventType.FINAL_ANSWER.value
    ]
    assert len(final_answer_calls) == 1
    assert final_answer_calls[0].args[0] == "The user is happy."


@pytest.mark.asyncio
async def test_streamed_chunks_are_still_emitted_as_answer_deltas():
    events = [
        _FakeEvent(parts=[_FakePart(text="Hello")]),
        _FakeEvent(parts=[_FakePart(text=" world")]),
        _FakeEvent(parts=[_FakePart(text="Hello")], is_final=True),
    ]
    adapter, context = _make_adapter(events)

    await adapter.run(_command())

    delta_calls = [
        call
        for call in context.emit_chunk.await_args_list
        if call.kwargs.get("event_type") != EventType.FINAL_ANSWER.value
    ]
    assert [call.args[0] for call in delta_calls] == ["Hello", " world"]


@pytest.mark.asyncio
async def test_final_event_with_no_prior_streamed_text_uses_its_own_text():
    # Non-streaming style: a single final event carrying the whole answer,
    # nothing streamed beforehand.
    events = [_FakeEvent(parts=[_FakePart(text="Just this.")], is_final=True)]
    adapter, context = _make_adapter(events)

    result = await adapter.run(_command())

    assert result == "Just this."
    final_answer_calls = [
        call
        for call in context.emit_chunk.await_args_list
        if call.kwargs.get("event_type") == EventType.FINAL_ANSWER.value
    ]
    assert final_answer_calls[0].args[0] == "Just this."
