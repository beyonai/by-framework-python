from dataclasses import dataclass

import pytest

from by_framework import AgentTaskResult
from by_framework.core.protocol.agent_state import AgentState
from by_framework.core.protocol.results import normalize_process_result


def test_normalize_agent_task_result_preserves_fields():
    result = normalize_process_result(
        AgentTaskResult(
            status=AgentState.COMPLETED.value,
            content="done",
            reply_data={"answer": 42},
            metadata={"tokens": 123},
            extra_payload={"debug_id": "abc"},
        )
    )

    assert result.status == AgentState.COMPLETED.value
    assert result.content == "done"
    assert result.reply_data == {"answer": 42}
    assert result.metadata == {"tokens": 123}
    assert result.extra_payload == {"debug_id": "abc"}


def test_normalize_legacy_dict_copies_metadata_without_removing_reply_data():
    result = normalize_process_result(
        {
            "status": AgentState.COMPLETED.value,
            "answer": "42",
            "metadata": {"tokens": 123},
        }
    )

    assert result.status == AgentState.COMPLETED.value
    assert result.reply_data == {
        "status": AgentState.COMPLETED.value,
        "answer": "42",
        "metadata": {"tokens": 123},
    }
    assert result.metadata == {"tokens": 123}


def test_normalize_structured_dict_uses_reply_data_field():
    result = normalize_process_result(
        {
            "status": AgentState.COMPLETED.value,
            "content": "done",
            "reply_data": {"answer": "42"},
            "metadata": {"tokens": 123},
            "extra_payload": {"debug_id": "abc"},
        }
    )

    assert result.reply_data == {"answer": "42"}
    assert result.metadata == {"tokens": 123}
    assert result.extra_payload == {"debug_id": "abc"}


def test_normalize_rejects_non_json_serializable_reply_data():
    @dataclass
    class CustomResult:
        value: str

    with pytest.raises(TypeError, match="reply_data.item"):
        normalize_process_result({"item": CustomResult("x")})
