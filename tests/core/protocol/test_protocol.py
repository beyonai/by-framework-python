import json
from dataclasses import dataclass

import pytest

from by_framework import ActionType, AgentState, DataMessage, RedisKeys
from by_framework.core.protocol.commands import (
    AskAgentCommand,
    BaseCommand,
    CancelTaskCommand,
    ResumeCommand,
    command_from_dict,
    register_command,
    unregister_command,
)
from by_framework.core.protocol.message_header import MessageHeader


def test_data_message_creation():
    """Test that DataMessage correctly stores trace_id, session_id,
    event_type, and data."""
    msg = DataMessage(
        trace_id="trace-1",
        session_id="sess-1",
        event_type="STREAM_CHUNK",
        data={
            "contentType": "1002",
            "choices": [{"delta": {"content": "hello"}}],
        },
    )
    assert msg.event_type == "STREAM_CHUNK"
    assert msg.data["choices"][0]["delta"]["content"] == "hello"


def test_cancel_action_and_states_exist():
    """Test ActionType.CANCEL_TASK and AgentState cancellation states values."""
    assert ActionType.CANCEL_TASK.value == "CANCEL_TASK"
    assert AgentState.CANCELLING.value == "CANCELLING"
    assert AgentState.CANCELLED.value == "CANCELLED"


def test_worker_control_stream_name():
    """Test that RedisKeys.worker_ctrl_stream generates correct stream name format."""
    assert (
        RedisKeys.worker_ctrl_stream("worker-1") == "byai_gateway:ctrl:worker:worker-1"
    )


def test_ask_agent_requires_content():
    """Test that AskAgentCommand raises ValueError when content is empty."""
    with pytest.raises(ValueError, match="content"):
        AskAgentCommand(
            header=MessageHeader(
                message_id="msg-ask-1",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="agent-a",
            ),
            content="",
        )


def test_resume_requires_status_or_content():
    """Test ResumeCommand raises ValueError when status or content missing."""
    with pytest.raises(ValueError, match="status or content"):
        ResumeCommand(
            header=MessageHeader(
                message_id="msg-resume-1",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="agent-a",
            ),
        )


def test_cancel_requires_target_message():
    """Test that CancelTaskCommand raises ValueError when target_message_id is empty."""
    with pytest.raises(ValueError):
        CancelTaskCommand(
            header=MessageHeader(
                message_id="msg-cancel-1",
                session_id="sess-1",
                trace_id="trace-1",
                target_agent_type="agent-a",
            ),
            target_message_id="",
        )


def test_cancel_command_serializes_to_header_body_wire_format():
    """Test that CancelTaskCommand serializes to header+body wire format correctly."""
    command = CancelTaskCommand(
        header=MessageHeader(
            message_id="msg-cancel-4",
            session_id="sess-1",
            trace_id="trace-2",
            target_agent_type="agent-a",
            parent_message_id="msg-task-2",
        ),
        target_message_id="msg-task-2",
        target_execution_id="exec-2",
        target_worker_id="worker-2",
        reason="user aborted",
        requested_by="frontend",
        cancel_mode="graceful",
    )

    payload = command.to_dict()

    assert payload == {
        "action_type": ActionType.CANCEL_TASK.value,
        "header": {
            "message_id": "msg-cancel-4",
            "session_id": "sess-1",
            "trace_id": "trace-2",
            "source_agent_type": "",
            "target_agent_type": "agent-a",
            "parent_message_id": "msg-task-2",
            "task_group_id": "",
            "user_code": "",
            "user_name": "",
            "metadata": {},
        },
        "body": {
            "target_message_id": "msg-task-2",
            "target_execution_id": "exec-2",
            "target_worker_id": "worker-2",
            "reason": "user aborted",
            "requested_by": "frontend",
            "cancel_mode": "graceful",
        },
    }


def test_ask_agent_command_round_trip_from_wire_dict():
    """Test AskAgentCommand serialization round-trip via dict."""
    command = AskAgentCommand(
        header=MessageHeader(
            message_id="msg-ask-2",
            session_id="sess-2",
            trace_id="trace-3",
            source_agent_type="agent-parent",
            target_agent_type="agent-b",
            parent_message_id="msg-parent",
        ),
        content="hello agent",
        wait_for_reply=True,
        extra_payload={"history": ["m1"]},
    )

    decoded = command_from_dict(command.to_dict())

    assert isinstance(decoded, AskAgentCommand)
    assert decoded.content == "hello agent"
    assert decoded.wait_for_reply is True
    assert decoded.extra_payload["history"] == ["m1"]


def test_resume_command_round_trip_from_wire_dict():
    """Test that ResumeCommand can be serialized to dict and decoded back correctly."""
    command = ResumeCommand(
        header=MessageHeader(
            message_id="msg-resume-3",
            session_id="sess-3",
            trace_id="trace-4",
            source_agent_type="agent-b",
            target_agent_type="agent-a",
            parent_message_id="msg-parent-2",
        ),
        status="SUCCESS",
        reply_data={"answer": 42},
        extra_payload={"resume_kind": "callback"},
    )

    decoded = command_from_dict(command.to_dict())

    assert isinstance(decoded, ResumeCommand)
    assert decoded.status == "SUCCESS"
    assert decoded.reply_data == {"answer": 42}
    assert decoded.extra_payload["resume_kind"] == "callback"


def test_command_to_redis_payload_serializes_wire_dict():
    """Test command.to_redis_payload produces valid Redis payload."""
    command = AskAgentCommand(
        header=MessageHeader(
            message_id="msg-ask-3",
            session_id="sess-3",
            trace_id="trace-3",
            target_agent_type="agent-c",
        ),
        content="hello",
    )

    payload = command.to_redis_payload()
    raw = json.loads(payload["data"])

    assert raw["action_type"] == ActionType.ASK_AGENT.value
    assert raw["header"]["message_id"] == "msg-ask-3"
    assert raw["body"]["content"] == "hello"


@dataclass
class CustomBusinessCommand(BaseCommand):
    action_type = "CUSTOM_BUSINESS"

    payload: dict[str, str]

    def to_dict(self) -> dict[str, object]:
        return {
            "action_type": self.action_type,
            "header": self.header.to_dict(),
            "body": {
                "payload": dict(self.payload),
            },
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> "CustomBusinessCommand":
        body = dict(data.get("body", {}))
        return cls(
            header=MessageHeader.from_dict(data["header"]),
            payload=dict(body.get("payload", {})),
        )


def test_custom_command_can_be_registered_and_decoded():
    """Test custom commands registered and decoded via command_from_dict."""
    register_command(CustomBusinessCommand)
    try:
        command = CustomBusinessCommand(
            header=MessageHeader(
                message_id="custom-1",
                session_id="sess-custom",
                trace_id="trace-custom",
                target_agent_type="custom-agent",
            ),
            payload={"mode": "custom"},
        )

        decoded = command_from_dict(command.to_dict())

        assert isinstance(decoded, CustomBusinessCommand)
        assert decoded.payload == {"mode": "custom"}
    finally:
        unregister_command(CustomBusinessCommand.action_type)
