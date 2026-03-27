import json

from by_framework.core.protocol.commands import AskAgentCommand
from by_framework.core.protocol.message_header import MessageHeader


def test_command_wire_metadata():
    """Test that command metadata is correctly serialized to Redis payload format."""
    command = AskAgentCommand(
        header=MessageHeader(
            message_id="test",
            session_id="test",
            trace_id="trace-test",
            target_agent_type="test",
            metadata={"foo": "bar"},
        ),
        content="hello",
    )
    assert command.header.metadata == {"foo": "bar"}
    payload = json.loads(command.to_redis_payload()["data"])
    assert payload["header"]["metadata"] == {"foo": "bar"}
