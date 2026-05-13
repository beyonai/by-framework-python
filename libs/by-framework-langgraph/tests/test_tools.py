"""Tests for tools module."""

from unittest.mock import AsyncMock, MagicMock

from by_framework_langgraph.tools import (make_ask_user_tool, make_remote_agent_tool)


def _make_mock_context(session_id: str = "test-session"):
    """Create a mock AgentContext for testing."""
    ctx = MagicMock()
    ctx.session_id = session_id
    ctx.redis = AsyncMock()
    ctx.call_agent = AsyncMock()
    ctx.ask_user = AsyncMock(return_value={"status": "WAITING_USER"})
    return ctx


class TestMakeRemoteAgentTool:
    """Tests for make_remote_agent_tool."""

    def test_returns_tool_with_correct_name(self):
        """Verify the returned tool uses the provided tool_name."""
        ctx = _make_mock_context()
        tool = make_remote_agent_tool(ctx, "invoke_poet", "poet-agent", "Invoke poet")
        assert tool.name == "invoke_poet"

    def test_returns_tool_with_description(self):
        """Verify the returned tool carries the provided description."""
        ctx = _make_mock_context()
        tool = make_remote_agent_tool(
            ctx, "invoke_poet", "poet-agent", "Invoke the poet agent"
        )
        assert "Invoke the poet agent" in tool.description


class TestMakeAskUserTool:
    """Tests for make_ask_user_tool."""

    def test_returns_tool_with_default_name(self):
        """Verify the default tool name is 'ask_user'."""
        ctx = _make_mock_context()
        tool = make_ask_user_tool(ctx)
        assert tool.name == "ask_user"

    def test_returns_tool_with_custom_name(self):
        """Verify a custom tool_name overrides the default."""
        ctx = _make_mock_context()
        tool = make_ask_user_tool(ctx, tool_name="confirm_action")
        assert tool.name == "confirm_action"

    def test_returns_tool_with_description(self):
        """Verify the returned tool carries the provided description."""
        ctx = _make_mock_context()
        tool = make_ask_user_tool(ctx, description="Ask user for confirmation")
        assert "Ask user for confirmation" in tool.description
