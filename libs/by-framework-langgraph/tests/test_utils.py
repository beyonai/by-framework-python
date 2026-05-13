"""Tests for _utils module."""

from by_framework.core.protocol.commands import ResumeCommand
from by_framework.core.protocol.message_header import MessageHeader

from by_framework_langgraph._utils import (extract_content_text, extract_resume_data)


class TestExtractContentText:
    """Tests for extract_content_text."""

    def test_plain_string(self):
        """Verify a plain string is returned unchanged."""
        assert extract_content_text("hello") == "hello"

    def test_empty_string(self):
        """Verify an empty string is returned unchanged."""
        assert extract_content_text("") == ""

    def test_list_of_dicts_with_text(self):
        """Verify list of dicts with 'text' key are joined by newline."""
        content = [{"type": "text", "text": "hello"}, {"type": "text", "text": "world"}]
        assert extract_content_text(content) == "hello\nworld"

    def test_list_of_dicts_with_content(self):
        """Verify list of dicts with 'content' key are joined by newline."""
        content = [{"content": "hello"}]
        assert extract_content_text(content) == "hello"

    def test_list_of_strings(self):
        """Verify a list of strings are joined by newline."""
        content = ["hello", "world"]
        assert extract_content_text(content) == "hello\nworld"

    def test_non_string_non_list(self):
        """Verify non-string, non-list values are stringified."""
        assert extract_content_text(42) == "42"

    def test_empty_list(self):
        """Verify an empty list is stringified to '[]'."""
        content: list = []
        assert extract_content_text(content) == "[]"


class TestExtractResumeData:
    """Tests for extract_resume_data."""

    def _make_resume(
        self,
        reply_data=None,
        content="placeholder",
        status="COMPLETED",
    ):
        """Create a ResumeCommand with the given parameters."""
        return ResumeCommand(
            header=MessageHeader(
                message_id="msg-001",
                session_id="sess-001",
                trace_id="trace-001",
            ),
            content=content,
            status=status,
            reply_data=reply_data,
        )

    def test_prefers_reply_data(self):
        """Verify reply_data is preferred over content when both present."""
        cmd = self._make_resume(reply_data="poem result", content="user text")
        assert extract_resume_data(cmd) == "poem result"

    def test_falls_back_to_content(self):
        """Verify content is used when reply_data is None."""
        cmd = self._make_resume(reply_data=None, content="user input")
        assert extract_resume_data(cmd) == "user input"

    def test_empty_when_both_missing(self):
        """Verify empty string when both reply_data and content are absent."""
        cmd = self._make_resume(reply_data=None, content="", status="COMPLETED")
        assert extract_resume_data(cmd) == ""

    def test_dict_reply_data_stringified(self):
        """Verify dict reply_data is JSON-serialized."""
        cmd = self._make_resume(reply_data={"key": "value"})
        result = extract_resume_data(cmd)
        assert "key" in result
        assert "value" in result
