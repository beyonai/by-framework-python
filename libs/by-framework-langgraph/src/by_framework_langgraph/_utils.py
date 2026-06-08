"""Internal utilities for LangGraph integration."""

from __future__ import annotations

from typing import Any

from by_framework.core.protocol.commands import ResumeCommand


def extract_content_text(content: Any) -> str:
    """Extract plain text from various command content formats.

    Handles:
    - str → return directly
    - list[dict] (BaiYing message format) → extract text fields
    - other → str() conversion
    """
    if isinstance(content, str):
        return content

    if isinstance(content, list):
        texts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                # BaiYing message format: {"type": "text", "text": "..."}
                text = item.get("text", "")
                if text:
                    texts.append(str(text))
                # Fallback: {"content": "..."}
                elif "content" in item:
                    texts.append(str(item["content"]))
            elif isinstance(item, str):
                texts.append(item)
        return "\n".join(texts) if texts else str(content)

    return str(content)


def extract_resume_data(command: ResumeCommand) -> str:
    """Extract resume data from a ResumeCommand.

    Prioritizes reply_data (from call_agent callback) over content
    (from ask_user reply), converting to string.
    """
    if command.reply_data is not None:
        if isinstance(command.reply_data, str):
            return command.reply_data
        return str(command.reply_data)

    if command.content:
        return extract_content_text(command.content)

    return ""
