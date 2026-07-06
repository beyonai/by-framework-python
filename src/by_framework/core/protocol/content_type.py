# pylint: disable=C0103
"""
Content type definitions for SSE messages.

Contains enum types for SSE message types and reasoning message types
used in the Gateway protocol.
"""

from enum import Enum


class SseMessageType(str, Enum):
    """SSE message type enum."""

    text = "1002"  # text
    echart = "2001"  # chart
    form = "2002"  # form
    digit = "2003"  # digit
    iframe = "2006"  # iframe
    task = "2008"  # task


class SseReasonMessageType(str, Enum):
    """SSE reasoning message type enum."""

    json_block = "2020" # tool json arguments
    think_title = "3003"  # thinking process title
    think_sub_title = "3005"  # thinking process subtitle
    think_resource = "3004"  # thinking process reference
    think_text = "1002"  # thinking process text
    think_code_answer = "3008"  # thinking process code final result
    think_code = "3006"  # thinking process code
    think_code_result = "3007"  # thinking process code execution result
    task_finished = "3009"  # task finished
    task_user_input = "3013"  # user input
    task_create_file = "3010"  # create file
    task_title = "3011"  # task title
    agent_card = "2015"  # agent card
    async_card = "2014"  # async card
