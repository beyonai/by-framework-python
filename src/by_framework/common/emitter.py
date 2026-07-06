"""Gateway data emitter for streaming events and messages via Redis streams."""

import json
import time
import uuid
from typing import Any, Dict, List, Optional, Protocol, Union

from by_framework.common.constants import RedisKeys
from by_framework.common.redis_client import Redis, get_redis
from by_framework.core.protocol.content_type import (
    SseMessageType,
    SseReasonMessageType,
)
from by_framework.core.protocol.data_message import DataMessage
from by_framework.core.protocol.event_type import EventType
from by_framework.core.protocol.events import (
    ArtifactEvent,
    AskUserEvent,
    StateChangeEvent,
    StreamChunkEvent,
)


class DataLayoutBuilder(Protocol):
    """Hook interface for building the ``data`` payload of emitted events."""

    def build(
        self,
        content: Optional[str],
        role: Optional[str],
        content_type: str,
        source_agent_type: str,
        function_call: Optional[Dict[str, Any]] = None,
        function_response: Optional[Dict[str, Any]] = None,
        tool_calls: Optional[List[Dict[str, Any]]] = None,
        tool_responses: Optional[List[Dict[str, Any]]] = None,
        order_id: Optional[str] = None,
        parent_order_id: Optional[str] = None,
        object_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build the event data payload."""
        raise NotImplementedError

    def build_ask_user(
        self,
        prompt: str,
        source_agent_type: str,
        order_id: Optional[str] = None,
        parent_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build the event data payload for requesting user input."""
        raise NotImplementedError


class DefaultSseLayoutBuilder(DataLayoutBuilder):
    """Default SSE layout builder compatible with BaiYingChatCompletion."""

    def build(
        self,
        content: Optional[str],
        role: Optional[str],
        content_type: str,
        source_agent_type: str,
        function_call: Optional[Dict[str, Any]] = None,
        function_response: Optional[Dict[str, Any]] = None,
        tool_calls: Optional[List[Dict[str, Any]]] = None,
        tool_responses: Optional[List[Dict[str, Any]]] = None,
        order_id: Optional[str] = None,
        parent_order_id: Optional[str] = None,
        object_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build deep structure compatible with native BaiYingChatCompletion."""
        return {
            "id": uuid.uuid4().hex.upper(),
            "created": int(time.time()),
            "model": "",
            "object": "",
            "contentType": content_type,
            "agentId": source_agent_type if source_agent_type else None,
            "orderId": order_id,
            "parentOrderId": parent_order_id,
            "objectType": object_type,
            "status": status,
            "choices": [
                {
                    "index": 0,
                    "finish_reason": "",
                    "delta": {
                        "role": role,
                        "content": content,
                        "function_call": function_call,
                        "function_response": function_response,
                        "tool_calls": tool_calls,
                        "tool_responses": tool_responses,
                    },
                }
            ],
        }

    def build_ask_user(
        self,
        prompt: str,
        source_agent_type: str,
        order_id: Optional[str] = None,
        parent_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build default BaiYing-compatible user input form payload."""
        input_form = {
            "formStatus": 0,
            "pluginMachineFields": [
                {
                    "formType": "textarea",
                    "fieldName": "user_input",
                    "fieldCode": "user_input",
                    "description": prompt,
                    "required": True,
                }
            ],
        }
        return self.build(
            content=json.dumps(input_form, ensure_ascii=False),
            role="assistant",
            content_type=SseReasonMessageType.task_user_input.value,
            source_agent_type=source_agent_type,
            order_id=order_id,
            parent_order_id=parent_order_id,
        )


class GatewayDataEmitter:
    """
    Atomic data reporter.

    Allows sending data to data stream anywhere independently of business context.
    """

    def __init__(
        self,
        redis_client: Optional[Redis] = None,
        data_stream_name: Optional[str] = None,
        layout_builder: Optional[DataLayoutBuilder] = None,
    ):
        self.redis = redis_client or get_redis()
        # If a fixed data_stream_name is provided, always use it (for compatibility)
        self.fixed_data_stream_name = data_stream_name
        self.layout_builder = layout_builder or DefaultSseLayoutBuilder()

    async def emit_event(
        self,
        session_id: str,
        trace_id: str,
        event_type: str,
        source_agent_type: str = "",
        message_id: str = "",
        parent_message_id: str = "",
        data: Optional[Dict[str, Any]] = None,
        state_msg: str = "",
        artifact_url: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Emit a generic event to the data stream."""
        msg = DataMessage(
            trace_id=trace_id,
            session_id=session_id,
            event_type=event_type,
            source_agent_type=source_agent_type,
            message_id=message_id,
            parent_message_id=parent_message_id,
            data=data or {},
            state_msg=state_msg,
            artifact_url=artifact_url,
            metadata=metadata or {},
        )

        # Determine stream name: prefer Session-isolated stream unless fixed
        # stream name is specified at init
        stream_name = self.fixed_data_stream_name or RedisKeys.session_data_stream(
            session_id
        )

        # Use Pipeline to execute XADD with TTL
        pipe = self.redis.pipeline()

        pipe.xadd(stream_name, msg.to_redis_payload(), approximate=True)
        # Set TTL for Session stream
        pipe.expire(stream_name, RedisKeys.DEFAULT_SESSION_TTL)
        await pipe.execute()

    async def emit_chunk(
        self,
        session_id: str,
        trace_id: str,
        event: Union[StreamChunkEvent, str],
        source_agent_type: str = "",
        message_id: str = "",
        parent_message_id: str = "",
        event_type: Optional[str] = None,
        content_type: Optional[str] = None,
        object_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> None:
        """Emit a stream chunk event."""
        if isinstance(event, str):
            event = StreamChunkEvent(content=event)
        await self.emit_event(
            session_id=session_id,
            trace_id=trace_id,
            event_type=event_type or EventType.ANSWER_DELTA.value,
            source_agent_type=source_agent_type,
            message_id=message_id,
            parent_message_id=parent_message_id,
            data=self.layout_builder.build(
                content=event.content,
                role=event.role,
                content_type=content_type or SseMessageType.text.value,
                source_agent_type=source_agent_type,
                function_call=event.function_call,
                function_response=event.function_response,
                tool_calls=event.tool_calls,
                tool_responses=event.tool_responses,
                order_id=message_id,
                parent_order_id=parent_message_id,
                object_type=object_type,
                status=status,
            ),
            metadata=event.metadata,
        )

    async def emit_state(
        self,
        session_id: str,
        trace_id: str,
        event: Union[StateChangeEvent, str],
        source_agent_type: str = "",
        message_id: str = "",
        parent_message_id: str = "",
        event_type: Optional[str] = None,
        content_type: Optional[str] = None,
    ) -> None:
        """Emit a state change event."""
        if isinstance(event, str):
            event = StateChangeEvent(state=event)
        await self.emit_event(
            session_id=session_id,
            trace_id=trace_id,
            event_type=event_type or EventType.REASONING_LOG_DELTA.value,
            source_agent_type=source_agent_type,
            message_id=message_id,
            parent_message_id=parent_message_id,
            data=self.layout_builder.build(
                content=event.state,
                role=None,
                content_type=content_type or SseReasonMessageType.think_title.value,
                source_agent_type=source_agent_type,
                order_id=message_id,
                parent_order_id=parent_message_id,
            ),
            metadata=event.metadata,
        )

    async def emit_artifact(
        self,
        session_id: str,
        trace_id: str,
        event: Union[ArtifactEvent, str],
        source_agent_type: str = "",
        message_id: str = "",
        parent_message_id: str = "",
        event_type: Optional[str] = None,
        content_type: Optional[str] = None,
    ) -> None:
        """Emit an artifact event."""
        if isinstance(event, str):
            event = ArtifactEvent(url=event)
        files_payload = [{"fileUrl": event.url}]
        await self.emit_event(
            session_id=session_id,
            trace_id=trace_id,
            event_type=event_type or EventType.REASONING_LOG_DELTA.value,
            source_agent_type=source_agent_type,
            message_id=message_id,
            parent_message_id=parent_message_id,
            data=self.layout_builder.build(
                content=json.dumps(files_payload, ensure_ascii=False),
                role=None,
                content_type=content_type
                or SseReasonMessageType.task_create_file.value,
                source_agent_type=source_agent_type,
                order_id=message_id,
                parent_order_id=parent_message_id,
            ),
            metadata=event.metadata,
        )

    async def ask_user(
        self,
        session_id: str,
        trace_id: str,
        event: Union[AskUserEvent, str],
        source_agent_type: str = "",
        message_id: str = "",
        parent_message_id: str = "",
    ) -> None:
        """Emit an ask user event."""
        if isinstance(event, str):
            event = AskUserEvent(prompt=event)
        await self.emit_event(
            session_id=session_id,
            trace_id=trace_id,
            event_type=EventType.REASONING_LOG_DELTA.value,
            source_agent_type=source_agent_type,
            message_id=message_id,
            parent_message_id=parent_message_id,
            data=self.layout_builder.build_ask_user(
                prompt=event.prompt,
                source_agent_type=source_agent_type,
                order_id=message_id,
                parent_order_id=parent_message_id,
            ),
            metadata=event.metadata,
        )
