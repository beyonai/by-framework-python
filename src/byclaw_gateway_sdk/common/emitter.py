import json
import time
import uuid
from typing import Any, Dict, List, Optional, Union

from byclaw_gateway_sdk.common.constants import RedisKeys
from byclaw_gateway_sdk.common.redis_client import Redis, get_redis
from byclaw_gateway_sdk.core.protocol.content_type import (
    SseMessageType,
    SseReasonMessageType,
)
from byclaw_gateway_sdk.core.protocol.data_message import DataMessage
from byclaw_gateway_sdk.core.protocol.event_type import EventType
from byclaw_gateway_sdk.core.protocol.events import (
    ArtifactEvent,
    AskUserEvent,
    StateChangeEvent,
    StreamChunkEvent,
)


def _build_sse_layout(
    content: Optional[str],
    role: Optional[str],
    content_type: str,
    source_agent_id: str,
    function_call: Optional[Dict[str, Any]] = None,
    tool_calls: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """构建兼容原生 BaiYingChatCompletion 的深层结构"""
    return {
        "id": uuid.uuid4().hex.upper(),
        "created": int(time.time()),
        "model": "",
        "object": "",
        "contentType": content_type,
        "agentId": source_agent_id if source_agent_id else None,
        "choices": [
            {
                "index": 0,
                "finish_reason": "",
                "delta": {
                    "role": role,
                    "content": content,
                    "function_call": function_call,
                    "tool_calls": tool_calls,
                },
            }
        ],
    }


class GatewayDataEmitter:
    """
    原子化数据上报器。
    允许在任何地方独立于业务上下文发送数据到数据流。
    """

    def __init__(
        self,
        redis_client: Optional[Redis] = None,
        data_stream_name: Optional[str] = None,
    ):
        self.redis = redis_client or get_redis()
        # 如果提供了固定的 data_stream_name，则始终使用它（保持兼容性）
        self.fixed_data_stream_name = data_stream_name

    async def emit_event(
        self,
        session_id: str,
        trace_id: str,
        event_type: str,
        source_agent_id: str = "",
        message_id: str = "",
        data: Optional[Dict[str, Any]] = None,
        state_msg: str = "",
        artifact_url: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ):
        msg = DataMessage(
            trace_id=trace_id,
            session_id=session_id,
            event_type=event_type,
            source_agent_id=source_agent_id,
            message_id=message_id,
            data=data or {},
            state_msg=state_msg,
            artifact_url=artifact_url,
            metadata=metadata or {},
        )

        # 确定流名称：优先使用 Session 隔离流，除非初始化时指定了固定流名
        stream_name = self.fixed_data_stream_name or RedisKeys.session_data_stream(
            session_id
        )

        # 使用 Pipeline 执行 XADD 并设置 TTL
        pipe = self.redis.pipeline()

        pipe.xadd(stream_name, msg.to_redis_payload(), approximate=True)
        # 为 Session 流设置 TTL
        pipe.expire(stream_name, RedisKeys.DEFAULT_SESSION_TTL)
        await pipe.execute()

    async def emit_chunk(
        self,
        session_id: str,
        trace_id: str,
        event: Union[StreamChunkEvent, str],
        source_agent_id: str = "",
        message_id: str = "",
        event_type: Optional[str] = None,
    ) -> None:
        if isinstance(event, str):
            event = StreamChunkEvent(content=event)
        await self.emit_event(
            session_id=session_id,
            trace_id=trace_id,
            event_type=event_type or EventType.ANSWER_DELTA.value,
            source_agent_id=source_agent_id,
            message_id=message_id,
            data=_build_sse_layout(
                content=event.content,
                role=event.role,
                content_type=SseMessageType.text.value,
                source_agent_id=source_agent_id,
                function_call=event.function_call,
                tool_calls=event.tool_calls,
            ),
            metadata=event.metadata,
        )

    async def emit_state(
        self,
        session_id: str,
        trace_id: str,
        event: Union[StateChangeEvent, str],
        source_agent_id: str = "",
        message_id: str = "",
        event_type: Optional[str] = None,
    ) -> None:
        if isinstance(event, str):
            event = StateChangeEvent(state=event)
        await self.emit_event(
            session_id=session_id,
            trace_id=trace_id,
            event_type=event_type or EventType.REASONING_LOG_DELTA.value,
            source_agent_id=source_agent_id,
            message_id=message_id,
            data=_build_sse_layout(
                content=event.state,
                role=None,
                content_type=SseReasonMessageType.think_title.value,
                source_agent_id=source_agent_id,
            ),
            metadata=event.metadata,
        )

    async def emit_artifact(
        self,
        session_id: str,
        trace_id: str,
        event: Union[ArtifactEvent, str],
        source_agent_id: str = "",
        message_id: str = "",
        event_type: Optional[str] = None,
    ) -> None:
        if isinstance(event, str):
            event = ArtifactEvent(url=event)
        files_payload = [{"fileUrl": event.url}]
        await self.emit_event(
            session_id=session_id,
            trace_id=trace_id,
            event_type=event_type or EventType.REASONING_LOG_DELTA.value,
            source_agent_id=source_agent_id,
            message_id=message_id,
            data=_build_sse_layout(
                content=json.dumps(files_payload, ensure_ascii=False),
                role=None,
                content_type=SseReasonMessageType.task_create_file.value,
                source_agent_id=source_agent_id,
            ),
            metadata=event.metadata,
        )

    async def ask_user(
        self,
        session_id: str,
        trace_id: str,
        event: Union[AskUserEvent, str],
        source_agent_id: str = "",
        message_id: str = "",
    ) -> None:
        if isinstance(event, str):
            event = AskUserEvent(prompt=event)
        input_form = {
            "formStatus": 0,
            "pluginMachineFields": [
                {
                    "formType": "textarea",
                    "fieldName": "用户输入",
                    "fieldCode": "user_input",
                    "description": event.prompt,
                    "required": True,
                }
            ],
        }
        await self.emit_event(
            session_id=session_id,
            trace_id=trace_id,
            event_type=EventType.REASONING_LOG_DELTA.value,
            source_agent_id=source_agent_id,
            message_id=message_id,
            data=_build_sse_layout(
                content=json.dumps(input_form, ensure_ascii=False),
                role="assistant",
                content_type=SseReasonMessageType.task_user_input.value,
                source_agent_id=source_agent_id,
            ),
            metadata=event.metadata,
        )
