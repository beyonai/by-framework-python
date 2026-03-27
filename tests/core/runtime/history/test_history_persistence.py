from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from by_framework.core.runtime.history import BaseHistoryStorage, HistoryManager
from by_framework.core.protocol.commands import AskAgentCommand
from by_framework.core.protocol.event_type import EventType
from by_framework.core.protocol.message_header import MessageHeader
from by_framework.worker.context import AgentContext
from by_framework.worker.worker import GatewayWorker


class MockWorker(GatewayWorker):

    def get_capabilities(self):
        return ["mock-agent"]

    async def process_command(self, command, context):
        # 模拟业务逻辑发出流式内容
        await context.emit_chunk("Hello", event_type=EventType.ANSWER_DELTA.name)
        await context.emit_chunk(" World", event_type=EventType.ANSWER_DELTA.name)
        # 不主动发送 appStreamResponse，测试兜底逻辑
        return {"status": "ok"}


@pytest.fixture
def mock_redis():
    mock = MagicMock()
    # 模拟 redis.pipeline().execute() 是异步的
    pipeline = MagicMock()
    pipeline.execute = AsyncMock(return_value=[])
    mock.pipeline.return_value = pipeline
    return mock


@pytest.fixture
def mock_history_manager():
    with patch(
        "by_framework.core.runtime.history.manager.HistoryManager.save_message",
        new_callable=AsyncMock,
    ) as mocked:
        yield mocked


@pytest.mark.asyncio
async def test_context_accumulates_and_flushes(mock_redis, mock_history_manager):
    """验证 AgentContext 的分片积累与 appStreamResponse 触发的持久化"""
    context = AgentContext(
        session_id="s1", trace_id="t1", redis_client=mock_redis, current_agent_id="a1"
    )

    # 模拟发送多个分片
    await context.emit_chunk("Hello")
    await context.emit_chunk(" World")

    # 此时不应触发保存
    mock_history_manager.assert_not_called()

    # 发送流结束标识
    await context.emit_chunk("", event_type=EventType.APP_STREAM_RESPONSE.value)

    # 验证保存内容
    mock_history_manager.assert_awaited_once()
    args, kwargs = mock_history_manager.call_args
    assert kwargs["role"] == "assistant"
    assert kwargs["content"] == "Hello World"


@pytest.mark.asyncio
async def test_worker_saves_user_and_assistant_history(
    mock_redis, mock_history_manager
):
    """验证 Worker 生命周期中对用户消息的自动留痕"""
    registry = MagicMock()
    ws_manager = AsyncMock()
    ws_manager.setup_workspace.return_value = {
        "private": "/tmp",
        "public": "/tmp/public",
    }

    worker = MockWorker(
        worker_id="w1",
        redis_client=mock_redis,
        registry=registry,
        workspace_manager=ws_manager,
    )

    command = AskAgentCommand(
        header=MessageHeader(
            session_id="s2",
            trace_id="t2",
            target_agent_type="mock-agent",
            message_id="m1",
            tenant_id="default",  # 补全缺失参数
        ),
        content="User Question",
    )

    # 执行处理逻辑
    await worker._handle_message(command)

    # 验证保存了两次：一次是 user，一次是 assistant (兜底触发)
    assert mock_history_manager.call_count == 2

    # 检查用户消息
    user_call = mock_history_manager.call_args_list[0]
    assert user_call.kwargs["role"] == "user"
    assert user_call.kwargs["content"] == "User Question"

    # 检查助手回复 (由 MockWorker 积累 of "Hello World")
    assistant_call = mock_history_manager.call_args_list[1]
    assert assistant_call.kwargs["role"] == "assistant"
    assert assistant_call.kwargs["content"] == "Hello World"


@pytest.mark.asyncio
async def test_duplicate_save_prevention(mock_redis, mock_history_manager):
    """验证不会重复保存历史记录（流结束+Worker结束兜底）"""
    context = AgentContext(session_id="s3", trace_id="t3", redis_client=mock_redis)

    await context.emit_chunk("Data")
    # 手动触发一次发送结束
    await context.emit_chunk("", event_type=EventType.APP_STREAM_RESPONSE.value)
    # 逻辑上再次尝试刷入（模拟 Worker 结束时的兜底调用）
    await context.flush_to_history()

    # 应该只调用了一次保存
    assert mock_history_manager.call_count == 1


@pytest.mark.asyncio
async def test_in_memory_storage_isolation():
    """验证 InMemoryHistoryStorage 的多会话隔离"""
    from by_framework.core.runtime.history import InMemoryHistoryStorage

    storage = InMemoryHistoryStorage()

    await storage.save_message("session-A", "user", "Hello A")
    await storage.save_message("session-B", "user", "Hello B")

    history_a = await storage.get_history("session-A")
    history_b = await storage.get_history("session-B")

    assert len(history_a) == 1
    assert history_a[0]["content"] == "Hello A"
    assert len(history_b) == 1
    assert history_b[0]["content"] == "Hello B"


@pytest.mark.asyncio
async def test_history_manager_backend_switch(mock_redis):
    """验证 HistoryManager 动态切换后端逻辑"""
    from by_framework.core.runtime.history import BaseHistoryStorage

    class MyCustomStorage(BaseHistoryStorage):

        def __init__(self):
            self.saved = False

        async def get_history(self, session_id, limit=10):
            return []

        async def save_message(self, session_id, role, content, metadata=None):
            self.saved = True

    custom_storage = MyCustomStorage()
    HistoryManager.set_default_storage(custom_storage)

    # 触发保存 (通过实例)
    manager = HistoryManager(session_id="s4")
    await manager.save_message("assistant", "test")
    assert custom_storage.saved is True

    # 恢复默认，避免影响其他测试
    from by_framework.core.runtime.history import InMemoryHistoryStorage

    HistoryManager.set_default_storage(InMemoryHistoryStorage())
