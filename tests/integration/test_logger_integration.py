import asyncio
import logging
import unittest

from byclaw_gateway_sdk import logger


class TestLoggerIntegration(unittest.TestCase):
    """测试日志功能与其他模块的集成"""

    def setUp(self):
        """在每个测试前重置 logger 配置"""
        # logger is now pre-configured or exported
        self.logger = logger

    def tearDown(self):
        """在每个测试后清理 logger 配置"""
        logger = logging.getLogger("test-integration-logger")
        logger.handlers = []

    def test_logger_with_client(self):
        """测试 logger 是否能在 Client 模块中正常工作"""
        from unittest.mock import AsyncMock, MagicMock

        from byclaw_gateway_sdk import ByaiGatewayClient, SendMessageResponse

        redis_mock = AsyncMock()
        redis_mock.pipeline = MagicMock(
            return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
        )
        registry_mock = AsyncMock()
        registry_mock.get_target_worker = AsyncMock(return_value="worker-1")

        client = ByaiGatewayClient(redis_client=redis_mock, registry=registry_mock)
        # 调用一个会产生日志的方法
        result = asyncio.run(
            client.send_message(
                "test-agent", "test-session", "test-tenant", "test-content"
            )
        )
        self.assertIsInstance(result, SendMessageResponse)

    def test_logger_with_worker(self):
        """测试 logger 是否能在 Worker 模块中正常工作"""
        import asyncio
        from unittest.mock import AsyncMock, MagicMock, Mock

        from byclaw_gateway_sdk import GatewayWorker

        # 创建一个简单的 Worker 子类进行测试
        class TestWorker(GatewayWorker):

            def get_capabilities(self):
                return ["test-agent"]

            async def process_command(self, command, context):
                from byclaw_gateway_sdk import StateChangeEvent

                self.logger.debug("Test worker processing task")
                await context.emit_state(StateChangeEvent(state="COMPLETED"))

        # 配置适当的模拟对象
        workspace_manager_mock = Mock()
        workspace_manager_mock.setup_workspace = AsyncMock(
            return_value={
                "root": "/tmp/test",
                "public": "/tmp/test/public",
                "private": "/tmp/test/private",
            }
        )
        workspace_manager_mock.cleanup_task = AsyncMock(return_value=None)

        # 配置 msg 模拟对象
        from byclaw_gateway_sdk.core.protocol.commands import AskAgentCommand
        from byclaw_gateway_sdk.core.protocol.message_header import MessageHeader

        msg_mock = AskAgentCommand(
            header=MessageHeader(
                message_id="test-msg-id",
                session_id="test-session-id",
                trace_id="trace-logger",
                source_agent_id="test-source",
                target_agent_type="test-agent",
            ),
            content="test-content",
        )

        registry_mock = AsyncMock()
        redis_mock = AsyncMock()
        redis_mock.pipeline = MagicMock(
            return_value=MagicMock(xadd=MagicMock(), execute=AsyncMock(return_value=[]))
        )
        worker = TestWorker(
            "test-worker", redis_mock, registry_mock, workspace_manager_mock
        )

        async def run_worker():
            # 模拟处理消息
            await worker.start_heartbeat()
            await worker._handle_message(msg_mock)

        try:
            asyncio.run(run_worker())
            self.assertTrue(True, "Worker 能正常处理任务")
        except Exception as e:
            self.fail(f"Worker 处理任务时抛出异常: {e}")

    def test_logger_file_output(self):
        """测试日志是否能写入到文件中"""
        import os

        log_file = "gateway-sdk.log"
        # 保存原来的 logger 级别
        original_level = self.logger.level
        try:
            # 设置 logger 级别为 DEBUG 以便 debug 日志可以被文件处理器捕获
            self.logger.setLevel(logging.DEBUG)
            self.logger.info("Test log file output")
            self.logger.debug("Debug log entry")
            self.logger.warning("Warning log entry")

            # 确保日志被刷新到文件
            for handler in self.logger.handlers:
                handler.flush()

            self.assertTrue(os.path.exists(log_file), "日志文件未创建")
            self.assertTrue(os.path.getsize(log_file) > 0, "日志文件内容为空")

            # 验证日志内容
            with open(log_file, "r") as f:
                content = f.read()
                self.assertIn("Test log file output", content)
                self.assertIn("Debug log entry", content)
                self.assertIn("Warning log entry", content)
        finally:
            # 恢复原来的 logger 级别
            self.logger.setLevel(original_level)


if __name__ == "__main__":
    unittest.main()
