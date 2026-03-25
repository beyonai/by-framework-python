import json
import unittest
from typing import Any

from byclaw_gateway_sdk import (
    GatewayWorker,
    RedisKeys,
    StateChangeEvent,
    StreamChunkEvent,
    WorkerRunner,
)
from byclaw_gateway_sdk.core.protocol.commands import (AskAgentCommand, ResumeCommand)
from byclaw_gateway_sdk.core.protocol.message_header import MessageHeader


class MockPipeline:

    def __init__(self, mq):
        self.mq = mq
        self.commands = []

    def xadd(self, name, fields, maxlen=None, approximate=True):
        self.commands.append(("xadd", name, fields))
        return self

    def expire(self, name, ttl):
        self.commands.append(("expire", name, ttl))
        return self

    async def execute(self):
        for cmd in self.commands:
            if cmd[0] == "xadd":
                await self.mq.xadd(cmd[1], cmd[2])
        return []


class LocalMemoryMQ:
    """Minimal in-memory Redis-like MQ for callback flow tests."""

    def __init__(self):
        self.streams = {}

    async def xgroup_create(self, name, groupname, id="0", mkstream=False):
        pass

    async def xadd(self, name, fields):
        if name not in self.streams:
            self.streams[name] = []
        msg_id = f"{len(self.streams[name]) + 1}-0"
        encoded = {
            (k.encode() if isinstance(k, str) else k): (
                v.encode() if isinstance(v, str) else v
            )
            for k, v in fields.items()
        }
        self.streams[name].append((msg_id.encode(), encoded))
        return msg_id

    async def xreadgroup(self, groupname, consumername, streams, count=1, block=0):
        results = []
        for stream_name, _ in streams.items():
            name = stream_name
            if name in self.streams and self.streams[name]:
                item = self.streams[name].pop(0)
                results.append((name.encode(), [item]))
        return results

    async def xack(self, name, groupname, *ids):
        pass

    def pipeline(self):
        return MockPipeline(self)


class MockRegistry:

    async def register_worker(self, worker_id: str, capabilities: list):
        pass


class MockWorkspaceManager:

    async def setup_workspace(self, session_id, task_id):
        return {"public": "/tmp/pub", "private": "/tmp/priv"}

    async def cleanup_task(self, session_id, task_id):
        pass


class AgentA(GatewayWorker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.first_hop_calls = 0
        self.resume_calls = 0

    def get_capabilities(self) -> list[str]:
        return ["agent-a"]

    async def process_command(self, command, context: Any):
        if isinstance(command, ResumeCommand):
            self.resume_calls += 1
            answer = (
                command.reply_data.get("answer", "")
                if isinstance(command.reply_data, dict)
                else ""
            )
            await context.emit_state(StateChangeEvent(state="A_RESUMED"))
            await context.emit_chunk(
                StreamChunkEvent(content=f"A final answer: {answer}")
            )
            return {"status": "done", "answer": answer}

        assert isinstance(command, AskAgentCommand)
        self.first_hop_calls += 1
        await context.emit_state(StateChangeEvent(state="A_CALLING_B"))
        await context.call_agent(
            target_agent_type="agent-b",
            content="Please process and return",
            payload={"from": "agent-a"},
            wait_for_reply=True,
        )
        return {"status": "waiting"}


class AgentB(GatewayWorker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls = 0

    def get_capabilities(self) -> list[str]:
        return ["agent-b"]

    async def process_command(self, command, context: Any):
        self.calls += 1
        await context.emit_state(StateChangeEvent(state="B_PROCESSING"))
        return {"answer": "42", "from": "agent-b"}


class TestCallbackFlow(unittest.IsolatedAsyncioTestCase):

    async def test_classic_a_b_a_callback_flow(self):
        """Test the classic A->B->A callback flow where A calls B and B resumes A with result."""
        mq = LocalMemoryMQ()
        registry = MockRegistry()
        workspace = MockWorkspaceManager()

        agent_a = AgentA("worker-a", mq, registry, workspace)
        agent_b = AgentB("worker-b", mq, registry, workspace)

        runner_a = WorkerRunner(mq, agent_a, group_name="test-group")
        runner_b = WorkerRunner(mq, agent_b, group_name="test-group")

        await runner_a.setup_streams()
        await runner_b.setup_streams()

        initial_msg = AskAgentCommand(
            header=MessageHeader(
                message_id="msg-root-1",
                session_id="sess-callback",
                trace_id="trace-root",
                target_agent_type="agent-a",
            ),
            content="start",
        )
        await mq.xadd(RedisKeys.ctrl_stream("agent-a"), initial_msg.to_redis_payload())

        # Round 1: A consumes root request, dispatches B
        await runner_a._run_once()
        await runner_a.wait_for_tasks()
        # Round 2: B consumes delegated request, emits callback to A
        await runner_b._run_once()
        await runner_b.wait_for_tasks()
        # Round 3: A consumes RESUME and finishes
        await runner_a._run_once()
        await runner_a.wait_for_tasks()

        self.assertEqual(agent_a.first_hop_calls, 1)
        self.assertEqual(agent_b.calls, 1)
        self.assertEqual(agent_a.resume_calls, 1)

        data_stream_name = RedisKeys.session_data_stream("sess-callback")
        data_messages = [
            json.loads(item[1][b"data"].decode())
            for item in mq.streams.get(data_stream_name, [])
        ]
        state_events = [
            m for m in data_messages if m.get("event_type") == "reasoningLogDelta"
        ]
        chunk_events = [
            m for m in data_messages if m.get("event_type") == "answerDelta"
        ]

        self.assertTrue(
            any(
                e.get("data", {})
                .get("choices", [{}])[0]
                .get("delta", {})
                .get("content")
                == "A_CALLING_B"
                for e in state_events
            )
        )
        self.assertTrue(
            any(
                e.get("data", {})
                .get("choices", [{}])[0]
                .get("delta", {})
                .get("content")
                == "B_PROCESSING"
                for e in state_events
            )
        )
        self.assertTrue(
            any(
                e.get("data", {})
                .get("choices", [{}])[0]
                .get("delta", {})
                .get("content")
                == "A_RESUMED"
                for e in state_events
            )
        )
        self.assertTrue(
            any(
                "A final answer: 42"
                in e.get("data", {})
                .get("choices", [{}])[0]
                .get("delta", {})
                .get("content", "")
                for e in chunk_events
            )
        )


if __name__ == "__main__":
    unittest.main()
