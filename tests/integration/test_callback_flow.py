import json
import unittest
from typing import Any

from by_framework import (
    GatewayWorker,
    RedisKeys,
    StateChangeEvent,
    StreamChunkEvent,
    WorkerRunner,
)
from by_framework.core.protocol.commands import AskAgentCommand, ResumeCommand
from by_framework.core.protocol.message_header import MessageHeader


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
        self.sets = {}
        self.zsets = {}
        self.kv = {}

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

    async def sadd(self, name, value):
        """Add a member to a set (for agent-type registration)."""
        if name not in self.sets:
            self.sets[name] = set()
        self.sets[name].add(value)
        return 1

    async def smembers(self, name):
        """Return members of a set (for agent-type workers lookup)."""
        return self.sets.get(name, set())

    async def zadd(self, name, mapping):
        """Add member with score to sorted set (for active worker heartbeat)."""
        if name not in self.zsets:
            self.zsets[name] = {}
        for k, v in mapping.items():
            self.zsets[name][k] = v

    async def zrem(self, name, *values):
        bucket = self.zsets.get(name, {})
        removed = 0
        for value in values:
            if value in bucket:
                del bucket[value]
                removed += 1
        return removed

    async def set(self, name, value, nx=False, ex=None):
        if nx and name in self.kv:
            return False
        self.kv[name] = value
        return True

    async def get(self, name):
        return self.kv.get(name)

    async def delete(self, name):
        self.kv.pop(name, None)
        self.sets.pop(name, None)
        self.zsets.pop(name, None)

    async def zrangebyscore(self, name, min_score, max_score, withscores=False):
        """Return members within score range (for active worker lookup)."""
        if name not in self.zsets:
            return [] if not withscores else []
        max_val = float("inf") if max_score == "+inf" else max_score
        result = [
            (k, v) for k, v in self.zsets[name].items() if min_score <= v <= max_val
        ]
        return result if withscores else [k for k, v in result]

    def pipeline(self):
        return MockPipeline(self)


class MockRegistry:

    def __init__(self, mq=None):
        self.executions = {}
        self.mq = mq

    async def register_worker_membership(self, worker_id: str, agent_types: list):
        """Register static worker agent types in LocalMemoryMQ
        for agent-type probing."""
        if self.mq is None:
            return
        for agent_type in agent_types:
            await self.mq.sadd(
                f"byai_gateway:registry:agent_type:workers:{agent_type}", worker_id
            )

    async def heartbeat_worker(self, worker_id: str, lease_ttl_seconds: int = 15):
        """Mark a worker online in LocalMemoryMQ."""
        import time

        if self.mq is None:
            return
        await self.mq.zadd(
            "byai_gateway:registry:active_workers", {worker_id: int(time.time() * 1000)}
        )
        await self.mq.set(
            f"byai_gateway:registry:worker:online:{worker_id}",
            "1",
            ex=lease_ttl_seconds,
        )

    async def save_execution(self, data):
        self.executions[data["message_id"]] = data
        if "execution_id" in data:
            self.executions[data["execution_id"]] = data

    async def get_execution_by_message_id(self, message_id, session_id=""):
        return self.executions.get(message_id)

    async def get_execution(self, execution_id, session_id=""):
        return self.executions.get(execution_id)

    async def persist_agent_configs_snapshot(self, execution_id, snapshot):
        snapshot_key = f"snapshot:{execution_id}"
        if self.mq:
            await self.mq.set(snapshot_key, snapshot)
        return snapshot_key

    async def update_execution_fields(self, execution_id, session_id, **kwargs):
        execution = self.executions.get(execution_id)
        if execution:
            execution.update(kwargs)

    async def load_agent_configs_snapshot(self, snapshot_key):
        if self.mq:
            return await self.mq.get(snapshot_key)
        return None


class MockWorkspaceManager:

    async def setup_workspace(
        self,
        session_id,
        task_id,
        user_code="default",
        agent_id="",
    ):
        del session_id, task_id, user_code, agent_id
        return {"public": "/tmp/pub", "private": "/tmp/priv"}

    async def cleanup_task(
        self,
        session_id,
        task_id,
        user_code="default",
        agent_id="",
    ):
        del session_id, task_id, user_code, agent_id
        pass


class AgentA(GatewayWorker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.first_hop_calls = 0
        self.resume_calls = 0

    def get_agent_types(self) -> list[str]:
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
            extra_payload={"from": "agent-a"},
            wait_for_reply=True,
        )
        return {"status": "waiting"}


class AgentB(GatewayWorker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls = 0

    def get_agent_types(self) -> list[str]:
        return ["agent-b"]

    async def process_command(self, command, context: Any):
        self.calls += 1
        await context.emit_state(StateChangeEvent(state="B_PROCESSING"))
        return {"answer": "42", "from": "agent-b"}


class TestCallbackFlow(unittest.IsolatedAsyncioTestCase):

    async def test_classic_a_b_a_callback_flow(self):
        """Test the classic A->B->A callback flow where A calls B
        and B resumes A with result."""
        mq = LocalMemoryMQ()
        registry = MockRegistry(mq=mq)
        workspace = MockWorkspaceManager()

        agent_a = AgentA("worker-a", mq, registry, workspace)
        agent_b = AgentB("worker-b", mq, registry, workspace)

        runner_a = WorkerRunner(mq, agent_a, group_name="test-group")
        runner_b = WorkerRunner(mq, agent_b, group_name="test-group")

        await runner_a.setup_streams()
        await runner_b.setup_streams()

        # Register workers with their agent types for agent-type probing
        await registry.register_worker_membership("worker-a", agent_a.get_agent_types())
        await registry.heartbeat_worker("worker-a")
        await registry.register_worker_membership("worker-b", agent_b.get_agent_types())
        await registry.heartbeat_worker("worker-b")

        initial_msg = AskAgentCommand(
            header=MessageHeader(
                message_id="msg-root-1",
                session_id="sess-callback",
                trace_id="trace-root",
                user_code="user-a",
                user_name="name-a",
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
