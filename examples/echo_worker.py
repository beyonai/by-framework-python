"""Minimal reference Worker: echoes back whatever content it receives, and
fans a request out to two "echo" sub-calls via call_agents.

Doubles as the target of .github/workflows/deploy-smoke-test.yml, which
builds deploy/Dockerfile in "local source" mode and drives this Worker
through the full client -> Redis control stream -> Worker -> Redis data
stream -> client round trip via examples/send_and_verify.py. The "fanout"
agent type specifically exercises call_agents end to end against a real,
separate Redis Streams deployment (not the in-memory fakes the unit/
integration tests use), so a real-Redis-only bug in Group Join couldn't
hide behind those.
"""

from by_framework import (
    AgentContext,
    AgentState,
    GatewayWorker,
    ResumeCommand,
    run_worker,
)


class EchoWorker(GatewayWorker):

    def get_agent_types(self):
        return ["echo", "fanout"]

    async def process_command(self, command, context: AgentContext):
        if context.current_agent_id == "fanout":
            return await self._handle_fanout(command, context)

        reply = f"echo: {command.content}"
        await context.emit_chunk(reply)
        return {"status": "completed", "content": reply}

    async def _handle_fanout(self, command, context: AgentContext):
        if isinstance(command, ResumeCommand):
            # The Task Group has completed: command.reply_data is the
            # aggregated list Group Join built, one entry per echo call.
            aggregate = command.reply_data or []
            parts = [str(item.get("content", "")) for item in aggregate]
            reply = "fanout: " + " | ".join(parts)
            await context.emit_chunk(reply)
            return {"status": "completed", "content": reply}

        await context.call_agents(
            tasks=[
                {"target_agent_type": "echo", "content": "one"},
                {"target_agent_type": "echo", "content": "two"},
            ],
        )
        return {"status": AgentState.QUEUED.value}


if __name__ == "__main__":
    run_worker(EchoWorker, worker_id="echo-worker-1")
