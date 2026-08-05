"""Minimal reference Worker: echoes back whatever content it receives.

Doubles as the target of .github/workflows/deploy-smoke-test.yml, which
builds deploy/Dockerfile in "local source" mode and drives this Worker
through the full client -> Redis control stream -> Worker -> Redis data
stream -> client round trip via examples/send_and_verify.py.
"""

from by_framework import AgentContext, GatewayWorker, run_worker


class EchoWorker(GatewayWorker):

    def get_agent_types(self):
        return ["echo"]

    async def process_command(self, command, context: AgentContext):
        reply = f"echo: {command.content}"
        await context.emit_chunk(reply)
        return {"status": "completed", "content": reply}


if __name__ == "__main__":
    run_worker(EchoWorker, worker_id="echo-worker-1")
