"""Smoke-test client for examples/echo_worker.py.

Sends one message to the "echo" agent type and waits for the Worker's
echoed reply on the session data stream, exiting 0 on success or 1 on
timeout. Used by .github/workflows/deploy-smoke-test.yml to validate that
deploy/'s Dockerfile + docker-compose.yml deliver a message through the
full pipeline: client -> Redis control stream -> Worker -> Redis data
stream -> client. Also runnable by hand against `deploy/docker-compose.yml`
locally.
"""

import asyncio
import os
import sys
import uuid

from by_framework import GatewayClient, WorkerRegistry, close_redis, init_redis

TIMEOUT_SECONDS = float(os.environ.get("SMOKETEST_TIMEOUT_SECONDS", "60"))
SEND_RETRY_INTERVAL_SECONDS = 1.0


async def _send_with_retry(client: GatewayClient, **kwargs):
    """Retry send_message until a Worker for the target agent type is
    online. Under FAIL_FAST (the default route policy), send_message does
    NOT raise when none is online yet - it returns
    SendMessageResponse(success=False, ...) via AvailabilityRouter instead,
    which is easy to miss since nothing looks like a failure at the call
    site. The Worker container may still be starting/registering when this
    script first runs, so callers MUST check .success, not just "did this
    not raise"."""
    while True:
        resp = await client.send_message(**kwargs)
        if resp.success:
            return resp
        print(f"waiting for an online worker: {resp.error or resp.status}")
        await asyncio.sleep(SEND_RETRY_INTERVAL_SECONDS)


async def main() -> int:
    redis = init_redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", "6379")),
    )
    registry = WorkerRegistry(redis_client=redis)
    client = GatewayClient(redis_client=redis, registry=registry)

    session_id = f"smoketest-{uuid.uuid4().hex[:8]}"
    sent_content = f"ping-{uuid.uuid4().hex[:8]}"
    expected_reply = f"echo: {sent_content}"

    try:
        async with asyncio.timeout(TIMEOUT_SECONDS):
            await _send_with_retry(
                client,
                target_agent_type="echo",
                session_id=session_id,
                content=sent_content,
            )
            print(f"sent {sent_content!r} on session {session_id}")

            last_id = "0-0"
            while True:
                entries = await client.read_data_messages(
                    session_id=session_id, last_id=last_id, block_ms=2000
                )
                for entry in entries:
                    last_id = entry.stream_id
                    delta = (
                        entry.message.data.get("choices", [{}])[0]
                        .get("delta", {})
                        .get("content")
                    )
                    print(
                        f"received event_type={entry.message.event_type!r} "
                        f"content={delta!r}"
                    )
                    if delta == expected_reply:
                        print("OK: received expected echo")
                        return 0
    except TimeoutError:
        print(f"FAILED: no matching echo within {TIMEOUT_SECONDS}s", file=sys.stderr)
        return 1
    finally:
        await close_redis()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
