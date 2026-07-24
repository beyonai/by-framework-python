"""Smoke-test client for examples/echo_worker.py.

Runs two cases against the Worker and waits for each expected reply on the
session data stream, exiting 0 on success or 1 on timeout:

1. "echo" - single-call round trip: client -> Redis control stream ->
   Worker -> Redis data stream -> client.
2. "fanout" - the same round trip, but the Worker's reply is only produced
   after it fans out to two more "echo" calls via call_agents and gets
   resumed with their aggregated results. This is the only place call_agents
   is exercised against a real, separate-process Redis Streams deployment
   instead of the in-memory fakes tests/ uses, so it can catch a real-Redis-
   only Group Join bug those can't.

Used by .github/workflows/deploy-smoke-test.yml to validate that deploy/'s
Dockerfile + docker-compose.yml deliver both flows end to end. Also runnable
by hand against `deploy/docker-compose.yml` locally.
"""

import asyncio
import os
import sys
import uuid
from typing import Callable

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
    not raise".

    Kept even though deploy-smoke-test.yml's `docker compose up --wait`
    already waits for /readyz to report "serving": readyz flips to serving
    the instant WorkerRunner's consume loop ticks once, which happens
    slightly *before* the Worker registers itself online in Redis
    (heartbeat/registration starts after that same tick, not before) - see
    docs/architecture/worker-readiness-endpoint.md. So "--wait succeeded"
    narrows the race but doesn't fully close it; this retry loop is the
    layer that actually does."""
    while True:
        resp = await client.send_message(**kwargs)
        if resp.success:
            return resp
        print(f"waiting for an online worker: {resp.error or resp.status}")
        await asyncio.sleep(SEND_RETRY_INTERVAL_SECONDS)


async def _run_case(
    client: GatewayClient,
    *,
    target_agent_type: str,
    content: str,
    is_expected_reply: Callable[[str], bool],
) -> None:
    """Send one message and block until a data-stream event's content
    satisfies is_expected_reply. Caller wraps this in asyncio.timeout."""
    session_id = f"smoketest-{uuid.uuid4().hex[:8]}"
    await _send_with_retry(
        client,
        target_agent_type=target_agent_type,
        session_id=session_id,
        content=content,
    )
    print(f"sent {content!r} to {target_agent_type!r} on session {session_id}")

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
                f"[{target_agent_type}] received "
                f"event_type={entry.message.event_type!r} content={delta!r}"
            )
            if delta is not None and is_expected_reply(delta):
                print(f"OK: {target_agent_type} received expected reply")
                return


def _is_expected_fanout_reply(delta: str, expected_parts: set[str]) -> bool:
    prefix = "fanout: "
    if not delta.startswith(prefix):
        return False
    parts = {part.strip() for part in delta[len(prefix) :].split("|")}
    return parts == expected_parts


async def main() -> int:
    redis = init_redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", "6379")),
    )
    registry = WorkerRegistry(redis_client=redis)
    client = GatewayClient(redis_client=redis, registry=registry)

    try:
        async with asyncio.timeout(TIMEOUT_SECONDS):
            echo_content = f"ping-{uuid.uuid4().hex[:8]}"
            expected_echo = f"echo: {echo_content}"
            await _run_case(
                client,
                target_agent_type="echo",
                content=echo_content,
                is_expected_reply=lambda delta: delta == expected_echo,
            )

            # echo_worker.py's "fanout" handler ignores the sent content and
            # always fans out to two fixed "echo" sub-calls ("one", "two").
            expected_fanout_parts = {"echo: one", "echo: two"}
            await _run_case(
                client,
                target_agent_type="fanout",
                content="ignored",
                is_expected_reply=lambda delta: _is_expected_fanout_reply(
                    delta, expected_fanout_parts
                ),
            )
        return 0
    except TimeoutError:
        print(f"FAILED: no matching reply within {TIMEOUT_SECONDS}s", file=sys.stderr)
        return 1
    finally:
        await close_redis()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
