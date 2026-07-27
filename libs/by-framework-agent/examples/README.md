# Examples

Four runnable, progressively more complete demos of `NativeAgentWorker`. Each
is self-contained — no real Redis, no real LLM API key required — using
`_infra.py`'s `InMemoryRedis` (enough of the Redis surface for
`AgentContext`/`call_agent`/`call_agents` to work unmodified) and
`by_framework_agent.testing.StubModelClient` (scripted model turns) in place
of the real deployment stack.

Run any of them directly:

```bash
cd libs/by-framework-agent
uv run python examples/01_basic_chat.py
```

| File | Demonstrates |
|---|---|
| [`01_basic_chat.py`](01_basic_chat.py) | The minimum: an `AgentConfig` with no tools/sub_agents, one model turn. |
| [`02_tools_and_ask_user.py`](02_tools_and_ask_user.py) | A local `ToolSpec` tool, then the built-in `ask_user` tool suspending the loop — resumed on a **separate** worker instance, proving the suspend/resume state lives in Redis, not process memory. |
| [`03_agent_delegation.py`](03_agent_delegation.py) | `AgentConfig.sub_agents` auto-registered as a callable tool; a single delegation dispatches via `call_agent`, and `Dispatcher.drain()` plays the role a real Redis Streams cluster would — routing the dispatched command to the sub-agent's own worker and relaying its reply back. |
| [`04_task_group.py`](04_task_group.py) | One turn delegating to **two** distinct sub-agents at once — per ADR-0001 this batches into a single `call_agents` Task Group dispatch instead of two sequential hops; Group Join resumes the orchestrator exactly once, after both replies are in. |

## What `_infra.py` is (and isn't)

`InMemoryRedis` and `Dispatcher` in [`_infra.py`](_infra.py) are **not** part
of the `by_framework_agent` public API — they exist purely to make these
examples runnable in one process with `python examples/NN_*.py`. A real
deployment runs `NativeAgentWorker` behind `WorkerRunner` against a real
Redis Streams cluster (see the root `by-framework-python` README/docs for
`run_worker()`), where competitive consumption across worker processes is
what `Dispatcher.drain()` stands in for here.

Swapping `StubModelClient` for the default `LiteLLMModelClient` (just omit
`model_client=...` when constructing your worker) is the only change needed
to call a real model provider — everything else in these examples is exactly
how you'd wire a real deployment.
