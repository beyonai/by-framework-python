# by-framework

<div align="center">

[![PyPI](https://img.shields.io/pypi/v/by-framework?color=blue)](https://pypi.org/project/by-framework/)
[![Python](https://img.shields.io/badge/python-3.12+-yellow.svg)](https://www.python.org/)
[![Redis](https://img.shields.io/badge/redis-7.0+-red.svg)](https://redis.io/)
[![License](https://img.shields.io/badge/license-Apache_2.0-green.svg)](LICENSE)

</div>

<div align="center">

[**English**](README.md) | [**中文**](README_zh.md)

</div>

---

**by-framework** is a distributed, high-performance Agent scheduling engine built on Redis Streams, purpose-built for multi-agent systems.

## Challenges in Traditional Architecture

Traditional AI application architectures often face three critical challenges when dealing with Agent scenarios:

- **Full-link Synchronous Blocking $\rightarrow$ Forced "Manual Monitoring"** — Strong coupling between frontend and backend means tasks are interrupted if the page is closed. Users cannot switch devices or tasks, making workflows fragile to network fluctuations or interruptions.
- **Inability to Support Long-running Tasks $\rightarrow$ System "Constant Accompaniment"** — For reasoning tasks taking minutes or hours, callers must block threads and wait. This leads to gateway timeouts and massive waste of idle compute resources.
- **Inter-Agent Orchestration Recovery Dilemma** — In complex cascaded calls, if a timeout or interruption occurs, it's nearly impossible to accurately resume state. Developers are forced to build extremely complex persistent state machines.

## The By-Framework Solution

![Architecture Overview](./assets/img/architecture_en.png)

By-Framework addresses these issues through an asynchronous architecture with **separated Control and Data Planes**:

- **Instruction Asynchrony**: The APP sends control instructions to the **Control Queue** via the **Gateway Client**. Being asynchronous, the APP never blocks, and backend threads are released immediately.
- **Agent Cluster Consumption**: A distributed cluster of **Agents** competitively consumes messages from the control queue. Logical routing (Agent Type) provides native load balancing and elastic scaling.
- **Data Stream Feedback**: During execution, Agents asynchronously push chunks, state changes, and artifacts to the **Data Queue**. The APP listens via the **Gateway Client** for progress, natively supporting ultra-long tasks.
- **Native Orchestration & Resumption**: When an Agent needs to call another Agent, it sends a new instruction to the **Control Queue**. This message-based mechanism allows tasks to release resources while waiting and resume context precisely upon receiving a reply.

## Highlights

- 🔌 **Plugin System** — Hot-reloadable plugins with lifecycle hooks, tools, prompts, and sub-agent configs
- 🤝 **Inter-Agent Orchestration** — Built-in `call_agent`, scatter-gather fan-out, and human-in-the-loop patterns
- 🧩 **Extension Ecosystem** — Drop-in packages for Langfuse, Phoenix, PostgreSQL, LangGraph, and Google ADK
- 🛡️ **Production-Ready** — Competitive consumption, graceful shutdown, message persistence, and execution state tracking


## Table of Contents

- [Architecture](#architecture)
  - [Data Flow](#data-flow)
  - [Component Hierarchy](#component-hierarchy)
  - [Worker Routing](#worker-routing)
  - [Redis Key Map](#redis-key-map)
- [Getting Started](#getting-started)
  - [Installation](#installation)
  - [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
  - [GatewayWorker](#gatewayworker)
  - [AgentContext](#agentcontext)
  - [Protocol & Messages](#protocol--messages)
  - [Plugin System](#plugin-system)
- [Advanced Features](#advanced-features)
  - [Inter-Agent Calling](#inter-agent-calling)
  - [Scatter-Gather Dispatch](#scatter-gather-dispatch)
  - [User-in-the-Loop](#user-in-the-loop)
  - [Service Discovery](#service-discovery)
- [Sending Tasks](#sending-tasks)
- [Extension Libraries](#extension-libraries)
- [Configuration Reference](#configuration-reference)
- [Development](#development)
- [Deployment](#deployment)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

---

## Architecture

The system is fully asynchronous and event-driven. Control messages and data events travel on separate Redis Streams, so scaling up Workers does not couple to the data delivery path.

### Data Flow

```
Client ──▶ Redis Control Stream ──▶ GatewayWorker (competitive consume)
               (queue:ctrl:{agent_type})
                                            │
                                            ▼
Consumer ◀──── Redis Data Stream ◀──── Business Logic (emit chunks/states/artifacts)
Backend       (queue:data:{session})
```

1. A **client** writes a command to the agent-type control stream.
2. Any online **GatewayWorker** subscribing to that agent type competitively pulls the message via Redis consumer groups.
3. The Worker processes the task and, through `AgentContext`, emits streaming chunks, state changes, and artifacts back to a **session-scoped data stream**.
4. A **backend** or **frontend** consumer reads that data stream in real time.

### Component Hierarchy

| Layer | Component | Source | Role |
|---|---|---|---|
| **Client** | `GatewayClient` / `ByaiGatewayClient` | `client/` | Publish control commands to Redis. Supports interceptors and cascade cancellation. |
| **Scheduler** | Redis Streams + consumer groups | (infrastructure) | Competitive consumption, automatic load balancing across Worker replicas. |
| **Execution** | `GatewayWorker` / `ByaiWorker` | `worker/` | Pulls tasks, executes business logic in isolated workspaces, hooks into plugin lifecycle. |
| **Orchestrator** | `WorkerRunner` | `worker/runner.py` | Manages message consumption loop, concurrency semaphore, graceful shutdown. |
| **Output** | `GatewayDataEmitter` | `common/emitter.py` | Pushes events to session-scoped data streams with TTL. |
| **Registry** | `WorkerRegistry` | `core/registry.py` | Redis-backed worker membership, heartbeats, execution state tracking. |
| **Plugin** | `PluginRegistry` | `core/extensions/` | Plugin discovery, lifecycle hooks, agent config versioning, hot-reload. |

### Worker Routing

Three semantic layers govern how tasks reach Workers:

| Layer | Purpose | Update Timing |
|---|---|---|
| **Membership** | Worker declares supported `agent_types` via `get_agent_types()`. | Startup / graceful shutdown |
| **Online / Heartbeat** | Redis key with TTL; each Worker refreshes periodically. Only online Workers are valid send targets. | Heartbeat cycle |
| **Worker ID Lock** | Prevents duplicate startup of the same `worker_id`. Instance mutex, not used for routing. | Startup / shutdown |

**Production path (agent type routing):**
- Client writes to `byai_gateway:ctrl:agent_type:{agent_type}`.
- Multiple Workers in the same consumer group compete for messages.
- Sender only verifies that at least one online Worker exists for the target agent type.

**Debug path (direct worker routing):**
- When `target_worker_id` is explicitly provided, the message goes to `byai_gateway:ctrl:worker:{worker_id}`.
- The sender explicitly checks that the target Worker is online.

### Redis Key Map

| Key Pattern | Type | Purpose |
|---|---|---|
| `byai_gateway:ctrl:agent_type:{agent_type}` | Stream | Per-agent-type control queue; competitive consume |
| `byai_gateway:ctrl:worker:{worker_id}` | Stream | Per-worker control queue; direct routing |
| `byai_gateway:session:{session_id}:data_stream` | Stream | Session-scoped output events |
| `byai_gateway:session:{session_id}:registry` | Hash | Execution records for a session |
| `byai_gateway:task_group:{group_id}` | Hash | Scatter-gather group progress tracker |
| `byai_gateway:task_group:{group_id}:results` | Hash | Scatter-gather results collection |
| `byai_gateway:registry:worker:online:{worker_id}` | String (TTL) | Heartbeat lease |
| `byai_gateway:registry:agent_type:workers:{agent_type}` | Set | Agent type ➜ worker IDs |
| `byai_gateway:registry:worker:agent_types:{worker_id}` | Set | Worker ➜ agent types |
| `byai_gateway:agent_configs_snapshot:{key}` | String | Serialized agent config snapshots for durable restart |
| `byai_gateway:plugin_reload:{id}:ack` | Stream | Hot-reload ACK channel |

---

## Getting Started

### Installation

**Prerequisites:** Python 3.12+, Redis 7.0+

```bash
# via pip
pip install by-framework
```

Optional extension packages:

```bash
pip install by-framework-trace-langfuse       # Langfuse observability
pip install by-framework-history-postgres     # PostgreSQL history backend
pip install by-framework-langgraph            # LangGraph integration
```

### Quick Start

**1. Define a Worker:**

```python
# my_agent.py
from by_framework import GatewayWorker, AgentContext, run_worker

class MyAgent(GatewayWorker):
    def get_agent_types(self):
        return ["my_agent"]

    async def process_command(self, command, context: AgentContext):
        await context.emit_chunk("Hello from your agent!")
        return {"status": "completed", "content": "Hello from your agent!"}

if __name__ == "__main__":
    run_worker(MyAgent, worker_id="worker-01")
```

**2. Start Redis:**

```bash
docker run -d -p 6379:6379 redis:7-alpine
```

**3. Start the Worker:**

```bash
uv run python my_agent.py
```

**4. Send a task:**

```python
# send_task.py
import asyncio
from by_framework import ByaiGatewayClient, WorkerRegistry, init_redis, close_redis

async def main():
    redis = init_redis(host="localhost", port=6379)
    registry = WorkerRegistry(redis_client=redis)
    client = ByaiGatewayClient(redis_client=redis, registry=registry)

    resp = await client.send_message(
        target_agent_type="my_agent",
        session_id="demo-session",
        content="Hello!",
    )
    print(f"Sent: {resp.message_id}")

    await close_redis()

asyncio.run(main())
```

---

## Core Concepts

### GatewayWorker

The abstract base class for all Workers. You implement two methods:

| Method | Required | Purpose |
|---|---|---|
| `get_agent_types()` | Yes | Returns the list of agent types this Worker handles. Drives routing and worker registration. |
| `process_command(command, context)` | Yes | Core business logic. Receives a command object and an `AgentContext`. Return an `AgentTaskResult` or dict. |

The base class handles, transparently:
- Message lifecycle (parse, decode, acknowledge, persist)
- Workspace provisioning per session/task
- Plugin hook execution (on_task_start, on_task_complete, etc.)
- Sub-agent call orchestration (suspend, resume, cascade cancel)
- History persistence (in-memory or pluggable backend)

### AgentContext

The per-task runtime context. Available as the second argument to `process_command()`.

```python
async def process_command(self, command, context: AgentContext):
    # Streaming output
    await context.emit_chunk("Step 1 complete\n")

    # State transitions
    await context.emit_state("analyzing")

    # Artifacts / structured data
    await context.emit_artifact(ArtifactEvent(url="https://example.com/output.json"))

    # Call another agent (with optional suspend-and-wait)
    reply = await context.call_agent(
        target_agent_type="translator",
        content="Hello world",
        wait_for_reply=True,
    )

    # Scatter-gather fan-out
    group = await context.dispatch_group([
        {"target_agent_type": "researcher", "content": "Find references"},
        {"target_agent_type": "writer",    "content": "Draft summary"},
    ])
    results = await context.collect_group_results(group["task_group_id"])

    # Ask the end-user a question (suspends the task)
    return await context.ask_user(AskUserEvent(prompt="Approve deployment?"))
```

Key properties: `session_id`, `trace_id`, `message_id`, `parent_message_id`, `current_agent_id`.

### Protocol & Messages

Commands and events are defined in `core/protocol/`. The system supports these command types:

| Command | Purpose |
|---|---|
| `AskAgentCommand` | Standard task request with content, header, and optional `extra_payload`. |
| `ResumeCommand` | Resumes a suspended task (e.g., after `ask_user` reply). Carries `reply_data`. |
| `CancelTaskCommand` | Graceful or forced cancellation. Supports BFS cascade through the task tree. |
| `ReloadPluginsCommand` | Hot-reload plugins on all Workers without restart. |

Event types emitted to data streams:

| Event | Purpose |
|---|---|
| `StreamChunkEvent` | Incremental streaming text / reasoning log. |
| `StateChangeEvent` | Agent state transitions (thinking, completed, failed, etc.). |
| `ArtifactEvent` | Structured output files, URLs, or attachments. |
| `AskUserEvent` | Prompt requesting human input (triggers task suspension). |

Message context is carried by `MessageHeader` (`message_id`, `session_id`, `trace_id`, `source_agent_type`, `target_agent_type`, `parent_message_id`, `task_group_id`, `user_code`).

### Plugin System

Plugins are the primary extensibility mechanism. They register **AgentConfigs** that declare tools, prompts, skills, callbacks, and sub-agents — and they hook into the Worker lifecycle.

#### Writing a Plugin

```python
from by_framework import Plugin, PluginManifest, AgentConfig, PluginBuildContext, AgentContext

class WeatherPlugin(Plugin):
    def __init__(self):
        super().__init__(PluginManifest(plugin_id="weather", version="1.0.0"))

    async def register_agent_configs(self, ctx: PluginBuildContext) -> list[AgentConfig]:
        return [
            AgentConfig(
                agent_id="weather_agent",
                tools={
                    "get_weather": self._get_weather,
                },
                prompts={
                    "system": "You are a weather assistant."
                },
            )
        ]

    async def _get_weather(self, city: str) -> dict:
        return {"city": city, "temp": 22, "condition": "sunny"}

    # Lifecycle hooks
    async def on_task_start(self, context: AgentContext): ...
    async def on_task_complete(self, context: AgentContext, result): ...
    async def on_task_error(self, context: AgentContext, error: Exception): ...
    async def on_task_cancel(self, context: AgentContext, reason: str): ...
    async def on_call_agent_start(self, context: AgentContext, target: str, content): ...
    async def on_call_agent_complete(self, context: AgentContext, target: str, result): ...
    async def on_worker_startup(self): ...
    async def on_worker_shutdown(self): ...
```

#### Loading Plugins

Three ways to provide plugins to `run_worker()`:

```python
# 1. Explicit list
run_worker(MyAgent, plugin_list=[WeatherPlugin()])

# 2. Configurator callback
def setup(registry):
    registry.register_bundle(WeatherPlugin())
run_worker(MyAgent, plugin_configurator=setup)

# 3. Directory scan (startup-time)
run_worker(MyAgent, plugin_dir="./my_plugins")
```

#### Plugin Hot-Reload

Send a `ReloadPluginsCommand` to trigger `reload()` on all plugins without restarting the Worker process. Config snapshots are versioned and persisted to Redis so that even during a restart, the last known-good configuration is recovered.

---

## Advanced Features

### Inter-Agent Calling

A Worker can delegate to another agent via `context.call_agent()`. When `wait_for_reply=True`, the current task **suspends**, the callee runs to completion, and the reply is delivered back to the caller as an `AgentTaskResult`.

The framework handles:
- Task tree construction (parent/child linking for cascade cancel)
- Callback notification when the callee finishes
- Automatic re-delivery of the reply message

### Scatter-Gather Dispatch

Fan out multiple sub-tasks in parallel and collect their results:

```python
group = await context.dispatch_group([
    {"target_agent_type": "researcher", "content": "Find papers"},
    {"target_agent_type": "analyst",    "content": "Summarize findings"},
], wait_for_reply=True)

results = await context.collect_group_results(group["task_group_id"])
for r in results.values():
    print(r["content"])
```

Group progress is tracked in Redis; `dispatch_group` returns immediately with a `task_group_id` that can be polled via `collect_group_results`.

### User-in-the-Loop

Suspend a task and wait for human input:

```python
from by_framework import AskUserEvent, ResumeCommand

async def process_command(self, command, context: AgentContext):
    if isinstance(command, ResumeCommand):
        # This is the user's reply
        await context.emit_chunk(f"You said: {command.content}")
        return {"status": "completed"}

    # Suspend and ask
    return await context.ask_user(
        AskUserEvent(prompt="What is the target deployment environment?")
    )
```

### Service Discovery

Redis-backed service discovery utilities:

| Component | Role |
|---|---|
| `ServiceRegistry` | Register / deregister / heartbeat for services. |
| `DiscoveryClient` | Cached service lookup with round-robin load balancing. |
| `DiscoveryHttpClient` | HTTP client that retries across discovered service nodes on failure. |

---

## Sending Tasks

### GatewayClient

```python
from by_framework import GatewayClient, WorkerRegistry, init_redis

redis = init_redis(host="localhost", port=6379)
client = GatewayClient(redis_client=redis, registry=WorkerRegistry(redis_client=redis))

# Send to an agent type
resp = await client.send_message(
    target_agent_type="my_agent",
    session_id="sess-001",
    content="Your task content",
    user_code="user-123",
    metadata={"priority": "high"},
)

# Cancel a task (including cascade through sub-tasks)
await client.cancel_task(
    message_id=resp.message_id,
    session_id="sess-001",
    reason="User requested",
)
```

### ByaiGatewayClient

A typed wrapper around `GatewayClient` that automatically serializes content through the Byai codec:

```python
from by_framework import ByaiGatewayClient, BaiYingMessage

client = ByaiGatewayClient(redis_client=redis, registry=registry)

# Automatically encodes BaiYingMessage content
resp = await client.send_message(
    target_agent_type="chat_agent",
    session_id="sess-001",
    content=BaiYingMessage(role="user", content="Hello"),
)
```

### Interceptors

Register request interceptors on the client side for custom pre-processing:

```python
class AuthInterceptor:
    async def before_send(self, command, header):
        header.metadata["auth_token"] = "..."
        return command, header

client = GatewayClient(...)
client.add_interceptor(AuthInterceptor())
```

---

## Extension Libraries

These are optional workspace member packages shipping alongside the core framework:

| Package | Purpose | Key Dependency |
|---|---|---|
| `by-framework-trace-langfuse` | Langfuse LLM observability plugin. Auto-discovered at Worker startup if env vars are set. | `langfuse` |
| `by-framework-trace-phoenix` | Arize Phoenix tracing integration. | `phoenix` |
| `by-framework-history-postgres` | Persistent message history in PostgreSQL via `asyncpg`. | `asyncpg` |
| `by-framework-history-byclaw` | Byclaw-specific history backend. | — |
| `by-framework-langgraph` | LangGraph state-graph adapter, worker, and tool bridge. | `langgraph`, `langchain-core` |

Tracing providers are auto-discovered via `TraceProviderFactory` at Worker startup when the corresponding package is installed and environment variables are configured.

---

## Configuration Reference

### `run_worker()` Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `worker_class` | `Type[GatewayWorker]` | *(required)* | Your Worker implementation. |
| `worker_id` | `str` | `"worker-1"` | Unique Worker instance ID. |
| `redis_host` | `str` | `"localhost"` | Redis host. |
| `redis_port` | `int` | `6379` | Redis port. |
| `redis_db` | `int` | `0` | Redis database number. |
| `redis_password` | `str \| None` | `None` | Redis password. |
| `redis_username` | `str \| None` | `None` | Redis username. |
| `redis_max_connections` | `int` | `max_concurrency + 10` | Redis connection pool size. |
| `workspace_dir` | `str` | `"/tmp/gateway-workspace"` | Local workspace root for task isolation. |
| `consumer_group` | `str` | `"agent_engines"` | Redis Streams consumer group name. |
| `max_concurrency` | `int` | `50` | Max concurrent tasks per Worker. |
| `fetch_count` | `int` | `10` | Batch size for Redis `XREADGROUP`. |
| `plugin_list` | `list[Plugin] \| None` | `None` | Explicit plugin instances. |
| `plugin_configurator` | `Callable \| None` | `None` | Callback for programmatic plugin registration. |
| `plugin_dir` | `str \| None` | `None` | Directory scanned for `.py` plugin modules at startup. |
| `plugin_hook_timeout_seconds` | `float \| None` | `None` | Timeout for individual plugin hooks. |
| `plugin_log_hook_stats_on_shutdown` | `bool` | `True` | Log per-hook success/failure stats at shutdown. |

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `BYAI_WORKER_CONCURRENCY` | `50` | Overrides `max_concurrency`. |
| `BYAI_WORKER_FETCH_COUNT` | `10` | Overrides `fetch_count`. |
| `BYAI_REDIS_MAX_CONNECTIONS` | `max_concurrency + 10` | Overrides `redis_max_connections`. |

---

## Development

```bash
# Install all workspace dependencies
make install

# Format (isort + ruff + pyink)
make format

# Lint (pylint + ruff)
make lint

# Run all tests
make test

# Run a single test file
uv run pytest tests/worker/test_gateway_worker.py

# Run tests matching a pattern
uv run pytest -k "test_name_pattern"

# Full CI check
make ci
```

**Code style:** isort for imports, ruff-format + pyink for formatting, pylint + ruff for linting. Pre-commit hooks run automatically on `git commit`.

Tests are organized by module under `tests/`:
- `tests/common/` — Logger, Redis client, config, exceptions
- `tests/core/` — Registry, protocol, discovery
- `tests/worker/` — Worker, runner, context, processor, emitter, sandbox
- `tests/client/` — Client functionality
- `tests/plugin/` — Plugin registry, system, discovery, tracing
- `tests/integration/` — End-to-end flows (scatter-gather, callbacks, ask_user)

---

## Deployment

### Single Machine

```bash
# 1. Start Redis
docker run -d --name by-redis -p 6379:6379 registry:7-alpine

# 2. Start a Worker
python -m by_framework \
  --worker-class my_agent.MyAgent \
  --worker-id worker-01 \
  --redis-host localhost
```

### Horizontal Scaling

Run multiple Worker processes with different `worker_id` values. They all consume from the same agent-type control stream. Redis consumer groups automatically handle load distribution.

```bash
python -m by_framework --worker-class my_agent.MyAgent --worker-id worker-01 &
python -m by_framework --worker-class my_agent.MyAgent --worker-id worker-02 &
python -m by_framework --worker-class my_agent.MyAgent --worker-id worker-03 &
```

### Reliability

- **Message persistence:** Messages are stored in Redis Streams until explicitly acknowledged (`XACK`). Unacknowledged messages are redelivered on Worker restart.
- **Durable config:** Agent config snapshots are persisted to Redis, so a restarted Worker recovers the last-known plugin configuration.
- **Gradual shutdown:** `WorkerRunner` drains in-flight tasks before shutting down, acknowledging completed work.
- **Separate data path:** Data output goes to session-scoped streams independently of control, so backend consumers are decoupled from Worker scaling.

### Logging

```python
from by_framework.common.logger import setup_logging
import logging

setup_logging(level=logging.INFO, use_json=True)  # JSON for log aggregation
```

### Observability Dashboard

Serve the built-in dashboard to inspect worker health, agent health, execution
state counts, recent executions, Redis stream queue depth, consumer-group
pending/lag, failure details, routing decisions, derived alerts, and segmented
queue/run/end-to-end task latency:

```bash
uv run python -m by_framework.observability.dashboard --host 127.0.0.1 --port 8765
```

Open `http://127.0.0.1:8765/`. For a local UI preview without Redis, open
`http://127.0.0.1:8765/?demo=1`. Prometheus-style metrics are available at
`http://127.0.0.1:8765/metrics`, and the dashboard keeps short in-memory trend
history at `http://127.0.0.1:8765/api/history`. The UI uses split polling
endpoints (`/api/workers`, `/api/executions`, `/api/queues`, `/api/history`)
instead of polling the full `/api/snapshot` endpoint on every refresh. Runtime
self-check data is exposed at `/api/health`, shown in the toolbar, and exported
through `/metrics`.
Alert thresholds can be tuned with `--queue-backlog-threshold`,
`--delivery-pending-threshold`, `--consumer-pending-threshold`, and
`--failed-execution-threshold`.

The dashboard frontend is built with React/Vite under
`src/by_framework/observability/frontend`; its production build is packaged in
`src/by_framework/observability/static`.

---

## Roadmap

- [x] Observability dashboard for Worker health and task streams
- [ ] WASM-based sandbox for stronger execution isolation
- [ ] Enhanced LangGraph multi-agent orchestration adapter

---

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## License

Apache 2.0 — see [LICENSE](LICENSE).

---

Maintained by the **byai team**.
