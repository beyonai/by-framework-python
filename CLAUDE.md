# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) and Codex when working with code in this repository. (`AGENTS.md` is a symlink to this file — one copy, no drift between agent ecosystems.)

## North Star

`by-framework-python` lets agent workloads scale horizontally across Redis
Streams without losing execution identity or silently corrupting shared
state under partial failure — a worker restart, Cluster-mode operation, a
misconfigured plugin, or a delayed `ask_user` reply. When a trade-off isn't
covered by an explicit rule, prefer a change that fails loudly (raise,
warn-log) or degrades safely (no-op, skip) over one that could silently
drop, duplicate, orphan, or misroute an execution, a message, or a
heartbeat — this repo's fix-commit history is dominated by exactly that
failure shape.

## Core mental model

1. **Control plane vs. data plane are separate Redis Streams, never
   conflated.** Commands travel on per-agent-type control streams
   (competitive consume via consumer groups); output travels on
   session-scoped data streams. Misrouting one onto the other silently
   breaks delivery.
2. **An execution's identity must survive suspend/resume, not just
   request/response.** A task can suspend (`ask_user`, or a `call_agent`
   hop) and later resume via a `RESUME` dispatch that must reattach to the
   *original* `execution_id` via the registry lookup — treating every
   dispatch as "mint a new execution" silently orphans the suspended one.
   This was the single most-recurring bug class in this repo's history (fix
   90764e1, closing #75/#76/#77).

## Project Overview

`by-framework` is a distributed, high-performance Agent scheduling engine built on Redis Streams. It provides a framework for building AI agents with self-driven orchestration and sandbox isolation capabilities.

## Build Commands

```bash
# Install dependencies
make install

# Format code (isort + ruff + pyink)
make format

# Lint code (pylint + ruff)
make lint

# Run all tests
make test

# Run a single test file
uv run pytest tests/worker/test_gateway_worker.py

# Run tests matching a pattern
uv run pytest -k "test_name_pattern"
```

## Architecture

### Core Data Flow

```
Client → Redis Input MQ (queue:ctrl:{agent_type}) → GatewayWorker
                                                              ↓
                                                       Redis Data MQ (queue:data:stream)
                                                              ↓
                                                         WebSocket Backend
```

### Key Components

| Component | Location | Purpose |
|-----------|----------|---------|
| `GatewayWorker` | `src/by_framework/worker/worker.py` | Abstract base class for workers; implement `get_capabilities()` and `process_command()` |
| `AgentContext` | `src/by_framework/worker/context.py` | Runtime context for task execution; emits chunks, states, artifacts; calls other agents |
| `run_worker()` | `src/by_framework/worker/app.py` | Main entry point for starting a worker |
| `GatewayClient` | `src/by_framework/client/client.py` | Sends commands to Redis Streams |
| `ByaiGatewayClient` | `src/by_framework/client/byai_client.py` | GatewayClient with ByaiMessageInterceptor |
| `Plugin` | `src/by_framework/core/extensions/plugin.py` | Abstract base for extensible plugins with lifecycle hooks |
| `PluginRegistry` | `src/by_framework/core/extensions/registry.py` | Manages plugin registration and discovery |

### Protocol System

Commands and events are defined in `src/by_framework/core/protocol/`:
- `commands.py` - `AskAgentCommand`, `CancelTaskCommand`, `ResumeCommand`
- `events.py` - `StreamChunkEvent`, `StateChangeEvent`, `ArtifactEvent`
- `message_header.py` - `MessageHeader` with session_id, trace_id, message_id

### Plugin Lifecycle Hooks

Plugins can implement: `on_worker_startup`, `on_worker_shutdown`, `on_task_start`, `on_task_complete`, `on_task_error`, `on_task_cancel`

### Redis Key Patterns

- `byai_gateway:ctrl:agent_type:{agent_type}` — Control stream; competitive consume per agent type
- `byai_gateway:ctrl:worker:{worker_id}` — Direct per-worker routing
- `byai_gateway:session:{session_id}:data_stream` — Session-scoped output events
- `byai_gateway:registry:worker:online:{worker_id}` — Heartbeat TTL key
- `byai_gateway:task_group:{group_id}` — Scatter-gather group tracker

Connection precedence and Cluster-mode/key-schema-versioning details: see
the Reference map below, not repeated here.

## Test Structure

Tests are organized by module in `tests/`:
- `tests/common/` - Logger, redis client, config, exceptions
- `tests/core/` - Registry, protocol, history
- `tests/worker/` - Worker, context, processor, sandbox
- `tests/client/` - Client functionality
- `tests/plugin/` - Plugin system and discovery
- `tests/integration/` - Cross-component flows (scatter-gather, callbacks, ask_user)

## Code Style

- **Import sorting**: isort
- **Formatting**: ruff-format + pyink
- **Linting**: pylint (with `pylintrc`) + ruff
- **Testing**: pytest with pytest-asyncio

Pre-commit hooks are configured in `.pre-commit-config.yaml` and run isort, ruff, pylint, pyink, and general checks.

## Development Notes

- Package is at `src/by_framework/` (configured in `pyproject.toml`)
- `pythonpath = ["src"]` is set in pytest config
- Redis 7.0+ is required for Streams functionality
- Worker capabilities are declared via `get_capabilities()` and used for task routing

## Cross-cutting invariants

- **An explicitly-passed kwarg must never be silently discarded because a
  config object's corresponding field defaults to `None`/"unset."** Incident:
  fix 6ec070c — `redis_client.init_redis()` let `config.max_connections`
  (default `None`, meaning "not specified") unconditionally override an
  explicitly-passed `max_connections` kwarg, discarding
  `worker/app.py`'s `max_concurrency`-derived pool size. Correct form:
  `if config.<field> is not None: value = config.<field>` — only let the
  config object win when it was actually set. No dedicated guard yet;
  applies to any new `Optional[...] = None` config field.

- **Plugin/trace hook code must fail soft (log + return/`None`), never raise
  into the primary task-execution or worker-startup path.** Incident: fix
  8b5bf93 — `LangfusePlugin._build_default_tracer()` used to `raise
  RuntimeError` when Langfuse env vars were unset, which would have broken
  worker startup for anyone with the plugin auto-discovered but
  unconfigured; changed to log + return `None`, with every call site guarded.
  Reinforced by `core/extensions/registry.py`'s `_execute_hook()`, which
  wraps every plugin lifecycle hook in try/except + timeout handling so no
  hook exception ever propagates to the caller. Correct form: any new
  plugin/trace integration point must degrade to a no-op (logged) rather
  than raising.

## Iron rules

<!-- No process rules (test invocation, release steps) have caused an
     incident yet — entries land here only via a real trigger (see
     "Maintaining this map" below), never invented up front. -->

## Reference map

| When you're working on... | Read first |
|---|---|
| any source file | `docs/architecture/KEY_FILES.md` — find the file's entry |
| Redis connection setup, cluster-mode, or key-schema versioning (`RedisConfig`, `RedisKeys`, `_get_redis()`, admin-index writes) | `docs/architecture/redis-cluster-mode.md` |
| Worker deployment/production-readiness — README's 部署 section, `__main__.py` CLI flags, `run_worker()`'s signature, or shutdown/signal handling | `docs/architecture/production-deployment.md` |

## Maintaining this map

The map grows only on triggers — never speculatively:

1. **After an incident** — write the postmortem in `docs/incidents/`
   (append-only is legal there), then ask "what grep would have prevented
   this" → a new guard, or an append to an existing one.
2. **After editing a load-bearing file** — rewrite its KEY_FILES entry in
   place. Behaviour changed means the entry changes; never append history.
3. **Same mistake twice** — triage into exactly one layer:
   machine-checkable → guard in `scripts/verify.sh`'s CHECKS;
   cross-cutting → quad bullet above;
   domain detail → docs/ file + a Reference-map row.
4. **Explored a subsystem to do a task and no doc held it** — diarize it into
   `docs/architecture/<x>.md` + a Reference-map row while the understanding is
   hot. Don't make the next agent re-read the same files to learn the same
   thing.
5. **A rule is already known — no incident needed** — someone states a
   requirement up front (deployment steps, a naming convention, whatever).
   Don't wait for it to bite twice: triage it right now, same three-way
   split as every other find. Machine can check it → write a guard per
   `docs/architecture/GUARD_AUTHORING.md`, register it in `verify.sh`'s
   `CHECKS`. Only judgement can check it → a quad-bullet above — **unless
   two or more existing bullets already cover the same concern** (e.g.
   several deployment steps), in which case lift the whole cluster into a
   `docs/` file + Reference-map row instead of adding a third bullet. Same
   move as "Diarize a subsystem" for file entries: one doc per concern
   beats a pile of bullets that only gets noticed at the next size-cap
   surgery. Domain detail from the start → a `docs/` file + a Reference-map
   row, no waiting to accumulate.

Guards: `bash scripts/verify.sh` before every push (kept under 30s).
Growth enforcement: `check-entry-freshness.sh` warns in verify on stale
entries, unindexed new source files, and untriaged fix commits; the Stop
hook (`scripts/map-stop-hook.sh`, wired into `.claude/settings.json` and
`.codex/hooks.json`) blocks session end once until stale entries are
rewritten or declared unchanged.
