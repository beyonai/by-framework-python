# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

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

- `queue:ctrl:{agent_type}` - Control stream for agent commands
- `queue:data:stream` - Data stream for events/chunks
- `worker:active:{worker_id}` - Worker heartbeat registration

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
