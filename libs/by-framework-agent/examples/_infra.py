"""Self-contained infra for running these examples with no real Redis and no
real LLM API key.

Real deployments run `NativeAgentWorker` behind `WorkerRunner` against a real
Redis Streams cluster, with `LiteLLMModelClient` calling a real provider. To
keep these examples runnable offline in one process, this module provides:

- `InMemoryRedis` — enough of the Redis surface (`xadd`, `pipeline`, hashes,
  sets, strings) for `AgentContext`/`call_agent`/`call_agents` to work
  unmodified, backed by plain Python dicts instead of a real server.
- `Dispatcher` — a tiny in-process stand-in for the "competitive consume
  across worker processes" that Redis Streams normally provides: it drains
  whatever `AskAgentCommand`/`ResumeCommand`s workers dispatched onto control
  streams and routes each to the right registered worker, tracking which
  `execution_id` a resume belongs to exactly the way `WorkerRunner` does.

None of this is part of the `by_framework_agent` public API — it exists only
to make these examples runnable top-to-bottom with `python examples/NN_*.py`.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any

from by_framework import RunningExecution
from by_framework.common.constants import RedisKeys
from by_framework.core.protocol.commands import (
    GatewayCommand,
    ResumeCommand,
    command_from_dict,
)


class _Pipeline:

    def __init__(self, redis: "InMemoryRedis"):
        self._redis = redis
        self._ops: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def xadd(self, stream: str, fields: dict[str, Any], **kwargs: Any) -> "_Pipeline":
        self._ops.append(("xadd", (stream, fields), kwargs))
        return self

    def expire(self, key: str, seconds: int) -> "_Pipeline":
        self._ops.append(("expire", (key, seconds), {}))
        return self

    def hset(self, key: str, *args: Any, **kwargs: Any) -> "_Pipeline":
        self._ops.append(("hset", (key, *args), kwargs))
        return self

    def rpush(self, key: str, value: str) -> "_Pipeline":
        self._ops.append(("rpush", (key, value), {}))
        return self

    async def execute(self) -> list[Any]:
        results = []
        for name, args, kwargs in self._ops:
            results.append(await getattr(self._redis, name)(*args, **kwargs))
        self._ops = []
        return results


class InMemoryRedis:
    """Just enough Redis to run a NativeAgentWorker without a real server."""

    def __init__(self) -> None:
        self.strings: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.sets: dict[str, set] = {}
        self.lists: dict[str, list[str]] = {}
        self.sorted_sets: dict[str, dict[str, float]] = {}
        self.streams: dict[str, list[dict[str, Any]]] = {}

    async def xadd(self, stream: str, fields: dict[str, Any], **_kwargs: Any) -> str:
        self.streams.setdefault(stream, []).append(dict(fields))
        return f"{len(self.streams[stream])}-0"

    async def rpush(self, key: str, value: str) -> int:
        self.lists.setdefault(key, []).append(value)
        return len(self.lists[key])

    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        self.sorted_sets.setdefault(key, {}).update(mapping)
        return len(mapping)

    def pipeline(self) -> _Pipeline:
        return _Pipeline(self)

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        del ex
        self.strings[key] = value

    async def get(self, key: str) -> str | None:
        return self.strings.get(key)

    async def delete(self, key: str) -> None:
        self.strings.pop(key, None)
        self.hashes.pop(key, None)
        self.sets.pop(key, None)

    async def sadd(self, key: str, *values: str) -> None:
        self.sets.setdefault(key, set()).update(values)

    async def smembers(self, key: str) -> set:
        return set(self.sets.get(key, set()))

    async def hset(
        self,
        key: str,
        field: str | None = None,
        value: Any = None,
        mapping: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        bucket = self.hashes.setdefault(key, {})
        if field is not None:
            bucket[field] = value
        bucket.update(mapping or kwargs)

    async def hget(self, key: str, field: str) -> str | None:
        return self.hashes.get(key, {}).get(field)

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    async def hincrby(self, key: str, field: str, amount: int = 1) -> int:
        current = int(self.hashes.get(key, {}).get(field, 0)) + amount
        self.hashes.setdefault(key, {})[field] = str(current)
        return current

    async def expire(self, key: str, seconds: int) -> bool:
        del key, seconds
        return True

    def mark_agent_type_online(self, agent_type: str, worker_id: str) -> None:
        """Seed the availability keys AvailabilityRouter checks, so
        call_agent/call_agents treat `agent_type` as deliverable."""
        self.sets.setdefault(RedisKeys.agent_type_members(agent_type), set()).add(
            worker_id
        )
        self.strings[RedisKeys.worker_online_lease(worker_id)] = "1"


@dataclass
class Dispatcher:
    """Routes dispatched commands to registered workers, in-process.

    Stands in for "any of N worker processes might pick this up" — the real
    property being demonstrated (a suspended loop resumes correctly
    regardless of which worker instance handles the reply) still holds here:
    each drained command gets a *fresh* worker instance constructed for it.
    """

    redis: InMemoryRedis
    worker_factories: dict[str, "WorkerFactory"] = field(default_factory=dict)
    _execution_by_message_id: dict[str, str] = field(default_factory=dict)

    def register(self, agent_type: str, factory: "WorkerFactory") -> None:
        self.worker_factories[agent_type] = factory

    async def dispatch_root(
        self, command: GatewayCommand, execution_id: str | None = None
    ) -> Any:
        """Feed the very first command into its target worker directly."""
        execution_id = execution_id or f"exec-{uuid.uuid4().hex[:8]}"
        self._execution_by_message_id[command.header.message_id] = execution_id
        worker = self.worker_factories[command.header.target_agent_type]()
        return await worker._handle_message(  # pylint: disable=protected-access
            command,
            execution=RunningExecution(
                execution_id=execution_id,
                message_id=command.header.message_id,
                session_id=command.header.session_id,
                worker_id=f"worker-{uuid.uuid4().hex[:6]}",
                task=None,  # type: ignore[arg-type]
                cancel_event=None,  # type: ignore[arg-type]
            ),
        )

    async def drain(self) -> list[tuple[str, Any]]:
        """Process every dispatched command until no new ones remain.

        This is the offline stand-in for N worker processes competitively
        consuming control streams — here it's just a queue we drain to a
        fixed point, routing each command to whichever worker owns its
        target_agent_type and reattaching it to the right execution_id.

        Returns every ``(agent_type, result)`` processed along the way, in
        order, so a caller can find e.g. the orchestrator's final answer
        once its Group Join fires.
        """
        processed: list[tuple[str, Any]] = []
        while True:
            pending = self._pop_all_pending()
            if not pending:
                return processed
            for _, raw in pending:
                command = command_from_dict(json.loads(raw["data"]))
                result = await self._route(command)
                processed.append((command.header.target_agent_type, result))

    _CTRL_STREAM_PREFIX = "byai_gateway:ctrl:"

    def _pop_all_pending(self) -> list[tuple[str, dict[str, Any]]]:
        """Only control streams carry GatewayCommands — session data streams
        carry StreamChunkEvent/AskUserEvent wire payloads, not commands."""
        drained: list[tuple[str, dict[str, Any]]] = []
        for stream_name, entries in list(self.redis.streams.items()):
            if not stream_name.startswith(self._CTRL_STREAM_PREFIX):
                continue
            while entries:
                drained.append((stream_name, entries.pop(0)))
        return drained

    async def _route(self, command: GatewayCommand) -> Any:
        target_agent_type = command.header.target_agent_type
        factory = self.worker_factories.get(target_agent_type)
        if factory is None:
            raise RuntimeError(f"No worker registered for {target_agent_type!r}")

        if isinstance(command, ResumeCommand):
            execution_id = self._execution_by_message_id.get(command.header.message_id)
            if execution_id is None:
                raise RuntimeError(
                    "Dispatcher has no execution_id on file for resume "
                    f"message_id={command.header.message_id!r} — a real "
                    "WorkerRunner would resolve this via the registry."
                )
        else:
            execution_id = f"exec-{uuid.uuid4().hex[:8]}"
            self._execution_by_message_id[command.header.message_id] = execution_id

        worker = factory()
        return await worker._handle_message(  # pylint: disable=protected-access
            command,
            execution=RunningExecution(
                execution_id=execution_id,
                message_id=command.header.message_id,
                session_id=command.header.session_id,
                worker_id=f"worker-{uuid.uuid4().hex[:6]}",
                task=None,  # type: ignore[arg-type]
                cancel_event=None,  # type: ignore[arg-type]
            ),
        )


WorkerFactory = (
    Any  # Callable[[], GatewayWorker], typed loosely to dodge a generic import cycle
)
