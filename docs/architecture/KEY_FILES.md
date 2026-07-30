# Key files — per-file index

On-demand reference, routed from CLAUDE.md's Reference map.
**Read a file's entry before editing that file. After editing, rewrite the
entry in place if behaviour changed.**

Entries describe CURRENT behaviour + load-bearing invariants only. Release
history lives in the changelog + git, never here — enforced by
`scripts/check-doc-discipline.sh`.

**Two doc types share this on-demand layer.** This index holds one entry per
file (single-file invariants). Knowledge about how *several* files interact —
or an invariant that would otherwise repeat across many entries — belongs in a
**subsystem doc** (`docs/architecture/<x>.md`, routed by a Reference-map row),
not smeared across entries. Lift it up when it spans files; see "Diarize a
subsystem".

## Diarize a file

An entry is a **diarization**: read many sources, write one page of judgement.
The move — whether you're seeding this index or the growth guard just flagged a
stale entry — is always the same:

1. **Read three sources**: the file, its tests, and the last ~10 commits that
   touched it (`git log -p -10 -- <file>`). The odd, specific test assertions
   and the fix commits are where invariants hide.
2. **Extract only load-bearing invariants** — what must not break when editing
   this file, precise to the expression level. A recorded incident number
   earns its place. Skip anything the code already states plainly.
3. **Write one entry**: a one-line role, then the invariants. **No feature
   lists** — features are legible from the code; invariants are not.

The discipline is subtractive: if a sentence describes what the file *does*
rather than what must *hold*, cut it.

Entry anatomy:

- `src/path/file.ts` — one-line role. Load-bearing invariants, precise to
  the expression level (e.g. "`ctx.remote === false` for trusted-only
  sites"): what must not break when editing this file. No feature lists —
  the code already says what it does.

## Entries

- `src/by_framework/worker/app.py` — Worker bootstrap (`run_worker`/`_run_worker_async`):
  resolves Redis config, wires plugins, starts the runner. Redis-connection
  precedence (see [[redis-cluster-mode]]) must be replicated here identically
  to `common/config.py` and `admin/cli.py`. `max_concurrency + 10` is the
  default connection-pool size when neither `redis_max_connections` nor
  `BYAI_REDIS_MAX_CONNECTIONS` is set — don't decouple pool sizing from
  concurrency without updating both. `_build_auto_trace_plugin()` must raise
  if more than one `by_framework_trace_*` provider factory activates from
  env — silently picking one would hide a misconfiguration. `close_redis()`
  must stay in the `finally` block (including on `asyncio.CancelledError`)
  so restarts don't leak the connection pool. `health_port` (readiness
  endpoint, see [[worker-readiness-endpoint]]) is opt-in only — unlike
  `max_concurrency`/`fetch_count`, its `BYAI_WORKER_HEALTH_PORT` env-var
  fallback must never resolve to a default port number; leave it `None`
  when unset so no port opens for deployments that never asked for one.

- `src/by_framework/common/config.py` — `RedisConfig`/`WorkerConfig`/`LoggingConfig`
  env-loaded dataclasses. `RedisConfig.from_env()`'s cluster-mode/key-schema
  precedence must stay mirrored across files — see [[redis-cluster-mode]].
  `REDIS_DB` must keep working as a deprecated fallback (with a warning) for
  `REDIS_DATABASE` — don't remove until deprecation is done.
  `RedisConfig.max_connections` must stay `Optional[int] = None` (meaning
  "unset") — `redis_client.init_redis()` distinguishes "unset" from an
  explicit value; defaulting it to a concrete int would silently discard a
  caller's explicit `max_connections` kwarg (fix 6ec070c).
  `WorkerConfig.heartbeat_lease_ttl_seconds` defaults to 30s = 6x the 5s
  heartbeat interval (`RedisKeys.WORKER_DEFAULT_LEASE_TTL_SECONDS`) — this
  margin is a deliberate second line of defense against event-loop stalls;
  don't shrink one without the other.

- `src/by_framework/admin/cli.py` — `by-admin` Typer CLI for cluster ops
  (worker list/suspend/evict, type deny/allow, metrics snapshot). `_get_redis()`
  must replicate the SDK-wide Redis resolution order — see
  [[redis-cluster-mode]]. The module-global `_redis_url` must be assigned
  unconditionally in `_global()` (not only `if redis_url:`), so a prior CLI
  invocation's URL doesn't leak into a later one that didn't pass
  `--redis-url`. `--help` text literally contains `REDIS_MODE=cluster` /
  `REDIS_CLUSTER_NODES` / `REDIS_KEY_SCHEMA_VERSION=v2` and is pinned by
  `test_help_mentions_cluster_env_configuration` — keep help text and actual
  precedence logic in sync.

- `src/by_framework/common/constants.py` — Central Redis key/naming registry
  (`RedisKeys`), key-schema versioning (v1/v2), core timing constants. Every
  key factory must route through `_versioned()`, and every SCAN-based
  enumeration must use the paired `_worker_scan_pattern()` /
  `_worker_id_from_scanned_key()` helpers — see [[redis-cluster-mode]].
  `get_key_schema_version()`'s precedence deliberately does *not* infer v2
  from `REDIS_MODE=cluster` alone — must stay mirrored with
  `RedisConfig.from_env()`'s mode precedence. Cross-entity index keys
  (`admin_workers()`, `trace_index_session/worker/agent`) are deliberately
  left *untagged* relative to the per-entity keys they index — never share a
  Cluster hash tag with them (fix 8501407). `WORKER_DEFAULT_LEASE_TTL_SECONDS
  = 30` must stay ~6x `WORKER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 5`.

- `src/by_framework/client/client.py` — `GatewayClient.send_message()` and
  friends; publishes commands to Redis control streams and drives registry
  execution-tracking as a side effect. On a `RESUME` dispatch, must look up
  the original execution via `registry.get_execution_by_message_id(message_id,
  session_id=...)`, reuse *that* `execution_id`, and skip
  `initialize_execution()` for it — calling `initialize_execution()`
  unconditionally silently detaches the `ResumeCommand` from the suspended
  `WAITING_USER` execution it's meant to continue, orphaning it (fix
  90764e1, #75/#76/#77). The registry lookup must stay guarded with
  `hasattr(registry, "get_execution_by_message_id")` so registry doubles/older
  implementations fall back to minting a fresh execution_id. Root-dispatch
  trace writes (`_write_trace_root_start/_end`) must only fire when
  `not parent_message_id` — firing them on every `call_agent` hop would
  duplicate trace roots.

- `src/by_framework/worker/runner.py` — `WorkerRunner`, the consume loop:
  `XREADGROUP` fetch, command dispatch, resume/suspend bookkeeping, denylist
  enforcement. `_active_agent_type_streams()` must read only the in-memory
  `self._denied_agent_types` frozenset — no Redis `SISMEMBER` call inside the
  hot consume-loop path; refreshed only by the heartbeat thread's
  `denylist_refresh` callback (bounded staleness ~1 heartbeat interval) (fix
  8f23c78). The frozenset must be swapped by whole-reference reassignment,
  relying on CPython GIL atomicity across the heartbeat thread and the async
  loop — never mutate the set in place without adding a lock. A
  `ResumeCommand` that fails to resolve to an existing execution must log a
  warning — silently starting a disconnected new execution is the exact
  failure mode this log surfaces (fix 90764e1, #77). Terminal-state
  replay-skip logic is coupled to `ResumeCommand` handling: skip replaying an
  execution already in a terminal state *unless* the command is a
  `ResumeCommand`. `_health_server` (see [[worker-readiness-endpoint]]) must
  start before any other step in `start()` (currently first line of the
  `try:` block) so a probe hitting the port during startup gets an honest
  `starting` 503 instead of connection-refused, and must `stop()` as the
  *last* step of `_shutdown()` — after every other teardown step, not
  before — so `/readyz` stays reachable (reporting `draining`) for the
  entire drain. `self._draining = True` must stay the first line of
  `_shutdown()`, ahead of every other teardown step, not just ahead of the
  health-server stop.

- `src/by_framework/worker/health_server.py` — `WorkerHealthServer`: the
  `/readyz` readiness HTTP endpoint, on its own daemon thread (mirrors
  `heartbeat.py`'s "don't share the main event loop" pattern — see that
  file's own docstring). Full design record, including why this exists and
  the hard rule against ever wiring it to a liveness check:
  [[worker-readiness-endpoint]]. `_compute_reason()`'s check order is the
  entire contract — `starting > draining > evicted > suspended >
  consumer_stalled > serving`, first match wins; reordering these checks
  silently changes what an operator is told during a real incident. All
  Worker state is read via constructor-injected callables (`has_started`,
  `is_draining`, `admin_lifecycle`, `consumer_healthy`) — this class must
  never reach into `WorkerRunner` directly, which is what keeps it testable
  standalone against fake state (see `tests/worker/test_health_server.py`).

- `src/by_framework/core/registry.py` — `WorkerRegistry`: Redis-backed worker
  membership/heartbeat/execution-state, admin lifecycle, locking primitives.
  `mark_execution_finished()` must stamp `finished_at` only
  `if is_terminal_state(status)` — stamping it unconditionally makes a
  suspended `WAITING_USER` execution look completed to
  `metrics/snapshot.py`'s latency/`completed_count` math (fix 90764e1, #76).
  `heartbeat_worker()` uses an atomic Lua CAS script with token-mode (verify
  stored token before overwrite) and legacy no-token mode — must not be
  replaced by a plain `SET`; return codes `1`=success / `0`=owned-by-another /
  `-1`=unparseable-legacy are relied on by callers. `_RELEASE_LOCK_SCRIPT` /
  `_REFRESH_LOCK_SCRIPT` are Redlock-style token-verified delete/expire — must
  stay atomic, and an empty-string token means "unconditional" (no-token
  legacy mode). `set_worker_admin_state`/`clear_worker_admin_state`: the
  per-worker `worker_admin(id)` hash write must complete independently of the
  `admin_workers()` global-index update — see [[redis-cluster-mode]].

- `src/by_framework/core/protocol/responses.py` — `SendMessageResponse` /
  `CancelTaskResponse` / `CancelSessionResponse` frozen dataclasses +
  `ExecutionStatus` string constants. `ExecutionStatus` string values
  (`"SUCCESS"`, `"NOT_FOUND"`, `"WORKER_NOT_ONLINE"`, etc.) are a wire-level
  contract matched by literal string elsewhere in the client and its tests —
  renaming a value is a cross-file breaking change.
  `ERR_AGENT_TYPE_NOT_FOUND = ERR_AGENT_TYPE_UNAVAILABLE` is a deliberate
  alias; both names must keep resolving to the same string. Response
  dataclasses are `@dataclass(frozen=True)` — don't drop `frozen` or add
  mutable defaults.

- `src/by_framework/core/protocol/content_type.py` — `SseMessageType` /
  `SseReasonMessageType` enums: numeric string codes for SSE messages sent to
  the frontend/other-language SDKs. These codes are an external protocol
  contract — once shipped, a code's meaning must never change; only append
  new codes. `SseReasonMessageType.think_text = "1002"` intentionally reuses
  `SseMessageType.text`'s value — looks like a copy-paste bug but is
  deliberate protocol code reuse; verify against frontend/other-language SDKs
  before "fixing".
