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
  Cluster hash tag with them (fix 8501407); `wait_index()` is one of them.
  `WORKER_DEFAULT_LEASE_TTL_SECONDS = 30` must stay ~6x
  `WORKER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 5`. `WAIT_INDEX_SHARDS` is a
  cross-SDK protocol constant, not a tunable — changing it re-maps every
  session to a different shard, so entries written before the change are
  swept by nobody. `DEFAULT_ASK_USER_TIMEOUT_MS` (machine waiting on a
  human, sized off `DEFAULT_SESSION_TTL`, which is in *seconds*) must stay
  decoupled from `DEFAULT_REPLY_TIMEOUT_MS` (machine waiting on machine) —
  one shared value either kills a human's turn or lets a hung callee sit.
  `LivenessErrorCode` values are wire contract: append only, never rename.
  `CLIENT_SOURCE_AGENT_TYPE` is the marker a client stamps on an execution
  record it dispatched (TS writes the same string; Java writes no field at
  all) — it is a sentinel, not an agent type, and every reader that treats a
  record's `source_agent_type` as somewhere to send a reply must exclude it.
  `wait_consumed()` is the idempotency gate's "already resolved" marker;
  `WAIT_CONSUMED_TTL_SECONDS` bounds how far apart two copies of one reply
  may be and still be recognized as duplicates, and is sized off
  `DEFAULT_SESSION_TTL` — the marker must outlive every wait it may have to
  arbitrate, and the longest is `ask_user`'s, whose deadline *equals* the
  session TTL. Sizing it shorter (it was `TASK_GROUP_TTL_SECONDS`) reopens
  two holes: a repeated user answer stops being a duplicate, and — worse — a
  stale duplicate sub-agent reply that loses its own marker falls through to
  the `ask_user` candidate for the same caller and claims a wait that is
  still live, after which the real answer is dropped. `wait_sweep_lock()`
  guards one wait-index shard while it is swept — see
  [[suspend-resume-liveness]] for why its ownership is advisory rather than
  a leader election, and why `WAIT_SWEEP_LOCK_TTL_SECONDS` must outlast one
  shard's pass. `wait_renew_origin()` remembers a wait's *original* deadline
  across the renewals that overwrite it; `WAIT_RENEW_MAX_MULTIPLE` is the
  budget measured from it, and its TTL must stay well above the largest
  `N * timeout` in use or the budget silently restarts mid-wait.
  `WAIT_PRUNE_AFTER_SECONDS` is how far in the past an entry's score must
  lie before the sweep's prune half deletes it unexamined; it must stay
  *strictly* above `DEFAULT_SESSION_TTL`, because `DEFAULT_ASK_USER_TIMEOUT_MS`
  equals that exactly and a threshold trimmed to it would sit on the boundary
  of a live ask_user wait.

- `src/by_framework/core/wait_index.py` — Pure codec for the wait-index ZSET
  member (`{session_id}|{parent_message_id}|{child_message_id}|{task_group_id}`)
  plus shard selection. Every field must remain derivable from a single
  `ResumeCommand` — `member_from_resume()` is what lets the idempotency gate
  `ZREM` before any registry lookup, so a field a reply doesn't carry can
  never be added. That function also *reverses* the header ids (a reply's
  `header.message_id` is the caller's, its `header.parent_message_id` is the
  sub-task's); keying by the reply's own `message_id` collapses every Task
  Group sibling onto one member. `session_id`/`message_id` are
  caller-controlled, so `|` and `\` are escaped rather than assumed absent.
  `wait_index_shard()` must stay a fixed, language-portable hash (FNV-1a
  32-bit) — Python's `hash()` is per-process salted, and TS/Java sweepers
  must land on the same shard; `member_digest()` (SHA-1 hex) is the same deal
  for every key named after a member. Keep this module I/O-free.

- `src/by_framework/core/wait_gate.py` — The idempotency gate: `ZREM`s the
  caller's wait-index entry so exactly one copy of a reply wakes it. Its
  whole difficulty is that `ZREM` returning 0 conflates "someone already
  claimed this wait" with "this wait was never registered" (a pre-upgrade
  dispatch, or an expired entry) — hence the `wait_consumed` marker written
  by the winner, which is the *only* thing that separates them. Treating an
  unmarked 0 as a duplicate drops every in-flight reply during a rolling
  upgrade. Both this and the surrounding `except` must keep failing **open**:
  a duplicated wake-up is recoverable, a dropped reply is permanent silence,
  so any doubt (Redis error, missing marker) allows the message. Concurrent
  copies are arbitrated by `ZREM`'s atomicity, not by the marker; the marker
  only matters for copies separated in time, which is why the write-after-
  claim window is not worth closing. `candidate_members()` must resolve each
  candidate fully (claim, then check *its* marker) before trying the next:
  the second candidate exists only because `ask_user` registers with an empty
  `child_message_id` while its reply carries a client-chosen
  `parent_message_id`, and falling through to it early would clear a live
  ask_user wait belonging to the same caller. `emit_orphaned_reply()` is
  observability for work that will now be thrown away — fail-soft, and never
  able to change the drop/allow decision.

- `src/by_framework/core/wait_sweeper.py` — `WaitIndexSweeper`: the only
  thing that can move a caller whose reply is never coming, since a
  suspended caller has *ended* and left no timer behind. Subsystem context
  and the full triage table: [[suspend-resume-liveness]]. It must never
  `ZREM` the entry it acts on — the synthesized reply and a late real one
  are meant to compete for the same entry in `wait_gate.py`, and clearing it
  here forces the synthesized copy to bypass the gate, i.e. a second
  ungated wake-up path. A callee that is itself suspended must be *renewed*,
  never failed: the innermost wait has the earliest deadline by
  construction, so failure climbs a chain hop by hop instead of collapsing
  it — and that is also why such a callee is exempt from the renewal
  ceiling, which arrives *earliest* for the outermost wait and would fail a
  chain top-down. A live worker lease buys more time (that is what keeps
  slow work from being killed) but only up to
  `registered_at + WAIT_RENEW_MAX_MULTIPLE * timeout`, after which the callee
  is failed with `CHILD_TIMEOUT`: a lease says the process is up, not that
  the work is moving, so renewing on it alone hangs the caller forever. The
  budget must be re-derived from the saved `wait_renew_origin`, never from
  the score a renewal just wrote, and never from whether the callee's
  `updated_at` advanced — a legitimate long model call stands just as still
  as a deadlock. A Task Group orphan gets the *same* synthesized reply,
  carrying the group id, so the existing join counts it; this file must
  never write `task_group_results`/`completed` itself, or the copy that
  reaches `total` leaves no reply to run the join. A caller that is already
  terminal gets its entry cleaned up
  and *no* reply — registration happens before the dispatch `xadd`, so a
  failed `xadd` leaves exactly that. Synthesized failures must stay shaped
  like a sub-agent's own failure (`error`/`error_code` in `reply_data`,
  same stream, same header id reversal). Two switches, and they must stay
  separate: *compensation* (the triage, replies, renewals, cancellation) is
  off unless `BY_FRAMEWORK_WAIT_SWEEPER_ENABLED` is set and is the rollback
  switch for the whole liveness feature, while *pruning* is on by default
  (`BY_FRAMEWORK_WAIT_PRUNE_ENABLED`) because nothing else ever removes a
  wait-index entry — with compensation off, every call whose reply never
  arrives would leak one forever into a shard ZSET that is shared across
  sessions and so cannot carry a TTL. Pruning may stay unguarded only because
  it decides nothing: it decodes no member and reads no execution record,
  just one `ZREMRANGEBYSCORE` whose bound is a proof (every writer sets the
  score to its own clock plus a non-negative offset, and only while the
  caller's execution record exists, so an old score means the session
  registry is gone and triage could reach nothing but "caller missing").
  That argument covers renewed entries too — a renewal *raises* the score.
  `CHILD_TIMEOUT` — and only it, since the
  other outcomes have no live process on the other end — also asks the callee
  to stop, by delegating wholesale to `GatewayClient.cancel_task` (which
  targets `worker_ctrl_stream(worker_id)`; a cancel on the agent type's
  competitive stream is claimed by an arbitrary worker whose in-memory table
  has no such execution, so it records a cancellation while cancelling
  nothing). That request must stay strictly *after* the reply is emitted and
  strictly swallowed on failure: cancellation is cooperative, so a callee
  wedged in a blocking call is both why the ceiling fired and the case it
  cannot reach — nothing in the wake-up may depend on it. It also does not
  silence the callee, whose own `CANCELLED` reply is dropped by the gate.
  Opt out with `BY_FRAMEWORK_WAIT_CANCEL_ON_TIMEOUT`; per-call opt-out is
  deliberately not offered (see the file's own note — the member is a wire
  format, so it would cost a side-key write on every dispatch). Fail-soft
  throughout: it runs inside every worker, so a raise here must not take the
  worker down, and one bad entry must not abort a shard.

- `src/by_framework/core/wait_reply.py` — The single construction of a reply
  built by someone other than the callee that owed it: the sweeper's
  stand-ins and `call_agents`' compensation for a sub-task whose target agent
  type was never available. Two spellings of that message drift, and the
  drift only shows up as a hung caller, which is why neither site builds it
  itself. `header.message_id` must be the caller's id (what the runner
  reattaches by) and `header.parent_message_id` the sub-task's (what the join
  keys by and what `wait_index.member_from_resume` rebuilds); the failure
  detail must ride in `reply_data`, exactly where a sub-agent that ran and
  raised puts it, so no caller can tell "failed" from "never got to fail".
  Provenance (`synthesized_by`, `liveness_error_code`) goes on
  `header.metadata` and must stay out of `reply_data` for the same reason.
  `flush_pending_group_replies()` is called only after a handler returns
  normally — inline delivery would put a reply on the caller's own control
  stream strictly before it suspends, making a rare race certain, and a
  handler that raised has already aborted its group.

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
  health-server stop. `is_resumed_execution` infers "already been through a
  worker" from `existing_execution["status"] != QUEUED`, so QUEUED must stay
  the *only* status an execution can hold before its first pickup — a
  suspended caller persists as `WAITING_AGENT`/`WAITING_USER` precisely to
  keep that inference true; reusing QUEUED for any post-pickup state silently
  makes a resume re-derive its identity from the message header instead of
  the record. Every `ResumeCommand` passes the `core/wait_gate.py`
  idempotency gate *here*, before the execution lookup — and being upstream
  of `GatewayWorker` is what also puts it before Task Group join, whose
  `HINCRBY completed` a duplicate would push past `total` and aggregate a
  second time. A dropped reply is acked and reported, never left pending.
  Background tasks (`MetricsCollector`, `WaitIndexSweeper`) are started
  best-effort — a background component that fails to construct must never
  prevent the worker from consuming — and every one of them must be
  cancelled and awaited in `_shutdown()`, or shutdown hangs on it.

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

- `src/by_framework/worker/worker.py` — `GatewayWorker`: per-message lifecycle
  (`_handle_message`), Task Group join, and the agent-return reply.
  `_enqueue_agent_return()` builds the *only* thing that resumes a suspended
  caller — its header must keep `message_id` = the caller's
  `parent_message_id` and `parent_message_id` = this sub-task's own
  `message_id`; Group Join keys `task_group_results` by the latter because
  it's the only per-sibling-unique value (fix 9d4a0a4). Group Join must stay
  the single accounting path: a result written or `completed` incremented
  anywhere else can be the increment that reaches `total`, leaving no reply
  to wake the caller (fix 55c7e6f; a dispatch-time failure must instead emit
  the `FAILED` reply a sub-agent would have sent).
  `_persist_single_call_result()` covers the non-group path only (early-return
  when `header.task_group_id` is set, or the group's own write is duplicated)
  and must stay fail-soft — a persist error costs recoverability, never the
  reply. Its stored payload must stay field-for-field isomorphic with the
  `result_data` the join path writes; both are read by the same recovery
  code. `should_emit_stream_end` reads `context._is_suspended` /
  `_permission_transferred`, so those flags' accuracy in `context.py` is
  load-bearing here.
  Who gets replied to comes from `_resolve_reply_command()`, never from a
  resume's own header: a `ResumeCommand`'s `source_agent_type` /
  `parent_message_id` / `task_group_id` describe the *sub-agent* that just
  finished, so a resumed execution must rebuild its caller from the execution
  record `initialize_execution()` wrote (`existing_data`). Treating "is a
  resume" as "has no caller" drops the middle link's result in any chain of
  depth >= 3. That record names `CLIENT_SOURCE_AGENT_TYPE` for anything a
  *client* dispatched, which is a marker and not an agent type — it must be
  excluded explicitly, or every root execution that ever resumes (an
  `ask_user` round is the common one) posts its result to a control stream
  nobody consumes and, worse, stops emitting the end-of-stream event the user
  is waiting on, because it now believes it owes an agent a reply.
  The success path must NOT reply while `context._is_suspended` — a suspended
  execution has no result, and forwarding the value the handler returned in
  order to unwind both wakes the caller early and consumes the single reply it
  was waiting for. "Suspended" here must carry the same terminal-status
  exception as `_apply_suspended_status()`: a handler that returned a terminal
  status is recorded finished and will never resume to reply later, so it owes
  its caller a reply now. The `CancelledError`/`Exception` paths must reply
  anyway: a dead execution will never resume to produce one.
  `_apply_suspended_status()` overwrites a non-terminal business status with
  `context._suspended_state`, so what lands in the registry is
  `WAITING_AGENT`/`WAITING_USER` rather than the handler's placeholder; a
  terminal status always wins.
  `flush_pending_group_replies()` runs immediately after `process_command`
  returns and nowhere else: the stand-ins it delivers belong to a group whose
  caller must already have finished suspending, and a handler that raised has
  aborted its group, so its queued stand-ins must die with it.

- `src/by_framework/worker/context.py` — `AgentContext`: the agent-facing
  runtime surface; `_dispatch_single_task()` is the one dispatch path behind
  both `call_agent` and `call_agents`. `_is_suspended` /
  `_permission_transferred` are flipped *before* the availability check, so
  every early return that means "nothing was dispatched" must restore the
  values captured on entry — restoring to `False` instead is wrong, since an
  earlier `call_agent` on the same context may legitimately have suspended
  it. An availability rejection returns a `FAILED` result dict, it does not
  raise; `call_agents` depends on that to keep fanning out. It must
  compensate such a member with a *reply* (`core/wait_reply.py`, queued on
  `_pending_group_replies` for the worker to flush after `process_command`
  returns) and must never write `task_group_results`/`HINCRBY completed`
  itself: that is a second implementation of the join's accounting, and when
  its increment is the one that reaches `total` no reply is left to run the
  join and the caller hangs forever — reachable both when every target is
  offline and when a sibling's reply is joined mid-fan-out. It also registers
  a wait entry for the member it could not dispatch, so an undelivered
  stand-in is still compensable by a sweep. On a genuine dispatch exception
  mid-batch, `call_agents` must mark the group `aborted` before re-raising,
  or already-sent siblings' replies resume a caller that is already dead;
  queued stand-ins die with the raise for the same reason. `call_agents`
  rejects `message_id` outright for more than one task: a group's per-sibling
  identity *is* its sub-task message_id (it keys both `task_group_results` and
  the wait index), so pinning one across the fan-out overwrites every
  sibling's result and leaves the gate one entry to claim, hanging the caller
  short of `total`. It fails loudly rather than silently minting ids, and the
  single-task use — where the id collides with nothing — is unaffected.
  Every `wait_for_reply=True` dispatch registers one wait-index entry via
  `_register_wait()` (one per sub-task for a group, so each can be resolved
  independently), keyed by `parent_message_id` = the id the awaited reply
  carries as `header.message_id`; getting that direction wrong makes the
  entry unmatchable by the reply that should clear it. `ask_user` registers
  with an empty `child_message_id` (no sub-task) and its own, much larger
  timeout — sharing `call_agent`'s would time out a human. Because that
  member repeats across consecutive ask_user rounds, registering also clears
  the previous round's `wait_consumed` marker, or the gate would read round
  2's answer as round 1's duplicate. Registration is fail-soft and must stay
  so: it is bookkeeping, not the dispatch.
  `initialize_execution()`'s payload carries `source_agent_type` and
  `task_group_id` because they are the only durable record of who a
  suspended callee owes its reply to (see `worker.py`).
  `_suspended_state` records *which* state the execution is waiting in and is
  what the framework persists; keep it set/rolled-back in lockstep with
  `_is_suspended`.

- `src/by_framework/worker/processor.py` — `GatewayProcessor`: the standalone
  message-lifecycle path for callers that don't subclass `GatewayWorker`. It
  duplicates `worker.py`'s reply logic, so every reply-side invariant there
  applies here too and the two must be changed together — the shape has
  already drifted once and taken the same bug twice. Specifically:
  `_enqueue_callback()`'s `header.message_id` must be the *caller's*
  message_id (this dispatch's `parent_message_id`), since that is what the
  caller's suspended execution is reattached by; a freshly minted id resolves
  to no execution. `_resolve_reply_header()` must read a resumed execution's
  caller from the execution record, not from the resume header (which names
  the sub-agent), while excluding `CLIENT_SOURCE_AGENT_TYPE` for the reason
  spelled out under `worker.py`; and no callback may be sent while
  `context._is_suspended` (with the same terminal-status exception).
  Being a *second* entry point for replies, it carries the same
  `core/wait_gate.py` gate as `runner.py` — a gate on one of two doors is not
  a gate, and an ungated reply here would both wake a resolved caller and
  leave its wait-index entry behind for a sweep to resolve all over again.
  `process()` returns `None` for a reply it drops. It also flushes
  `call_agents`' queued stand-ins on the same terms as `worker.py` — after
  the handler returns, never when it raised.

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
  legacy mode). Those two scripts are also the shared Redlock primitives
  behind `acquire_scoped_lock()`/`release_scoped_lock()` (used for wait-index
  shard claims), so the value written must stay a cjson-decodable object
  carrying a `token` field — a bare token string parses as unparseable legacy
  data and leaves the holder unable to release its own lock.
  `set_worker_admin_state`/`clear_worker_admin_state`: the
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
