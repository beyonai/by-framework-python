# Langfuse tracing: what the worker holds, and for how long

## What this covers

`libs/by-framework-trace-langfuse` is the only plugin that keeps **live
per-task state in worker memory across hook calls**. Everything else in the
trace layer is either fire-and-forget (`RedisSpanExporter`) or bounded by the
SDK (`LangfuseSpanProcessor` wraps OTel's `BatchSpanProcessor`, whose queue
tops out at 2048 spans and drops the overflow).

This doc exists because three separate unbounded-growth bugs shipped in that
plugin and none of them were visible from reading a single function. They
shared one root cause, stated below, and the fixes are only correct as long
as that framing holds.

## The rule everything follows from

**Langfuse reporting is best-effort telemetry, not business data.** A slow or
unreachable Langfuse must cost trace fidelity — never worker memory, never
worker shutdown, never a shared executor.

Every degraded path therefore **sheds load**: drop the work, count it, log it
sparsely. The failure mode to avoid is the intuitive one — "queue it, we'll
send it when Langfuse recovers" — because a worker completing tasks faster
than Langfuse accepts them turns that queue into an OOM with the whole
serialized task output pinned in each entry.

This is a stricter rule than the repo-wide fail-soft invariant in `CLAUDE.md`.
Fail-soft says *don't raise into the task path*. This says *don't accumulate
in the task path either* — a plugin that swallows every exception can still
kill the worker by growing.

## The three things the plugin holds

### 1. `_active_workflows` — durable workflow spans

An `agent.workflow` observation spans the **logical** task, which outlives any
single worker execution: `ask_user` and `call_agent` suspend the execution and
a later `RESUME` reattaches to it (see `suspend-resume-liveness.md`). So the
handle cannot be closed when the first execution returns — it lives in an
`OrderedDict` keyed by `(session_id, message_id)` until the task reaches a
terminal status.

Three things can end an entry, and all three must exist:

| Exit | Trigger |
|---|---|
| Normal | terminal `on_task_complete` / `on_task_error` / `on_task_cancel` |
| TTL | `_expire_active_workflows()`, for a suspend whose resume never came |
| LRU | `_evict_active_workflows_if_needed()`, at `max_active_workflows` |

**Invariant — LRU order equals expiry order.** `_expire_active_workflows()`
scans from the head and stops at the first live entry, which is only valid
because every write goes through `_track_active_workflow()`, which refreshes
`expires_at` *and* moves the entry to the tail. A future write that bypasses
that helper silently turns expiry into a no-op for everything behind it.

**Ownership is claimed at registration, not at the end of the hook.**
`on_task_start` registers the workflow and immediately sets
`LANGFUSE_WORKFLOW_OBSERVATION_ATTR` on the context, because the tracer calls
that follow it can raise — and `PluginRegistry._execute_hook()` swallows that.
`_end_workflow_observation()` additionally falls back to the
`(session_id, message_id)` key when the context attribute is missing. Both
halves are load-bearing: without the fallback, a mid-hook SDK error orphaned
100% of entries until LRU eviction, each one a live span that would never
export.

For the same reason `on_task_error` / `on_task_cancel` must **not** return
early when the context has no agent/worker observations. The workflow entry
can outlive them, and bailing is what stranded it.

### 2. `_pending_trace_output_updates` — in-flight trace-output uploads

`update_trace_output` is a *synchronous* HTTP POST — it patches the trace-list
`output` field, which the OTel span pipeline does not carry. It runs on the
plugin's **own** `ThreadPoolExecutor`, never the asyncio default executor:
that pool is shared with the rest of the framework's blocking calls, and a
stalled Langfuse would starve unrelated work.

The bound is enforced **before submitting**, not by trimming the tracking set.
Dropping only the future leaves the work item — and the payload it closes
over — sitting in the executor queue, which is exactly the growth being
guarded. At the limit the upload is discarded and counted.

`on_worker_shutdown` waits on the remainder with a deadline and abandons
whatever misses it. The discard callback catches `BaseException`, not
`Exception`: shutdown cancels the stragglers, and `CancelledError` is a
`BaseException`, so a narrower catch dumps one traceback per abandoned upload.

### 3. Serialized output

`_serialize_value` deep-copies the whole task result. The end-of-task path
serializes **once**, at the top of `on_task_complete`; `_end_observation`,
`_end_workflow_observation` and `_update_trace_output` all take an
already-serialized value and must not re-serialize it. Their docstrings state
this precondition — it is a contract, not an optimization, because copy #3 is
what an in-flight upload pins.

## Tunables

All have safe defaults; unset behaves no worse than the built-in values.
Explicit constructor kwargs always win over the environment, per `CLAUDE.md`'s
"explicitly-passed kwarg" invariant.

| Env | Default | What it bounds |
|---|---|---|
| `BYAI_LANGFUSE_MAX_ACTIVE_WORKFLOWS` | 10000 | live workflow spans |
| `BYAI_LANGFUSE_WORKFLOW_TTL_SECONDS` | 3600 | how long a suspended workflow is held |
| `BYAI_LANGFUSE_MAX_PENDING_TRACE_OUTPUTS` | 128 | in-flight trace-output uploads |
| `BYAI_LANGFUSE_SHUTDOWN_FLUSH_TIMEOUT_SECONDS` | 5 | shutdown flush deadline |

## Accepted trade-off

A workflow reclaimed by TTL or LRU whose resume arrives later gets a *new*
observation built from the same deterministic span id
(`str_to_uint64(f"{execution_anchor}:agent.workflow")`), so Langfuse sees a
duplicate span id. This is pre-existing LRU behaviour, not new; the 1-hour
default TTL keeps it rare. Trace-output drops under load are likewise
deliberate — the trace and its observations still ship via the SDK's own
batch pipeline, so only the trace-list `output` preview is lost.

## One more thing that is not a leak

`build_langchain_callback` subclasses the Langfuse `CallbackHandler` to stop
LangChain runs being promoted to trace roots. That subclass is cached per base
class in a `WeakKeyDictionary`. Minting one per call was *not* a hard leak —
the classes are reclaimable — but it ran once per task, and type objects are
only collected by the generational GC while every creation invalidates
CPython's type method cache. Per-callback state lives on the instance
(`_by_framework_metadata`), so one shared subclass is safe.
