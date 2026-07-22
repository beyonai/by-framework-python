# Redis connection & cluster-mode architecture

## What it is

`by-framework-python` connects to Redis in two modes — standalone and
Cluster — and, independently, reads/writes Redis keys under one of two key
schemas (v1/v2, `RedisKeys` in `common/constants.py`). Every entry point that
opens a Redis connection (worker bootstrap, the shared default client, the
admin CLI) must resolve mode and key-schema version the same way. There is no
shared helper that does this resolution once — three fix commits (38e86c7,
35d45fe, 62180fe) were each "this entry point forgot the precedence the
others already have." This doc is the single place that precedence is
written down; keep every entry point's logic mirrored against it.

## Covers

- `src/by_framework/common/config.py` (`RedisConfig.from_env()`)
- `src/by_framework/common/constants.py` (`RedisKeys`, `get_key_schema_version()`)
- `src/by_framework/worker/app.py` (`run_worker`/`_run_worker_async`)
- `src/by_framework/admin/cli.py` (`_get_redis()`)
- `src/by_framework/core/registry.py` (`set_worker_admin_state`/`clear_worker_admin_state`)

## Flow

1. Each entry point resolves Redis config independently: explicit kwarg/CLI
   flag > env var > default. `redis_cluster_nodes`/`REDIS_CLUSTER_HOST` alone
   (no explicit `redis_mode`/`REDIS_MODE`) implies cluster mode; an explicit
   `redis_mode`/`REDIS_MODE` always wins over that inference.
2. Key-schema version resolves separately, via `get_key_schema_version()`:
   explicit `REDIS_KEY_SCHEMA_VERSION` env var > `REDIS_CLUSTER_HOST`-implies-v2
   > default v1. This deliberately does **not** infer v2 from `REDIS_MODE=cluster`
   alone — the two precedence chains are parallel, not derived from each other.
3. Under v2, every key that includes a worker/entity id wraps that id in a
   Cluster hash tag in the *middle* of the key
   (`prefix + "{" + id + "}" + suffix`) so all of one entity's keys land in
   the same Cluster slot. Cross-entity index keys (`admin_workers()`,
   `trace_index_session/worker/agent`) are deliberately left *untagged*
   relative to the per-entity keys they index.

## Cross-file invariants

- **Redis connection/cluster-mode precedence has no shared helper — every new
  Redis-connecting entry point must manually replicate it.** Evidence: three
  separate fixes, each "this path forgot to honor the precedence the other
  paths already have" — 38e86c7 (`run_worker`'s programmatic path didn't
  infer cluster mode from `redis_cluster_nodes` alone), 35d45fe (the
  shared-default `get_redis()` singleton wasn't honoring env config at all),
  62180fe (the admin CLI had no cluster-config path at all). Correct form:
  when adding a new Redis-connecting call path, explicitly test it against
  the precedence in "Flow" above — there's no single source of truth to
  inherit from, only this doc to check against.

- **Never build a Redis SCAN/KEYS pattern, or extract an id from a scanned
  key, via bare prefix/`str.startswith` when the key schema is versioned.**
  Under v2 the id sits inside `{...}` in the middle of the key, and `{`/`}`
  are literal characters in Redis glob matching, not wildcards — a bare
  `f"{prefix}*"` pattern silently matches nothing. Incident: fix 6ec070c —
  `metrics/snapshot.py`'s worker/admin SCAN discovery reported 0 online
  workers under Cluster+v2 with live lease keys present; no existing test
  caught it because none set `REDIS_KEY_SCHEMA_VERSION=v2`. Correct form:
  always use the paired `RedisKeys.*_scan_pattern()` /
  `RedisKeys.*_id_from_*_key()` helpers for any SCAN-based enumeration —
  never a hand-rolled pattern or `startswith` check.

- **Never pipeline Redis keys belonging to different logical entities
  together under Cluster mode.** Redis Cluster requires every key in one
  pipeline/transaction to share a hash-tag slot; mixing a per-entity key with
  a cross-entity index key (e.g. `worker_admin(id)` + `admin_workers()`, or
  a trace's `meta`+`spans` + its session/worker/agent index entries) throws
  CROSSSLOT. Incident: fix 8501407 — split `registry.py`, `span_recorder.py`,
  `trace_writer.py` so the same-entity group stays atomic and cross-entity
  index writes go out as independent, individually try/except'd calls
  (logged, never propagated — a transient index-write failure must never
  lose the primary write). Correct form: keep the primary/source-of-truth
  write atomic; make every cross-entity index update its own best-effort
  call.
