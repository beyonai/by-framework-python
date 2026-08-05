# Production deployment readiness

## What it is

README's "部署"/"Deployment" section pitches a "生产就绪" (production-ready)
Worker deployment path — `docker run redis` + `python -m by_framework`, scale
by launching more processes. That claim had never been audited against what a
real container orchestrator (Docker, Kubernetes) actually does to a running
process. Audited 2026-07-22 on direct request, not an incident — this doc
exists per CLAUDE.md's "a rule is already known" trigger (Maintaining this
map, item 5): domain detail goes straight to a docs file, not a growing pile
of quad-bullets.

## Covers

- `src/by_framework/__main__.py` — CLI arg surface (`parse_args()`)
- `src/by_framework/worker/app.py` — `run_worker()`/`_run_worker_async()`:
  env var precedence, top-level signal/exception handling
- `src/by_framework/worker/runner.py` — `WorkerRunner._shutdown()`: the
  drain-in-flight-tasks sequence that "优雅退出" refers to
- `README.md` / `README_zh.md` — "部署"/"Deployment" section
- `libs/by-framework-dashboard/Dockerfile` — the only containerization
  example that exists in this repo today
- `examples/echo_worker.py` / `examples/send_and_verify.py` — minimal
  Worker + client pair used to exercise the full message pipeline
- `.github/workflows/deploy-smoke-test.yml` — builds `deploy/` from this
  commit's own source and drives the pipeline end to end in CI

## Known gaps (audited 2026-07-22)

1. ~~**No `SIGTERM` handling — graceful shutdown is unreachable under
   container orchestration.**~~ **Fixed.** `run_worker()` now drives
   `_run_worker_async(...)` through `_run_with_graceful_shutdown()`
   (`worker/app.py`), which registers `loop.add_signal_handler` for both
   `SIGTERM` and `SIGINT` and cancels the running task on either — the
   cancellation propagates into `runner.start()`'s existing
   `finally: await self._shutdown()` drain sequence, the same path that used
   to be reachable only via `KeyboardInterrupt`/Ctrl+C. Falls back silently
   (logs at debug level) on platforms where `add_signal_handler` raises
   `NotImplementedError` (e.g. Windows' default event loop), where only
   `SIGINT` still works via the pre-existing `except KeyboardInterrupt`.
   Deploy examples under `deploy/` set `stop_grace_period`/
   `terminationGracePeriodSeconds` so the drain has room to finish before a
   hard `SIGKILL`.

2. ~~**CLI arg surface is narrower than what `run_worker()` actually
   supports.**~~ **Fixed.** `__main__.py` now exposes `--redis-password`,
   `--redis-username`, `--redis-mode`, and `--redis-cluster-nodes`
   (comma-separated `host:port`, parsed the same way as
   `REDIS_CLUSTER_HOST`/`REDIS_CLUSTER_NODES`). All Redis-related flags now
   default to `None` instead of a literal (e.g. `"localhost"`) — previously
   `--redis-host`'s hardcoded `"localhost"` default meant the CLI path
   silently shadowed `REDIS_HOST` even when the flag was never passed,
   breaking the "explicit arg > env var > default" precedence documented in
   `run_worker()`'s docstring for every field reachable through the CLI.

3. ~~**No official Worker Dockerfile / docker-compose / Kubernetes
   manifest.**~~ **Fixed.** Added `deploy/Dockerfile` (template — installs
   `by-framework` from PyPI, expects the caller's own worker module copied
   alongside it), `deploy/entrypoint.sh` (uses `exec` so Python replaces the
   shell as PID 1 and actually receives `SIGTERM` — a shell left as PID 1
   swallows it by default, which would silently defeat gap 1's fix under
   Docker), `deploy/docker-compose.yml`, and
   `deploy/kubernetes/worker-deployment.yaml`.

4. ~~**"水平扩展" example is shell `&`, not a real orchestration
   pattern.**~~ **Fixed** by the same `deploy/` additions as gap 3: Compose
   `--scale`/Kubernetes `replicas` give restart-on-failure supervision; each
   replica gets a distinct worker id for free via `entrypoint.sh`'s
   `worker-$(hostname)` (Compose assigns each scaled replica a unique
   container hostname; Kubernetes sets a Pod's hostname to its Pod name by
   default — no downward-API wiring needed). README's shell-`&` example is
   kept as a quick-local-check note, no longer the only scaling story.

5. ~~**Worker process exposes no health-check surface.**~~ **Fixed.** The
   Worker now has an opt-in `/readyz` HTTP endpoint (`--health-port`/
   `BYAI_WORKER_HEALTH_PORT`), readiness-only by design — never wired to a
   liveness probe. `deploy/`'s reference Dockerfile/Compose/Kubernetes
   artifacts all wire it up, and `deploy-smoke-test.yml` verifies it
   reports healthy in a real container via `docker compose up --wait`.
   Full design record, including why readiness-only and the hard rule
   against ever adding liveness: `docs/architecture/worker-readiness-endpoint.md`.

## CI validation

`deploy/Dockerfile`, `deploy/entrypoint.sh`, and `deploy/docker-compose.yml`
were never actually run before this doc's 2026-07-22 audit — a "reference"
example nobody executes tends to bit-rot silently. `.github/workflows/
deploy-smoke-test.yml` now builds the Worker image from **this commit's own
source** (`deploy/Dockerfile`'s `BY_FRAMEWORK_SOURCE=local` build arg —
installs an editable checkout instead of pulling the last PyPI release, so a
breaking change is caught before it's ever published) and drives the full
pipeline: `examples/send_and_verify.py` sends a message to
`examples/echo_worker.py` via the real Redis control stream and asserts the
echoed reply arrives on the real Redis data stream, retrying the send until
the Worker container finishes registering. This retry loop caught a real bug
in itself the first time this workflow ran in CI (not just locally, where
worker startup is fast enough the race never surfaces): under the default
`FAIL_FAST` route policy, `send_message()` does **not** raise when no worker
is online yet — it returns `SendMessageResponse(success=False, ...)` via
`AvailabilityRouter`. The first version of `send_and_verify.py` only retried
on a raised exception and never checked `.success`, so it silently treated a
failed send (worker container still starting) as delivered and then waited
out the full timeout for a reply to a message that was never actually
enqueued. Fixed by retrying on `resp.success is False`. Runs on every PR
touching `deploy/**`, `examples/**`, or
`src/by_framework/**`.

## Rule: keep the deployment story from drifting again

Any change touching README's 部署 section, `__main__.py`'s CLI surface,
`run_worker()`'s signature, or the shutdown sequence must satisfy all of the
following in the *same* change — this is what let gaps 1–5 above
accumulate silently:

- **A new configurable field on `run_worker()`/`RedisConfig` (Redis auth,
  cluster nodes, TLS, etc.) must get an equivalent `__main__.py` CLI flag in
  the same change.** The CLI is the entry point README documents; letting it
  lag behind the programmatic API silently turns every new field into an
  undocumented env-var-only feature.
- **A "graceful shutdown" or "production-ready" claim must be validated
  against `SIGTERM`, not just `KeyboardInterrupt`/`SIGINT`.** `SIGTERM` is
  what Docker/Kubernetes actually send on stop/terminate. A shutdown path
  that only reacts to `Ctrl+C` does not qualify for that claim.
- **A containerization claim needs a Worker-facing example, not only an
  auxiliary-component one.** The Worker is the thing the README's own
  "水平扩展" pitch says to scale horizontally — a Dockerfile that only
  covers the dashboard doesn't back that claim.
- **Prefer/also document a process-supervised scaling form** (systemd unit,
  Kubernetes `Deployment`) alongside any bare shell `&` example — `&` alone
  should not be the only scaling story in a doc claiming production
  readiness.
- **A change to the client/worker message protocol (command/event shape,
  route policy behavior) that breaks `examples/echo_worker.py` or
  `examples/send_and_verify.py` must update them in the same change**, not
  leave `deploy-smoke-test.yml` red — a smoke test nobody keeps green stops
  being a smoke test.

No dedicated guard yet (signal handling and CLI/API parity are judgement
calls, not greppable in one line) — if the same class of drift reappears
after this doc exists, that's the trigger to write one per
`docs/architecture/GUARD_AUTHORING.md`.
