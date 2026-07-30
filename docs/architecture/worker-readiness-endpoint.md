# Worker readiness endpoint

**Status: implemented**, per GitHub issues #84-#88 against #83. This doc is
the design record from the 2026-07-23 `/grilling` + `/domain-modeling`
session that closed gap 5 in `docs/architecture/production-deployment.md`
("Worker process exposes no health-check surface"). Keep it in sync with
the code; update it in place if a future change touches this behavior —
don't let code and doc drift apart.

## What it is

The Worker process (`GatewayWorker`/`WorkerRunner`) has no way to tell an
external orchestrator "am I ready to be counted as a healthy replica" — the
only existing signal is the Redis-backed heartbeat TTL key
(`byai_gateway:registry:worker:online:{worker_id}`), which requires querying
Redis directly or via `by-admin worker info`, not something Docker's
`HEALTHCHECK` or a Kubernetes probe can point at locally. This doc defines a
local HTTP endpoint that closes that gap — readiness only, deliberately not
liveness (see Decision 1).

## Ubiquitous language

- **Ready** — this Worker should currently be counted as an available
  replica (safe to route work to, safe to count toward a rollout's "new
  replicas are up" check). Maps 1:1 to the endpoint's HTTP status: `200` =
  ready, `503` = not ready. This is the *only* thing the status code
  encodes — never layer "why" into the status code, only into `reason`
  (Decision 2).
- **`reason`** — a single enum naming *why* the Worker is or isn't ready
  right now, evaluated in strict priority order (`starting` > `draining` >
  `evicted` > `suspended` > `consumer_stalled` > `serving`; first match
  wins — see Body schema). Distinct from `admin_lifecycle`: `admin_lifecycle`
  answers "did an operator deliberately pause this Worker" (an admin-control
  concept, values `active`/`suspended`/`evicted`, already exists on
  `WorkerRunner`); `reason` answers "is this Worker actually serving right
  now" (a readiness concept). They coincide in the happy path
  (`admin_lifecycle=active` and `reason=serving`) but are not the same
  field and must not be collapsed into one — e.g. immediately after process
  start, `admin_lifecycle` is already `active` but `reason` is `starting`
  until the consume loop's first successful tick.
- **Serving** — the specific `reason` value meaning "ready, and the reason
  is simply that everything is fine" (not starting, not draining, not
  suspended/evicted, consumer loop ticking normally). Deliberately not
  reusing `admin_lifecycle`'s `active` string for this, per the distinction
  above.
- **Draining** — the `reason` value from the moment `SIGTERM`/`SIGINT` is
  received until process exit. A draining Worker is *not ready* even though
  it may still be correctly processing in-flight tasks — readiness here
  means "should new work/rollout progress count on this replica," and a
  Worker mid-shutdown should not be counted, independent of whether it's
  still functioning correctly in its final seconds.

## Covers

- `src/by_framework/worker/health_server.py` — new module, `WorkerHealthServer`
- `src/by_framework/worker/runner.py` — `WorkerRunner` holds an optional
  `WorkerHealthServer` instance; starts it early in `start()`, flips it to
  `draining` as the first step of `_shutdown()`
- `src/by_framework/worker/app.py` — `run_worker()` gains `health_port`
- `src/by_framework/__main__.py` — `--health-port` CLI flag
- `deploy/Dockerfile` — `HEALTHCHECK` instruction
- `deploy/docker-compose.yml` — `healthcheck:` on the `worker` service,
  `depends_on: condition: service_healthy` for anything that should wait on it
- `deploy/kubernetes/worker-deployment.yaml` — `readinessProbe` (only —
  see Decision 1)
- `.github/workflows/deploy-smoke-test.yml` — `docker compose up --wait`
  fails the CI job if the readiness endpoint never reports healthy in a
  real container, not just in the Python contract tests

## Decisions

### 1. Readiness only — no liveness probe, ever

**Context.** Kubernetes and Docker both support two different kinds of
health signal. A failing *liveness* probe gets the container killed and
restarted by the orchestrator. A failing *readiness* probe only removes it
from consideration (Service endpoints, rollout-progress bookkeeping) without
touching the running process.

**Decision.** This endpoint is wired to readiness only. No liveness probe is
provided, and `deploy/kubernetes/worker-deployment.yaml` must never gain a
`livenessProbe` pointed at it (or at anything else, absent a separate,
deliberate design for one).

**Why.** A Worker's own health depends heavily on Redis being reachable. If
this endpoint's "not ready" state were wired to `livenessProbe`, a transient
Redis outage would cause every Worker replica's liveness probe to fail
*simultaneously*, and Kubernetes would kill and restart the entire fleet at
once — replacing a transient dependency outage with a self-inflicted
thundering-herd restart storm, which is strictly worse. Readiness failures
carry no such risk: they never trigger a kill, only routing/bookkeeping
changes. This is also what makes Decision 4 (flip to `draining`/`suspended`
immediately, even while still mid-drain and technically still functioning)
safe — none of those states can ever cause a restart, because nothing here
is ever liveness.

**Alternative rejected.** Add both probes, liveness scoped to something
narrower (e.g. "is the process not deadlocked" via a watchdog thread
independent of Redis). Rejected for now as unnecessary scope — no incident
has ever indicated a Worker gets stuck in a way `SIGTERM` doesn't resolve;
revisit only if that changes.

### 2. Status code is binary; `reason` carries the detail

**Decision.** The HTTP status code encodes only `ready`/`not ready` (`200`/
`503`). All diagnostic detail — *why* — lives in the JSON body's `reason`
field, never in the status code (e.g. no `410` for evicted vs `503` for
stalled).

**Why.** Every consumer of this endpoint (kubelet's `httpGet` probe,
Docker's `HEALTHCHECK`, Compose's `service_healthy` condition) only acts on
the binary pass/fail signal — none of them branch on which non-2xx code
came back. Encoding detail in the status code would add complexity with no
consumer able to use it, while making the body's `reason` field redundant.
One bit of machine-readable signal (status code), unlimited human-readable
detail (body) — don't blend the two.

### 3. Dedicated thread, not the main asyncio loop

**Decision.** `WorkerHealthServer` runs on its own `threading.Thread` using
stdlib `http.server.BaseHTTPRequestHandler`, mirroring
`worker/heartbeat.py`'s existing pattern — not an async handler on
`WorkerRunner`'s main event loop.

**Why.** `heartbeat.py` already documents the exact failure mode this
avoids: "heartbeat renewal is never starved by long-running tasks in the
main event loop (e.g. LLM/LangGraph calls that hold the loop for tens of
seconds)." An async health handler on the main loop would be unreachable
during exactly the scenario an operator most wants to observe — the loop
being stuck — degrading a diagnosable `503 consumer_stalled` into an
unexplained connection timeout. A dedicated thread can always respond,
because `WorkerRunner`'s internal state (`_is_consumer_healthy()`,
`_admin_lifecycle`) is read via simple attribute access, safe across
threads under the GIL without extra locking.

**Alternative rejected.** Async handler on the main loop. Rejected because
it degrades precisely when the signal matters most (see above). Also
rejected: a new HTTP framework dependency (aiohttp/FastAPI) — stdlib
`http.server` is already the proven, dependency-free pattern this codebase
uses for `libs/by-framework-dashboard`.

### 4. Opt-in, no default port

**Decision.** `health_port: Optional[int] = None` on `run_worker()`;
`--health-port`/`BYAI_WORKER_HEALTH_PORT` on the CLI. Unset (the default) =
the health-check thread never starts, no port opens. No fallback default
port number is provided if the flag is set — a concrete port must be given.

**Why.** Opening a new network port is a behavior change for every existing
deployment that upgrades. Defaulting it on would silently add a listening
port to processes that never asked for one — a footgun for bare-metal/plain-
process deployments that don't containerize at all. Requiring an explicit
port (rather than a flag-with-a-baked-in-default) forces the deployer to
consciously pick a port that doesn't collide with anything else in their
environment, consistent with `deploy/`'s pattern: the framework provides
the primitive, opt-in; `deploy/`'s reference artifacts turn it on by
default *for that reference setup only*.

### 5. `/readyz`, distinct from the dashboard's `/api/health`

**Decision.** Path is `/readyz`.

**Why.** Kubernetes' own control-plane components expose `/healthz`,
`/readyz`, and `/livez` as three *separately-named, differently-behaved*
endpoints specifically to avoid the ambiguity of one generic "health" path
meaning different things to different callers. `/readyz` borrows that
convention to say, unambiguously, "this is readiness, nothing else." It is
deliberately not named `/health` or `/healthz` to avoid confusion with
`libs/by-framework-dashboard`'s `/api/health`, which is a *different kind of
endpoint* — an always-200 observability payload for humans/dashboards, not
a pass/fail probe (see Decision 2's "why" for why conflating those two
shapes under a similar name would be a trap for the next person wiring a
probe against "the health endpoint").

### 6. Starts early, flips to `draining` immediately on shutdown

**Decision.** `WorkerHealthServer` starts as early as possible in
`WorkerRunner.start()` (before `setup_control_streams()`), reporting
`starting` honestly until the consume loop's first successful tick. On
`SIGTERM`/`SIGINT`, `reason` flips to `draining` as the very first step of
`WorkerRunner._shutdown()` — before in-flight tasks finish draining, not
after.

**Why (start early).** If the port doesn't open until the Worker is fully
initialized, a probe hitting it during startup gets connection-refused,
indistinguishable from "this Worker crashed before ever starting" — losing
exactly the diagnostic value (a body saying `"reason": "starting"`) this
endpoint exists to provide, and making `initialDelaySeconds`/
`start_period` tuning guesswork instead of "however long `starting` is
normally observed to last."

**Why (flip on signal, not after drain).** Readiness means "should this
replica currently be counted as available" — a Worker that has already
begun exiting should stop being counted the instant that's true, not once
it finishes exiting (at which point the process is gone anyway and the
question is moot). This is safe only because of Decision 1: a `503` here
never kills anything, it only stops counting a replica that was going away
regardless.

## Body schema

```json
{
  "ready": false,
  "reason": "suspended",
  "worker_id": "worker-01",
  "admin_lifecycle": "suspended",
  "consumer_healthy": true,
  "uptime_ms": 12345
}
```

`reason` priority order, first match wins:

1. `starting` — heartbeat/consumer loop hasn't completed its first tick yet
2. `draining` — `SIGTERM`/`SIGINT` received, shutdown in progress
3. `evicted` — `admin_lifecycle == "evicted"`
4. `suspended` — `admin_lifecycle == "suspended"`
5. `consumer_stalled` — consume loop stale past `_consumer_health_timeout_seconds`
6. `serving` — none of the above; `ready=true`

`ready` is `true` if and only if `reason == "serving"`.

No authentication. The body must never carry secrets (Redis host,
credentials, connection strings) — enforce this at review time for any
future field added here, since that's the only thing that would turn "no
auth needed" from a correct call into a vulnerability.

## Hard rule

**This endpoint must never be wired to a Kubernetes `livenessProbe`, a
Docker restart-on-unhealthy supervisor, or anything else that reacts to
"not ready" by killing/restarting the process.** Everything in Decision 6
(flip to `draining` while still mid-drain, report `suspended`/`evicted` as
not-ready) is safe *specifically because* nothing currently reacts to this
endpoint by killing anything. Adding such a consumer later — even a
seemingly unrelated one, e.g. a script that restarts "unhealthy" containers
— silently reintroduces the exact restart-storm/premature-kill risk
Decision 1 was written to avoid. If a genuine liveness need shows up later,
it needs its own signal and its own design, not a repurposing of this one.
