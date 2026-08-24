# Suspended-caller liveness

## What it is

When an agent calls `context.call_agent(wait_for_reply=True)` (or
`call_agents`, or `ask_user`), the calling execution does not block — it
**ends**. Its handler returns, the control message is acked, and the
registry records it as `WAITING_AGENT`/`WAITING_USER`. The only thing that
can ever run it again is a `ResumeCommand` addressed to its `message_id`
landing on its agent type's control stream.

That leaves no timer anywhere. If the callee's worker is killed mid-task,
nobody sends the reply: the message it was processing sits in the consumer
group's PEL (nothing in this repo runs `XAUTOCLAIM`), and the caller stays
suspended for as long as its session data lives. The same shape covers a
lost reply message, and — one level up — a whole chain, since a caller
waiting on a caller waiting on a dead worker is just as stuck.

This subsystem is the missing timer. It is deliberately built out of data
the system already keeps (execution records, worker heartbeat leases) plus
**one** new structure — a sharded ZSET recording who is waiting on whom. The
property that matters is that **nothing is written periodically**: the cost
is a fixed handful of operations per call (the `ZADD` and `ZREM` on that
ZSET, the marker that disambiguates them, and one result copy), and it does
not grow with how long the wait lasts. A liveness signal that had to be
refreshed while waiting would scale with concurrent suspended callers
instead, which is the design this one exists to avoid.

## Covers

- `src/by_framework/core/wait_index.py` — member codec + shard selection
- `src/by_framework/core/wait_gate.py` — the idempotency gate
- `src/by_framework/core/wait_sweeper.py` — `WaitIndexSweeper`, the sweep
- `src/by_framework/core/wait_reply.py` — the one construction of a stand-in
- `src/by_framework/worker/context.py` — registration (`_register_wait`),
  compensation for a member that was never dispatched
- `src/by_framework/worker/worker.py` — result persistence before the reply,
  stand-in flush
- `src/by_framework/worker/runner.py` / `processor.py` — where the gate runs
- `src/by_framework/common/constants.py` — keys, deadlines, `LivenessErrorCode`

## Flow

1. **Register.** `_dispatch_single_task` writes one wait-index entry per
   sub-task next to `initialize_execution()`, i.e. *before* the dispatch
   `xadd`: the window that must not exist is "dispatched, but nobody knows
   we are waiting". Member is
   `{session_id}|{parent_message_id}|{child_message_id}|{task_group_id}`,
   score is the deadline in epoch ms. `ask_user` registers with an empty
   `child_message_id` and its own, much larger default. A group member whose
   target agent type is unavailable is registered too, and compensated by a
   stand-in reply the worker flushes once the caller's handler returns (see
   "Task Group orphans").
2. **Persist before replying.** A callee stores its result
   (`task_group_results`, under `tg-single-{child_message_id}` for a
   non-group call) *before* it sends the reply, so a lost reply loses a
   message rather than an answer.
3. **Clear.** Every `ResumeCommand` passes the gate before the execution
   lookup and before Task Group join. The gate `ZREM`s the entry the reply
   is clearing; exactly one claimant can win.
4. **Sweep.** `WaitIndexSweeper` claims a shard, reads entries whose
   deadline passed, and interrogates existing evidence: the callee's
   execution record and its worker's lease. It renews, cleans up, or
   synthesizes the reply the callee would have sent — and renewal itself is
   bounded, so an alive-but-stuck callee cannot be renewed forever. The same
   pass also *prunes*, on a much coarser cadence and independently of the
   switch above: entries too old to be interrogated at all are deleted
   unexamined, because nothing else ever removes them (see "Rollout").

## Triage

Read the entries in order; the first match wins. Everything establishing
*someone is still waiting* is checked before anything that can produce a
reply, because a reply nobody awaits is not free — it re-enters a finished
execution.

| Evidence | Action |
|---|---|
| Member unparseable | drop the entry |
| Caller has no execution record | drop (session data expired; no reply can reattach it) |
| Caller terminal | drop, synthesize nothing (see "already-failed caller" below) |
| Caller not suspended, its worker gone | drop — nothing can resume it; its own caller's wait rescues the chain |
| Caller not suspended, worker alive | renew (it has not finished the handler that registered the wait) |
| `ask_user` entry | skip — no compensation, ever |
| Group tracker gone / aborted / this member already joined | drop (see "Task Group orphans" below) |
| Callee terminal, result stored | rebuild the real reply, `REPLY_LOST_RECOVERED` |
| Callee terminal, no stored result | `FAILED` + `REPLY_LOST_RECOVERED` |
| Callee itself suspended | renew, no ceiling |
| Callee has no `worker_id` | `FAILED` + `CHILD_NEVER_STARTED` |
| Callee's worker lease alive, inside the renewal ceiling | renew |
| Callee's worker lease alive, past the ceiling | `FAILED` + `CHILD_TIMEOUT`, then ask it to stop |
| Callee's worker lease gone | `FAILED` + `CHILD_WORKER_LOST` |

## Cross-file invariants

- **The sweeper must not clear the entry it acts on.** It emits a
  synthesized reply and leaves the entry for the gate, so the synthesized
  copy and a late real copy compete through the same `ZREM` — either order
  yields exactly one wake-up, and the loser is dropped. Clearing the entry
  in the sweep instead makes the synthesized reply the one copy nothing can
  arbitrate: it would have to bypass the gate, and a reply that bypasses the
  gate is a second wake-up path, which is precisely what the gate exists to
  prevent. The entry's deadline is pushed out after emitting, so a caller
  whose control stream is not being consumed gets at most one stand-in per
  renewal window rather than one per sweep.

- **A callee that is itself suspended must be renewed, never failed, and is
  exempt from the renewal ceiling.** "Suspended" is both `WAITING_AGENT` and
  `WAITING_USER`, so this is also what keeps a sub-agent that asks the user
  from killing the caller that delegated to it — the person is not late, and
  the sub-agent replies on its own the moment they answer.
  The innermost wait in a chain has the
  earliest deadline by construction, so it fails first and its failure
  travels up hop by hop as ordinary replies — each level reporting a cause it
  actually knows. Timing out ancestors alongside it turns one dead worker
  into a chain-wide outage and destroys the causal chain in the process. The
  ceiling has to be exempted explicitly because it runs the other way: an
  outer wait was registered *earlier* than the one it is blocked on, so its
  ceiling arrives first and would fail the chain top-down. The chain still
  terminates, because the deepest wait is the one blocked on real work and it
  is not exempt.

- **A synthesized failure must be indistinguishable from a callee that ran
  and failed**: same command shape, `error`/`error_code` in the same place
  in `reply_data`, same control stream. Any special-casing downstream
  re-splits the reply path into two, which is the failure mode that already
  cost this repo a hung caller once (`worker.py`'s Group Join note).

- **The reply's id reversal is not cosmetic.** `header.message_id` is the
  *caller's* id (what the runner reattaches the suspended execution by) and
  `header.parent_message_id` is the *sub-task's* (the only per-sibling
  unique value, and what the gate rebuilds the member from). Getting it
  backwards either fails to resume the caller or collapses every Task Group
  sibling onto one member.

- **The gate's "consumed" marker must outlive every wait it may have to
  arbitrate.** It is the only thing separating "already resolved" from "never
  registered", and the gate is required to let the latter through — so an
  expired marker is not a lost optimization, it is a wrong answer. The
  longest wait anything registers is `ask_user`'s, whose deadline equals
  `DEFAULT_SESSION_TTL`, hence `WAIT_CONSUMED_TTL_SECONDS` is that same span.
  Shorter reopens two holes, and the second is the dangerous one: a repeated
  user answer stops being recognized as a duplicate; and a stale duplicate
  sub-agent reply, having lost the marker that would stop it at its own
  candidate, falls through to the `ask_user` candidate for the same caller,
  claims a wait that is still pending, and marks *that* consumed — so the
  person's real answer is then dropped and the caller is never resumed.

- **`ask_user` waits are registered but never compensated.** A human taking
  days is not a fault; a synthesized failure there would be a fabricated
  event, and the caller would then be woken a second time when the person
  actually answers. Registration exists only so the gate can recognize a
  repeated answer. Suspended sessions are reclaimed by
  `DEFAULT_SESSION_TTL`, and a due `ask_user` entry whose caller is gone is
  cleaned up by the same "caller missing/terminal" rules as any other entry.

- **An already-failed caller can own a live entry.** Registration happens
  before the dispatch `xadd`, so an `xadd` that raises fails the caller and
  leaves the entry behind. The sweep must clean up rather than synthesize —
  a reply for a terminal execution wakes something that is already done.

- **Everything in the sweep path is fail-soft.** It is a safety net running
  inside every worker; a raise there must never take down the worker, and
  one poisonous entry must not stop the rest of a shard. The gate fails
  *open* for the mirrored reason: a duplicate wake-up is recoverable, a
  dropped reply is permanent silence.

## The renewal ceiling

A live worker lease answers "is the process up", not "is the work moving",
so renewing on it alone is unconditional: a callee stuck in a call that never
returns, or deadlocked, keeps its lease and suspends its caller forever —
the exact hang this subsystem exists to bound. Renewals therefore stop at
`registered_at + N * timeout` (`WAIT_RENEW_MAX_MULTIPLE`, overridable with
`BY_FRAMEWORK_WAIT_RENEW_MAX_MULTIPLE`), after which the callee is reported
as `CHILD_TIMEOUT` like any other synthesized failure.

Three things about it are deliberate:

- **It is measured against the caller's own timeout, not a renewal count.**
  Renewals happen at `WAIT_RENEW_INCREMENT_MS`, which is a tunable, so
  counting them would let a config change silently move the bound; and a
  caller that asked for ten minutes should not be held to the same absolute
  budget as one that asked for four hours. The timeout is recovered as the
  span between the sub-task's `created_at` (written by
  `initialize_execution` immediately before the wait is registered) and the
  wait's original deadline. Unusable spans fall back to
  `DEFAULT_REPLY_TIMEOUT_MS`, erring toward waiting longer.
- **The original deadline has to be saved, because renewing destroys it.**
  `RedisKeys.wait_renew_origin` holds it (SET NX on the first renewal); with
  the score alone, every sweep would re-measure from the deadline it had
  just pushed out and no ceiling could ever be reached. That key is
  sweeper-private and deliberately *not* part of the wait-index member,
  which must stay rebuildable from a reply alone. Its expiry is safe: the
  budget simply restarts from the current deadline, so the TTL only needs to
  sit well above any plausible budget.
- **Progress is not used as evidence.** A callee's `updated_at` stands just
  as still during a legitimate 20-minute model call as during a deadlock, so
  a ceiling keyed on it would kill exactly the work it is meant to protect.
  The ceiling is crude on purpose: generous, absolute, predictable. Work
  that genuinely runs longer than `N * timeout` should say so with a longer
  `reply_timeout_ms`.

## Task Group orphans

An orphaned group member is compensated by the *same* synthesized reply as
any other, carrying `header.task_group_id`, so `GatewayWorker`'s existing
join stores it, increments `completed`, and aggregates only when it is the
last sibling outstanding. Nothing in the sweep writes `task_group_results`
or `completed` itself: a second writer of that accounting is what hangs a
caller when *its* increment is the one that reaches `total` and no reply is
left to trigger the join — a bug this codebase has already shipped once from
the dispatch side.

That also means the group path needs no idempotency mechanism of its own.
Two stand-ins, or a stand-in and a late real reply, compete through the same
`ZREM` as everything else, so exactly one is ever counted; the design's
"`completed` overshoots `total` and aggregates twice" hazard exists only for
a compensation that skips the gate.

**The same rule binds the dispatcher, and there it is not hypothetical.**
`call_agents` fans out one sub-task at a time, and a target agent type that
is unavailable is rejected before anything is dispatched — so that member has
no callee and will never be replied to. Booking it inline (writing its result
and incrementing `completed`) is the same second accounting path, and it hangs
the caller whenever that increment is the one that reaches `total`: every
target being offline reaches it directly, and a sibling replying fast enough
to be joined mid-fan-out reaches it by race. The compensation is therefore the
*same stand-in reply* the sweep would send, queued on the context and flushed
by the worker once `process_command` returns — never inline, which would put a
reply on the caller's own control stream strictly before it suspends and turn
that race from rare into certain.

A related constraint falls out of the same keying. A group's per-sibling
identity *is* its sub-task `message_id`: it keys `task_group_results` and it
is the only field distinguishing one member's wait-index entry from the
next's. `call_agents(tasks, message_id=...)` pins one across the whole
fan-out, so every sibling's result overwrites the last and every sibling's
wait collapses onto one entry — which the gate then correctly claims once and
drops the rest of, leaving `completed` stuck below `total`. That parameter
was always wrong for a batch; the gate only upgraded scrambled results into a
hang. It now raises `ValueError` for more than one task rather than quietly
minting ids, which would trade one invisible behaviour for another. A single
task keeps it, because there one id for one sub-task collides with nothing.

Such a member is also registered in the wait index, even though nothing was
dispatched. The stand-in then *claims* its entry like any other reply rather
than passing as unregistered, and — the reason it is worth a `ZADD` — an
undelivered stand-in (the flush is fail-soft by necessity) leaves the sweep
able to compensate the same member from the `FAILED` execution record
`record_failed_route_decision` wrote, instead of the group hanging on a member
that provably cannot answer.

Three states make compensation wrong rather than merely unnecessary, and are
cleaned up instead:

- **The group tracker is gone** (its TTL passed). A reply would find no
  group to join and would resume the caller with one sibling's payload where
  the aggregate belongs.
- **The group is aborted.** Its dispatch failed partway through the fan-out,
  the caller was failed with it, and every reply is discarded on arrival.
- **A result is already stored under this sub-task's id.** Only the join
  writes that, so the reply arrived and was counted; the entry outliving it
  is a gate `ZREM` that did not land, and a stand-in would be counted twice.

## Sharding and ownership

Shards are claimed opportunistically with a short-TTL token-verified lock
(`RedisKeys.wait_sweep_lock`, reusing `registry.py`'s Redlock primitives),
held only for the duration of one shard's pass. There is no leader
election, and none is needed: ownership is advisory, every action a sweep
takes is idempotent (a duplicate synthesized reply is caught by the same
gate as a duplicate real one), and a dead worker's claim simply expires.
`WAIT_INDEX_SHARDS` is a protocol constant, not a tunable — changing it
re-maps every session, so entries written before the change are swept by
nobody.

## Cancelling a timed-out callee

`CHILD_TIMEOUT` is the only outcome with a live process on the other end, so
it is the only one where stopping the work means anything: `CHILD_WORKER_LOST`
and `CHILD_NEVER_STARTED` have nothing to stop, and a callee that finished is
already done. There the sweep additionally asks the callee to stop, by
delegating the whole path to `GatewayClient.cancel_task`.

Delegating is not laziness — that function already gets the two things right
that are easy to get wrong. It delivers to
`RedisKeys.worker_ctrl_stream(worker_id)`, because `handle_cancel_task` finds
the execution in its *worker's own in-memory table*: a cancel posted to the
agent type's competitive stream is claimed by an arbitrary worker, finds
nothing, and records a cancellation while cancelling nothing. And it walks the
sub-tree, so the callee's own callees stop with it — after which
`worker.py`'s `cancel_requested` check keeps the deeper ones from replying at
all, leaving only the top one's reply to reach the gate.

Three properties are load-bearing:

- **It happens after the reply, and every failure is swallowed.**
  Cancellation is cooperative — `task.cancel()` needs the target to yield the
  event loop — and the archetypal `CHILD_TIMEOUT` is a callee wedged in a
  blocking call, so *the case that most needs cancelling is the case it
  cannot reach*. The wake-up must therefore never depend on it, and does not.
- **It does not silence the callee.** `GatewayWorker`'s `CancelledError`
  branch still sends a `CANCELLED` reply. That copy is dropped by the gate,
  which is exactly why cancellation can never substitute for the gate.
- **The opt-out is per deployment** (`BY_FRAMEWORK_WAIT_CANCEL_ON_TIMEOUT`),
  not per `call_agent`. A per-call flag has to reach a sweeper that sees only
  the wait-index member, and the member must stay rebuildable from a reply
  alone — so carrying it would mean a side key written on *every* dispatch to
  serve a decision taken only after a timeout, i.e. exactly the periodic
  happy-path cost this design exists to avoid. A knob whose blast radius is
  "a timed-out callee keeps burning CPU" does not earn that.

A repeat is avoided by the callee's own `cancel_requested` flag rather than by
remembering anything: once `cancel_task` has marked it, later sweeps of the
same entry skip straight past.

## Deliberate gaps

- **A group member's result is not recovered, only failed.** A callee stores
  its result before replying only on the single-call path
  (`tg-single-{child}`); a group member's result is written by the join when
  its reply arrives, so a lost reply there loses the answer and the orphan
  is compensated as `REPLY_LOST_RECOVERED`-flavoured failure instead.
- **PEL residue is not reclaimed.** The caller's wake-up no longer depends
  on it, but a dead consumer's pending entries still pollute lag metrics.

## Rollout

The sweep has two halves, switched separately because only one of them
decides anything.

| Half | Switch | Default | What it does |
|---|---|---|---|
| Compensate | `BY_FRAMEWORK_WAIT_SWEEPER_ENABLED` | off | triage, synthesized replies, renewal, cancellation |
| Prune | `BY_FRAMEWORK_WAIT_PRUNE_ENABLED` | on | deletes entries whose score is beyond `WAIT_PRUNE_AFTER_SECONDS` |

Compensation is the only part that changes observable behaviour, so it
carries the rollback position for the whole subsystem: with it off, entries
are still written and cleared but nothing acts on them and the system
behaves exactly as it did before. That needs no deploy.

Pruning cannot ride on that switch, because **an entry is removed only by a
reply or by a sweep**, and the shard ZSET is shared across sessions so it
cannot carry a TTL of its own. With compensation off, every call whose reply
never arrives — the exact failures this subsystem exists for — leaves an
entry behind permanently. The rollback position would otherwise be a slow
leak in the structure meant to bound the leak.

It also does not *need* a switch, because it decides nothing. It decodes no
member, reads no execution record and produces no reply: one
`ZREMRANGEBYSCORE` over a range whose upper bound is a proof rather than a
guess. Every writer of an entry sets its score to its own clock plus a
non-negative offset (registration adds the caller's timeout, a renewal adds
`WAIT_RENEW_INCREMENT_MS`), and only ever while the caller's execution record
exists. So a score older than the threshold means the entry was last touched
more than a session TTL ago, its session registry has expired, and the only
verdict triage could still reach for it is "caller missing" — which deletes
it anyway. Renewed entries are covered by the same argument, since a renewal
*raises* the score: an old score is evidence about the most recent write,
whichever write that was.

The threshold is `DEFAULT_SESSION_TTL` **plus a day**, and the margin is not
decoration: `DEFAULT_ASK_USER_TIMEOUT_MS` equals the session TTL exactly, so
a threshold trimmed to it would sit on the boundary of a live `ask_user`
wait and lose to any clock skew between the worker that registered the entry
and the one sweeping it. Erring long costs one ZSET member per unresolved
call for one extra day; erring short deletes a wait nothing will ever
compensate. Pruning runs on its own, far coarser cadence
(`WAIT_PRUNE_INTERVAL_SECONDS`, hourly) — with compensation off it is the
only work left, and the whole loop drops to that interval rather than waking
every 30 seconds to do nothing.

Registration and the gate are per-SDK; the sweep is not. It reads and
writes plain Redis data, so a Python sweeper resolves suspended callers
created by the TS and Java SDKs too, including cross-language chains — but
only for waits those SDKs actually register. Key schema, member encoding,
`LivenessErrorCode` values, the `tg-single-` prefix and the gate's
semantics are therefore a cross-SDK wire contract.

`RedisKeys.wait_renew_origin` is the exception that proves the rule: it is
written and read only by a sweep, never by a reply, so a port that skips it
loses the ceiling but nothing else. Keeping it out of the member is what
makes that true — anything added *there* would have to be reproduced by
every SDK's gate, since a reply is all the gate has to rebuild it from.
