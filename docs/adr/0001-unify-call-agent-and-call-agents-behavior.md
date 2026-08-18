# ADR 0001 — Unify `call_agent` and `call_agents` behavior

Status: accepted
Date: 2026-08-17
Applies to: `by-framework-python`, `by-framework-java`, `by-framework-ts`

## Context

`call_agent` (one sub-task) and `dispatch_group` (a batch) grew separately. They built
their `AskAgentCommand`s differently, and only `call_agent` consulted the availability
control plane; the batch path did a blind `xadd`. Group Join, on the worker side, resumed
the caller with whichever sub-task's reply happened to complete the group, and stored every
sibling's result under the *caller's* own `message_id` — a key identical across siblings, so
the result hash never held more than one entry and `collect_group_results` for an N-task
group polled to its timeout and returned one result.

The two APIs were therefore not two spellings of one idea; they were two behaviors, and the
batch one was the easier to misuse.

## Decision

**`call_agents` is `call_agent`'s plural.** Per-task behavior is indistinguishable from a
single `call_agent` call. The only increment is that the caller is resumed once, with every
sub-task's result aggregated, after all tasks complete.

Concretely:

1. One dispatch path. Both APIs build, availability-check, and dispatch through the same
   internal single-task routine. Every per-call option `call_agent` accepts —
   `route_policy`, `availability_timeout_ms`, `region`, `priority`, `message_id` — is
   accepted per task in a batch, with the same defaults.
2. One accounting path. Storing a sub-task result, incrementing the completion counter,
   deciding the group is done, and aggregating happen **only** in the worker's Group Join.
   A dispatcher must never book-keep the group itself.
3. A sub-task whose target agent type is unavailable at dispatch time produces the reply a
   sub-agent *would* have sent had it started and failed, delivered to the caller's control
   stream. It is not special-cased anywhere downstream.
4. `dispatch_group` (Python/TS) and its equivalents remain permanent, source-compatible
   aliases. Not deprecated.

## The Task Group contract

This section is normative. Implementations in other runtimes must satisfy it exactly.

### Group hash — `task_group:{group_id}`

| Field | Written by | Meaning |
|---|---|---|
| `total` | dispatcher | number of tasks dispatched |
| `completed` | Group Join **only** | replies counted so far |
| `source_agent_type` | dispatcher | the caller's agent type |
| `aborted` | dispatcher | `"1"` once a dispatch-time infrastructure failure aborted the batch |
| `protocol_version` | dispatcher | `"2"` for this contract; **absent** means a pre-2 dispatcher |
| `task_order` | dispatcher, after the dispatch loop | JSON array of sub-task dispatch `message_id`s, in dispatch order |

`protocol_version` is the rolling-upgrade hinge. A joiner reads it and branches:

- `"2"` → this contract.
- absent → the legacy contract (results keyed by the reply's `message_id`, no aggregation,
  `content` left untouched). An in-flight group created by a not-yet-upgraded dispatcher
  must keep being joined the way it was written, or a mixed fleet silently reinterprets it.

`task_order` is written after the loop so it records exactly what was dispatched.

### Result hash — `task_group:{group_id}:results`

Under `protocol_version = "2"`, each reply is stored under the reply's
**`header.parent_message_id`**.

This is not arbitrary. An agent return sets:

- `header.message_id` = the original dispatch's `parent_message_id` — the **caller's own**
  message id. The runner reattaches a `RESUME` to the suspended execution by looking this
  up, so it must not change. It is identical across every sibling in a group.
- `header.parent_message_id` = the original dispatch's `message_id` — the **sub-task's own**
  id, unique per sibling.

So the reply's `parent_message_id` is the only per-sibling-unique key available, and keying
by `message_id` (the legacy behavior) is what made siblings overwrite each other.

Stored value:

```json
{
  "status": "<sub-task's final status>",
  "reply_data": "<sub-task's reply_data>",
  "content": "<sub-task's content>",
  "target_agent_type": "<reply header.source_agent_type — the agent that produced it>",
  "metadata": {},
  "extra_payload": {}
}
```

### Dispatch-time failure

When availability rejects a task, the dispatcher emits to the **caller's** control stream:

| Field | Value |
|---|---|
| `header.message_id` | the caller's resolved message id |
| `header.parent_message_id` | that sub-task's dispatch `message_id` |
| `header.source_agent_type` | the requested target agent type |
| `header.target_agent_type` | the caller's agent type |
| `header.task_group_id` | the group id |
| `status` | `FAILED` |
| `content` | `""` |
| `reply_data` | `{"error": <reason>, "error_code": <code, default "AGENT_TYPE_UNAVAILABLE">}` |

`reply_data` carries the detail because that is where a *real* failure carries it — the
worker's error path returns `status=FAILED, reply_data={"error": ...}`. A dispatch-time
failure that put the reason anywhere else would defeat the whole point of decision 3.

**Timing.** These replies are flushed *after* the caller's `process_command` returns, not
inline in the dispatch loop. Sending inline would put a reply on the caller's control stream
strictly before the caller suspends, turning the pre-existing "very fast sub-agent replies
first" race from unlikely into certain. If `process_command` raises, the replies are
dropped — the group is aborted and the caller execution they would resume has already
failed.

### Group Join

```
if header.task_group_id and group hash has `total`:
    if `aborted` is set:            discard the reply, do not count it
    read `protocol_version`
    store the result under (v2 ? header.parent_message_id : header.message_id)
    completed = hincrby(completed, 1)
    if completed < total:           the caller stays suspended
    else if v2:
        aggregate in `task_order` order
        resume the caller with reply_data = aggregate, content = ""
```

### Aggregate shape

An ordered list, one entry per dispatched task, in `task_order` order:

```json
[{"message_id": "...", "status": "...", "reply_data": ..., "content": "...",
  "target_agent_type": "...", "metadata": {}, "extra_payload": {}}]
```

Rules:
- Order is dispatch order, never the Redis hash's iteration order.
- Results present in Redis but absent from `task_order` are **appended**, never dropped.
- A short result set still resumes the caller, but must be logged at error level naming the
  missing `message_id`s. Silently returning fewer results than were dispatched is forbidden.
- `content` on the resume is `""`. `reply_data` is the single aggregation channel; leaving
  `content` as whichever sibling replied last gives the caller two channels that disagree.

## Consequences

### For callers

- `reply_data` on a Task Group resume is a list, not a single sub-task's payload.
- `content` on a Task Group resume is `""`.
- An unavailable target fails that task only; siblings still dispatch.
- `route_policy: "SEND_ANYWAY"` per task restores the pre-unification behavior of queueing
  for an agent type whose workers have not started (control-stream consumer groups are
  created at id `0`, so such a message is delivered on start).
- One `message_id` cannot be shared across a batch — results are keyed per sub-task. Pass a
  per-task `message_id` instead.

### For operators — upgrade requirement

`protocol_version` makes a *new* worker safe on an *old* group. The reverse — an old worker
joining a new group — cannot be solved in-band: the old code has no version branch and will
key results the legacy way.

**Therefore: drain in-flight task groups (or restart the whole agent-type pool at once)
before upgrading a mixed pool.** A partial rollout with live task groups can resume a caller
with an incomplete aggregate.

### Rejected alternatives

- *After the dispatch loop, check `completed >= total` and resume locally.* Fixes the known
  deadlock but leaves two implementations of group accounting alive, so the next variant of
  the same bug is still reachable. Decision 2 exists to make it unrepresentable.
- *Aggregate into `content` as well.* `content`'s list arm is the multimodal content-block
  position; reusing it for sub-task results is type abuse.
- *Keep the top-level `error`/`error_code` keys on failed aggregate entries.* Diverges from
  how real sub-agent failures arrive, which is exactly the asymmetry this ADR removes.
