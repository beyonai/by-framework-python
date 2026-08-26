"""The inbound half of a resumed execution's metadata.

Two different directions restore metadata on a resume, and they are NOT the
same rule. Keep them apart:

- **Outbound** (``GatewayWorker._resolve_reply_command`` /
  ``GatewayProcessor._resolve_reply_header``): the header a resumed execution
  *sends* to its caller. The stored dispatch metadata **replaces**
  ``header.metadata`` wholesale — the waking hop's data is plumbing the
  original caller never asked for, and ``_enqueue_agent_return``'s
  ``{**header.metadata, **task_result.metadata}`` is the one sanctioned
  channel for a handler to forward part of it.
- **Inbound** (this module): the header a resumed execution's own handler
  *reads*. Here the waking message's metadata is legitimate payload — an
  ``ask_user`` answer's metadata was sent BY a client TO this agent — so it is
  merged on top of the original dispatch metadata rather than discarded.

Without this, everything a handler was originally dispatched with disappears
the first time it suspends: it comes back seeing only whatever woke it up.
"""

from typing import Any, Mapping, Optional

# Injected per dispatch by AgentContext._dispatch_single_task, not supplied by
# business code. A stored copy of them is stale by definition — it describes
# the hop that dispatched the execution, not the hop resuming it now — so they
# are dropped from the restored base and always come from the current message.
# Letting them through would surface a dispatch-time span id where the waking
# message has none, and AgentContext._resolve_call_langfuse_parent_id() reads
# exactly that as its last-resort fallback: a call made after a resume would
# parent to the original dispatch's observation.
FRAMEWORK_HOP_METADATA_KEYS = frozenset(
    {
        "trace_parent_span_id",
        "framework_parent_span_id",
        "langfuse_parent_observation_id",
    }
)


def merge_resume_metadata(
    stored: Optional[Mapping[str, Any]],
    incoming: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    """Merge an execution's original dispatch metadata under this hop's.

    ``stored`` is the ``metadata`` field of the execution record (what the
    execution was originally dispatched with); ``incoming`` is the waking
    message's own metadata. The waking message wins on key collisions: it is
    the newer, more specific hop, and this keeps the property that every key a
    handler can read today stays readable — the restore only ever adds keys.

    A ``stored`` that is missing (an execution recorded before this field
    existed, or one dispatched by an SDK that doesn't write it) degrades to
    ``incoming`` unchanged, i.e. exactly the pre-restore behaviour.

    Neither argument is mutated.
    """
    merged = {
        key: value
        for key, value in (stored or {}).items()
        if key not in FRAMEWORK_HOP_METADATA_KEYS
    }
    merged.update(incoming or {})
    return merged
