"""Replies built by someone other than the callee that owed them.

Two places have to produce a ``ResumeCommand`` on behalf of a sub-agent that
will never send one itself:

* ``core/wait_sweeper.py`` — the callee's worker died, or it ran past its
  renewal ceiling, or its reply was lost after it finished.
* ``worker/context.py`` — ``call_agents`` fanned out to a target agent type
  that was not available, so that sub-task never reached a worker at all.

Both must produce the *same* message a real sub-agent would have produced,
because everything downstream (the runner's execution reattachment, the
idempotency gate, Task Group join) keys off that exact shape. Two
independent constructions of it drift, and the drift is invisible until a
caller hangs — which is why the construction lives here once rather than
being spelled out at each site.

The three load-bearing details, all of them easy to get backwards:

* ``header.message_id`` is the **caller's** message id. ``WorkerRunner``
  reattaches the suspended execution with
  ``get_execution_by_message_id(header.message_id)``, so any other value
  starts a fresh, disconnected execution instead of resuming the caller.
* ``header.parent_message_id`` is the **sub-task's** dispatch-time message
  id. It is the only per-sibling-unique id a reply carries, so Task Group
  join keys results by it and ``core/wait_index.member_from_resume`` rebuilds
  the wait-index member from it.
* The failure detail rides in ``reply_data``, where a sub-agent that ran and
  raised puts it (``GatewayWorker._handle_message``'s error path). A caller
  must not be able to tell "failed" from "never got to fail"; putting the
  error anywhere else grows a second error path in every caller.
"""

from __future__ import annotations

import uuid
from typing import Any, Optional

from by_framework.common.logger import logger
from by_framework.core.protocol.commands import ResumeCommand
from by_framework.core.protocol.message_header import MessageHeader

# Who built a stand-in, carried on the reply's metadata (never in
# reply_data, which is business-visible payload).
SYNTHESIZED_BY_SWEEPER = "wait_sweeper"
SYNTHESIZED_BY_DISPATCH = "dispatch"


def failure_reply_data(
    *, error: str, error_code: str, child_message_id: str
) -> dict[str, Any]:
    """The ``reply_data`` of a stand-in failure.

    ``child_message_id`` is duplicated out of the header so an operator
    reading the payload alone can still identify the sub-task that vanished.
    """
    return {
        "error": error,
        "error_code": error_code,
        "child_message_id": child_message_id,
    }


def stand_in_reply(
    *,
    session_id: str,
    caller_message_id: str,
    caller_agent_type: str,
    child_message_id: str,
    child_agent_type: str = "",
    task_group_id: str = "",
    trace_id: str = "",
    status: str,
    content: Any = "",
    reply_data: Any = None,
    extra_payload: Optional[dict] = None,
    metadata: Optional[dict] = None,
    error_code: str = "",
    synthesized_by: str,
    user_code: str = "",
    user_name: str = "",
) -> ResumeCommand:
    """Build the reply the callee would have sent, addressed at its caller.

    Field-for-field the same command ``GatewayWorker._enqueue_agent_return``
    produces — see the module docstring for the three ids that must not be
    swapped. It is deliberately *not* marked in ``reply_data``: business code
    reads that, and a caller that behaves differently for a synthesized
    failure than for a real one has two error paths again. The provenance
    goes on ``header.metadata`` instead, for operators.

    A reply carrying ``task_group_id`` is stored and counted by the group's
    existing join like any other, so an orphaned member resolves the caller
    only when it is the last sibling outstanding.
    """
    merged_metadata = dict(metadata or {})
    merged_metadata.update(
        {
            "synthesized_by": synthesized_by,
            "liveness_error_code": error_code,
            "child_message_id": child_message_id,
        }
    )
    return ResumeCommand(
        header=MessageHeader(
            message_id=caller_message_id,
            session_id=session_id,
            trace_id=trace_id or uuid.uuid4().hex,
            source_agent_type=child_agent_type,
            target_agent_type=caller_agent_type,
            parent_message_id=child_message_id,
            task_group_id=task_group_id,
            user_code=user_code,
            user_name=user_name,
            metadata=merged_metadata,
        ),
        status=status,
        content=content,
        reply_data=reply_data,
        extra_payload=dict(extra_payload or {}),
    )


async def flush_pending_group_replies(
    redis: Any, context: Any, worker_id: str = ""
) -> None:
    """Send the stand-ins ``call_agents`` queued, once the caller is done.

    ``AgentContext.call_agents`` queues a reply for every sub-task whose
    target agent type was unavailable instead of sending it inline. Sending
    inline would put a reply on the caller's own control stream strictly
    *before* the caller's handler returns and its execution is recorded as
    suspended — turning the pre-existing "a very fast sub-agent replies first"
    race from unlikely into certain. Flushing after the handler returns leaves
    that race exactly as bad as it is for real sub-agents, and no worse.

    Only called when the handler returned normally. If it raised, the group
    was already marked aborted and these replies must not go out at all: the
    caller execution they would resume is the one that just failed.

    Fail-soft per reply: a stand-in that cannot be delivered costs this group
    its dispatch-time failure notice — which the wait index and its sweep can
    still compensate — whereas raising here would also destroy the caller's
    own result.
    """
    pending = getattr(context, "_pending_group_replies", None)
    if not pending:
        return
    replies = list(pending)
    pending.clear()
    from by_framework.common.constants import RedisKeys

    for reply in replies:
        try:
            await redis.xadd(
                RedisKeys.ctrl_stream(reply.header.target_agent_type),
                reply.to_redis_payload(),
            )
        except Exception as error:  # pylint: disable=broad-exception-caught
            logger.error(
                "[%s] Failed to deliver the dispatch-failure reply for Task "
                "Group %s sub-task %s: %s",
                worker_id,
                reply.header.task_group_id,
                reply.header.parent_message_id,
                error,
            )
