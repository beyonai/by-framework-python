"""Unit tests for the inbound resume-metadata merge rule."""

from by_framework.worker._resume_metadata import (
    FRAMEWORK_HOP_METADATA_KEYS,
    merge_resume_metadata,
)


def test_original_dispatch_metadata_is_the_base():
    """What the execution was dispatched with survives the resume."""
    merged = merge_resume_metadata({"tenant": "acme", "req": "r-1"}, {})
    assert merged == {"tenant": "acme", "req": "r-1"}


def test_waking_message_metadata_is_layered_on_top():
    merged = merge_resume_metadata({"tenant": "acme"}, {"answer": "Pink"})
    assert merged == {"tenant": "acme", "answer": "Pink"}


def test_waking_message_wins_on_collision():
    """The newer, more specific hop overrides — never the other way round."""
    merged = merge_resume_metadata({"tag": "dispatch"}, {"tag": "waking"})
    assert merged["tag"] == "waking"


def test_framework_hop_keys_are_never_restored_from_the_snapshot():
    """Stale span ids must not surface where this hop supplied none.

    They describe the dispatch that created the execution, not the hop
    resuming it, and AgentContext's langfuse fallback reads them straight off
    the current command.
    """
    stored = {
        "tenant": "acme",
        "trace_parent_span_id": "stale-trace",
        "framework_parent_span_id": "stale-framework",
        "langfuse_parent_observation_id": "stale-langfuse",
    }
    merged = merge_resume_metadata(stored, {})
    assert merged == {"tenant": "acme"}
    for key in FRAMEWORK_HOP_METADATA_KEYS:
        assert key not in merged


def test_framework_hop_keys_from_the_waking_message_are_kept():
    """Only the stored copy is stale; this hop's own values are current."""
    merged = merge_resume_metadata(
        {"trace_parent_span_id": "stale"},
        {"trace_parent_span_id": "current"},
    )
    assert merged == {"trace_parent_span_id": "current"}


def test_missing_snapshot_degrades_to_the_waking_message():
    """Records written before this field existed, or by another SDK."""
    assert merge_resume_metadata(None, {"client_tag": "t"}) == {"client_tag": "t"}
    assert merge_resume_metadata({}, {"client_tag": "t"}) == {"client_tag": "t"}


def test_both_sides_missing_is_an_empty_dict():
    assert merge_resume_metadata(None, None) == {}


def test_neither_input_is_mutated():
    stored = {"tenant": "acme"}
    incoming = {"answer": "Pink"}
    merge_resume_metadata(stored, incoming)
    assert stored == {"tenant": "acme"}
    assert incoming == {"answer": "Pink"}
