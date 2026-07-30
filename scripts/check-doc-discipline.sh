#!/usr/bin/env bash
# Meta-guard for the map. Elsewhere, a resident instruction file grew to
# ~147k tokens once its index went append-only; a prose rule failed to stop
# it. This guard makes recurrence structural.
#
# HARD GATES (exit 1):
#   1. Resident size cap — CLAUDE.md <= MAP_CLAUDE_MAX_BYTES. The resident
#      layer loads every session; bloat here taxes every turn.
#   2. Version-narrative ban in current-state docs — both the bolded
#      `**vX.Y` marker AND version-cluster headings (`## ... vX.Y ...`).
#      History belongs in the changelog + git. If the heading gate
#      false-fires on a legitimate heading in your repo, loosen ITS regex —
#      don't drop the gate.
#
# SOFT WARNS (stderr, non-fatal): prose history markers suggesting
# narration creeping back.
#
# Env:
#   MAP_ROOT              repo root         (default: git toplevel)
#   MAP_RESIDENT_FILE     resident doc      (default: CLAUDE.md; AGENTS.md for
#                         a Codex-targeted install — see MAP_AGENTS in
#                         .map.conf. If both are installed, one is a symlink
#                         to the other, so checking either checks the same
#                         content.)
#   MAP_CLAUDE_MAX_BYTES  resident cap      (default: 40000)
#   MAP_REFERENCE_DOCS    space-separated current-state docs
#                         (default: "docs/architecture/KEY_FILES.md docs/TESTING.md")

set -uo pipefail

# Layout params: env → scripts/.map.conf → the built-in defaults below.
_MAP_DIR="$(cd "$(dirname "$0")" && pwd)"
[ -f "$_MAP_DIR/.map.conf" ] && . "$_MAP_DIR/.map.conf"

ROOT="${MAP_ROOT:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
RESIDENT="${MAP_RESIDENT_FILE:-CLAUDE.md}"
MAX_BYTES="${MAP_CLAUDE_MAX_BYTES:-40000}"
DOCS="${MAP_REFERENCE_DOCS:-docs/architecture/KEY_FILES.md docs/TESTING.md}"

fail=0

# ── Gate 1: resident size cap ─────────────────────────────────────────────
f="$ROOT/$RESIDENT"
if [ -f "$f" ]; then
  bytes=$(wc -c < "$f")
  if [ "$bytes" -gt "$MAX_BYTES" ]; then
    fail=1
    echo "FAIL: $RESIDENT is ${bytes}B (cap ${MAX_BYTES}B)." >&2
    echo "      Move detail into docs/ and add a Reference-map row;" >&2
    echo "      delete history — git has it." >&2
  fi
fi

# ── Gate 2: version narrative in current-state docs ───────────────────────
for rel in $DOCS; do
  doc="$ROOT/$rel"
  [ -f "$doc" ] || continue
  hits=$(grep -nE '\*\*v[0-9]+\.[0-9]|^#{1,6} .*\bv[0-9]+\.[0-9]+' "$doc" || true)
  if [ -n "$hits" ]; then
    fail=1
    echo "FAIL: $rel carries version narrative (append-only history is the" >&2
    echo "      decay this guard prevents). Rewrite each chain into the" >&2
    echo "      single current truth; history goes to the changelog + git." >&2
    echo "      Offending lines:" >&2
    printf '%s\n' "$hits" | sed 's/^/        /' | cut -c1-140 >&2
  fi
  warns=$(grep -nE 'pre-fix|superseded by|, then v[0-9]' "$doc" || true)
  if [ -n "$warns" ]; then
    echo "WARN: $rel has history-flavoured prose:" >&2
    printf '%s\n' "$warns" | sed 's/^/        /' | cut -c1-140 >&2
  fi
done

[ "$fail" -eq 0 ] && echo "OK: map discipline holds"
exit "$fail"
