#!/usr/bin/env bash
# Map–territory consistency: every doc path the resident file points at must
# exist. A moved or deleted doc that leaves a dangling pointer fails here, at
# commit time — not at the moment an agent needs the material.
#
# Scans backtick-quoted paths like `docs/.../file.md` in the resident file
# (MAP_RESIDENT_FILE, default CLAUDE.md; AGENTS.md for Codex — if both are
# installed one is a symlink to the other, so this scans the same content
# either way). Extend the prefix list (docs|skills) to match your repo's
# layout.

set -uo pipefail

# Layout params: env → scripts/.map.conf → the built-in defaults below.
_MAP_DIR="$(cd "$(dirname "$0")" && pwd)"
[ -f "$_MAP_DIR/.map.conf" ] && . "$_MAP_DIR/.map.conf"

cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"

RESIDENT="${MAP_RESIDENT_FILE:-CLAUDE.md}"
[ -f "$RESIDENT" ] || { echo "OK: no $RESIDENT yet"; exit 0; }

fail=0
for p in $(grep -oE '`(docs|skills)/[A-Za-z0-9._/-]+\.md`' "$RESIDENT" | tr -d '`' | sort -u); do
  if [ ! -f "$p" ]; then
    fail=1
    echo "FAIL: $RESIDENT points at missing $p" >&2
    echo "      Fix the pointer or restore the doc — a dangling map row" >&2
    echo "      silently strands the agent." >&2
  fi
done

[ "$fail" -eq 0 ] && echo "OK: all map pointers resolve"
exit "$fail"
