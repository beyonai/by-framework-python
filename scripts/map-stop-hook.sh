#!/usr/bin/env bash
# Growth enforcement (tier 3 of the growth ladder): Claude Code Stop hook.
# If the session leaves indexed files changed without touching the key-files
# index, block the stop ONCE and hand the agent the maintenance instruction.
# The re-stop after the agent responds (stop_hook_active) always passes —
# no nag loop.
#
# Wire in the repo's committed .claude/settings.json:
#   {"hooks": {"Stop": [{"hooks": [{"type": "command",
#     "command": "bash scripts/map-stop-hook.sh"}]}]}}

set -uo pipefail

input=$(cat 2>/dev/null || true)
case "$input" in
  *'"stop_hook_active":true'* | *'"stop_hook_active": true'*) exit 0 ;;
esac

here="$(cd "$(dirname "$0")" && pwd)"
if ! out=$(MAP_ENTRY_FRESHNESS_STRICT=1 bash "$here/check-entry-freshness.sh" 2>&1); then
  {
    echo "Map maintenance before stopping (growth ladder tier 3):"
    printf '%s\n' "$out"
    echo "Rewrite the stale entries in place if behaviour changed; if it did"
    echo "not, say so explicitly, then stop."
  } >&2
  exit 2
fi
exit 0
