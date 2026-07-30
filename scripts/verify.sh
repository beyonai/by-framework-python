#!/usr/bin/env bash
# Aggregator for all guards. The CHECKS array is the single source of truth:
# CI calls this script, and so do you, before every push.
#
# This is a BUDGET MONITOR, not an execution guarantee: it measures total
# wallclock after all guards finish (via bash's SECONDS) and WARNs past
# MAP_VERIFY_BUDGET_SECONDS (default 30) — a chronically slow gate
# eventually gets skipped BY CONTRIBUTORS (disabled, bypassed, routed
# around), and a gate nobody runs doesn't exist. This script has no skip
# logic of its own; the WARN is what keeps that human erosion visible
# instead of discovered by feel. Non-fatal on purpose — a flaky CI runner
# shouldn't fail the actual rules, but a durably slow guard should show up
# every run. It does NOT bound any single guard's runtime: a guard that
# hangs (infinite loop, blocked on stdin) blocks this script forever —
# there is no per-guard timeout. Adding one portably (macOS ships no GNU
# `timeout` by default) is open, tracked here rather than silently assumed
# solved.
#
# Adding a guard: append one line to CHECKS, as "$_MAP_DIR/check-<x>.sh" —
# NOT a "scripts/..." literal, which silently breaks under a custom
# --scripts-dir (this bit us once; test-installer.sh T5 pins it). If the new
# rule has legacy violators, ship it with a shrink-only allowlist (violators
# enumerated in check-<x>.allowlist next to it, header "MUST shrink — never
# add"; new violations fail immediately).

set -uo pipefail

# Layout params: env → scripts/.map.conf → the built-in defaults below.
_MAP_DIR="$(cd "$(dirname "$0")" && pwd)"
[ -f "$_MAP_DIR/.map.conf" ] && . "$_MAP_DIR/.map.conf"

cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"

CHECKS=(
  "$_MAP_DIR/check-doc-discipline.sh"
  "$_MAP_DIR/check-map-territory.sh"
  "$_MAP_DIR/check-entry-freshness.sh"
)

if [ "${#CHECKS[@]}" -eq 0 ]; then
  echo "ERROR: CHECKS is empty in verify.sh" >&2
  exit 2
fi

if [ "${1:-}" = "--dry-list" ]; then
  printf '%s\n' "${CHECKS[@]}"
  exit 0
fi

: "${MAP_VERIFY_BUDGET_SECONDS:=30}"
SECONDS=0

fail=0
for c in "${CHECKS[@]}"; do
  if out=$(bash "$c" 2>&1); then
    # soft warns must survive a green run
    warns=$(printf '%s\n' "$out" | grep -A 20 '^WARN' || true)
    [ -n "$warns" ] && printf '%s\n' "$warns" | sed "s|^|[$c] |" >&2
  else
    fail=1
    echo "FAIL: $c" >&2
    printf '%s\n' "$out" | sed 's/^/  /' >&2
  fi
done

elapsed=$SECONDS
if [ "$elapsed" -gt "$MAP_VERIFY_BUDGET_SECONDS" ]; then
  echo "WARN: verify.sh took ${elapsed}s (budget: ${MAP_VERIFY_BUDGET_SECONDS}s)." >&2
  echo "      A chronically slow gate eventually gets skipped by contributors —" >&2
  echo "      split or optimize the slow guard(s) above." >&2
fi

if [ "$fail" -eq 0 ]; then
  echo "OK: all ${#CHECKS[@]} guards green (${elapsed}s)"
fi
exit "$fail"
