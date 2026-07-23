#!/usr/bin/env bash
# Growth reminder (tier 2 of the growth ladder). Three signals:
#
#   1. STALE ENTRY (blocking under STRICT): a change set touches an indexed
#      source file but not the index — the entry may be stale.
#   2. UNINDEXED NEW FILE (advisory): a new source file has no presence in
#      the index at all — neither an entry nor a mention in the skip-reason
#      comment block. Silence it by adding either.
#   3. UNTRIAGED FIX (advisory): the last commit looks like a fix/revert but
#      carries no map artifact (index, CLAUDE.md, docs/incidents/,
#      scripts/check-*) — a lesson may be evaporating; consider triage.
#
# SOFT by default: not every edit changes behaviour, and a hard gate breeds
# perfunctory index edits — decay of its own kind. MAP_ENTRY_FRESHNESS_STRICT=1
# turns signal 1 (only) into a failure — that's what the tier-3 Stop hook
# uses to block a session's end; signals 2–3 stay advisory even then, since
# their heuristics are fuzzier and a spurious block teaches hook-dodging.
#
# Change sets, each evaluated independently (so a fresh map commit can't
# mask new uncommitted work):
#   MAP_BASE_REF set + resolvable → one set: merge-base diff vs HEAD + uncommitted
#   otherwise                     → two sets: uncommitted; the last commit
#
# Env:
#   MAP_KEY_FILES                index (default: docs/architecture/KEY_FILES.md)
#   MAP_SOURCE_GLOBS             space-separated source prefixes for signal 2
#                                (default: "src/")
#   MAP_BASE_REF                 e.g. origin/master (CI)
#   MAP_ENTRY_FRESHNESS_STRICT   1 → exit 1 when signal 1 fires

set -uo pipefail

# Layout params: env → scripts/.map.conf → the built-in defaults below.
_MAP_DIR="$(cd "$(dirname "$0")" && pwd)"
[ -f "$_MAP_DIR/.map.conf" ] && . "$_MAP_DIR/.map.conf"

cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"

KEY="${MAP_KEY_FILES:-docs/architecture/KEY_FILES.md}"
GLOBS="${MAP_SOURCE_GLOBS:-src/}"
[ -f "$KEY" ] || { echo "OK: no key-files index yet"; exit 0; }

indexed=$(grep -oE '^- `[^`]+`' "$KEY" | sed 's/^- `//; s/`$//' | sort -u)

# ── Signal 1: stale entries ───────────────────────────────────────────────
# Emits the indexed files a change set touches, unless the set also touches
# the index itself (that set is then considered maintained).
stale_in() {
  local set
  set=$(printf '%s\n' "$1" | sed '/^$/d' | sort -u)
  [ -n "$set" ] || return 0
  printf '%s\n' "$set" | grep -qxF "$KEY" && return 0
  comm -12 <(printf '%s\n' "$indexed") <(printf '%s\n' "$set")
}

# git diff never sees untracked files (it only compares tracked content), so
# a new file the agent never `git add`ed would otherwise be invisible to
# signal 2 below — add it to the `added` set explicitly.
untracked=$(git ls-files --others --exclude-standard 2>/dev/null)

if [ -n "${MAP_BASE_REF:-}" ] && git rev-parse -q --verify "${MAP_BASE_REF}^{commit}" >/dev/null 2>&1; then
  base=$(git merge-base "$MAP_BASE_REF" HEAD)
  stale=$(stale_in "$( { git diff --name-only "$base" HEAD; git diff --name-only HEAD; } 2>/dev/null )")
  added=$( { git diff --name-only --diff-filter=A "$base" HEAD; git diff --name-only --diff-filter=A HEAD; printf '%s\n' "$untracked"; } 2>/dev/null )
else
  stale=$( { stale_in "$(git diff --name-only HEAD 2>/dev/null)"
             stale_in "$(git log -1 --format= --name-only 2>/dev/null)"; } | sed '/^$/d' | sort -u )
  added=$( { git diff --name-only --diff-filter=A HEAD; git log -1 --format= --name-only --diff-filter=A; printf '%s\n' "$untracked"; } 2>/dev/null )
fi

warned=0
if [ -n "$stale" ]; then
  warned=1
  echo "WARN: change set touches indexed files but not $KEY:"
  printf '%s\n' "$stale" | sed 's/^/        /'
  echo "      If behaviour changed, rewrite each file's entry in place;"
  echo "      if it did not, this warn is safe to ignore."
fi

# ── Signal 2: unindexed new source files ──────────────────────────────────
for f in $(printf '%s\n' "$added" | sed '/^$/d' | sort -u); do
  case "$f" in *test*|*.md) continue ;; esac
  hit=0
  for g in $GLOBS; do case "$f" in "$g"*) hit=1 ;; esac; done
  [ "$hit" = 1 ] || continue
  if ! grep -qF "$f" "$KEY"; then
    warned=1
    echo "WARN: new source file has no presence in $KEY: $f"
    echo "      Add an entry if it's load-bearing, or a skip reason in the"
    echo "      index's comment block."
  fi
done

# ── Signal 3: untriaged fix commit ────────────────────────────────────────
subj=$(git log -1 --format=%s 2>/dev/null || true)
case "$subj" in
  fix*|Fix*|revert*|Revert*|*修复*|*回滚*)
    last=$(git log -1 --format= --name-only | sed '/^$/d')
    if ! printf '%s\n' "$last" | grep -qE '^(CLAUDE\.md$|docs/incidents/|scripts/check-)' \
       && ! printf '%s\n' "$last" | grep -qxF "$KEY"; then
      warned=1
      echo "WARN: last commit looks like a fix (\"$subj\") but carries no map"
      echo "      artifact — a lesson may be evaporating. Consider triage:"
      echo "      machine-checkable → guard; cross-cutting → invariant quad;"
      echo "      worth a postmortem → docs/incidents/. Or: nothing to keep."
    fi ;;
esac

if [ "${MAP_ENTRY_FRESHNESS_STRICT:-0}" = "1" ] && [ -n "$stale" ]; then
  exit 1
fi
[ "$warned" = 0 ] && echo "OK: entry freshness holds"
exit 0
