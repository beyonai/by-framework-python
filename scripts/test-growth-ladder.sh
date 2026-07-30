#!/usr/bin/env bash
# Growth-ladder regression harness. Proves the three-tier growth ladder still
# fires correctly. It's both the codebase-map-init author's red line (run before/after
# any change to the guard scripts — behaviour must stay identical) AND a
# shippable self-test any mapped repo can run to prove its ladder isn't dead.
#
# Builds a throwaway git repo, exercises every signal, asserts exit codes and
# output. Zero dependency on the host repo's layout — it sets its own
# .map.conf. Exit 0 = all pass; first failure prints and aborts.
#
# Usage:
#   MAP_TEMPLATES=/abs/path/to/templates bash test-growth-ladder.sh
#   (defaults MAP_TEMPLATES to this script's own directory)

set -uo pipefail

TPL="${MAP_TEMPLATES:-$(cd "$(dirname "$0")" && pwd)}"
pass=0; fail=0
ok()   { pass=$((pass+1)); echo "  ok   — $1"; }
bad()  { fail=$((fail+1)); echo "  FAIL — $1"; echo "         $2"; }

# assert_exit <expected> <label> <cmd...>
assert_exit() {
  local want="$1" label="$2"; shift 2
  local out rc
  out=$("$@" 2>&1); rc=$?
  if [ "$rc" = "$want" ]; then ok "$label (exit $rc)"; else bad "$label" "want exit $want, got $rc: $out"; fi
}
# assert_contains <needle> <label> <cmd...>
assert_contains() {
  local needle="$1" label="$2"; shift 2
  local out
  out=$("$@" 2>&1)
  case "$out" in *"$needle"*) ok "$label" ;; *) bad "$label" "missing '$needle' in: $out" ;; esac
}

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"
git init -q .; git config user.email t@t; git config user.name t
mkdir -p docs scripts src .claude

# Install guards from templates into the fixture.
for f in check-doc-discipline.sh check-map-territory.sh check-entry-freshness.sh map-stop-hook.sh verify.sh; do
  cp "$TPL/$f" scripts/ 2>/dev/null || { echo "cannot copy $f from $TPL"; exit 2; }
done
chmod +x scripts/*.sh

# Fixture layout, declared once — the guards source it, exactly as in a real
# install.
cat > scripts/.map.conf <<'CONF'
: "${MAP_KEY_FILES:=docs/KEY_FILES.md}"
: "${MAP_REFERENCE_DOCS:=docs/KEY_FILES.md}"
: "${MAP_SOURCE_GLOBS:=src/}"
: "${MAP_CLAUDE_MAX_BYTES:=40000}"
CONF

cat > docs/KEY_FILES.md <<'EOF'
# Key files
- `src/a.py` — role. invariant precise to the expression level.
<!-- skip: src/skip.py — trivial helper -->
EOF
printf '# CLAUDE.md\nSee `docs/KEY_FILES.md`\n' > CLAUDE.md
echo "x=1" > src/a.py
git add -A; git commit -qm "init map"

echo "T1 — stale entry: indexed file edited, index untouched"
echo "y=2" >> src/a.py
assert_contains "WARN" "T1 soft warn present"        bash scripts/check-entry-freshness.sh
assert_exit    0       "T1 soft is non-blocking"     bash scripts/check-entry-freshness.sh
assert_exit    1       "T1 strict blocks"            env MAP_ENTRY_FRESHNESS_STRICT=1 bash scripts/check-entry-freshness.sh
git checkout -q -- src/a.py

echo "T2 — unindexed new source file warns; skip-comment silences"
echo "n" > src/new.py; echo "s" > src/skip.py; git add -A
assert_contains "src/new.py" "T2 new file flagged"   bash scripts/check-entry-freshness.sh
out=$(bash scripts/check-entry-freshness.sh 2>&1); case "$out" in *src/skip.py*) bad "T2 skip silenced" "skip.py wrongly flagged";; *) ok "T2 skip-comment silences";; esac
git reset -q; rm -f src/new.py src/skip.py

echo "T2b — untracked (never git add'ed) new source file is also flagged"
echo "n" > src/untracked.py
assert_contains "src/untracked.py" "T2b untracked file flagged" bash scripts/check-entry-freshness.sh
rm -f src/untracked.py

echo "T3 — untriaged fix commit is advisory only (strict must NOT block)"
echo "z=3" > src/b.py; git add -A; git commit -qm "fix: boom"
assert_exit 0 "T3 fix advisory, strict non-blocking" env MAP_ENTRY_FRESHNESS_STRICT=1 bash scripts/check-entry-freshness.sh

echo "T4 — Stop hook: advisory-only passes, stale blocks once, re-stop passes"
assert_exit 0 "T4 hook passes with only advisory"    bash -c 'echo "{\"stop_hook_active\":false}" | bash scripts/map-stop-hook.sh'
echo "w" >> src/a.py
assert_exit 2 "T4 hook blocks on stale entry"        bash -c 'echo "{\"stop_hook_active\":false}" | bash scripts/map-stop-hook.sh'
assert_exit 0 "T4 re-stop (hook_active) never loops" bash -c 'echo "{\"stop_hook_active\":true}" | bash scripts/map-stop-hook.sh'
git checkout -q -- src/a.py

echo "T5 — clean once code+map move together"
printf -- '- `src/b.py` — role. invariant.\n' >> docs/KEY_FILES.md
git add -A; git commit -qm "map: index b.py"
assert_contains "OK" "T5 green after map-maintained commit" bash scripts/check-entry-freshness.sh

echo
echo "growth-ladder: $pass passed, $fail failed"
[ "$fail" = 0 ]
