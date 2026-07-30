# Guard authoring — turning a rule into an enforced check

Canonical home for "how do I turn a rule into a guard." `archaeology.md`
(inside the codebase-map-init skill) and this repo's `CLAUDE.md` "Maintaining this
map" section both point here instead of restating it. Applies whether the
rule came from mining an incident, or from someone stating a known
requirement up front — no incident required.

## Picking a pattern

| Pattern | When |
|---|---|
| Pattern grep (ban a spelling) | Violation has a stable textual signature |
| AST scan | Violation spans lines or hides behind variables |
| Shrink-only allowlist | New rule, legacy violators exist (below) |
| Freshness/drift test | A generated artifact is committed next to its source |
| Structural cap | Backstop for bloat that routes around prose |

## Writing the guard

- **Header** states: the rule, the incident it came from (or, for a
  proactively-stated rule with no incident yet, one line on why it
  matters), the guard's own limits, and what backstops it.
- **The error message teaches the fix** — never just "FAIL", always show
  the correct form.
- **Total `verify.sh` wallclock stays under 30s** — a chronically slow gate
  eventually gets skipped by contributors, and a gate nobody runs doesn't
  exist. `verify.sh` measures this and WARNs past budget, but has no
  per-guard timeout of its own — a hung guard blocks the whole run.
- **Register it in two places, not one**: append a line to `verify.sh`'s
  `CHECKS` array (this is what enforces it), and add a quad-bullet to
  CLAUDE.md's Iron Rules / Cross-cutting invariants pointing at the guard
  (this is what explains why). A guard with no CLAUDE.md bullet is
  invisible until it fails; a CLAUDE.md bullet with no guard is a promise
  nobody checks.

## Shrink-only allowlist

Ship a new guard the same day despite legacy violations: enumerate
violators in `scripts/check-<x>.allowlist`, header line "MUST shrink —
never add"; the guard passes allowlisted files and fails any new
violation. Debt stays explicit and pays down file by file — never wait for
a clean slate to enable a rule.

## Adding a rule you already know about (no incident needed)

Trigger 5 in CLAUDE.md's "Maintaining this map": you don't have to wait
for a rule to bite twice before it earns a place in the map. State it,
then triage it right here, same three-way split as every other find:

- **Machine can check it** (a deploy step, a required file, a naming
  convention) → write the guard per this page, add it to `verify.sh`.
- **Only judgement can check it** (a design tradeoff, a "consider X before
  Y") → a quad-bullet in CLAUDE.md's Iron Rules / Cross-cutting
  invariants, no guard. Already two-plus bullets on this same concern?
  Lift the cluster into a `docs/` file instead — CLAUDE.md's "Maintaining
  this map" trigger 5 has the full rule, not restated here.
- **Domain detail, not cross-cutting** → a `docs/` file + a Reference-map
  row in CLAUDE.md.

No codebase-map-init re-run needed for any of the three — this recipe lives in your
repo now, not only inside the skill.
