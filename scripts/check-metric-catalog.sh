#!/usr/bin/env bash
# Every Prometheus metric declared in src/ must have a catalog entry.
#
# metrics/catalog.py is the source of truth for metric names, units, labels and
# interpretation; dashboards and the metrics API read from it. A metric that
# ships without an entry is invisible there.
#
# Incident: metrics/collector.py's four self-monitoring metrics were never added
# to _CATALOG (fixed in #142). tests/metrics/test_catalog.py already asserted
# this rule, but it only inspects the *live* Prometheus registry and returns
# early when prometheus-client is absent — and CI's `make test` was syncing the
# root project without its `observability` extra, so the assertion ran against
# an empty registry and reported success. The rule was right; enforcing it only
# at runtime made it hostage to which optional dependencies happened to be
# installed. This guard reads the source instead, so it cannot be silenced that
# way.
#
# Limits: matches literal metric names passed to Counter/Gauge/Histogram within
# two lines of the constructor. Metric names built by string concatenation or
# f-string are not detected — none exist today, and the runtime test in
# tests/metrics/test_catalog.py remains the backstop for those.

set -uo pipefail

cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"

CATALOG="src/by_framework/metrics/catalog.py"
SOURCE_DIR="src/by_framework"

[ -f "$CATALOG" ] || { echo "OK: no $CATALOG yet"; exit 0; }

declared=$(
  grep -rh -A 2 -E '=[[:space:]]*(Counter|Gauge|Histogram)\(' \
    "$SOURCE_DIR" --include='*.py' 2>/dev/null |
    grep -oE '"by_framework_[a-z0-9_]+"' | tr -d '"' | sort -u
)

if [ -z "$declared" ]; then
  echo "WARN: no Prometheus metric declarations found under $SOURCE_DIR --" >&2
  echo "      the extraction pattern in $0 has probably drifted." >&2
  echo "OK: metric catalog (nothing to check)"
  exit 0
fi

fail=0
for name in $declared; do
  if ! grep -q "\"$name\": MetricDefinition(" "$CATALOG"; then
    fail=1
    echo "FAIL: metric '$name' is exported but missing from $CATALOG" >&2
    echo "      Add an entry so dashboards and /metrics API can describe it:" >&2
    echo "" >&2
    echo "          \"$name\": MetricDefinition(" >&2
    echo "              name=\"$name\"," >&2
    echo "              kind=MetricKind.COUNTER,  # or GAUGE / HISTOGRAM" >&2
    echo "              unit=MetricUnit.NONE,     # pick the real unit" >&2
    echo "              labels=(),                # match the declaration" >&2
    echo "              description=\"...\"," >&2
    echo "              interpretation=\"...\"," >&2
    echo "          )," >&2
    echo "" >&2
  fi
done

[ "$fail" -eq 0 ] && echo "OK: all $(echo "$declared" | wc -l | tr -d ' ') declared metrics are catalogued"
exit "$fail"
