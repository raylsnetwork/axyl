#!/usr/bin/env bash
#
# The fault-tolerance case in PR #371: two binaries, each built with outage-hook.patch, catch up
# 1500 blocks with 60 s epochs (100 ms round trip, 50 tx/s, 1 observer) three ways: with no outage,
# with every consensus header request failing at once for 10 s, and with each request hanging for
# the 10 s request timeout before it fails. The outage starts 5 s after the observer's first
# consensus header request. Takes about an hour.
#
# usage: outage.sh OUTDIR LABEL=BIN LABEL=BIN [ROUNDS]
#
# The first binary also runs the validators, with no outage. The run writes OUTDIR/summary.txt and
# keeps the logs; node datadirs are deleted at the end to save disk.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
[[ $# -ge 3 && $2 == *=* && $3 == *=* ]] || { echo "usage: $0 OUTDIR LABEL=BIN LABEL=BIN [ROUNDS]"; exit 2; }
OUT=$1 A=$2 B=$3 ROUNDS=${4:-2}
FAST="RL_BENCH_OUTAGE_START_MS=5000,RL_BENCH_OUTAGE_MS=10000"
SILENT="$FAST,RL_BENCH_OUTAGE_SILENT=1"

ARGS=()
for kind in "" fast silent; do
    for pair in "$A" "$B"; do
        label=${pair%%=*}${kind:+-$kind} bin=${pair#*=}
        ARGS+=(-b "$label=$bin")
        [[ $kind == fast ]] && ARGS+=(-x "$label=$FAST")
        [[ $kind == silent ]] && ARGS+=(-x "$label=$SILENT")
    done
done

"$HERE/run.sh" 50 "$OUT" -v "${A#*=}" "${ARGS[@]}" -t 50 -g 1500 -e 60 -m 1 -r "$ROUNDS" 2>&1 |
    grep -E "warm-up|round|INCONCLUSIVE|done"
echo "outage failures per run (each line in a log is one failed request attempt):"
for f in "$OUT"/observer-1-*-*.log; do
    printf '  %s: %s\n' "$(basename "$f" .log)" "$(grep -c 'BENCH OUTAGE' "$f")"
done
find "$OUT" -mindepth 1 -maxdepth 1 -type d -exec rm -rf {} +
