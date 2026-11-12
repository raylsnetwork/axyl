#!/usr/bin/env bash
#
# The cases behind the catch-up numbers in PR #371: 100 ms round trip, 50 tx/s, 1 and 3 observers,
# gaps of about 30, 1500 and 6000 blocks with 10-hour epochs (no epoch end inside the gap), and a
# 1500-block gap with 60 s epochs (several epoch ends inside the gap). Takes about 3 hours.
#
# usage: matrix.sh OUTDIR LABEL=BIN [LABEL=BIN ...]
#
# The first binary also runs the validators. Each case writes OUTDIR/<case>/summary.txt and keeps
# the logs; node datadirs are deleted after each case to save disk.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
[[ $# -ge 2 ]] || { echo "usage: $0 OUTDIR LABEL=BIN [LABEL=BIN ...]"; exit 2; }
OUT=$1
shift
mkdir -p "$OUT"
BINS=()
for b in "$@"; do BINS+=(-b "$b"); done
VALIDATOR=${1#*=}

case_run() {
    local name=$1; shift
    echo "=== $name $(date -u +%T), load $(cut -d' ' -f1-3 /proc/loadavg)"
    "$HERE/run.sh" 50 "$OUT/$name" -v "$VALIDATOR" "${BINS[@]}" -t 50 "$@" 2>&1 |
        grep -E "warm-up|round|INCONCLUSIVE|done"
    find "$OUT/$name" -mindepth 1 -maxdepth 1 -type d -exec rm -rf {} +
}

case_run gap30-obs1 -g 20 -r 3 -m 1
case_run gap1500-obs1 -g 1500 -r 2 -m 1
case_run gap1500-obs3 -g 1500 -r 1 -m 3
case_run gap6000-obs1 -g 6000 -r 1 -m 1
case_run epoch60-gap1500-obs1 -e 60 -g 1500 -r 1 -m 1
case_run epoch60-gap1500-obs3 -e 60 -g 1500 -r 1 -m 3
echo "matrix done $(date -u +%T)"
