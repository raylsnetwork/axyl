#!/usr/bin/env bash
#
# Reproduces #399: how long a node catching up takes to produce its first new block, and the load
# catch-up puts on the validators. Compares two binaries, for example `main` and this branch.
#
# Starts 4 validators and observers in a container whose loopback adds 50 ms each way (100 ms
# round trip), with 50 tx/s. The observers stop, fall 1500 blocks behind and restart, in three
# cases:
#   one-end-obs1: 240 s epochs, so the gap holds one epoch end; 1 observer, 3 rounds
#   one-end-obs3: the same with 3 observers catching up together, 2 rounds
#   no-end-obs1:  10-hour epochs, so no epoch end in the gap; 1 observer, 2 rounds
#
# usage: run.sh OUTDIR LABEL=BIN LABEL=BIN     (about 75 minutes)
#
# The first binary also runs the validators. Each case writes OUTDIR/<case>/summary.txt.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
[[ $# -eq 3 && $2 == *=* && $3 == *=* ]] || { echo "usage: $0 OUTDIR LABEL=BIN LABEL=BIN"; exit 2; }
OUT=$1 A=$2 B=$3
mkdir -p "$OUT"

case_run() {
    local name=$1; shift
    echo "=== $name $(date -u +%T)"
    "$HERE/bench/run.sh" 50 "$OUT/$name" -v "${A#*=}" -b "$A" -b "$B" -t 50 -g 1500 "$@" 2>&1 |
        grep -E "warm-up|round|INCONCLUSIVE|done"
    find "$OUT/$name" -mindepth 1 -maxdepth 1 -type d -exec rm -rf {} +
}

case_run one-end-obs1 -e 240 -m 1 -r 3
case_run one-end-obs3 -e 240 -m 3 -r 2
case_run no-end-obs1 -e 36000 -m 1 -r 2
