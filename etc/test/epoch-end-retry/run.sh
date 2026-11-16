#!/usr/bin/env bash
#
# Reproduces #397: what one failed walk to an epoch end costs a node catching up.
#
# Starts 4 validators and 1 observer in a container whose loopback adds 50 ms each way (100 ms round
# trip), with 50 tx/s and 60 s epochs. Each round the observer stops, falls 1500 blocks behind and
# restarts twice: once as is ("none"), and once with RL_EPOCH_END_FAULT set ("refuse"), so the
# first walk to an epoch end fails its fetch of that header once (3 attempts, each refused at
# once). BIN must be built with `--features epoch-end-fault`.
#
# usage: run.sh OUTDIR BIN [ROUNDS]     (3 rounds take about 45 minutes)
#
# Prints one result line per run, then whether the fault fired in each "refuse" run.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
[[ $# -ge 2 ]] || { echo "usage: $0 OUTDIR BIN [ROUNDS]"; exit 2; }
OUT=$1 BIN=$2 ROUNDS=${3:-3}
grep -qa "EPOCH END FAULT" "$BIN" || { echo "$BIN was not built with --features epoch-end-fault"; exit 2; }

"$HERE/bench/run.sh" 50 "$OUT" -v "$BIN" \
    -b none="$BIN" -b refuse="$BIN" -x refuse=RL_EPOCH_END_FAULT=1 \
    -t 50 -g 1500 -e 60 -m 1 -r "$ROUNDS" 2>&1 | grep -E "warm-up|round|INCONCLUSIVE|done"
echo "fault fired (a refuse run without it did not test the failure):"
for f in "$OUT"/observer-1-*-refuse.log; do
    [[ -e $f ]] || continue
    if grep -q "EPOCH END FAULT: failing" "$f"; then fired=yes; else fired=NO; fi
    printf '  %s: %s\n' "$(basename "$f" .log)" "$fired"
done
find "$OUT" -mindepth 1 -maxdepth 1 -type d -exec rm -rf {} +
