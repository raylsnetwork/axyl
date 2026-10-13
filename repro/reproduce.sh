#!/bin/bash
# Reproduce the power-loss equivocation halt on a live N-validator network, retrying until it lands.
#
# The durability gap itself is deterministic: a vote/header/certificate made durable before it is
# sent is committed but not fsynced under MDBX SafeNoSync, so a power loss (modelled here by
# restoring the pre-write datafile snapshot) drops it even though peers already saw it. Whether that
# gap then ESCALATES to two certificates for one round and a CertificateEquivocation halt depends on
# a consensus-timing race (does the restarted author rebuild the lost round before it re-fetches its
# own certificate 1 from the surviving holder?). Per run that lands ~1 in 3, so this wrapper repeats
# the scenario until the halt is observed, making the end-to-end reproduction reliable on demand.
#
# Usage: reproduce.sh [N] [max_attempts]   (defaults: N=4, max_attempts=8)
# This file lives only on the repro worktree and is never committed.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
cd "$HERE/.." || exit 1
N=${1:-4}; MAX=${2:-8}
# Per-attempt logs land under the gitignored runs/ dir, so a fresh clone is self-contained.
EVID="$HERE/runs/evidence"
mkdir -p "$EVID"

kill_nodes() {
  for n in $(seq 1 "$N"); do p=$(cat repro/runs/live/validator-$n.pid 2>/dev/null); [ -n "$p" ] && kill -9 "$p" 2>/dev/null; done
  for pid in $(ps -eo pid,cmd | grep '[r]ayls-network node' | awk '{print $1}'); do
    c=$(readlink /proc/$pid/cwd 2>/dev/null); case "$c" in *axyl-power-loss-durability*) kill -9 "$pid" 2>/dev/null;; esac
  done
  for p in $(ps -eo pid,cmd | grep "[n]et.sh start $N" | awk '{print $1}'); do kill -9 "$p" 2>/dev/null; done
  sleep 3
}

for a in $(seq 1 "$MAX"); do
  echo "########## ATTEMPT $a/$MAX (N=$N) ##########"
  kill_nodes
  rl="$EVID/reproduce_${N}_attempt_${a}.log"
  nice -n 19 ./repro/power_loss.sh "$N" powerloss > "$rl" 2>&1 </dev/null
  v=$(grep -E "^VERDICT:" "$rl" | tail -1)
  echo "attempt $a => $v"
  if echo "$v" | grep -q "PASS"; then
    echo "########## REPRODUCED on attempt $a/$MAX (N=$N) ##########"
    grep -E "REPRODUCED:|frozen|validator-[0-9]+ (alive|[0-9]+ 0x|down)" "$rl" | tail -8
    echo "evidence: $rl"
    kill_nodes
    exit 0
  fi
done
kill_nodes
echo "########## NOT reproduced in $MAX attempts (N=$N); rerun or raise max_attempts ##########"
exit 1
