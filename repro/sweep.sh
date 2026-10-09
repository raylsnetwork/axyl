#!/bin/bash
# Run the two-certificate reproduction at several network sizes, one after another.
# Records, per N, whether two certificates formed for one round and whether nodes equivocated.
# This file lives only on the repro worktree and is never committed.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
OUT="$HERE/sweep-results.txt"
: > "$OUT"
for N in "$@"; do
  echo "######## N=$N $(date -u +%T) ########" | tee -a "$OUT"
  pkill -9 -f "datadir.*/repro/runs/live/validator" 2>/dev/null || true
  sleep 2
  log="$HERE/runs/two_cert-N$N.log"
  bash "$HERE/two_cert.sh" "$N" > "$log" 2>&1 || true
  two=$(grep -c "TWO DIFFERENT CERTIFICATES" "$log" 2>/dev/null || echo 0)
  eq=$(grep -rh "CertificateEquivocation" "$HERE"/runs/live/validator-*.log 2>/dev/null | grep -c "C[0-9]" || echo 0)
  nodes_eq=$(grep -rl "CertificateEquivocation" "$HERE"/runs/live/validator-*.log 2>/dev/null | wc -l)
  pair=$(grep "TWO DIFFERENT CERTIFICATES" "$log" 2>/dev/null | tail -1)
  echo "N=$N two_cert_lines=$two equivocation_lines=$eq nodes_equivocated=$nodes_eq" | tee -a "$OUT"
  [[ -n "$pair" ]] && echo "   $pair" | tee -a "$OUT"
  grep -m1 "did not observe\|never assembled\|never proposed" "$log" 2>/dev/null | sed 's/^/   /' | tee -a "$OUT" || true
done
pkill -9 -f "datadir.*/repro/runs/live/validator" 2>/dev/null || true
echo "######## sweep done $(date -u +%T) ########" | tee -a "$OUT"
