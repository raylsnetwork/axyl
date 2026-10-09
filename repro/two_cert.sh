#!/bin/bash
# Drive a live N-validator network into two certificates for one author and round,
# defeating the re-propose dedup guard (certifier.rs, PR #112) using only the durability gap.
#
# The guard holds only "because a finished task is guaranteed to have already written its
# certificate". But the certificate write is queued, not awaited to disk, just like the vote and
# the header. If the author crashes with that write still in the queue, the certificate is lost,
# the guard's premise breaks, and the author rebuilds a different header for the same round.
#
# Roles (A equivocates by crashing, never by malicious code):
#   A       the author. Loses its header AND its own certificate on the crash (kept in memory only).
#   forget  voters that vote for header 1 then lose that vote AND any certificate copy on the crash.
#   holder  one honest node that keeps certificate 1 on disk. Down during the rebuild, back at the end.
#   fresh   nodes kept down during header 1, so they vote only for header 2.
#
# This file lives only on the repro worktree and is never committed.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
NET="$HERE/net.sh"
ROOT="$HERE/runs/live"
N=${1:?N}
: "${MINMS:=10000}" "${MAXMS:=20000}" "${EPOCH:=600}"
export MINMS MAXMS

Q=$(( 2*N/3 + 1 ))
A=1
HOLDER=$N
X=$(( Q - 2 )); (( X < 1 )) && X=1            # forgetful voters
forget_set=$(seq 2 $(( 1 + X )))
fresh_set=""
[[ $(( 2 + X )) -le $(( N - 1 )) ]] && fresh_set=$(seq $(( 2 + X )) $(( N - 1 )))
echo "N=$N quorum=$Q author=$A holder=$HOLDER forget=[$(echo $forget_set)] fresh=[$(echo $fresh_set)] rounds=${MINMS}-${MAXMS}ms"

log() { echo "[$(date -u +%T)] $*"; }
alog() { echo "$ROOT/validator-$1.log"; }
lines() { wc -l < "$(alog $1)" 2>/dev/null || echo 0; }
wait_log() { local n=$1 pat=$2 from=${3:-0} t=${4:-120} l;
  for _ in $(seq 1 $(( t*2 ))); do
    l=$(tail -n +$(( from+1 )) "$(alog $n)" 2>/dev/null | grep -m1 -E "$pat" || true)
    [[ -n "$l" ]] && { echo "$l"; return 0; }
    sleep 0.5
  done; return 1; }
kill9() { local pid; pid=$(cat "$ROOT/validator-$1.pid" 2>/dev/null || echo ""); [[ -n "$pid" ]] && kill -KILL "$pid" 2>/dev/null; }
cert_digest() { echo "$1" | grep -oE 'Assembled [1-9A-HJ-NP-Za-km-z]+' | awk '{print $2}'; }

# 0. fresh network, slow rounds, all nodes up and caught up.
"$NET" setup "$N" "$EPOCH" >/dev/null
"$NET" start-all "$N" >/dev/null
log "waiting for the chain to produce blocks..."
for _ in $(seq 1 90); do "$NET" tip "$N" 2>/dev/null | grep -qE ' [1-9][0-9]* 0x' && break; sleep 2; done
log "tip: $("$NET" tip "$N" | tr '\n' ' ')"

# 1. fresh nodes down (so they never vote for header 1).
for f in $fresh_set; do "$NET" kill "$N" "$f" >/dev/null 2>&1; log "fresh node $f down"; done
sleep 5

# 2. arm A (header + own cert) and the forget voters (vote + cert), in place.
mA=$(lines $A)
touch "$ROOT/validator-$A.armheader" "$ROOT/validator-$A.armcert"
for b in $forget_set; do touch "$ROOT/validator-$b.armvote" "$ROOT/validator-$b.armcert"; done
log "armed A=$A (header+cert) and forget voters [$(echo $forget_set)] (vote+cert) in place"

# 3. wait for A to propose a mem-only header, then assemble certificate 1 for that round.
mo=$(wait_log "$A" "RL_REPRO next write kept in memory only" "$mA" 120) \
  || { log "A never proposed a stalled header"; "$NET" scan "$N"; exit 1; }
moln=$(grep -n "RL_REPRO next write kept in memory only" "$(alog $A)" | tail -1 | cut -d: -f1)
log "A proposed a header kept in memory only (not persisted)"
c1=$(wait_log "$A" "Assembled .*: C[0-9]+" "$moln" 90) \
  || { log "A never assembled certificate 1"; "$NET" scan "$N"; exit 1; }
DH1=$(cert_digest "$c1"); R=$(echo "$c1" | grep -oE ': C[0-9]+' | head -1 | tr -d ': ')
log "certificate 1 assembled by A for round $R: $(echo "$c1" | cut -c1-150)"
# give the holder a moment to receive and persist certificate 1 (its cert write is NOT armed).
sleep 3
grep -q "Assembled .*: ${R}\(\|${DH1}" "$(alog $HOLDER)" 2>/dev/null \
  && log "holder $HOLDER has certificate 1" || log "holder $HOLDER cert-1 state unconfirmed (continuing)"

# 4. kill A, the forget voters, and the holder. Clear A/forget arm files so their restart persists.
#    A and the forget voters lose certificate 1 (memory only). The holder keeps it on disk.
kill9 "$A"; for b in $forget_set; do kill9 "$b"; done; kill9 "$HOLDER"
rm -f "$ROOT/validator-$A".armheader "$ROOT/validator-$A".armcert
for b in $forget_set; do rm -f "$ROOT/validator-$b".armvote "$ROOT/validator-$b".armcert; done
log "killed A, forget voters, and holder $HOLDER at round $R; A and forget voters lost certificate 1"
sleep 2

# 5. restart A, forget voters, and fresh nodes clean. Holder STAYS DOWN (keeps certificate 1).
mA2=$(lines $A)
for f in $fresh_set; do "$NET" start "$N" "$f" >/dev/null; log "fresh node $f up"; done
for b in $forget_set; do "$NET" start "$N" "$b" >/dev/null; done
"$NET" start "$N" "$A" >/dev/null
log "restarted A, forget voters, fresh nodes (holder $HOLDER stays down); A should rebuild round $R"

# 6. wait for A to assemble certificate 2: a different certificate for the SAME round $R.
c2=""
for _ in $(seq 1 180); do
  c2=$(tail -n +$(( mA2+1 )) "$(alog $A)" 2>/dev/null | grep -E "Assembled .*: ${R}\(" | tail -1 || true)
  dh=$(cert_digest "$c2")
  [[ -n "$dh" && "$dh" != "$DH1" ]] && break
  sleep 1
done
DH2=$(cert_digest "$c2")
[[ -n "$c2" ]] && log "certificate 2 assembled by A: $(echo "$c2" | cut -c1-150)"
if [[ -n "$DH1" && -n "$DH2" && "$DH1" != "$DH2" ]]; then
  log "TWO DIFFERENT CERTIFICATES for author $A round $R: cert1=$DH1 cert2=$DH2"
else
  log "did not observe two different certificates (cert1=$DH1 cert2=$DH2)"
fi

# 7. bring the holder back; it has certificate 1 and should now meet certificate 2.
sleep 3
"$NET" start "$N" "$HOLDER" >/dev/null
log "holder $HOLDER back up; watching for CertificateEquivocation and aborts for 150s..."
for _ in $(seq 1 75); do
  grep -rql "CertificateEquivocation" "$ROOT"/validator-*.log 2>/dev/null && break
  sleep 2
done
echo "================ RESULT ================"
"$NET" scan "$N"
echo "---- CertificateEquivocation / real abort lines ----"
grep -rhE "CertificateEquivocation|critical task .* returned Err|panic" "$ROOT"/validator-*.log 2>/dev/null | grep -v "aborting doomed" | head
echo "---- node liveness ----"
"$NET" alive "$N"
echo "---- tips ----"
"$NET" tip "$N"
