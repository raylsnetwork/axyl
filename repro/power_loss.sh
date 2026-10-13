#!/bin/bash
# Drive a live N-validator network into two certificates for one author and round via a POWER LOSS,
# defeating the re-propose dedup guard (certifier.rs, PR #112) AND the durable-before-send barrier.
#
# The node persists the vote, header and certificate before they are sent, so they survive `kill -9`.
# But the consensus DB runs MDBX SafeNoSync: the commit is not fsynced for up to the sync period.
# A power loss in that window drops the committed-but-unsynced record even though peers already saw
# it. After restart the author no longer knows it proposed the round, rebuilds a different header,
# and a second certificate forms -> CertificateEquivocation -> chain halt.
#
# Modes:
#   power_loss.sh <N>            powerloss: restore each armed node's pre-write snapshot after kill
#                              (the record was persisted but not synced) -> EXPECT equivocation+halt
#   power_loss.sh <N> crash     contrast: plain kill -9, NO restore -> the persisted record survives,
#                              the node recovers -> EXPECT no equivocation
#
# Roles (A equivocates by losing state to a power loss, never by malicious code):
#   A       the author. Power loss drops its header AND its own certificate for the round.
#   forget  voters that vote for header 1 then lose that vote to the same power loss.
#   holder  one honest node that keeps certificate 1 on disk. Down during the rebuild, back at end.
#   fresh   nodes kept down during header 1, so they vote only for header 2.
#
# This file lives only on the repro worktree and is never committed.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
NET="$HERE/net.sh"
ROOT="$HERE/runs/live"
N=${1:?N}
MODE=${2:-powerloss}            # powerloss | crash
: "${MINMS:=10000}" "${MAXMS:=20000}" "${EPOCH:=600}"
export MINMS MAXMS

Q=$(( 2*N/3 + 1 ))
A=1
HOLDER=$N
X=$(( Q - 2 )); (( X < 1 )) && X=1            # forgetful voters
forget_set=$(seq 2 $(( 1 + X )))
fresh_set=""
[[ $(( 2 + X )) -le $(( N - 1 )) ]] && fresh_set=$(seq $(( 2 + X )) $(( N - 1 )))
armed_set="$A $forget_set"
echo "N=$N mode=$MODE quorum=$Q author=$A holder=$HOLDER forget=[$(echo $forget_set)] fresh=[$(echo $fresh_set)] rounds=${MINMS}-${MAXMS}ms"

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

# 2. arm A and the forget voters for a power-loss snapshot, in place. The binary snapshots each
#    node's force-synced consensus DB just before its next header/vote write (so the snapshot
#    excludes this round's record), logs it, and proceeds to persist+send that record normally.
mA=$(lines $A)
for b in $armed_set; do touch "$ROOT/validator-$b.armpowerloss"; done
log "armed A=$A and forget voters [$(echo $forget_set)] for a power-loss snapshot in place"

# 3. wait for A to snapshot (pre-write) then assemble certificate 1 for that round.
mo=$(wait_log "$A" "RL_REPRO power-loss snapshot taken" "$mA" 120) \
  || { log "A never snapshotted a header"; "$NET" scan "$N"; exit 1; }
moln=$(grep -n "RL_REPRO power-loss snapshot taken" "$(alog $A)" | tail -1 | cut -d: -f1)
log "A snapshotted its consensus DB before proposing (record persisted but will be lost on restore)"
c1=$(wait_log "$A" "Assembled .*: C[0-9]+" "$moln" 90) \
  || { log "A never assembled certificate 1"; "$NET" scan "$N"; exit 1; }
DH1=$(cert_digest "$c1"); R=$(echo "$c1" | grep -oE ': C[0-9]+' | head -1 | tr -d ': ')
log "certificate 1 assembled by A for round $R: $(echo "$c1" | cut -c1-150)"
# give the holder a moment to receive and persist certificate 1 (it is a cert-1 signer).
sleep 3

# 4. power loss: kill A and the forget voters (NOT the holder). In powerloss mode, restore A+forget to
#    their pre-write snapshot (drop the unsynced header/vote/cert). The HOLDER stays UP the whole
#    time: it signed cert 1, keeps it, and stays in the live consensus DAG. When A rebuilds and
#    cert 2 forms, the holder receives it over normal gossip and `try_insert_in_dag` flags the
#    conflict (the equivocation check runs during active consensus, not during catch-up sync, so the
#    witness must be a node that never left the DAG).
kill9 "$A"; for b in $forget_set; do kill9 "$b"; done
sleep 1
if [[ "$MODE" == powerloss ]]; then
  for b in $armed_set; do "$NET" power-loss "$N" "$b" || true; done
  log "POWER LOSS: killed A+forget (holder $HOLDER stays up with cert 1); restored A+forget to the last synced DB (lost round $R)"
else
  for b in $armed_set; do rm -f "$ROOT/validator-$b/consensus-db/mdbx.dat.pl-snap"; done
  log "CRASH (contrast): killed A+forget (holder $HOLDER stays up); NO restore (the persisted record should survive)"
fi
sleep 2

# 5. restart A and the forget voters clean, and bring the fresh nodes up. The holder is already up.
#    A lost round $R, so it rebuilds a different header; the forget voters lost their vote, so they
#    vote again. cert 2 forms from A+forget+fresh (the holder is blocked by equivocation protection).
#    NOTE: whether A rebuilds vs. re-fetches cert 1 from the live holder is a consensus-timing race;
#    the reproduce.sh wrapper retries until it lands (per-run ~1 in 3).
mA2=$(lines $A)
for f in $fresh_set; do "$NET" start "$N" "$f" >/dev/null; log "fresh node $f up"; done
for b in $forget_set; do "$NET" start "$N" "$b" >/dev/null; done
"$NET" start "$N" "$A" >/dev/null
log "restarted A, forget voters, fresh nodes (holder $HOLDER up throughout); A should rebuild round $R"

# 6+7. A lost round $R, rebuilds a different header, and a second certificate forms for A at round
#   $R (or a round or two later, once A is past the committed point). The holder, rejoined with
#   cert 1, inserts the conflicting cert into its DAG and the consensus task panics. Wait for the
#   CertificateEquivocation halt (the authoritative success signal), up to 150s.
EQ_LINE=""
for _ in $(seq 1 75); do
  EQ_LINE=$(grep -rhE "CertificateEquivocation\(" "$ROOT"/validator-*.log 2>/dev/null | head -1 || true)
  [[ -n "$EQ_LINE" ]] && break
  sleep 2
done
echo "================ RESULT (mode=$MODE) ================"
if [[ -n "$EQ_LINE" ]]; then
  eqr=$(echo "$EQ_LINE" | grep -oE 'C[0-9]+' | head -1)
  nhalt=$(grep -rlE "critical task consensus returned Err: CertificateEquivocation" "$ROOT"/validator-*.log 2>/dev/null | wc -l)
  log "REPRODUCED: CertificateEquivocation (two certs for one author round $eqr) -> consensus task panicked on $nhalt/$N nodes -> chain halt"
  echo "VERDICT: PASS (equivocation+halt)"
else
  log "no CertificateEquivocation observed (cert1 round $R digest $DH1)"
  echo "VERDICT: FAIL (no halt)"
fi
"$NET" scan "$N"
echo "---- CertificateEquivocation / consensus-panic lines ----"
grep -rhE "CertificateEquivocation\(|critical task consensus returned Err" "$ROOT"/validator-*.log 2>/dev/null | sort -u | head -4
echo "---- node liveness (debug build unwinds; consensus dead = tips frozen) ----"
"$NET" alive "$N"
echo "---- tips (frozen across nodes == halted) ----"
"$NET" tip "$N"
