#!/bin/bash
# Try to reproduce the 2026-10-26 gossip echo storm on the native local 4v+2o network.
#
# Phases:
#   1. Fresh network: stop the running nodes, move local-validators/ aside, recreate with a
#      long epoch (late copies must not cross an epoch boundary), every node started with the
#      chaos toggle RAYLS_CHAOS_GOSSIP_DELAY_DIR so each one reads its own <dir>/<pid> file.
#   2. Background: BG_MS on every node, so the whole mesh runs close to its limit.
#   3. Wait until a healthy validator's consensus cache is near full (OCC_TARGET rows).
#   4. Seed: SEED_MS on SEED_NODE for SEED_SECS (slow but connected socket reading).
#   5. Release SEED_NODE back to BG_MS and watch for WATCH_SECS.
#   6. Report: re-accepted gossip ids per minute and node, their ages, and a verdict.
#
# Needs the chaos changes in crates/consensus/network (runtime.rs, gossip.rs): the
# "gossip read delay toggle ENABLED" and "gossip message re-accepted" log lines.
#
# Usage (from anywhere):
#   etc/test-network/echo-storm.sh                   full run, fresh network
#   KEEP=1 etc/test-network/echo-storm.sh            reuse the running chaos network (skip 1)
#   BG_MS=50 SEED_MS=2000 etc/test-network/echo-storm.sh
#
# Tunables (environment):
#   BG_MS=30          background delay per gossip message on every node, ms
#   SEED_MS=1000      seed delay on SEED_NODE, ms
#   SEED_NODE=validator-2
#   SEED_SECS=210     how long the seed is held
#   WATCH_SECS=300    how long to watch after the release
#   OCC_TARGET=8500   cache occupancy to wait for before seeding
#   OCC_TIMEOUT=900   give up waiting for OCC_TARGET after this many seconds and seed anyway
#   EPOCH=3600        epoch duration for the fresh network, s
#   DEV_FUNDS=0x57b9D26eF4a6d4738E17932AC4d0191EfE6dBc88
#   GD=/tmp/gd        toggle directory
#   TICK=15           status interval, s
#
# The network is left running at the end; the script prints how to stop it.
set -u

TN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LV="$TN/local-validators"
ENVF="$TN/.env"
BG_MS="${BG_MS:-30}"
SEED_MS="${SEED_MS:-1000}"
SEED_NODE="${SEED_NODE:-validator-2}"
SEED_SECS="${SEED_SECS:-210}"
WATCH_SECS="${WATCH_SECS:-300}"
OCC_TARGET="${OCC_TARGET:-8500}"
OCC_TIMEOUT="${OCC_TIMEOUT:-900}"
EPOCH="${EPOCH:-3600}"
DEV_FUNDS="${DEV_FUNDS:-0x57b9D26eF4a6d4738E17932AC4d0191EfE6dBc88}"
GD="${GD:-/tmp/gd}"
TICK="${TICK:-15}"
NODES="validator-1 validator-2 validator-3 validator-4 observer-5 observer-6"
OUT="$TN/echo-storm-$(date +%Y%m%d-%H%M%S).log"

exec > >(tee -a "$OUT") 2>&1

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
die() { log "ERROR: $*"; exit 1; }

# --------------------------------------------------------------- node helpers ---------------
num() { echo "${1##*-}"; }                       # validator-2 -> 2
port() { echo $((9100 + $(num "$1") - 1)); }     # validator-N / observer-N -> 9100+N-1
short() { echo "$1" | sed -e 's/validator-/v/' -e 's/observer-/o/'; }
pid_of() { cat "$LV/$1.pid" 2>/dev/null; }
alive() { local p; p=$(pid_of "$1"); [ -n "$p" ] && kill -0 "$p" 2>/dev/null; }
setd() { echo "$2" > "$GD/$(pid_of "$1")"; }
setall() { local n; for n in $NODES; do setd "$n" "$1"; done; }

dup() {
    curl -s -m 3 "127.0.0.1:$(port "$1")/metrics" 2>/dev/null |
        awk '/^[a-z_]*duplicate_certificates_processed /{print $2; f=1} END {if (!f) print 0}'
}
reacc_total() {
    local c
    c=$(grep -a -c 'gossip message re-accepted' "$LV/$1.log" 2>/dev/null)
    echo "${c:-0}"
}
round_of() {
    local r
    r=$(tail -n 20000 "$LV/$1.log" 2>/dev/null | grep -a 'output executed' | tail -1 |
        sed -n 's/.*leader_round=\([0-9]*\).*/\1/p')
    echo "${r:-0}"
}
occ_of() {
    local o
    o=$(tail -n 20000 "$LV/$1.log" 2>/dev/null | grep -a 'mem cache occupancy' | tail -1 |
        sed -n 's/.*occupancy=\([0-9]*\).*/\1/p')
    if [ -z "$o" ]; then
        # Older builds (v1.4.x) have no occupancy log line: sum the per-table
        # memdb_<table>_count gauges instead.
        o=$(curl -s -m 3 "127.0.0.1:$(port "$1")/metrics" 2>/dev/null |
            awk '/^memdb_[A-Za-z0-9_]*_count /{s += $2} END {if (s) printf "%d", s}')
    fi
    echo "${o:-0}"
}

# A healthy validator to compare against (the first validator that is not the seed node).
REF=""
for n in $NODES; do
    case "$n" in validator-*) [ "$n" != "$SEED_NODE" ] && [ -z "$REF" ] && REF="$n" ;; esac
done

# ------------------------------------------------------------------ status line --------------
# One line per TICK: per node the duplicate-certificate rate (/s) and the re-accepts in the
# last tick, then the seed node's round lag behind REF. HEALTHY_RE holds the healthy nodes'
# re-accepts per tick for the verdict.
PREV_DUP=()
PREV_RE=()
HEALTHY_RE=()
status() {
    local phase="$1" line i=0 n d c pd pc rate re sum=0 lag
    line="$(date -u +%H:%M:%S) $(printf '%-8s' "$phase")"
    for n in $NODES; do
        d=$(dup "$n"); c=$(reacc_total "$n")
        pd="${PREV_DUP[$i]:-$d}"; pc="${PREV_RE[$i]:-$c}"
        rate=$(awk -v a="$pd" -v b="$d" -v t="$TICK" 'BEGIN {printf "%.1f", (b - a) / t}')
        re=$((c - pc))
        [ "$n" != "$SEED_NODE" ] && sum=$((sum + re))
        line="$line $(short "$n") $(printf '%5s/s %4s' "$rate" "$re")"
        PREV_DUP[$i]="$d"; PREV_RE[$i]="$c"
        i=$((i + 1))
    done
    lag=$(( $(round_of "$REF") - $(round_of "$SEED_NODE") ))
    HEALTHY_RE+=("$sum")
    echo "$line | lag $(short "$SEED_NODE") $lag"
}
status_header() {
    echo "columns: per node <duplicate certs/s> <re-accepted ids in the last ${TICK}s>," \
        "then $(short "$SEED_NODE")'s round lag behind $(short "$REF")"
}

# On Ctrl-C, take the seed off so the network is not left impaired.
trap 'log "interrupted: setting $SEED_NODE back to $BG_MS ms"; setd "$SEED_NODE" "$BG_MS" 2>/dev/null; exit 130' INT TERM

# ---------------------------------------------------------- phase 1: fresh network -----------
grep -q '^NUM_VALIDATORS=4' "$ENVF" && grep -q '^NUM_OBSERVERS=2' "$ENVF" \
    || die "$ENVF must have NUM_VALIDATORS=4 and NUM_OBSERVERS=2"

if [ "${KEEP:-0}" != 1 ]; then
    log "phase 1: fresh network (epoch ${EPOCH}s, toggle dir $GD)"
    for n in $NODES; do alive "$n" && kill "$(pid_of "$n")" 2>/dev/null; done
    for _ in $(seq 1 30); do
        up=0; for n in $NODES; do alive "$n" && up=1; done
        [ "$up" = 0 ] && break; sleep 1
    done
    if [ -d "$LV" ]; then
        ARCH="/tmp/lv-$(date +%Y%m%d-%H%M%S)"
        mv "$LV" "$ARCH" || die "could not move $LV aside"
        log "previous network moved to $ARCH"
    fi
    rm -rf "$GD"; mkdir -p "$GD"
    cp "$ENVF" "$ENVF.echo-storm.bak"
    sed -i '' "s/^EPOCH_DURATION=.*/EPOCH_DURATION=$EPOCH/" "$ENVF"
    RAYLS_CHAOS_GOSSIP_DELAY_DIR="$GD" "$TN/local-testnet.sh" --start --dev-funds "$DEV_FUNDS"
    rc=$?
    mv "$ENVF.echo-storm.bak" "$ENVF"
    [ "$rc" = 0 ] || die "local-testnet.sh --start failed ($rc)"
else
    log "phase 1 skipped (KEEP=1): reusing the running network"
    mkdir -p "$GD"
fi

log "waiting for all six nodes to report the toggle"
for _ in $(seq 1 120); do
    ok=0; for n in $NODES; do
        alive "$n" && grep -a 'gossip read delay toggle ENABLED' "$LV/$n.log" 2>/dev/null |
            grep -q "path=$GD/$(pid_of "$n")" && ok=$((ok + 1))
    done
    [ "$ok" = 6 ] && break; sleep 5
done
[ "$ok" = 6 ] || die "only $ok of 6 nodes report the toggle; was the network started with RAYLS_CHAOS_GOSSIP_DELAY_DIR?"

log "waiting for the chain to pass round 50"
for _ in $(seq 1 120); do [ "$(round_of "$REF")" -gt 50 ] && break; sleep 5; done
[ "$(round_of "$REF")" -gt 50 ] || die "chain is not advancing"

# ------------------------------------------------------------- phase 2: background -----------
log "phase 2: background ${BG_MS} ms on every node"
setall "$BG_MS"
status_header
for _ in $(seq 1 $((60 / TICK))); do sleep "$TICK"; status bg; done
r0=$(round_of "$REF")
for n in $NODES; do
    lagn=$(( r0 - $(round_of "$n") ))
    [ "$lagn" -gt 50 ] && log "WARNING: $n is $lagn rounds behind $REF at ${BG_MS} ms; consider a lower BG_MS"
done

# -------------------------------------------------------- phase 3: wait for full cache -------
log "phase 3: waiting for $REF cache occupancy >= $OCC_TARGET (timeout ${OCC_TIMEOUT}s)"
waited=0
while [ "$(occ_of "$REF")" -lt "$OCC_TARGET" ]; do
    [ "$waited" -ge "$OCC_TIMEOUT" ] && { log "occupancy target not reached, seeding anyway"; break; }
    sleep "$TICK"; waited=$((waited + TICK))
    status "occ=$(occ_of "$REF")"
done

# ------------------------------------------------------------------ phase 4: seed ------------
SEED_AT=$(date -u +%H:%M:%S)
log "phase 4: SEED ${SEED_MS} ms on $SEED_NODE for ${SEED_SECS}s (occupancy $(occ_of "$REF"))"
setd "$SEED_NODE" "$SEED_MS"
for _ in $(seq 1 $((SEED_SECS / TICK))); do sleep "$TICK"; status seed; done

# --------------------------------------------------------- phase 5: release, watch -----------
RELEASE_AT=$(date -u +%H:%M:%S)
log "phase 5: RELEASE $SEED_NODE back to ${BG_MS} ms, watching ${WATCH_SECS}s"
setd "$SEED_NODE" "$BG_MS"
WATCH_START=${#HEALTHY_RE[@]}
for _ in $(seq 1 $((WATCH_SECS / TICK))); do sleep "$TICK"; status watch; done

# ------------------------------------------------------------------- phase 6: report ---------
echo
log "phase 6: report (seed at $SEED_AT, release at $RELEASE_AT UTC)"
echo "--- re-accepted gossip ids per minute (UTC), per node"
for n in $NODES; do
    printf '%-12s ' "$n"
    grep -a 'gossip message re-accepted' "$LV/$n.log" 2>/dev/null |
        awk '{print substr($1, 15, 5)}' | uniq -c | awk '{printf "%s=%s ", $2, $1}'
    echo
done
echo "--- re-accepted ids by age since first acceptance (all nodes)"
grep -a -h 'gossip message re-accepted' "$LV"/*.log 2>/dev/null | grep -o 'age_s=[0-9]*' |
    cut -d= -f2 | awk '{c[int($1 / 60) * 60]++} END {for (k in c) print k "s+", c[k]}' | sort -n
echo "--- re-accepted ids by forwarding peer (all nodes, top 6)"
grep -a -h 'gossip message re-accepted' "$LV"/*.log 2>/dev/null |
    grep -o 'propagation_source=[^ ]*' | sort | uniq -c | sort -rn | head -6

# Verdict: healthy nodes' re-accepts in the last 2 minutes of the watch, and whether the seed
# node had caught up by then.
last=${#HEALTHY_RE[@]}; from=$((last - 120 / TICK)); [ "$from" -lt "$WATCH_START" ] && from=$WATCH_START
tail_re=0; i=$from; while [ "$i" -lt "$last" ]; do tail_re=$((tail_re + HEALTHY_RE[i])); i=$((i + 1)); done
lag=$(( $(round_of "$REF") - $(round_of "$SEED_NODE") ))
max_age=$(grep -a -h 'gossip message re-accepted' "$LV"/*.log 2>/dev/null |
    grep -o 'age_s=[0-9]*' | cut -d= -f2 | sort -n | tail -1)
echo
echo "healthy re-accepts in the last 2 min of the watch: $tail_re"
echo "$SEED_NODE round lag behind $REF at the end: $lag"
echo "oldest re-accepted copy: ${max_age:-none}s"
if [ "$tail_re" -gt 0 ] && [ "$lag" -le 20 ]; then
    echo "VERDICT: SUSTAINED - re-accepts continue after $SEED_NODE caught up"
elif [ "$tail_re" -gt 0 ]; then
    echo "VERDICT: INCONCLUSIVE - re-accepts continue but $SEED_NODE is still $lag rounds behind;" \
        "rerun with a longer WATCH_SECS"
else
    echo "VERDICT: DIED OUT - expected with the 15 min duplicate cache; on a 60 s baseline," \
        "raise BG_MS (50, 80) or SEED_MS, or seed a second node"
fi
echo
log "network left running; status log $OUT"
echo "stop it with: for f in $LV/*.pid; do kill \$(cat \$f); done"
echo "re-run on the same network with: KEEP=1 $0"
