#!/usr/bin/env bash
#
# Live reproduction of finding N1: an unsigned reputation_score crash-loops a following node.
# See README.md in this directory.
#
# A consensus block's reputation_score is excluded from the block digest on purpose. A node that
# follows the chain fetches missing consensus blocks from peers by number and caches the reply,
# including the peer's reputation_score, in its canonical store. On its next start the node rebuilds
# its leader schedule from the newest stored final-of-schedule row, and the swap table divides by
# the number of scored authorities. A peer that answers a by-number gap fetch with an empty,
# final-of-schedule score map makes that count zero, so the follower divides by zero and aborts.
# The poisoned row persists, so every restart repeats the abort: a crash-loop that needs operator
# DB surgery to clear.
#
# Topology: N validators plus one observer. The observer falls behind, then catches up through a
# gap. Validators 1..K (default all) are armed from their first start to answer every
# consensus-header request, by number, by hash and the latest probe, with the poison (empty
# final-of-schedule scores) for every number after the warm-up. With K = N whatever block is newest
# in the observer's store after catch-up is a poisoned final row; with K < N it shows what one liar
# among honest peers achieves. We then restart the observer while the validators keep running: it
# aborts when it starts consensus and rebuilds its leader schedule, and again on each restart.
# The validators must stay up: with no peer the observer waits before starting consensus and never
# reaches the leader-schedule rebuild.
#
# The attacker behaviour is compiled only with the `byzantine-test-hooks` cargo feature and armed
# per process by environment, so the armed validators lie while their own consensus is unaffected
# (the hook only rewrites replies served to a follower). With -a off the validators are honest and
# the observer catches up and restarts cleanly.
#
# Scenarios, picked with -s:
#   observer           the flow above, the default
#   observer-attacker  the validators stay honest and a second observer, the attacker, is armed
#                      instead. It follows the chain like any observer and advertises a dialable
#                      address, so the victim observer can reach it. Same victim flow as above.
#   rolling            no observer. Validators 1..K are armed and the validators listed with -v
#                      are restarted one after another the way an operator rolls an upgrade: stop,
#                      let the chain move GAP blocks on, start, wait until the node rejoins the
#                      committee or aborts, then restart it RESTARTS more times. Chain heights are
#                      logged after every step, so a halt shows up as a height that stops moving.
#                      With -l the armed validators are then stopped for that many seconds and
#                      restarted, to show whether the liar can halt the chain and whether it resumes.
#   rolling-observer-attacker
#                      the same rolling restart, but every validator is honest and the attacker is
#                      one armed observer with a dialable address, as in observer-attacker.
#
# In the rolling scenarios the chain height is sampled every few seconds into heights.log, and the
# summary ends with every halt found there and, per victim, the altered replies served while it was
# down or starting and the block numbers they covered.
#
# Exit status: 1 the observer crash-looped at startup, or in the rolling scenario any restarted
# validator aborted on the divide-by-zero (reproduced), 0 every victim restarted and followed
# cleanly (healthy; bug absent or fixed), 2 the run was inconclusive or could not start.

set -uo pipefail

usage() {
    cat <<EOF
Usage: $0 [-s SCENARIO] [-a on|off] [-n VALIDATORS] [-k ARMED_VALIDATORS] [-v ROLLED] [-b BINARY] [-d WORKDIR] [-g GAP] [-r RESTARTS] [-l LIAR_STOP_SECS] [-i INSTANCE_BASE]

  -s  scenario (default observer):
      observer           armed validators, the victim is the observer
      observer-attacker  honest validators, an armed second observer, the victim is the observer
      rolling            armed validators 1..K, rolling restart of the validators given by -v
      rolling-observer-attacker
                         honest validators, an armed observer, rolling restart of the validators
                         given by -v
  -a  whether the armed nodes poison consensus-header replies (default on):
      on   armed nodes serve empty final-of-schedule scores for every number after warm-up
      off  every node serves honest replies (control)
  -n  validators in the committee, at least 4 (default 4)
  -k  arm only validators 1..K, the rest stay honest (default: all N, ignored by the
      observer-attacker scenarios)
  -v  validators to restart one after another in the rolling scenarios, comma separated
      (default 2,3)
  -b  node binary built with --features byzantine-test-hooks and panic=abort
      (default: build target/panic-abort/debug/rayls-network from this checkout)
  -d  directory for datadirs and logs (default: target/rep-poison-crash/<timestamp>)
  -g  how many blocks a victim falls behind before catching up (default: 6 per peer the victim
      can fetch from, at least 40, so the attacker is asked several times per victim)
  -r  restarts of the victim after its catch-up start, to show the crash-loop (default 3)
  -l  rolling scenario only: after the roll, stop the armed validators 1..K for this many
      seconds while the chain height is watched, then restart them and watch whether the chain
      resumes (default 0, off)
  -i  first --instance number, which sets the RPC ports (default 40)
EOF
    exit 2
}

SCENARIO=observer
ROLLED=2,3
ARMED=on
N=4
K=""
BIN=""
WORKDIR=""
GAP=""
RESTARTS=3
LIAR_STOP=0
INSTANCE_BASE=40
while getopts "s:a:n:k:v:b:d:g:r:l:i:h" opt; do
    case "$opt" in
        s) SCENARIO=$OPTARG ;;
        v) ROLLED=$OPTARG ;;
        a) ARMED=$OPTARG ;;
        n) N=$OPTARG ;;
        k) K=$OPTARG ;;
        b) BIN=$OPTARG ;;
        d) WORKDIR=$OPTARG ;;
        g) GAP=$OPTARG ;;
        r) RESTARTS=$OPTARG ;;
        l) LIAR_STOP=$OPTARG ;;
        i) INSTANCE_BASE=$OPTARG ;;
        *) usage ;;
    esac
done
[[ $ARMED == on || $ARMED == off ]] || { echo "unknown -a: $ARMED"; usage; }
[[ $N =~ ^[0-9]+$ && $N -ge 4 ]] || { echo "need at least 4 validators"; usage; }
K=${K:-$N}
[[ $K =~ ^[0-9]+$ && $K -ge 1 && $K -le $N ]] || { echo "-k must be between 1 and $N"; usage; }
for value in "${GAP:-0}" "$RESTARTS" "$LIAR_STOP" "$INSTANCE_BASE"; do
    [[ $value =~ ^[0-9]+$ ]] || { echo "not a number: $value"; usage; }
done
[[ $RESTARTS -ge 1 ]] || { echo "need at least one restart"; usage; }
case "$SCENARIO" in
    observer | observer-attacker | rolling | rolling-observer-attacker) ;;
    *) echo "unknown -s: $SCENARIO"; usage ;;
esac
[[ $LIAR_STOP -eq 0 || $SCENARIO == rolling ]] || { echo "-l needs -s rolling"; usage; }
# The attacker is an observer in the two observer-attacker scenarios, otherwise validators 1..K.
OBS_ATTACKER=0
[[ $SCENARIO == *observer-attacker ]] && OBS_ATTACKER=1
ROLLING=0
[[ $SCENARIO == rolling* ]] && ROLLING=1
# A catching-up node asks its peers for the gap round-robin, so the attacker answers about one
# number in PEERS. The default gap gives it several answers per victim.
if [[ $ROLLING -eq 1 ]]; then PEERS=$((N - 1 + OBS_ATTACKER)); else PEERS=$((N + OBS_ATTACKER)); fi
GAP_WARNING=""
if [[ -z $GAP ]]; then
    GAP=$((6 * PEERS)); [[ $GAP -lt 40 ]] && GAP=40
elif [[ $GAP -lt $((3 * PEERS)) ]]; then
    GAP_WARNING="gap $GAP is under 3 per peer for $PEERS peers, the attacker may be asked only once or twice per victim"
fi
ROLLED_LIST=()
if [[ $ROLLING -eq 1 ]]; then
    IFS=',' read -ra ROLLED_LIST <<<"$ROLLED"
    for value in "${ROLLED_LIST[@]}"; do
        [[ $value =~ ^[0-9]+$ && $value -ge 1 && $value -le $N ]] || { echo "-v: no validator $value"; usage; }
    done
fi

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
WORKDIR=${WORKDIR:-$REPO/target/rep-poison-crash/$(date -u +%Y%m%dT%H%M%SZ)}
if [[ -d $WORKDIR && -n $(ls -A "$WORKDIR" 2>/dev/null) ]]; then
    echo "not empty: $WORKDIR"; exit 2
fi
RUN=$WORKDIR

OBS_INSTANCE=$((INSTANCE_BASE + N + 1))
ATK_INSTANCE=$((OBS_INSTANCE + 1))
WARMUP_BLOCKS=20
CHAIN_ID=0x1e7
EPOCH_SECS=3600
DEV_ACCOUNT=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
export RL_BLS_PASSPHRASE="rep-poison-crash-local-only"

# the observer aborts on purpose; keep its debug core dumps off the disk
ulimit -c 0

declare -A PIDS
OBS_PID=""
log() { echo "$(date -u +%T) $*" | tee -a "$RUN/summary.txt"; }
fail() { log "INCONCLUSIVE: $*"; echo "logs and datadirs: $RUN"; exit 2; }

command -v curl >/dev/null || { echo "missing required tool: curl"; exit 2; }

if [[ -z $BIN ]]; then
    echo "building rayls-network (debug, panic=abort) with --features byzantine-test-hooks"
    (cd "$REPO" && CARGO_PROFILE_DEV_PANIC=abort CARGO_INCREMENTAL=0 nice -n 19 cargo build \
        -p rayls-network --features byzantine-test-hooks -j 4 --target-dir target/panic-abort) || exit 2
    BIN=$REPO/target/panic-abort/debug/rayls-network
fi
[[ -x $BIN ]] || { echo "not executable: $BIN"; exit 2; }
if [[ $ARMED == on ]] && ! grep -qa 'BYZANTINE GAP-FETCH' "$BIN"; then
    echo "$BIN was built without --features byzantine-test-hooks"; exit 2
fi

rpc_port() {
    if [[ $1 == obs ]]; then echo $((8546 - OBS_INSTANCE))
    elif [[ $1 == atk ]]; then echo $((8546 - ATK_INSTANCE))
    else echo $((8546 - (INSTANCE_BASE + $1))); fi
}

height() {
    local out
    out=$(curl -s -m 2 -H 'content-type: application/json' \
        -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
        "http://127.0.0.1:$(rpc_port "$1")" 2>/dev/null) || return 0
    [[ $out =~ \"result\":\"0x([0-9a-fA-F]+)\" ]] && echo $((16#${BASH_REMATCH[1]}))
}

max_val_height() {
    local best=-1 h v
    for v in $(seq 1 "$N"); do h=$(height "$v"); [[ -n $h && $h -gt $best ]] && best=$h; done
    echo "$best"
}

node_args() {
    local seq=$1
    NODE_ARGS=(node --datadir "$RUN/validator-$seq" --network local
        --instance $((INSTANCE_BASE + seq)) --log.stdout.format log-fmt --full --storage.v2
        --db.growth-step 1MB --consensus-db.growth-step 1MB --txpool.minimal-protocol-fee 0
        --gpo.default-suggested-fee 0 -vvv --http --http.addr 127.0.0.1 --http.api all)
}

start_validator() {
    local seq=$1 logfile=$2
    node_args "$seq"
    # Validators 1..K are armed once ARM_RANGE is set. With K = N the newest block the observer
    # ingests during catch-up is a poisoned final-of-schedule row no matter which peer it fetches
    # from. The hook only rewrites replies served to a follower; the validator's own
    # consensus is unaffected.
    if [[ $ARMED == on && $OBS_ATTACKER -eq 0 && -n ${ARM_RANGE:-} && $seq -le $K ]]; then
        RL_BYZ_GAP_TARGET="$ARM_RANGE" RL_BYZ_GAP_MODE=rep-poison \
            "$BIN" "${NODE_ARGS[@]}" >>"$logfile" 2>&1 &
    else
        "$BIN" "${NODE_ARGS[@]}" >>"$logfile" 2>&1 &
    fi
    PIDS[$seq]=$!
}

start_observer() {
    local logfile=$1
    PRIMARY_LISTENER_MULTIADDR="/ip4/0.0.0.0/udp/$((49000 + OBS_INSTANCE))/quic-v1" \
    WORKER_LISTENER_MULTIADDR="/ip4/0.0.0.0/udp/$((49100 + OBS_INSTANCE))/quic-v1" \
        "$BIN" node --datadir "$RUN/observer" --network local --observer \
        --instance "$OBS_INSTANCE" --log.stdout.format log-fmt --full --storage.v2 \
        --db.growth-step 1MB --consensus-db.growth-step 1MB --txpool.minimal-protocol-fee 0 \
        --gpo.default-suggested-fee 0 -vvv --http --http.addr 127.0.0.1 --http.api all \
        >>"$logfile" 2>&1 &
    OBS_PID=$!
}

# The attacker is an observer, not a committee member. Unlike the victim it advertises a concrete
# address, which any operator may do, so other nodes can dial it. It is armed from its first start.
ATK_PID=""
start_attacker() {
    local logfile=$1
    local args=(node --datadir "$RUN/attacker" --network local --observer
        --instance "$ATK_INSTANCE" --log.stdout.format log-fmt --full --storage.v2
        --db.growth-step 1MB --consensus-db.growth-step 1MB --txpool.minimal-protocol-fee 0
        --gpo.default-suggested-fee 0 -vvv --http --http.addr 127.0.0.1 --http.api all)
    if [[ $ARMED == on ]]; then
        RL_BYZ_GAP_TARGET="$ARM_RANGE" RL_BYZ_GAP_MODE=rep-poison "$BIN" "${args[@]}" >>"$logfile" 2>&1 &
    else
        "$BIN" "${args[@]}" >>"$logfile" 2>&1 &
    fi
    ATK_PID=$!
}

stop_pid() {
    local pid=$1
    [[ -z $pid ]] && return 0
    kill -TERM "$pid" 2>/dev/null
    for _ in $(seq 1 40); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
    kill -KILL "$pid" 2>/dev/null
    return 0
}

stop_all() {
    local seq
    [[ -n ${SAMPLER_PID:-} ]] && { kill -TERM "$SAMPLER_PID" 2>/dev/null; SAMPLER_PID=""; }
    for seq in "${!PIDS[@]}"; do kill -TERM "${PIDS[$seq]}" 2>/dev/null; done
    [[ -n $OBS_PID ]] && kill -TERM "$OBS_PID" 2>/dev/null
    [[ -n $ATK_PID ]] && kill -TERM "$ATK_PID" 2>/dev/null
    for seq in "${!PIDS[@]}"; do stop_pid "${PIDS[$seq]}"; done
    [[ -n $OBS_PID ]] && stop_pid "$OBS_PID"
    [[ -n $ATK_PID ]] && stop_pid "$ATK_PID"
    PIDS=(); OBS_PID=""; ATK_PID=""
    return 0
}
trap stop_all EXIT
trap 'exit 2' INT TERM

wait_for() {
    local secs=$1; shift
    for _ in $(seq 1 "$secs"); do "$@" && return 0; sleep 1; done
    return 1
}
val_reached() { [[ $(max_val_height) -ge $1 ]]; }
obs_reached() { local h; h=$(height obs); [[ -n $h && $h -ge $1 ]]; }

setup_network() {
    rm -rf "$RUN"; mkdir -p "$RUN/genesis/validators"
    local seq
    for seq in $(seq 1 "$N") obs atk; do
        local port; port=$(rpc_port "$seq")
        if (echo >"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
            fail "RPC port $port is in use; pick other instance numbers with -i"
        fi
    done
    for seq in $(seq 1 "$N"); do
        "$BIN" keytool generate validator --datadir "$RUN/validator-$seq" \
            --address "$(printf '0x%040x' $((0x5eed0000 + seq)))" >/dev/null 2>&1 || return 1
        cp "$RUN/validator-$seq/node-info.yaml" "$RUN/genesis/validators/validator-$seq.yaml"
    done
    "$BIN" genesis --datadir "$RUN" --chain-id $CHAIN_ID --epoch-duration-in-secs $EPOCH_SECS \
        --dev-funded-account $DEV_ACCOUNT --max-header-delay-ms 1000 --min-header-delay-ms 500 \
        --consensus-registry-owner $DEV_ACCOUNT --network-admin $DEV_ACCOUNT >/dev/null 2>&1 || return 1
    for seq in $(seq 1 "$N"); do
        mkdir -p "$RUN/validator-$seq/genesis"
        cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$RUN/validator-$seq/genesis/"
        cp "$RUN/parameters.yaml" "$RUN/validator-$seq/"
    done
    "$BIN" keytool generate observer --datadir "$RUN/observer" \
        --address 0x0000000000000000000000000000000000000000 --advertise-identity-only >/dev/null 2>&1 || return 1
    mkdir -p "$RUN/observer/genesis"
    cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$RUN/observer/genesis/"
    cp "$RUN/parameters.yaml" "$RUN/observer/"
    [[ $OBS_ATTACKER -eq 1 ]] || return 0
    "$BIN" keytool generate observer --datadir "$RUN/attacker" \
        --address 0x0000000000000000000000000000000000000000 \
        --external-primary-addr "/ip4/127.0.0.1/udp/$((49000 + ATK_INSTANCE))/quic-v1" \
        --external-worker-addrs "/ip4/127.0.0.1/udp/$((49100 + ATK_INSTANCE))/quic-v1" >/dev/null 2>&1 || return 1
    mkdir -p "$RUN/attacker/genesis"
    cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$RUN/attacker/genesis/"
    cp "$RUN/parameters.yaml" "$RUN/attacker/"
}

# primary peer id of a node, from its node-info.yaml
peer_id_of() {
    grep -aoE '12D3KooW[1-9A-HJ-NP-Za-km-z]+' "$1/node-info.yaml" 2>/dev/null | head -1
}

# count how many poison replies the armed nodes logged (proof of delivery)
poison_served() {
    local f total=0 c
    for f in "$RUN"/validator-*.log "$RUN"/attacker.log; do
        [[ -f $f ]] || continue
        c=$(grep -ac 'BYZANTINE GAP-FETCH' "$f" 2>/dev/null)
        total=$((total + ${c:-0}))
    done
    echo "$total"
}

# UTC time in the format the node logs use, to the second, so it compares as a string
now_ts() { date -u +%Y-%m-%dT%H:%M:%S; }

# Altered replies the armed nodes logged between two now_ts stamps, inclusive, as
# "<count> <lowest number> <highest number>", or "0 - -". The hook does not log the requester, so
# a window holds one victim only while no other node is catching up.
served_window() {
    local from=$1 to=$2 f files=()
    for f in "$RUN"/validator-*.log "$RUN"/attacker.log; do [[ -f $f ]] && files+=("$f"); done
    [[ ${#files[@]} -gt 0 ]] || { echo "0 - -"; return; }
    grep -ah 'BYZANTINE GAP-FETCH' "${files[@]}" 2>/dev/null | sed 's/\x1b\[[0-9;]*m//g' \
        | awk -v from="$from" -v to="$to" '
            { ts = substr($1, 4, 19); if (ts < from || ts > to) next
              if (match($0, / number=[0-9]+/)) { n = substr($0, RSTART + 8, RLENGTH - 8) + 0
                  c++; if (lo == "" || n < lo) lo = n; if (hi == "" || n > hi) hi = n } }
            END { if (c) print c, lo, hi; else print "0 - -" }'
}

# panic location of the first divide-by-zero in a log, for example leader_schedule.rs:93:9
panic_site() {
    sed 's/\x1b\[[0-9;]*m//g' "$1" 2>/dev/null | grep -aB1 'attempt to divide by zero' \
        | grep -aoE 'panicked at [^ ]+' | head -1 | sed -E 's/panicked at //; s/:$//; s#.*/##'
}

# did the observer abort on the divide-by-zero while building its leader schedule?
obs_crashed() {
    grep -qaE 'attempt to divide by zero|divide by zero' "$RUN/observer.log" 2>/dev/null
}

# --- rolling scenario --------------------------------------------------------------------------

heights_line() {
    local v h out=""
    for v in $(seq 1 "$N"); do h=$(height "$v"); out+=" v$v=${h:-down}"; done
    echo "${out# }"
}

# Log the heights now and SECS later, and whether the chain moved in between.
watch_chain() {
    local secs=$1 label=$2 before after
    before=$(max_val_height)
    log "  $label heights: $(heights_line)"
    sleep "$secs"
    after=$(max_val_height)
    if [[ $after -gt $before ]]; then
        log "  $label +${secs}s heights: $(heights_line), chain ADVANCING ($before to $after)"
    else
        log "  $label +${secs}s heights: $(heights_line), chain HALTED at $after"
    fi
}

# Wait for a started validator to rejoin the committee or exit. Rejoined means it logged a mode
# decision of CvvActive, then stayed up REJOIN_HOLD seconds and reached the chain tip. Sets OUTCOME.
REJOIN_HOLD=30
await_validator() {
    local seq=$1 logfile=$2 active_at="" h top
    OUTCOME="stuck"
    for t in $(seq 1 300); do
        if ! kill -0 "${PIDS[$seq]}" 2>/dev/null; then
            if grep -qaE 'attempt to divide by zero' "$logfile"; then
                if grep -qa 'mode-change phase 3/3: APPLY' "$logfile"; then
                    OUTCOME="ABORTED at the switch to voting"
                else
                    OUTCOME="ABORTED at startup"
                fi
                OUTCOME+=", divide by zero at $(panic_site "$logfile")"
            else
                OUTCOME="exited without a divide-by-zero"
            fi
            unset "PIDS[$seq]"
            return
        fi
        if [[ -z $active_at ]] && grep -qaE 'identify_node_mode: decided.* mode=CvvActive' "$logfile"; then
            active_at=$t
        fi
        if [[ -n $active_at && $((t - active_at)) -ge $REJOIN_HOLD ]]; then
            h=$(height "$seq"); top=$(max_val_height)
            if [[ -n $h && $h -ge $((top - 3)) ]]; then
                OUTCOME="rejoined the committee at height $h"
                return
            fi
        fi
        sleep 1
    done
    h=$(height "$seq")
    OUTCOME="stuck: no abort and no rejoin within 300s, height ${h:-none}"
}

# Sample every validator's height into heights.log until the run ends, one line per sample:
# epoch seconds, UTC time, highest height, then every validator's height. The highest height is
# taken from the same reads as the per-validator heights, so a slow sample stays consistent.
HEIGHT_SAMPLE_SECS=5
SAMPLER_PID=""
start_sampler() {
    (
        trap 'exit 0' TERM
        local line top
        while true; do
            line=$(heights_line)
            top=$(grep -oE '=[0-9]+' <<<"$line" | tr -d = | sort -n | tail -1)
            echo "$(date -u +%s) $(date -u +%T) ${top:--1} $line"
            sleep "$HEIGHT_SAMPLE_SECS"
        done
    ) >>"$RUN/heights.log" 2>/dev/null &
    SAMPLER_PID=$!
}

# Every stretch of at least HALT_MIN_SECS in heights.log where the highest height did not grow.
# The edges are only as precise as the sample interval.
HALT_MIN_SECS=20
report_halts() {
    local lines
    lines=$(awk -v th="$HALT_MIN_SECS" '
        $3 < 0 { next }
        !seen { last = $3; since = $1; since_t = $2; seen = 1; next }
        { if ($3 > last) {
              if ($1 - since >= th)
                  printf "halted at height %d from %s to %s, about %ds\n", last, since_t, $2, $1 - since
              last = $3; since = $1; since_t = $2 }
          end_s = $1; end_t = $2 }
        END { if (seen && end_s - since >= th)
                  printf "halted at height %d from %s to the last sample at %s, about %ds, STILL HALTED\n", last, since_t, end_t, end_s - since }
    ' "$RUN/heights.log" 2>/dev/null)
    if [[ -z $lines ]]; then
        log "  none: the highest height grew within every ${HALT_MIN_SECS}s window"
    else
        while IFS= read -r line; do log "  $line"; done <<<"$lines"
    fi
}

# After the roll, stop the armed validators for LIAR_STOP seconds while the chain height is
# watched, then start them again, armed as before, and wait for the chain to move.
liar_step() {
    local k h0 h1 hr waited t0 elapsed step
    log "liar step: stopping validators 1..$K for ${LIAR_STOP}s: $(heights_line)"
    for k in $(seq 1 "$K"); do
        [[ -n ${PIDS[$k]:-} ]] && { stop_pid "${PIDS[$k]}"; unset "PIDS[$k]"; }
    done
    h0=$(max_val_height)
    log "  validators 1..$K stopped, chain at $h0: $(heights_line)"
    waited=0
    while [[ $waited -lt $LIAR_STOP ]]; do
        step=30; [[ $((LIAR_STOP - waited)) -lt $step ]] && step=$((LIAR_STOP - waited))
        sleep "$step"; waited=$((waited + step))
        log "  liar down +${waited}s heights: $(heights_line)"
    done
    h1=$(max_val_height)
    if [[ $h1 -gt $h0 ]]; then
        log "  chain ADVANCED from $h0 to $h1 in the ${LIAR_STOP}s validators 1..$K were down"
    else
        log "  chain HALTED at $h1 for the whole ${LIAR_STOP}s validators 1..$K were down"
    fi
    for k in $(seq 1 "$K"); do start_validator "$k" "$RUN/validator-$k-liar-restart.log"; done
    hr=$(max_val_height); t0=$SECONDS
    log "  validators 1..$K restarted at height $hr"
    if wait_for 300 val_reached $((hr + 5)); then
        elapsed=$((SECONDS - t0))
        if [[ $h1 -gt $h0 ]]; then
            log "  chain kept advancing, 5 more blocks within ${elapsed}s of the restart: $(heights_line)"
        else
            log "  chain RESUMED within ${elapsed}s of the restart: $(heights_line)"
        fi
    else
        log "  chain did NOT resume within 300s of the restart: $(heights_line)"
    fi
    for k in $(seq 1 "$K"); do
        if kill -0 "${PIDS[$k]}" 2>/dev/null; then
            log "  validator $k is up, height $(height "$k"), a node still catching up may not answer RPC yet"
        else
            log "  validator $k exited after its restart, divide by zero at: $(panic_site "$RUN/validator-$k-liar-restart.log")"
            unset "PIDS[$k]"
        fi
    done
    watch_chain 15 "after the liar restart"
}

run_rolling() {
    local seq v start stop_h stop_wait logfile outcomes=() aborted=0 rejoined=0 t_from t_to
    local w_from w_to w v_aborted v_starts v_streak verdict victims=()
    if [[ $OBS_ATTACKER -eq 1 ]]; then
        log "network: $N honest validators + attacker observer; armed=$ARMED on the attacker for numbers $ARM_RANGE; rolling restart of validators ${ROLLED_LIST[*]}"
    else
        log "network: $N validators, no observer; armed=$ARMED on validators 1..$K for numbers $ARM_RANGE; rolling restart of validators ${ROLLED_LIST[*]}"
    fi
    log "gap $GAP blocks per victim, $PEERS peers to fetch from"
    [[ -n $GAP_WARNING ]] && log "WARNING: $GAP_WARNING"
    for seq in $(seq 1 "$N"); do start_validator "$seq" "$RUN/validator-$seq.log"; done
    [[ $OBS_ATTACKER -eq 1 ]] && start_attacker "$RUN/attacker.log"
    sleep 5
    for seq in $(seq 1 "$N"); do
        kill -0 "${PIDS[$seq]}" 2>/dev/null || fail "validator $seq did not start; see its log"
    done
    [[ $OBS_ATTACKER -eq 1 ]] && { kill -0 "$ATK_PID" 2>/dev/null || fail "attacker did not start; see its log"; }
    wait_for 240 val_reached "$WARMUP_BLOCKS" || fail "chain did not reach block $WARMUP_BLOCKS"
    if [[ $OBS_ATTACKER -eq 1 ]]; then
        atk_reached() { local h; h=$(height atk); [[ -n $h && $h -ge $1 ]]; }
        wait_for 120 atk_reached "$WARMUP_BLOCKS" || fail "attacker did not follow to block $WARMUP_BLOCKS"
        log "attacker following at $(height atk), peer id $(peer_id_of "$RUN/attacker")"
    fi
    log "warm-up done"
    start_sampler
    watch_chain 15 "warm-up"

    stop_wait=$((120 + GAP))
    for v in "${ROLLED_LIST[@]}"; do
        w_from=$(now_ts)
        stop_pid "${PIDS[$v]:-}"; unset "PIDS[$v]"
        stop_h=$(max_val_height)
        log "validator $v stopped, chain at $stop_h"
        if wait_for "$stop_wait" val_reached $((stop_h + GAP)); then
            log "  chain advanced $GAP blocks with validator $v down: $(heights_line)"
        else
            log "  chain did NOT advance $GAP blocks in ${stop_wait}s with validator $v down: $(heights_line)"
        fi
        watch_chain 15 "validator $v down"

        # Start 0 catches up through the gap. Starts 1..RESTARTS are the further restarts, made
        # whether or not the previous start aborted.
        v_aborted=0; v_starts=0; v_streak=0
        for start in $(seq 0 "$RESTARTS"); do
            [[ -n ${PIDS[$v]:-} ]] && { stop_pid "${PIDS[$v]}"; unset "PIDS[$v]"; }
            logfile="$RUN/validator-$v-start-$start.log"
            t_from=$(now_ts)
            start_validator "$v" "$logfile"
            await_validator "$v" "$logfile"
            t_to=$(now_ts)
            read -r w _ _ <<<"$(served_window "$t_from" "$t_to")"
            log "validator $v start $start: $OUTCOME; armed nodes served $w altered replies meanwhile"
            outcomes+=("v$v start $start: $OUTCOME")
            v_starts=$((v_starts + 1)); v_streak=$((v_streak + 1))
            [[ $OUTCOME == ABORTED* ]] && { aborted=$((aborted + 1)); v_aborted=$((v_aborted + 1)); }
            [[ $OUTCOME == ABORTED* ]] || v_streak=0
            [[ $OUTCOME == rejoined* ]] && rejoined=$((rejoined + 1))
            watch_chain 15 "after validator $v start $start"
        done
        w_to=$(now_ts)
        read -r w lo hi <<<"$(served_window "$w_from" "$w_to")"
        # Crash-looped: the last two starts or more aborted. One abort on the last start means the
        # poison landed, but no later start was made to show the loop.
        if [[ $v_streak -ge 2 ]]; then
            verdict="CRASH-LOOPED"
        elif [[ $v_streak -eq 1 ]]; then
            verdict="POISONED on its last start, no later start to show the loop"
        else
            verdict="not crash-looped"
        fi
        victims+=("validator $v: served $w altered replies, numbers $lo..$hi, aborted $v_aborted of $v_starts starts, $verdict")
    done
    [[ $LIAR_STOP -gt 0 ]] && liar_step
    watch_chain 30 "final"

    log "per start:"
    local line
    for line in "${outcomes[@]}"; do log "  $line"; done
    log "per victim, altered replies logged from its stop to the end of its last start:"
    for line in "${victims[@]}"; do log "  $line"; done
    log "halts in heights.log, at least ${HALT_MIN_SECS}s without a new block:"
    report_halts
    log "armed nodes served $(poison_served) altered replies in total"
    echo "logs and datadirs: $RUN"
    if [[ $aborted -ge 1 ]]; then
        log "ISSUE REPRODUCED: $aborted validator starts aborted on the divide-by-zero"
        exit 1
    fi
    if [[ $rejoined -eq ${#outcomes[@]} ]]; then
        log "HEALTHY: every restarted validator rejoined"
        exit 0
    fi
    log "INCONCLUSIVE: some starts neither aborted nor rejoined"
    exit 2
}

# --- run -------------------------------------------------------------------------------------

setup_network || fail "network setup failed"
# Validators 1..K are armed from their first start, open ended from the block after warm-up, so the
# range covers the observer's whole catch-up gap and every block after it. No validator restarts:
# a validator restarted next to armed peers boots as a follower, takes the poison itself and aborts
# when it rejoins the committee, which would halt the chain before the observer is tested.
ARM_RANGE="$((WARMUP_BLOCKS + 1))-999999999"
[[ $ROLLING -eq 1 ]] && run_rolling

if [[ $SCENARIO == observer-attacker ]]; then
    log "network: $N honest validators + attacker observer + victim observer; armed=$ARMED on the attacker for numbers $ARM_RANGE"
else
    log "network: $N validators + 1 observer; armed=$ARMED on validators 1..$K of $N for numbers $ARM_RANGE, victim is the observer"
fi
log "gap $GAP blocks, $PEERS peers to fetch from"
[[ -n $GAP_WARNING ]] && log "WARNING: $GAP_WARNING"

for seq in $(seq 1 "$N"); do start_validator "$seq" "$RUN/validator-$seq.log"; done
[[ $SCENARIO == observer-attacker ]] && start_attacker "$RUN/attacker.log"
start_observer "$RUN/observer.log"
sleep 5
for seq in $(seq 1 "$N"); do
    kill -0 "${PIDS[$seq]}" 2>/dev/null || fail "validator $seq did not start; see its log"
done
kill -0 "$OBS_PID" 2>/dev/null || fail "observer did not start; see its log"
if [[ $SCENARIO == observer-attacker ]]; then
    kill -0 "$ATK_PID" 2>/dev/null || fail "attacker did not start; see its log"
fi

wait_for 240 val_reached "$WARMUP_BLOCKS" || fail "chain did not reach block $WARMUP_BLOCKS"
wait_for 120 obs_reached "$WARMUP_BLOCKS" || fail "observer did not follow to block $WARMUP_BLOCKS"
if [[ $SCENARIO == observer-attacker ]]; then
    atk_reached() { local h; h=$(height atk); [[ -n $h && $h -ge $1 ]]; }
    wait_for 120 atk_reached "$WARMUP_BLOCKS" || fail "attacker did not follow to block $WARMUP_BLOCKS"
    ATK_PEER=$(peer_id_of "$RUN/attacker")
    log "attacker following at $(height atk), peer id $ATK_PEER"
fi
OBS_STOP=$(height obs)
log "warm-up done: validators at $(max_val_height), observer following at $OBS_STOP"

# Stop the observer to open a gap, then let the validators build $GAP more blocks.
stop_pid "$OBS_PID"; OBS_PID=""
log "observer stopped at block $OBS_STOP"
TARGET_TOP=$((OBS_STOP + GAP))
wait_for 300 val_reached "$TARGET_TOP" || fail "validators did not reach block $TARGET_TOP"
log "validators advanced to $(max_val_height); gap for the observer is $((OBS_STOP + 1))..$TARGET_TOP"


# Restart the observer. It catches up through the gap, fetching the missing blocks from the
# validators, and caches the poisoned final-of-schedule scores in its canonical store. It may
# already hold a poisoned row from following live just before it was stopped, in which case this
# start is the first abort.
CRASHES=0
STARTS=0
: >"$RUN/observer.log"
start_observer "$RUN/observer.log"
kill -0 "$OBS_PID" 2>/dev/null || fail "observer did not restart; see its log"
log "observer restarted to catch up through the poisoned gap"
obs_down_or_reached() { ! kill -0 "$OBS_PID" 2>/dev/null || obs_reached "$TARGET_TOP"; }
if ! wait_for 300 obs_down_or_reached; then
    # It may have caught up and then been poisoned already; only fail if it never progressed.
    h=$(height obs)
    [[ -n $h && $h -gt $OBS_STOP ]] || fail "observer did not catch up past $OBS_STOP (now at ${h:-none})"
    log "observer reached ${h} (did not fully reach $TARGET_TOP, continuing)"
fi
SERVED=$(poison_served)
cp "$RUN/observer.log" "$RUN/observer-catchup.log"
if [[ $SCENARIO == observer-attacker ]]; then
    log "victim log lines naming the attacker peer id during catch-up: $(grep -ac "${ATK_PEER:-none}" "$RUN/observer.log")"
fi
if ! kill -0 "$OBS_PID" 2>/dev/null; then
    obs_crashed || fail "observer exited during catch-up without a divide-by-zero; see its log"
    CRASHES=1
    STARTS=1
    panic_line=$(grep -aE 'attempt to divide by zero|divide by zero' "$RUN/observer.log" | head -1 \
        | sed 's/\x1b\[[0-9;]*m//g' | cut -c1-200)
    log "catch-up restart: observer ABORTED at startup: $panic_line"
    log "  armed nodes had served $SERVED poison replies"
    cp "$RUN/observer.log" "$RUN/observer-crash-0.log"
    OBS_PID=""
else
    log "observer caught up to $(height obs); armed nodes served $SERVED poison replies"
    if [[ $ARMED == on && $SERVED -eq 0 ]]; then
        fail "no poison was served; the observer did not gap-fetch from an armed node"
    fi
    # Stop only the observer, keeping its DB. The validators keep running: an observer with no
    # peer waits before starting consensus and would never reach the leader-schedule rebuild.
    log "stopping the observer, DB kept, before the crash-loop test; validators stay up"
    stop_pid "$OBS_PID"; OBS_PID=""
fi

# Restart the observer, repeatedly. Each start rebuilds the leader schedule from the newest stored
# final-of-schedule row when it starts consensus, and aborts on the divide-by-zero if that row is
# the poison.
for attempt in $(seq 1 "$RESTARTS"); do
    STARTS=$((STARTS + 1))
    : >"$RUN/observer.log"
    start_observer "$RUN/observer.log"
    # a crash is fast (it happens at startup); give it a short window to either abort or come up
    crashed=0
    for _ in $(seq 1 60); do
        if ! kill -0 "$OBS_PID" 2>/dev/null; then crashed=1; break; fi
        sleep 1
    done
    if [[ $crashed -eq 1 ]] && obs_crashed; then
        CRASHES=$((CRASHES + 1))
        panic_line=$(grep -aE 'attempt to divide by zero|divide by zero' "$RUN/observer.log" | head -1 \
            | sed 's/\x1b\[[0-9;]*m//g' | cut -c1-200)
        log "restart $attempt: observer ABORTED at startup: $panic_line"
        cp "$RUN/observer.log" "$RUN/observer-crash-$attempt.log"
    elif [[ $crashed -eq 1 ]]; then
        log "restart $attempt: observer exited but without a divide-by-zero (see log)"
        stop_pid "$OBS_PID"; OBS_PID=""
        break
    else
        h=$(height obs)
        log "restart $attempt: observer stayed up (now at ${h:-none}); no crash"
        stop_pid "$OBS_PID"; OBS_PID=""
        break
    fi
    OBS_PID=""
done

SERVED=$(poison_served)
if [[ $CRASHES -ge 1 ]]; then
    log "ISSUE REPRODUCED: observer aborted on $CRASHES of $STARTS starts after it took the poison"
    log "  armed nodes served $SERVED poison replies (proof of delivery)"
    log "  the poisoned row is in the observer's consensus DB at $RUN/observer; cross-check with db-inspect"
    echo "logs and datadirs: $RUN"
    exit 1
fi

log "HEALTHY: observer did not crash-loop (armed=$ARMED, poison served=$SERVED)"
echo "logs and datadirs: $RUN"
exit 0
