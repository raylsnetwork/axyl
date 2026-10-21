#!/usr/bin/env bash
#
# Live reproduction: one committee validator strands a following node by returning a forged header to its tip probe.
# See README.md in this directory.
#
# A following node with no consensus gossip learns the chain tip only by probing every committee
# member for its latest consensus header and keeping the highest number. One validator answers that
# probe with a fake header numbered u64::MAX, so it wins every probe. The follower then fetches that
# header, waits out a committee it will never get, and never reaches the real tip.
#
# The attacker behaviour is compiled only with the `byzantine-test-hooks` cargo feature and is
# armed per process by environment, so the one armed validator returns a forged header while the rest stay honest.
#
# This run has two phases on one network: first with the adversary armed, then a control with it honest.
#
# Exit status: 0 the observer reached the tip in both the adversarial phase and the control (healthy; bug
# absent or fixed), 1 the observer never reached the tip in the adversarial phase but did in the control
# (bug present, issue reproduced), 2 the run was inconclusive or could not start.

set -uo pipefail

usage() {
    cat <<EOF
Usage: $0 [-b BINARY] [-d WORKDIR] [-w WATCH_SECS] [-i INSTANCE_BASE]

  -b  node binary built with --features byzantine-test-hooks
      (default: build target/debug/rayls-network from this checkout with the feature)
  -d  directory for datadirs and logs (default: target/tip-probe-strand/<timestamp>)
  -w  seconds to watch the observer in each phase (default 180)
  -i  first --instance number, which sets the RPC ports (default 40)
EOF
    exit 2
}

BIN=""
WORKDIR=""
WATCH=180
INSTANCE_BASE=40
while getopts "b:d:w:i:h" opt; do
    case "$opt" in
        b) BIN=$OPTARG ;;
        d) WORKDIR=$OPTARG ;;
        w) WATCH=$OPTARG ;;
        i) INSTANCE_BASE=$OPTARG ;;
        *) usage ;;
    esac
done
for value in "$WATCH" "$INSTANCE_BASE"; do
    [[ $value =~ ^[0-9]+$ ]] || { echo "not a number: $value"; usage; }
done

# Four validators and one observer. The adversary is validator 1. The two stopped validators are 3 and
# 4; keeping 1 (adversary) and 2 (honest) up halts consensus (2 of 4 is below the quorum of 3) so no new
# output is gossiped, which is the condition the probe is the sole tip source under.
N=4
ADVERSARY=1
HONEST=2
HALT_A=3
HALT_B=4
WARMUP_BLOCKS=20
CHAIN_ID=0x1e7
EPOCH_SECS=3600
DEV_ACCOUNT=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
FAKE_NUMBER=18446744073709551615
OBS_INSTANCE=$((INSTANCE_BASE + N + 1))
export RL_BLS_PASSPHRASE="tip-probe-strand-local-only"

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
WORKDIR=${WORKDIR:-$REPO/target/tip-probe-strand/$(date -u +%Y%m%dT%H%M%SZ)}
if [[ -d $WORKDIR && -n $(ls -A "$WORKDIR" 2>/dev/null) ]]; then
    echo "not empty: $WORKDIR"; exit 2
fi
RUN=$WORKDIR

declare -A PIDS
OBS_PID=""
REACHED=0
log() { echo "$(date -u +%T) $*" | tee -a "$RUN/summary.txt"; }
fail() { log "INCONCLUSIVE: $*"; echo "logs and datadirs: $RUN"; exit 2; }

command -v curl >/dev/null || { echo "missing required tool: curl"; exit 2; }

if [[ -z $BIN ]]; then
    echo "building rayls-network (debug) with --features byzantine-test-hooks"
    (cd "$REPO" && cargo build -p rayls-network --features byzantine-test-hooks) || exit 2
    BIN=$REPO/target/debug/rayls-network
fi
[[ -x $BIN ]] || { echo "not executable: $BIN"; exit 2; }

# RPC port for a validator seq (1..N) or for the observer when given "obs".
rpc_port() {
    if [[ $1 == obs ]]; then echo $((8546 - OBS_INSTANCE))
    else echo $((8546 - (INSTANCE_BASE + $1))); fi
}

# eth_blockNumber as a decimal, empty when the node does not answer.
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

# Start validator $1 to log $2. When $ARM=1 and it is the adversary, arm the forged-tip hook.
start_validator() {
    local seq=$1 logfile=$2
    node_args "$seq"
    if [[ $seq -eq $ADVERSARY && ${ARM:-0} -eq 1 ]]; then
        RL_BYZ_FAKE_TIP=1 "$BIN" "${NODE_ARGS[@]}" >>"$logfile" 2>&1 &
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
    for seq in "${!PIDS[@]}"; do kill -TERM "${PIDS[$seq]}" 2>/dev/null; done
    [[ -n $OBS_PID ]] && kill -TERM "$OBS_PID" 2>/dev/null
    for seq in "${!PIDS[@]}"; do stop_pid "${PIDS[$seq]}"; done
    [[ -n $OBS_PID ]] && stop_pid "$OBS_PID"
    PIDS=(); OBS_PID=""
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
    for seq in $(seq 1 "$N") obs; do
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
}

# Run one observer phase. $1 is a label used for the observer log and messages. The caller sets ARM.
# Sets the global REACHED to the highest block number the observer reached. The adversary must already be
# (re)started with the matching ARM value, and consensus halted.
watch_observer() {
    local label=$1
    local logfile="$RUN/observer-$label.log"
    : >"$logfile"
    start_observer "$logfile"
    kill -0 "$OBS_PID" 2>/dev/null || fail "observer did not start in phase $label; see its log"
    local waited=0 best=0 h
    while [[ $waited -lt $WATCH ]]; do
        sleep 5; waited=$((waited + 5))
        h=$(height obs); [[ -n $h && $h -gt $best ]] && best=$h
        [[ $best -ge $TIP ]] && break
        [[ $((waited % 30)) -eq 0 ]] && log "  phase $label: observer at ${h:-?} (target $TIP), ${waited}s"
    done
    stop_pid "$OBS_PID"; OBS_PID=""
    REACHED=$best
}

# --- run -------------------------------------------------------------------------------------

setup_network || fail "network setup failed"
log "network: $N validators + 1 observer; adversary is validator $ADVERSARY, victim is the observer"

for seq in $(seq 1 "$N"); do start_validator "$seq" "$RUN/validator-$seq.log"; done
sleep 5
for seq in $(seq 1 "$N"); do
    kill -0 "${PIDS[$seq]}" 2>/dev/null || fail "validator $seq did not start; see its log"
done

wait_for 240 val_reached "$WARMUP_BLOCKS" || fail "chain did not reach block $WARMUP_BLOCKS"
TIP=$(max_val_height)
log "warm-up done: validators at $TIP"

# Halt consensus and silence gossip: stop two validators so only the adversary and one honest node
# remain, which is below the quorum of 3. A few already-certified outputs may still drain and
# execute after the stop, so wait for the height to go stable before taking it as the halted tip.
stop_pid "${PIDS[$HALT_A]}"; unset "PIDS[$HALT_A]"
stop_pid "${PIDS[$HALT_B]}"; unset "PIDS[$HALT_B]"
log "stopped validators $HALT_A and $HALT_B; waiting for consensus to halt"
PREV=-1; STILL=$(max_val_height); SETTLE=0
while [[ $SETTLE -lt 40 ]]; do
    sleep 10; SETTLE=$((SETTLE + 10))
    STILL=$(max_val_height)
    if [[ $STILL -eq $PREV ]]; then break; fi
    PREV=$STILL
done
[[ $SETTLE -lt 40 ]] || fail "chain did not stop advancing after stopping two validators (at $STILL); gossip not silent"
TIP=$STILL
log "consensus halted at block $TIP (gossip silent)"

# Phase 1: the adversary is armed. Restart validator $ADVERSARY with the forged-tip hook.
stop_pid "${PIDS[$ADVERSARY]}"; unset "PIDS[$ADVERSARY]"
ARM=1 start_validator "$ADVERSARY" "$RUN/validator-$ADVERSARY.log"
sleep 5
kill -0 "${PIDS[$ADVERSARY]}" 2>/dev/null || fail "armed adversary validator $ADVERSARY did not restart; see its log"
log "phase 1 (adversarial): validator $ADVERSARY armed with RL_BYZ_FAKE_TIP; starting observer, watching ${WATCH}s"
watch_observer adversarial
ADVERSARIAL_REACHED=$REACHED
log "phase 1 result: observer reached block $ADVERSARIAL_REACHED (target $TIP)"

# Phase 2 control: restart the adversary honest (no RL_BYZ_FAKE_TIP). Fresh observer datadir.
stop_pid "${PIDS[$ADVERSARY]}"; unset "PIDS[$ADVERSARY]"
ARM=0 start_validator "$ADVERSARY" "$RUN/validator-$ADVERSARY.log"
sleep 5
kill -0 "${PIDS[$ADVERSARY]}" 2>/dev/null || fail "honest validator $ADVERSARY did not restart; see its log"
rm -rf "$RUN/observer"
"$BIN" keytool generate observer --datadir "$RUN/observer" \
    --address 0x0000000000000000000000000000000000000000 --advertise-identity-only >/dev/null 2>&1 \
    || fail "control observer keygen failed"
mkdir -p "$RUN/observer/genesis"
cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$RUN/observer/genesis/"
cp "$RUN/parameters.yaml" "$RUN/observer/"
log "phase 2 (control): validator $ADVERSARY honest; starting fresh observer, watching ${WATCH}s"
watch_observer control
CONTROL_REACHED=$REACHED
log "phase 2 result: observer reached block $CONTROL_REACHED (target $TIP)"

# Evidence from the adversarial-phase observer log.
PROBE_LINE=$(grep -a "peer_latest=$FAKE_NUMBER" "$RUN/observer-adversarial.log" | head -1 \
    | sed 's/\x1b\[[0-9;]*m//g' | cut -c1-200)
TIMEOUT_LINE=$(grep -a "timed out waiting for committee keys" "$RUN/observer-adversarial.log" | head -1 \
    | sed 's/\x1b\[[0-9;]*m//g' | cut -c1-200)

log "--- evidence (adversarial phase) ---"
[[ -n $PROBE_LINE ]] && log "  probe followed the forged header: $PROBE_LINE"
[[ -n $TIMEOUT_LINE ]] && log "  walk stalled: $TIMEOUT_LINE"

if [[ $ADVERSARIAL_REACHED -lt $TIP && $CONTROL_REACHED -ge $TIP ]]; then
    log "ISSUE REPRODUCED: one validator stranded the follower"
    log "  adversarial phase: observer reached $ADVERSARIAL_REACHED of $TIP (stranded)"
    log "  control phase: observer reached $CONTROL_REACHED of $TIP (caught up)"
    echo "logs and datadirs: $RUN"
    exit 1
fi

if [[ $CONTROL_REACHED -lt $TIP ]]; then
    fail "control did not reach the tip ($CONTROL_REACHED of $TIP); cannot attribute the adversarial result to the forged header"
fi

log "HEALTHY: observer reached the tip in the adversarial phase ($ADVERSARIAL_REACHED of $TIP); no stranding"
echo "logs and datadirs: $RUN"
exit 0
