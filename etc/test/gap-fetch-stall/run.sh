#!/usr/bin/env bash
#
# Live reproduction: a single peer forks a following node through the by-number consensus gap
# fetch. See README.md in this directory.
#
# Starts a local network of validators plus one observer, lets the observer fall behind, arms a
# single validator to answer the observer's by-number gap fetches with a real leader but a bumped
# commit timestamp, and checks whether the observer executes that forged header and forks.
#
# The attacker behaviour is compiled only with the `byzantine-test-hooks` cargo feature and is
# armed per process by environment, so the one armed validator lies while the rest stay honest.
#
# Exit status: 0 the observer stayed on the canonical chain (healthy; bug absent or fixed),
# 1 the observer executed a forged block and forked (bug present, issue reproduced),
# 2 the run was inconclusive or could not start.

set -uo pipefail

usage() {
    cat <<EOF
Usage: $0 [-n VALIDATORS] [-b BINARY] [-d WORKDIR] [-w WATCH_SECS] [-g GAP] [-i INSTANCE_BASE]

  -n  validators in the committee, at least 4 (default 4)
  -b  node binary built with --features byzantine-test-hooks
      (default: build target/debug/rayls-network from this checkout with the feature)
  -d  directory for datadirs and logs (default: target/gap-fetch-stall/<timestamp>)
  -w  seconds to watch the observer after it restarts (default 150)
  -g  how many blocks the observer falls behind before catching up (default 50)
  -i  first --instance number, which sets the RPC ports (default 30)
EOF
    exit 2
}

N=4
BIN=""
WORKDIR=""
WATCH=150
GAP=50
INSTANCE_BASE=30
while getopts "n:b:d:w:g:i:h" opt; do
    case "$opt" in
        n) N=$OPTARG ;;
        b) BIN=$OPTARG ;;
        d) WORKDIR=$OPTARG ;;
        w) WATCH=$OPTARG ;;
        g) GAP=$OPTARG ;;
        i) INSTANCE_BASE=$OPTARG ;;
        *) usage ;;
    esac
done
[[ $N =~ ^[0-9]+$ && $N -ge 4 ]] || { echo "need at least 4 validators"; usage; }
for value in "$WATCH" "$GAP" "$INSTANCE_BASE"; do
    [[ $value =~ ^[0-9]+$ ]] || { echo "not a number: $value"; usage; }
done

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
WORKDIR=${WORKDIR:-$REPO/target/gap-fetch-stall/$(date -u +%Y%m%dT%H%M%SZ)}
if [[ -d $WORKDIR && -n $(ls -A "$WORKDIR" 2>/dev/null) ]]; then
    echo "not empty: $WORKDIR"; exit 2
fi
RUN=$WORKDIR

# The attacker is validator 1. The victim observer has this instance number.
ATTACKER=1
OBS_INSTANCE=$((INSTANCE_BASE + N + 1))
WARMUP_BLOCKS=20
BUMP=1000000
CHAIN_ID=0x1e7
EPOCH_SECS=3600
DEV_ACCOUNT=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
export RL_BLS_PASSPHRASE="gap-fetch-stall-local-only"

declare -A PIDS
OBS_PID=""
log() { echo "$(date -u +%T) $*" | tee -a "$RUN/summary.txt"; }
fail() { log "INCONCLUSIVE: $*"; echo "logs and datadirs: $RUN"; exit 2; }

for tool in curl; do
    command -v "$tool" >/dev/null || { echo "missing required tool: $tool"; exit 2; }
done

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

# Fetch field "$3" (hash|timestamp) of block number $2 (decimal) from node $1. Timestamp is decimal.
block_field() {
    local port num_hex out
    port=$(rpc_port "$1")
    printf -v num_hex '0x%x' "$2"
    out=$(curl -s -m 3 -H 'content-type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getBlockByNumber\",\"params\":[\"$num_hex\",false]}" \
        "http://127.0.0.1:$port" 2>/dev/null) || return 0
    case "$3" in
        hash) [[ $out =~ \"hash\":\"(0x[0-9a-fA-F]+)\" ]] && echo "${BASH_REMATCH[1]}" ;;
        timestamp) [[ $out =~ \"timestamp\":\"0x([0-9a-fA-F]+)\" ]] && echo $((16#${BASH_REMATCH[1]})) ;;
    esac
}

max_val_height() {
    local best=-1 h v
    for v in $(seq 1 "$N"); do h=$(height "$v"); [[ -n $h && $h -gt $best ]] && best=$h; done
    echo "$best"
}

# A healthy committee validator that is not the attacker, for canonical comparison.
HEALTHY=$((ATTACKER == N ? 1 : N))

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
    if [[ $seq -eq $ATTACKER && -n ${ARM_RANGE:-} ]]; then
        RL_BYZ_GAP_TARGET="$ARM_RANGE" RL_BYZ_GAP_MODE=fork RL_BYZ_GAP_BUMP="$BUMP" \
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
    # The observer is not a committee member. It is generated after genesis and only follows.
    "$BIN" keytool generate observer --datadir "$RUN/observer" \
        --address 0x0000000000000000000000000000000000000000 --advertise-identity-only >/dev/null 2>&1 || return 1
    mkdir -p "$RUN/observer/genesis"
    cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$RUN/observer/genesis/"
    cp "$RUN/parameters.yaml" "$RUN/observer/"
}

# --- run -------------------------------------------------------------------------------------

setup_network || fail "network setup failed"
log "network: $N validators + 1 observer; attacker is validator $ATTACKER, victim is the observer"

for seq in $(seq 1 "$N"); do start_validator "$seq" "$RUN/validator-$seq.log"; done
start_observer "$RUN/observer.log"
sleep 5
for seq in $(seq 1 "$N"); do
    kill -0 "${PIDS[$seq]}" 2>/dev/null || fail "validator $seq did not start; see its log"
done
kill -0 "$OBS_PID" 2>/dev/null || fail "observer did not start; see its log"

wait_for 240 val_reached "$WARMUP_BLOCKS" || fail "chain did not reach block $WARMUP_BLOCKS"
wait_for 120 obs_reached "$WARMUP_BLOCKS" || fail "observer did not follow to block $WARMUP_BLOCKS"
OBS_STOP=$(height obs)
log "warm-up done: validators at $(max_val_height), observer following at $OBS_STOP"

# Stop the observer to open a gap, then let the validators build $GAP more blocks.
stop_pid "$OBS_PID"; OBS_PID=""
log "observer stopped at block $OBS_STOP"
TARGET_TOP=$((OBS_STOP + GAP))
wait_for 300 val_reached "$TARGET_TOP" || fail "validators did not reach block $TARGET_TOP"
log "validators advanced to $(max_val_height); gap for the observer is $((OBS_STOP + 1))..$TARGET_TOP"

# Arm validator $ATTACKER for the gap range and restart it so the armed process rejoins.
ARM_RANGE="$((OBS_STOP + 1))-$TARGET_TOP"
stop_pid "${PIDS[$ATTACKER]}"; unset "PIDS[$ATTACKER]"
start_validator "$ATTACKER" "$RUN/validator-$ATTACKER.log"
log "validator $ATTACKER restarted and armed for numbers $ARM_RANGE (fork mode, timestamp bump $BUMP)"
wait_for 180 val_reached "$TARGET_TOP" || fail "armed validator $ATTACKER did not rejoin"

# Restart the observer. It now catches up through the gap and gap-fetches the missing numbers.
: >"$RUN/observer.log"
start_observer "$RUN/observer.log"
kill -0 "$OBS_PID" 2>/dev/null || fail "observer did not restart; see its log"
log "observer restarted, watching ${WATCH}s for a fork"

# Detect a fork: the observer executes a block whose timestamp is exactly $BUMP above the
# committee's block of the same number. Its executed tip stalls at that forged block.
FORKED_AT=""; waited=0
while [[ $waited -lt $WATCH ]]; do
    sleep 5; waited=$((waited + 5))
    h=$(height obs); [[ -z $h ]] && continue
    obs_ts=$(block_field obs "$h" timestamp)
    can_ts=$(block_field "$HEALTHY" "$h" timestamp)
    if [[ -n $obs_ts && -n $can_ts && $obs_ts -ne $can_ts && $((obs_ts - can_ts)) -eq $BUMP ]]; then
        FORKED_AT=$h; break
    fi
    [[ $((waited % 20)) -eq 0 ]] && log "watching: observer at ${h:-?}, validators at $(max_val_height)"
done

if [[ -n $FORKED_AT ]]; then
    obs_hash=$(block_field obs "$FORKED_AT" hash)
    can_hash=$(block_field "$HEALTHY" "$FORKED_AT" hash)
    obs_ts=$(block_field obs "$FORKED_AT" timestamp)
    can_ts=$(block_field "$HEALTHY" "$FORKED_AT" timestamp)
    sleep 10; stalled=$(height obs)
    log "ISSUE REPRODUCED: observer forked at block $FORKED_AT"
    log "  observer  block $FORKED_AT: hash $obs_hash timestamp $obs_ts"
    log "  canonical block $FORKED_AT: hash $can_hash timestamp $can_ts (difference $((obs_ts - can_ts)) = bump)"
    log "  observer executed tip stalled at $stalled while validators are at $(max_val_height)"
    grep -a 'BYZANTINE GAP-FETCH' "$RUN/validator-$ATTACKER.log" | tail -1 \
        | sed 's/\x1b\[[0-9;]*m//g' | cut -c1-200 | sed 's/^/  attacker log: /' | tee -a "$RUN/summary.txt"
    echo "logs and datadirs: $RUN"
    exit 1
fi

log "HEALTHY: observer stayed on the canonical chain (at $(height obs), validators at $(max_val_height)); no fork in ${WATCH}s"
echo "logs and datadirs: $RUN"
exit 0
