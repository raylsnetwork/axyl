#!/usr/bin/env bash
#
# Live reproduction: one crafted missing-certificates request crashes all victims in a committee.
# See README.md in this directory.
#
# Starts a local network of validators, lets them advance past a warmup height, stops one
# validator and restarts it armed with the overflow hook. 20 seconds later the armed validator
# sends the crafted request to every other validator. On current code all victims abort at
# certificate_store.rs:415 ("attempt to add with overflow") and block production halts.
#
# Exit status: 0 all victims survived (healthy; bug absent or fixed),
# 1 at least one victim aborted (issue reproduced),
# 2 the run was inconclusive or could not start.

set -uo pipefail

usage() {
    cat <<EOF
Usage: $0 [-n VALIDATORS] [-b BINARY] [-d WORKDIR] [-i INSTANCE_BASE]

  -n  validators in the committee, at least 4 (default 4)
  -b  node binary built with --features byzantine-test-hooks in release mode
      (default: build target/release/rayls-network from this checkout)
  -d  directory for datadirs and logs (default: target/round-overflow-crash/<timestamp>)
  -i  first --instance number, which sets the RPC ports (default 60)
EOF
    exit 2
}

N=4
BIN=""
WORKDIR=""
INSTANCE_BASE=60
while getopts "n:b:d:i:h" opt; do
    case "$opt" in
        n) N=$OPTARG ;;
        b) BIN=$OPTARG ;;
        d) WORKDIR=$OPTARG ;;
        i) INSTANCE_BASE=$OPTARG ;;
        *) usage ;;
    esac
done
[[ $N =~ ^[0-9]+$ && $N -ge 4 ]] || { echo "need at least 4 validators"; usage; }
[[ $INSTANCE_BASE =~ ^[0-9]+$ ]] || { echo "not a number: $INSTANCE_BASE"; usage; }

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
WORKDIR=${WORKDIR:-$REPO/target/round-overflow-crash/$(date -u +%Y%m%dT%H%M%SZ)}
if [[ -d $WORKDIR && -n $(ls -A "$WORKDIR" 2>/dev/null) ]]; then
    echo "not empty: $WORKDIR"; exit 2
fi
RUN=$WORKDIR

ATTACKER=1
WARMUP_BLOCKS=10
ATTACK_DELAY=20
CHAIN_ID=0x1e8
EPOCH_SECS=3600
DEV_ACCOUNT=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
export RL_BLS_PASSPHRASE="round-overflow-local-only"

declare -A PIDS
log() { echo "$(date -u +%T) $*" | tee -a "$RUN/summary.txt"; }
fail() { log "INCONCLUSIVE: $*"; echo "logs and datadirs: $RUN"; exit 2; }

for tool in curl; do
    command -v "$tool" >/dev/null || { echo "missing required tool: $tool"; exit 2; }
done

if [[ -z $BIN ]]; then
    echo "building rayls-network (release) with --features byzantine-test-hooks"
    (cd "$REPO" && cargo build -p rayls-network --release --features byzantine-test-hooks) || exit 2
    BIN=$REPO/target/release/rayls-network
fi
[[ -x $BIN ]] || { echo "not executable: $BIN"; exit 2; }

rpc_port() { echo $((8546 - (INSTANCE_BASE + $1))); }

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
    if [[ $seq -eq $ATTACKER && ${ARM:-0} -eq 1 ]]; then
        RL_ROUND_OVERFLOW_ATTACK=1 RL_ROUND_OVERFLOW_DELAY="$ATTACK_DELAY" \
            "$BIN" "${NODE_ARGS[@]}" >>"$logfile" 2>&1 &
    else
        "$BIN" "${NODE_ARGS[@]}" >>"$logfile" 2>&1 &
    fi
    PIDS[$seq]=$!
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
    for seq in "${!PIDS[@]}"; do stop_pid "${PIDS[$seq]}"; done
    PIDS=()
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

setup_network() {
    rm -rf "$RUN"; mkdir -p "$RUN/genesis/validators"
    local seq
    for seq in $(seq 1 "$N"); do
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
}

# --- run -------------------------------------------------------------------------------------

setup_network || fail "network setup failed"
log "network: $N validators; attacker is validator $ATTACKER"

for seq in $(seq 1 "$N"); do start_validator "$seq" "$RUN/validator-$seq.log"; done
sleep 5
for seq in $(seq 1 "$N"); do
    kill -0 "${PIDS[$seq]}" 2>/dev/null || fail "validator $seq did not start; see its log"
done

wait_for 240 val_reached "$WARMUP_BLOCKS" || fail "chain did not reach block $WARMUP_BLOCKS"
log "warm-up done: all $N validators at $(max_val_height)"

# Stop the attacker and restart it armed.
stop_pid "${PIDS[$ATTACKER]}"; unset "PIDS[$ATTACKER]"
ARM=1 start_validator "$ATTACKER" "$RUN/validator-$ATTACKER.log"
log "validator $ATTACKER restarted with RL_ROUND_OVERFLOW_ATTACK=1 (fires in ${ATTACK_DELAY}s)"

# Wait for the attack to fire and check for crashes.
WAIT=$((ATTACK_DELAY + 15))
log "waiting ${WAIT}s for the attack to fire and victims to abort..."
sleep "$WAIT"

dead=0; alive=0
for seq in $(seq 1 "$N"); do
    [[ $seq -eq $ATTACKER ]] && continue
    if kill -0 "${PIDS[$seq]}" 2>/dev/null; then
        alive=$((alive + 1))
    else
        dead=$((dead + 1))
        log "DEAD: validator $seq (pid ${PIDS[$seq]})"
    fi
done

if kill -0 "${PIDS[$ATTACKER]}" 2>/dev/null; then
    log "attacker validator $ATTACKER is alive"
else
    log "WARNING: attacker validator $ATTACKER also died (unexpected)"
fi

# Check block production after the crash.
h1=$(max_val_height); sleep 5; h2=$(max_val_height)

if [[ $dead -gt 0 ]]; then
    log "ISSUE REPRODUCED: $dead of $((N - 1)) victim validators aborted"
    # Show the panic from the victims' stderr (which goes to their log file).
    for seq in $(seq 1 "$N"); do
        [[ $seq -eq $ATTACKER ]] && continue
        panic=$(grep -a 'panicked at\|attempt to add with overflow' "$RUN/validator-$seq.log" 2>/dev/null | tail -1)
        [[ -n $panic ]] && log "  validator $seq: $(echo "$panic" | sed 's/\x1b\[[0-9;]*m//g' | cut -c1-200)"
    done
    # Show the attacker's send log.
    grep -a 'round-overflow-attack' "$RUN/validator-$ATTACKER.log" 2>/dev/null \
        | sed 's/\x1b\[[0-9;]*m//g' | cut -c1-200 | sed 's/^/  attacker: /' | tee -a "$RUN/summary.txt"
    if [[ $h1 -eq $h2 || $h2 -le 0 ]]; then
        log "  chain halted: block height stuck at $h1"
    else
        log "  chain still advancing: $h1 -> $h2 (quorum not lost)"
    fi
    echo "logs and datadirs: $RUN"
    exit 1
fi

log "HEALTHY: all $((N - 1)) victims survived; chain at $(max_val_height)"
echo "logs and datadirs: $RUN"
exit 0
