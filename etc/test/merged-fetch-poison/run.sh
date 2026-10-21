#!/usr/bin/env bash
#
# Live reproduction: one validator makes the certificate catch-up of a restarting validator fail.
# See README.md in this directory.
#
# Starts a local committee of validators, one of them armed to answer missing-certificates requests
# with a single unusable certificate. Restarts a different validator so it enters catch-up mode and
# fetches missing certificates from all peers at once, merging the answers into one list. The one
# bad certificate fails the whole merged batch, so the victim's catch-up fetch never succeeds while
# the armed node answers. A control phase then disarms the attacker and shows the fetch recovering,
# isolating the attacker as the cause.
#
# The poison behaviour is compiled only with the `merged-fetch-poison` cargo feature and is armed
# per process by environment, so the one armed validator lies while the rest stay honest.
#
# Exit status: 0 the victim's catch-up fetch succeeded despite the attacker (healthy; bug absent or
# fixed), 1 the victim's catch-up fetch failed on every round while the attacker answered (bug
# present, issue reproduced), 2 the run was inconclusive or could not start.

set -uo pipefail

usage() {
    cat <<EOF
Usage: $0 [-n VALIDATORS] [-b BINARY] [-d WORKDIR] [-w WATCH_SECS] [-g GAP] [-i INSTANCE_BASE]

  -n  validators in the committee, at least 5 so quorum is 4 and the chain keeps
      producing with the victim down (default 5)
  -b  node binary built with --features merged-fetch-poison
      (default: build target/debug/rayls-network from this checkout with the feature)
  -d  directory for datadirs and logs (default: target/merged-fetch-poison/<timestamp>)
  -w  seconds to watch the victim after it restarts in catch-up mode (default 150)
  -g  how many blocks the victim falls behind before catching up (default 40)
  -i  first --instance number, which sets the RPC ports (default 40)
EOF
    exit 2
}

N=5
BIN=""
WORKDIR=""
WATCH=150
GAP=40
INSTANCE_BASE=40
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
[[ $N =~ ^[0-9]+$ && $N -ge 5 ]] || { echo "need at least 5 validators"; usage; }
for value in "$WATCH" "$GAP" "$INSTANCE_BASE"; do
    [[ $value =~ ^[0-9]+$ ]] || { echo "not a number: $value"; usage; }
done

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
WORKDIR=${WORKDIR:-$REPO/target/merged-fetch-poison/$(date -u +%Y%m%dT%H%M%SZ)}
if [[ -d $WORKDIR && -n $(ls -A "$WORKDIR" 2>/dev/null) ]]; then
    echo "not empty: $WORKDIR"; exit 2
fi
RUN=$WORKDIR

# The attacker is validator 1; it answers every missing-certificates request with one bad
# certificate. The victim is validator 2; it restarts with local history and catches up.
ATTACKER=1
VICTIM=2
WARMUP_BLOCKS=20
CHAIN_ID=0x1e7
EPOCH_SECS=3600
DEV_ACCOUNT=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
export RL_BLS_PASSPHRASE="merged-fetch-poison-local-only"

declare -A PIDS
log() { echo "$(date -u +%T) $*" | tee -a "$RUN/summary.txt"; }
fail() { log "INCONCLUSIVE: $*"; echo "logs and datadirs: $RUN"; exit 2; }

for tool in curl; do
    command -v "$tool" >/dev/null || { echo "missing required tool: $tool"; exit 2; }
done

if [[ -z $BIN ]]; then
    echo "building rayls-network (debug) with --features merged-fetch-poison"
    (cd "$REPO" && cargo build -p rayls-network --features merged-fetch-poison) || exit 2
    BIN=$REPO/target/debug/rayls-network
fi
[[ -x $BIN ]] || { echo "not executable: $BIN"; exit 2; }

# RPC port for validator seq (1..N).
rpc_port() { echo $((8546 - (INSTANCE_BASE + $1))); }

# eth_blockNumber as a decimal, empty when the node does not answer.
height() {
    local out
    out=$(curl -s -m 2 -H 'content-type: application/json' \
        -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
        "http://127.0.0.1:$(rpc_port "$1")" 2>/dev/null) || return 0
    [[ $out =~ \"result\":\"0x([0-9a-fA-F]+)\" ]] && echo $((16#${BASH_REMATCH[1]}))
}

# Highest height across the honest active validators (every validator except the attacker and the
# victim), so "the chain advanced" does not depend on either of those two.
max_honest_height() {
    local best=-1 h v
    for v in $(seq 1 "$N"); do
        [[ $v -eq $ATTACKER || $v -eq $VICTIM ]] && continue
        h=$(height "$v"); [[ -n $h && $h -gt $best ]] && best=$h
    done
    echo "$best"
}

VLOG() { echo "$RUN/validator-$VICTIM.log"; }

# Count occurrences of a fixed string in the victim log since the current catch-up phase started.
# Colour codes are stripped so the match is on the plain message.
vcount() { sed 's/\x1b\[[0-9;]*m//g' "$(VLOG)" 2>/dev/null | grep -c -F "$1"; }

node_args() {
    local seq=$1
    NODE_ARGS=(node --datadir "$RUN/validator-$seq" --network local
        --instance $((INSTANCE_BASE + seq)) --log.stdout.format log-fmt --full --storage.v2
        --db.growth-step 1MB --consensus-db.growth-step 1MB --txpool.minimal-protocol-fee 0
        --gpo.default-suggested-fee 0 -vvv --http --http.addr 127.0.0.1 --http.api all
        --metrics "127.0.0.1:$((9200 + seq))")
}

# Start validator $1, logging to $2. The attacker is armed only while ARM is non-empty.
start_validator() {
    local seq=$1 logfile=$2
    node_args "$seq"
    if [[ $seq -eq $ATTACKER && -n ${ARM:-} ]]; then
        RL_POISON_CERT_FETCH=1 "$BIN" "${NODE_ARGS[@]}" >>"$logfile" 2>&1 &
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
honest_reached() { [[ $(max_honest_height) -ge $1 ]]; }
victim_reached() { local h; h=$(height "$VICTIM"); [[ -n $h && $h -ge $1 ]]; }

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
log "network: $N validators; attacker is validator $ATTACKER (armed), victim is validator $VICTIM"

# The attacker is armed from the start. Poisoning only triggers for missing-certificates requests,
# which no one sends until a node catches up, so the warm-up proceeds normally.
ARM=1
for seq in $(seq 1 "$N"); do start_validator "$seq" "$RUN/validator-$seq.log"; done
sleep 5
for seq in $(seq 1 "$N"); do
    kill -0 "${PIDS[$seq]}" 2>/dev/null || fail "validator $seq did not start; see its log"
done

wait_for 240 honest_reached "$WARMUP_BLOCKS" || fail "chain did not reach block $WARMUP_BLOCKS"
wait_for 120 victim_reached "$WARMUP_BLOCKS" || fail "victim did not follow to block $WARMUP_BLOCKS"
VICTIM_STOP=$(height "$VICTIM")
log "warm-up done: honest validators at $(max_honest_height), victim at $VICTIM_STOP"

# Stop the victim to open a gap, then let the honest validators build $GAP more blocks. With the
# victim down there are still $((N - 1)) >= 4 validators up, so the chain keeps producing.
stop_pid "${PIDS[$VICTIM]}"; unset "PIDS[$VICTIM]"
log "victim (validator $VICTIM) stopped at block $VICTIM_STOP"
TARGET_TOP=$((VICTIM_STOP + GAP))
wait_for 300 honest_reached "$TARGET_TOP" || fail "honest validators did not reach block $TARGET_TOP"
log "honest validators advanced to $(max_honest_height); the victim is $GAP blocks behind"

# Restart the victim. It has local history, so it enters catch-up mode (CvvInactive) and fetches
# missing certificates from all peers at once, merging the answers. The attacker's one bad
# certificate fails the whole merged batch on every round.
: >"$(VLOG)"
start_validator "$VICTIM" "$(VLOG)"
kill -0 "${PIDS[$VICTIM]}" 2>/dev/null || fail "victim did not restart; see its log"
log "victim restarted in catch-up mode, watching ${WATCH}s for its merged fetch to fail"

waited=0
while [[ $waited -lt $WATCH ]]; do
    sleep 10; waited=$((waited + 10))
    [[ $((waited % 30)) -eq 0 ]] && log "watching: victim at $(height "$VICTIM"), honest at $(max_honest_height), merged-fetch errors so far $(vcount 'Error from fetch certificates task')"
done

POISON_ANSWERS=$(sed 's/\x1b\[[0-9;]*m//g' "$(VLOG)" | grep -c -E 'peer .* returned 1 certificates')
FETCH_ERRORS=$(vcount 'Error from fetch certificates task')
SKIP_CLEARS=$(vcount 'clearing skip_rounds - stale session fallback after repeated failures')
FETCH_SUCCESSES=$(vcount 'Finished task to fetch certificates successfully')
ATTACKER_POISONS=$(sed 's/\x1b\[[0-9;]*m//g' "$RUN/validator-$ATTACKER.log" | grep -c -F 'POISON CERT-FETCH')

log "measured over ${WATCH}s of catch-up:"
log "  attacker poison replies served ....... $ATTACKER_POISONS"
log "  victim 'peer returned 1 certificates'  $POISON_ANSWERS"
log "  victim merged-fetch hard failures .... $FETCH_ERRORS"
log "  victim skip_rounds clears ............ $SKIP_CLEARS"
log "  victim merged-fetch successes ........ $FETCH_SUCCESSES"

REPRODUCED=0
if [[ $ATTACKER_POISONS -gt 0 && $FETCH_ERRORS -gt 0 && $FETCH_SUCCESSES -eq 0 ]]; then
    REPRODUCED=1
fi

if [[ $REPRODUCED -ne 1 ]]; then
    log "HEALTHY: victim's catch-up fetch did not fail under the attacker (successes $FETCH_SUCCESSES, attacker poisons $ATTACKER_POISONS); bug absent or fixed"
    echo "logs and datadirs: $RUN"
    exit 0
fi

# Control: disarm the attacker and restart it honest. The victim's merged fetch should now succeed,
# which isolates the attacker as the cause.
log "ISSUE REPRODUCED: the victim's catch-up fetch failed on every round while the attacker answered"
log "control: disarming the attacker and watching the victim's fetch recover"
BEFORE_SUCCESS=$FETCH_SUCCESSES
stop_pid "${PIDS[$ATTACKER]}"; unset "PIDS[$ATTACKER]"
ARM=""
start_validator "$ATTACKER" "$RUN/validator-$ATTACKER.log"
RECOVERED=0
for _ in $(seq 1 18); do
    sleep 10
    now_success=$(vcount 'Finished task to fetch certificates successfully')
    if [[ $now_success -gt $BEFORE_SUCCESS ]]; then RECOVERED=1; break; fi
done
if [[ $RECOVERED -eq 1 ]]; then
    log "control confirmed: with the attacker disarmed the victim's merged fetch succeeded (successes now $(vcount 'Finished task to fetch certificates successfully')); the attacker was the sole cause"
else
    log "control inconclusive: the victim's fetch did not record a success within the control window; see $(VLOG)"
fi
grep -a 'POISON CERT-FETCH' "$RUN/validator-$ATTACKER.log" | tail -1 \
    | sed 's/\x1b\[[0-9;]*m//g' | cut -c1-200 | sed 's/^/  attacker log: /' | tee -a "$RUN/summary.txt"
echo "logs and datadirs: $RUN"
exit 1
