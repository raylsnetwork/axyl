#!/usr/bin/env bash
#
# Live reproduction: a node that syncs across an epoch where a validator joined the committee
# crashes when the first commit of that epoch reaches it before it has left the previous epoch.
# See README.md in this directory.
#
# Starts a local network of four validators, a fifth (post-genesis) node that follows the chain,
# and, in observer mode, an observer. Stakes the fifth node through the on-chain registry, stops
# the victim (validator 4, or the observer) in the last epoch before the fifth node is seated, and
# waits until it is seated and producing. Then restarts the victim a few times, as a supervisor
# would. On each restart the victim catches up across the committee change.
#
# A syncing node's header streamer releases a commit only once the node has executed the block its
# leader's header is anchored on. Honest headers of a new epoch are anchored on the previous
# epoch's closing block or later, and a node executes that block only after it has stopped the
# previous epoch's subscriber, so honest commits never reach that subscriber and the bug stays
# hidden. Two faults, both from the `fault-injection-test-hooks` build, open it up:
#
# - a faulty proposer (-s): every committee member except the victim anchors its headers of the
#   first rounds of each epoch on an older block, so whichever validator leads the new epoch's
#   first commit is faulty and that commit is released before the victim leaves the old epoch;
# - a slow victim (-t): the victim waits before stopping the old epoch's subscriber, as with a
#   slow checkpoint write or a stalled task, so the old subscriber is still running when that
#   commit arrives.
#
# The victim then looks the new validator up in the old committee, and exits. Run with -s 0 for
# honest proposers, which stays healthy with any -t.
#
# Exit status: 0 the victim synced past the committee change (healthy; bug absent, fixed, or
# hidden by honest proposers), 1 the victim's subscriber panicked on the new validator's commit
# (reproduced), 2 the run was inconclusive or could not start.

set -uo pipefail

usage() {
    cat <<EOF
usage: $0 [-m validator|observer] [-s rounds] [-t ms] [-l senders] [-b binary] [-d workdir] [-r restarts] [-e epoch-secs] [-i instance]
  -m  which node syncs across the committee change (default validator):
      validator  genesis validator 4, catching up to rejoin
      observer   an observer following the chain
  -s  rounds at the start of each epoch whose headers the other committee members anchor on an
      older block (default 20); 0 for honest proposers
  -t  milliseconds the victim waits at each epoch boundary before stopping the epoch's
      consensus tasks (default 5000); 0 for no delay
  -l  parallel transfer senders, so commits carry batches (default 16)
  -b  rayls-network binary built with --features fault-injection-test-hooks, and panic=abort as
      in release (default: build target/panic-abort/debug/rayls-network from this checkout)
  -d  empty directory for keys, genesis, datadirs and logs
      (default: target/committee-change-sync-crash/<timestamp>)
  -r  restarts of the victim after the committee change (default 3)
  -e  epoch duration in seconds (default 60)
  -i  first --instance number, which sets the RPC ports (default 40)
EOF
    exit 2
}

MODE=validator
STALE_ROUNDS=20
TEARDOWN_DELAY_MS=5000
# senders of a steady stream of transfers, so commits carry batches: the subscriber looks a
# certificate's author up only for a commit that has batches
LOAD_SENDERS=16
BIN=""
WORKDIR=""
RESTARTS=3
EPOCH_SECS=60
INSTANCE_BASE=40
while getopts "m:s:t:l:b:d:r:e:i:h" opt; do
    case "$opt" in
        m) MODE=$OPTARG ;;
        s) STALE_ROUNDS=$OPTARG ;;
        t) TEARDOWN_DELAY_MS=$OPTARG ;;
        l) LOAD_SENDERS=$OPTARG ;;
        b) BIN=$OPTARG ;;
        d) WORKDIR=$OPTARG ;;
        r) RESTARTS=$OPTARG ;;
        e) EPOCH_SECS=$OPTARG ;;
        i) INSTANCE_BASE=$OPTARG ;;
        *) usage ;;
    esac
done
[[ $MODE == validator || $MODE == observer ]] || { echo "unknown mode: $MODE"; usage; }
for value in "$STALE_ROUNDS" "$TEARDOWN_DELAY_MS" "$LOAD_SENDERS" "$RESTARTS" "$EPOCH_SECS" "$INSTANCE_BASE"; do
    [[ $value =~ ^[0-9]+$ ]] || { echo "not a number: $value"; usage; }
done
[[ $RESTARTS -ge 1 ]] || { echo "need at least one restart"; usage; }

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
WORKDIR=${WORKDIR:-$REPO/target/committee-change-sync-crash/$(date -u +%Y%m%dT%H%M%SZ)}
if [[ -d $WORKDIR && -n $(ls -A "$WORKDIR" 2>/dev/null) ]]; then
    echo "not empty: $WORKDIR"; exit 2
fi
RUN=$WORKDIR

# Nodes, by name: validator-1..4 (genesis committee), node5 (staked after genesis), observer.
N=4
REFERENCE=validator-1
if [[ $MODE == validator ]]; then VICTIM=validator-4; else VICTIM=observer; fi
WARMUP_BLOCKS=10
# blocks the chain must commit after the new validator is seated, before the victim restarts
SEATED_BLOCKS=20
# seconds a restarted victim gets to either crash or catch up
SYNC_SECS=180
CHAIN_ID=0x1e7
ADMIN_ADDR=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266   # anvil #0, local only
ADMIN_KEY=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
OPERATOR_KEY=0x$(printf '%064x' 1005)
STAKE_AMOUNT=5000000000000000000000000
RLS_ADDRESS=0x07E17e17E17e17E17e17E17E17E17e17e17E17eA
REGISTRY=0x07E17e17E17e17E17e17E17E17E17e17e17E17e1
GAS_FUND=1000000000000000000
ERC1967_IMPL_SLOT=0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc
export RL_BLS_PASSPHRASE="committee-change-local-only"

# the victim aborts on purpose; keep its multi-gigabyte debug core dumps off the disk
ulimit -c 0

declare -A PIDS
LOAD_PIDS=()
log() { echo "$(date -u +%T) $*" | tee -a "$RUN/summary.txt"; }
fail() { log "INCONCLUSIVE: $*"; echo "logs and datadirs: $RUN"; exit 2; }

for tool in curl cast; do
    command -v "$tool" >/dev/null || { echo "missing required tool: $tool (cast is part of Foundry)"; exit 2; }
done

if [[ -z $BIN ]]; then
    # Release builds abort the process on a panic. Build the debug binary the same way, in its own
    # target directory since the panic strategy applies to every crate: with the default unwinding
    # a panicking task does not stop the node.
    echo "building rayls-network (debug, panic=abort, --features fault-injection-test-hooks)"
    (cd "$REPO" && CARGO_PROFILE_DEV_PANIC=abort cargo build -p rayls-network \
        --features fault-injection-test-hooks --target-dir target/panic-abort) || exit 2
    BIN=$REPO/target/panic-abort/debug/rayls-network
fi
[[ -x $BIN ]] || { echo "not executable: $BIN"; exit 2; }
if [[ $STALE_ROUNDS -gt 0 || $TEARDOWN_DELAY_MS -gt 0 ]] && ! grep -qa 'FAULT TEARDOWN DELAY' "$BIN"; then
    echo "$BIN was built without --features fault-injection-test-hooks"; exit 2
fi

instance_of() {
    case "$1" in
        validator-*) echo $((INSTANCE_BASE + ${1#validator-})) ;;
        node5) echo $((INSTANCE_BASE + 5)) ;;
        observer) echo $((INSTANCE_BASE + 6)) ;;
    esac
}
rpc_port() { echo $((8546 - $(instance_of "$1"))); }

height() {
    local out
    out=$(curl -s -m 2 -H 'content-type: application/json' \
        -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
        "http://127.0.0.1:$(rpc_port "$1")" 2>/dev/null) || return 0
    [[ $out =~ \"result\":\"0x([0-9a-fA-F]+)\" ]] && echo $((16#${BASH_REMATCH[1]}))
}

start_node() {
    local name=$1
    local -a extra=() env=()
    [[ $name == observer ]] && extra=(--observer)
    # the victim's subscriber logs each header it takes at debug
    [[ $name == "$VICTIM" ]] && extra+=(--log.stdout.filter info,subscriber=debug)
    if [[ $name == "$VICTIM" ]]; then
        env=(RL_FAULT_TEARDOWN_DELAY_MS="$TEARDOWN_DELAY_MS")
    elif [[ $STALE_ROUNDS -gt 0 && $name != observer ]]; then
        env=(RL_FAULT_STALE_ANCHOR_ROUNDS="$STALE_ROUNDS")
    fi
    env "${env[@]}" "$BIN" node "${extra[@]}" --datadir "$RUN/$name" --network local --instance "$(instance_of "$name")" \
        --log.stdout.format log-fmt --full --storage.v2 --db.growth-step 1MB \
        --consensus-db.growth-step 1MB --txpool.minimal-protocol-fee 0 \
        --gpo.default-suggested-fee 0 -vvv --http --http.addr 127.0.0.1 --http.api all \
        >>"$RUN/$name.log" 2>&1 &
    PIDS[$name]=$!
}

stop_node() {
    local name=$1 pid=${PIDS[$1]:-}
    [[ -z $pid ]] && return 0
    kill -TERM "$pid" 2>/dev/null
    for _ in $(seq 1 40); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
    kill -KILL "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
    unset "PIDS[$name]"
    return 0
}

stop_all() {
    local name
    for name in "${LOAD_PIDS[@]}"; do kill "$name" 2>/dev/null; done
    for name in "${!PIDS[@]}"; do kill -TERM "${PIDS[$name]}" 2>/dev/null; done
    for name in "${!PIDS[@]}"; do stop_node "$name"; done
    return 0
}
trap stop_all EXIT
trap 'exit 2' INT TERM

wait_for() {
    local secs=$1; shift
    for _ in $(seq 1 "$secs"); do "$@" && return 0; sleep 1; done
    return 1
}
reached() { local h; h=$(height "$1"); [[ -n $h && $h -ge $2 ]]; }
bls_of() { grep '^bls_public_key:' "$1/node-info.yaml" | awk '{print $2}'; }

setup_network() {
    mkdir -p "$RUN/genesis/validators"
    local seq name
    for name in $(seq -f 'validator-%g' 1 "$N") node5 observer; do
        local port; port=$(rpc_port "$name")
        if (echo >"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
            fail "RPC port $port is in use; pick other instance numbers with -i"
        fi
    done
    for seq in $(seq 1 "$N"); do
        "$BIN" keytool generate validator --datadir "$RUN/validator-$seq" \
            --address "$(printf '0x%040x' $((0x5eed0000 + seq)))" >/dev/null 2>&1 || return 1
        cp "$RUN/validator-$seq/node-info.yaml" "$RUN/genesis/validators/validator-$seq.yaml"
    done
    "$BIN" genesis --datadir "$RUN" --chain-id $CHAIN_ID --epoch-duration-in-secs "$EPOCH_SECS" \
        --dev-funded-account $ADMIN_ADDR --max-header-delay-ms 1000 --min-header-delay-ms 500 \
        --consensus-registry-owner $ADMIN_ADDR --network-admin $ADMIN_ADDR >/dev/null 2>&1 || return 1
    # node5 joins after genesis: its operator address is baked into its proof of possession, and it
    # is not in committee.yaml.
    "$BIN" keytool generate validator --datadir "$RUN/node5" \
        --address "$(cast wallet address --private-key "$OPERATOR_KEY")" >/dev/null 2>&1 || return 1
    "$BIN" keytool generate observer --datadir "$RUN/observer" \
        --address 0x0000000000000000000000000000000000000000 >/dev/null 2>&1 || return 1
    for name in $(seq -f 'validator-%g' 1 "$N") node5 observer; do
        mkdir -p "$RUN/$name/genesis"
        cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$RUN/$name/genesis/"
        cp "$RUN/parameters.yaml" "$RUN/$name/"
    done
}

stake_node5() {
    local rpc="http://127.0.0.1:$(rpc_port "$REFERENCE")" opaddr ready=0 rls_impl reg_owner calldata
    opaddr=$(cast wallet address --private-key "$OPERATOR_KEY")
    for _ in $(seq 1 90); do
        rls_impl=$(cast storage "$RLS_ADDRESS" "$ERC1967_IMPL_SLOT" --rpc-url "$rpc" 2>/dev/null || true)
        reg_owner=$(cast call "$REGISTRY" "owner()(address)" --rpc-url "$rpc" 2>/dev/null || true)
        if [[ $rls_impl =~ [1-9a-fA-F] && -n $reg_owner && $reg_owner != 0x0000000000000000000000000000000000000000 ]]; then
            ready=1; break
        fi
        sleep 2
    done
    [[ $ready -eq 1 ]] || { log "genesis system contracts not live"; return 1; }
    cast send --private-key "$ADMIN_KEY" --rpc-url "$rpc" --value "$GAS_FUND" "$opaddr" >/dev/null 2>&1 || return 1
    cast send "$RLS_ADDRESS" "mint(address,uint256)" "$opaddr" "$STAKE_AMOUNT" \
        --private-key "$ADMIN_KEY" --rpc-url "$rpc" >/dev/null 2>&1 || return 1
    cast send "$REGISTRY" "allowlistValidator(address)" "$opaddr" \
        --private-key "$ADMIN_KEY" --rpc-url "$rpc" >/dev/null 2>&1 || log "allowlist may already be set"
    cast send "$RLS_ADDRESS" "approve(address,uint256)" "$REGISTRY" "$STAKE_AMOUNT" \
        --private-key "$OPERATOR_KEY" --rpc-url "$rpc" >/dev/null 2>&1 || return 1
    calldata=$("$BIN" keytool stake-calldata --datadir "$RUN/node5" | grep 'Calldata:' | awk '{print $2}')
    [[ -n $calldata ]] || { log "no stake calldata"; return 1; }
    cast send "$REGISTRY" "$calldata" --private-key "$OPERATOR_KEY" --rpc-url "$rpc" >/dev/null 2>&1 \
        || { log "stake tx failed"; return 1; }
    cast send "$REGISTRY" "activate()" --private-key "$OPERATOR_KEY" --rpc-url "$rpc" >/dev/null 2>&1 \
        || { log "activate tx failed"; return 1; }
}

start_load() {
    local rpc="http://127.0.0.1:$(rpc_port "$REFERENCE")" seq key
    for seq in $(seq 1 "$LOAD_SENDERS"); do
        key=0x$(printf '%064x' $((2000 + seq)))
        cast send --private-key "$ADMIN_KEY" --rpc-url "$rpc" --value "$GAS_FUND" \
            "$(cast wallet address --private-key "$key")" >/dev/null 2>&1 || return 1
        (
            while true; do
                cast send --async --private-key "$key" --rpc-url "$rpc" --value 1 \
                    0x000000000000000000000000000000000000dEaD >/dev/null 2>&1
                sleep 0.1
            done
        ) &
        LOAD_PIDS+=("$!")
    done
}

# The reference validator builds an epoch's committee from the registry at the epoch's start, and
# reads the committees of the next two epochs.
seated() { grep -a 'Created committee from state' "$RUN/$REFERENCE.log" | grep -qa "$NODE5_BLS"; }
committees_built() { grep -ac 'Created committee from state' "$RUN/$REFERENCE.log"; }
# The first epoch whose committee includes node5.
seat_epoch() {
    grep -a 'validators_for_epoch: contract returned validators' "$RUN/$REFERENCE.log" \
        | grep -a "validator_count=$((N + 1))" | grep -aom1 ' epoch=[0-9]*' | cut -d= -f2
}
scheduled() { [[ -n $(seat_epoch) ]]; }
# Epoch `$1` has started (epoch 0 is the first committee built).
epoch_started() { [[ $(committees_built) -gt $1 ]]; }

PANIC='failed fatally: Consensus referenced unexpected authority'

# One restart of the victim. Sets OUTCOME to "crashed", "synced" or "stuck", and saves the log
# lines it wrote to <victim>.restart-<n>.log.
restart_victim() {
    local attempt=$1 target=$2 mark pid
    mark=$(stat -c %s "$RUN/$VICTIM.log" 2>/dev/null || echo 0)
    log "restart $attempt: starting $VICTIM (chain is at block $target)"
    start_node "$VICTIM"
    pid=${PIDS[$VICTIM]}
    for _ in $(seq 1 "$SYNC_SECS"); do
        if ! kill -0 "$pid" 2>/dev/null; then
            wait "$pid" 2>/dev/null
            unset "PIDS[$VICTIM]"
            OUTCOME=crashed; break
        fi
        if reached "$VICTIM" "$target"; then OUTCOME=synced; break; fi
        OUTCOME=stuck
        sleep 1
    done
    tail -c +"$((mark + 1))" "$RUN/$VICTIM.log" >"$RUN/$VICTIM.restart-$attempt.log"
}

setup_network || fail "network setup failed"
NODE5_BLS=$(bls_of "$RUN/node5")
log "network: $N genesis validators, node5 joins later (bls ${NODE5_BLS:0:16}...); victim is $VICTIM; epochs of ${EPOCH_SECS}s; stale anchors in the first $STALE_ROUNDS rounds of each epoch; victim teardown delay ${TEARDOWN_DELAY_MS}ms"
for name in $(seq -f 'validator-%g' 1 "$N") node5; do start_node "$name"; done
[[ $MODE == observer ]] && start_node observer
sleep 5
for name in "${!PIDS[@]}"; do
    kill -0 "${PIDS[$name]}" 2>/dev/null || fail "$name did not start; see its log"
done
wait_for 240 reached "$REFERENCE" "$WARMUP_BLOCKS" || fail "chain did not reach block $WARMUP_BLOCKS"
wait_for 120 reached "$VICTIM" "$WARMUP_BLOCKS" || fail "$VICTIM did not follow the chain"
log "warm-up done at block $(height "$REFERENCE")"

log "staking node5 through the registry"
stake_node5 || fail "staking node5 failed"
start_load || fail "could not fund the transaction load"
log "sending transfers through $REFERENCE so commits carry batches"
wait_for $((EPOCH_SECS * 3 + 60)) scheduled || fail "node5 never entered a scheduled committee"
SEAT_EPOCH=$(seat_epoch)
# Stop the victim in the last epoch before node5 is seated, so the committee change is the first
# epoch boundary it crosses when it catches up: a node far behind takes in many commits at once,
# and the new epoch's first commit is then already fetched when the previous epoch closes.
log "node5 is scheduled for epoch $SEAT_EPOCH; waiting for epoch $((SEAT_EPOCH - 1)) to start"
epoch_started "$SEAT_EPOCH" && fail "epoch $SEAT_EPOCH started before the victim could be stopped"
wait_for $((EPOCH_SECS * 3 + 60)) epoch_started $((SEAT_EPOCH - 1)) \
    || fail "epoch $((SEAT_EPOCH - 1)) never started"
log "stopping $VICTIM before the committee changes"
stop_node "$VICTIM"
# With validator 4 down, a quorum of the new committee (4 of 5) needs node5, so every commit of the
# new epoch carries node5's certificates; in validator mode validator 4 is the victim.
if [[ $MODE == observer ]]; then
    log "stopping validator-4 too; it stays down"
    stop_node validator-4
fi
log "waiting for node5 to be seated at the start of epoch $SEAT_EPOCH"
wait_for $((EPOCH_SECS * 2 + 60)) seated || fail "node5 was never seated; check staking and epoch timing"
seat=$(height "$REFERENCE")
log "node5 seated (chain at block $seat); waiting for $SEATED_BLOCKS more blocks"
wait_for 240 reached "$REFERENCE" $((seat + SEATED_BLOCKS)) \
    || fail "chain stalled after node5 was seated"

crashes=0
panics=0
for attempt in $(seq 1 "$RESTARTS"); do
    target=$(height "$REFERENCE")
    restart_victim "$attempt" "$target"
    restart_log=$RUN/$VICTIM.restart-$attempt.log
    panicked=0
    if grep -qa "$PANIC" "$restart_log"; then
        panicked=1
        panics=$((panics + 1))
        log "restart $attempt: $VICTIM panicked: $(grep -aom1 "subscriber [a-z -]*$PANIC [A-Za-z0-9]*" "$restart_log")"
    fi
    case "$OUTCOME" in
        crashed)
            [[ $panicked -eq 1 ]] || fail "$VICTIM exited for another reason; see $restart_log"
            crashes=$((crashes + 1))
            log "restart $attempt: $VICTIM exited"
            ;;
        synced)
            log "restart $attempt: $VICTIM synced past the committee change (block $(height "$VICTIM"))"
            break
            ;;
        *)
            fail "$VICTIM neither crashed nor caught up within ${SYNC_SECS}s"
            ;;
    esac
done
after=$(height "$REFERENCE")
log "chain at block $after (the other validators kept producing)"
if [[ $STALE_ROUNDS -gt 0 ]]; then
    log "stale-anchored headers proposed: $(cat "$RUN"/validator-[1-3].log "$RUN/node5.log" | grep -ac 'FAULT STALE ANCHOR')"
fi

if [[ $panics -gt 0 ]]; then
    if [[ $crashes -eq 0 ]]; then
        log "REPRODUCED ($MODE): $VICTIM's subscriber panicked on the new validator's commit $panics time(s); the process survived (built with panic=unwind, unlike release)"
    elif [[ $crashes -eq $attempt && $OUTCOME == crashed ]]; then
        log "REPRODUCED ($MODE): $VICTIM crashed on the new validator's commit in all $crashes restarts"
    else
        log "REPRODUCED ($MODE): $VICTIM crashed on the new validator's commit $crashes time(s), then synced"
    fi
    echo "logs and datadirs: $RUN"
    exit 1
fi
log "HEALTHY ($MODE): $VICTIM synced past the committee change"
echo "logs and datadirs: $RUN"
exit 0
