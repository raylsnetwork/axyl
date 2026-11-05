#!/usr/bin/env bash
#
# Live reproduction of the crash form of the gap-fetch issue, delivered through the backwards walk
# instead of a by-number request (the "second route", issue #276). See README.md in this directory.
#
# The crash itself is the same as in ../gap-fetch-crash: a follower stops with a fatal
# "unexpected authority" when it executes an older block whose sub-dag carries a certificate whose
# author is not in that block's committee. What differs here is how the poison reaches the follower.
#
# A follower that is behind asks its peers for their latest block to know where to sync from. That
# reply is not verified at all. A committee member answers the follower's first (startup) probe with
# the poison: a real older block that carries batches, with the extra certificate added, labelled
# with a block number just above the follower's own. The follower's backwards walk fetches it, the
# forward streamer takes it as its target and hands it to execution, and the follower crashes. No
# by-number request is involved, so hardening only the by-number reply would not stop this.
#
# Two conditions make this deterministic in a local network:
#   1. The follower stops while it is behind, at a block in an early epoch (before the attacker
#      joined the committee), so the block it is served is from that early epoch and its committee
#      does not contain the attacker.
#   2. A feature-gated fault on the follower restricts its probe to the attacker, which models the
#      real-world startup window in which the attacker answers the probe before honest peers do.
#      With the attacker honest (-a off) the follower syncs normally: that is the control.
#
# The attacker's behaviour is compiled only with byzantine-test-hooks and is armed by environment,
# so the attacker misbehaves while the other nodes stay honest.
#
# Exit status: 1 the follower stopped on the added certificate (reproduced), 0 it caught up without
# stopping, 2 the run was inconclusive or could not start.

set -uo pipefail

usage() {
    cat <<EOF
usage: $0 [-a selfless|honest|off] [-n] [-l senders] [-b binary] [-d workdir] [-r restarts] [-e epoch-secs] [-i instance]
  -a  what the attacker adds to the older block it serves on the probe (default selfless):
      selfless  a certificate of node5's own built from the other members' votes only (the issue)
      honest    node5's real certificate, which carries its own vote (control)
      off       nothing; the attacker serves honest replies (control)
  -n  no eclipse: the observer keeps its connections to the honest validators, and only its probe
      goes to the attacker. Use this to test a fix: a fully eclipsed observer has no honest source
      of the latest block, so it cannot catch up with any fix (default: eclipse)
  -l  parallel transfer senders, so blocks carry batches (default 16)
  -b  rayls-network binary built with --features byzantine-test-hooks,fault-injection-test-hooks and
      panic=abort, as in release (default: build target/panic-abort/debug/rayls-network from here)
  -d  empty directory for keys, genesis, datadirs and logs
      (default: target/gap-fetch-crash-probe/<timestamp>)
  -r  restarts of the follower after it is armed (default 3)
  -e  epoch duration in seconds (default 60)
  -i  first --instance number, which sets the RPC ports (default 60)
EOF
    exit 2
}

ADD=selfless
ECLIPSE=1
LOAD_SENDERS=16
BIN=""
WORKDIR=""
RESTARTS=3
EPOCH_SECS=60
INSTANCE_BASE=60
while getopts "a:nl:b:d:r:e:i:h" opt; do
    case "$opt" in
        a) ADD=$OPTARG ;;
        n) ECLIPSE=0 ;;
        l) LOAD_SENDERS=$OPTARG ;;
        b) BIN=$OPTARG ;;
        d) WORKDIR=$OPTARG ;;
        r) RESTARTS=$OPTARG ;;
        e) EPOCH_SECS=$OPTARG ;;
        i) INSTANCE_BASE=$OPTARG ;;
        *) usage ;;
    esac
done
[[ $ADD == selfless || $ADD == honest || $ADD == off ]] || { echo "unknown -a: $ADD"; usage; }
for value in "$LOAD_SENDERS" "$RESTARTS" "$EPOCH_SECS" "$INSTANCE_BASE"; do
    [[ $value =~ ^[0-9]+$ ]] || { echo "not a number: $value"; usage; }
done
[[ $RESTARTS -ge 1 ]] || { echo "need at least one restart"; usage; }

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
WORKDIR=${WORKDIR:-$REPO/target/gap-fetch-crash-probe/$(date -u +%Y%m%dT%H%M%SZ)}
if [[ -d $WORKDIR && -n $(ls -A "$WORKDIR" 2>/dev/null) ]]; then
    echo "not empty: $WORKDIR"; exit 2
fi
RUN=$WORKDIR

# Nodes, by name: validator-1..4 (genesis committee), node5 (staked after genesis), observer.
# validator-4 is the attacker: a genesis committee member, so the follower's probe queries it.
N=4
REFERENCE=validator-1
ATTACKER=validator-4
VICTIM=observer
CERT_FILE=$RUN/extra-cert.bcs
WARMUP_BLOCKS=10
# blocks the chain must commit after node5 is seated, before the follower is restarted
SEATED_BLOCKS=20
# seconds a restarted follower gets to either stop or catch up
SYNC_SECS=300
CHAIN_ID=0x1e7
ADMIN_ADDR=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266   # anvil #0, local only
ADMIN_KEY=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
OPERATOR_KEY=0x$(printf '%064x' 1005)
STAKE_AMOUNT=5000000000000000000000000
RLS_ADDRESS=0x07E17e17E17e17E17e17E17E17E17e17e17E17eA
REGISTRY=0x07E17e17E17e17E17e17E17E17E17e17e17E17e1
GAS_FUND=1000000000000000000
ERC1967_IMPL_SLOT=0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc
export RL_BLS_PASSPHRASE="gap-fetch-crash-probe-local-only"

# the follower aborts on purpose; keep its multi-gigabyte debug core dumps off the disk
ulimit -c 0

declare -A PIDS
LOAD_PIDS=()
PROBE_NUMBER=""
ATTACKER_BLS=""
log() { echo "$(date -u +%T) $*" | tee -a "$RUN/summary.txt"; }
fail() { log "INCONCLUSIVE: $*"; echo "logs and datadirs: $RUN"; exit 2; }

for tool in curl cast; do
    command -v "$tool" >/dev/null || { echo "missing required tool: $tool (cast is part of Foundry)"; exit 2; }
done

if [[ -z $BIN ]]; then
    echo "building rayls-network (debug, panic=abort, byzantine + fault-injection hooks)"
    (cd "$REPO" && CARGO_PROFILE_DEV_PANIC=abort cargo build -p rayls-network \
        --features byzantine-test-hooks,fault-injection-test-hooks --target-dir target/panic-abort) || exit 2
    BIN=$REPO/target/panic-abort/debug/rayls-network
fi
[[ -x $BIN ]] || { echo "not executable: $BIN"; exit 2; }
if [[ $ADD != off ]] && ! grep -qa 'BYZANTINE PROBE' "$BIN"; then
    echo "$BIN was built without the crash-probe hook (--features byzantine-test-hooks)"; exit 2
fi
if ! grep -qa 'RL_FAULT_PROBE_ONLY_PEER\|FAULT TEARDOWN DELAY' "$BIN"; then
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

# Arming for each node. The attacker is armed only once PROBE_NUMBER is known (after the follower is
# stopped). The follower is armed only on its armed restart (PROBE_ONLY_PEER set).
start_node() {
    local name=$1 armed=${2:-} ; local -a extra=() env=()
    [[ $name == observer ]] && extra=(--observer --log.stdout.filter info,subscriber=debug,state-sync=debug)
    if [[ $name == node5 && $ADD != off ]]; then
        env=(RL_BYZ_EXTRA_CERT="$ADD" RL_BYZ_EXTRA_CERT_OUT="$CERT_FILE")
    elif [[ $name == "$ATTACKER" && $armed == armed && $ADD != off ]]; then
        env=(RL_BYZ_GAP_MODE=crash-probe RL_BYZ_PROBE_NUMBER="$PROBE_NUMBER" RL_BYZ_EXTRA_CERT_IN="$CERT_FILE")
    elif [[ $name == "$VICTIM" && $armed == armed ]]; then
        env=(RL_FAULT_PROBE_ONLY_PEER="$ATTACKER_BLS")
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

# True if block number $1 (on the reference node) carries transactions, i.e. the consensus block
# carries batches. The crash fires only on batch blocks.
# Route the observer's committee connections to the attacker, keeping every committee member's BLS
# key (so the observer still verifies consensus against the real keys) but pointing each member's
# network address at the attacker. This makes the attacker the peer that answers the observer's
# requests, which models an eclipse position; in a real network the attacker instead only needs to
# answer the observer's first probe before honest peers do.
eclipse_observer_committee() {
    local committee=$RUN/observer/genesis/committee.yaml a4=$RUN/$ATTACKER/node-info.yaml
    local pa pk wa wk
    pa=$(awk '/^  primary:/{p=1} p&&/network_address:/{print $2; exit}' "$a4")
    pk=$(awk '/^  primary:/{p=1} p&&/network_key:/{print $2; exit}' "$a4")
    wa=$(awk '/^  worker:/{w=1} w&&/network_address:/{print $2; exit}' "$a4")
    wk=$(awk '/^  worker:/{w=1} w&&/network_key:/{print $2; exit}' "$a4")
    [[ -n $pa && -n $wa ]] || return 1
    awk -v bls4="$ATTACKER_BLS" -v pa="$pa" -v pk="$pk" -v wa="$wa" -v wk="$wk" '
        /^bootstrap_servers:/ {inb=1; print; next}
        inb && /^  [A-Za-z0-9]+:$/ {cur=$1; sub(/:$/,"",cur); isatk=(cur==bls4); print; next}
        inb && /^    primary:/ {sec="p"; print; next}
        inb && /^    worker:/ {sec="w"; print; next}
        inb && /network_address:/ && !isatk {print "      network_address: " (sec=="p"?pa:wa); next}
        inb && /network_key:/ && !isatk {print "      network_key: " (sec=="p"?pk:wk); next}
        {print}
    ' "$committee" > "$committee.tmp" && mv "$committee.tmp" "$committee"
}

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

seated() { grep -a 'Created committee from state' "$RUN/$REFERENCE.log" | grep -qa "$NODE5_BLS"; }
cert_saved() { grep -qa 'BYZANTINE EXTRA CERT: saved' "$RUN/node5.log"; }
seat_epoch() {
    grep -a 'validators_for_epoch: contract returned validators' "$RUN/$REFERENCE.log" \
        | grep -a "validator_count=$((N + 1))" | grep -aom1 ' epoch=[0-9]*' | cut -d= -f2
}
scheduled() { [[ -n $(seat_epoch) ]]; }

PANIC='failed fatally: Consensus referenced unexpected authority'

restart_victim() {
    local attempt=$1 target=$2 mark pid
    mark=$(stat -c %s "$RUN/$VICTIM.log" 2>/dev/null || echo 0)
    log "restart $attempt: starting $VICTIM armed (chain is at block $target)"
    start_node "$VICTIM" armed
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
ATTACKER_BLS=$(bls_of "$RUN/$ATTACKER")
log "network: $N genesis validators (attacker is $ATTACKER, bls ${ATTACKER_BLS:0:16}...), node5 joins later; follower is the observer; epochs of ${EPOCH_SECS}s; adds: $ADD"

# Start every node, including the observer, honest. The observer follows the chain, then is stopped
# while it is still behind in epoch 1.
for name in $(seq -f 'validator-%g' 1 "$N") node5 "$VICTIM"; do start_node "$name"; done
sleep 5
for name in "${!PIDS[@]}"; do
    kill -0 "${PIDS[$name]}" 2>/dev/null || fail "$name did not start; see its log"
done
wait_for 240 reached "$REFERENCE" "$WARMUP_BLOCKS" || fail "chain did not reach block $WARMUP_BLOCKS"
log "warm-up done at block $(height "$REFERENCE")"

# Load before staking so blocks from all epochs carry batches (the crash fires only on batch blocks).
start_load || fail "could not fund the transaction load"
LOAD_START=$(height "$REFERENCE")
log "sending transfers through $REFERENCE so blocks carry batches (chain at block $LOAD_START)"

# Wait for the chain to advance past the load start so consensus blocks carry batches. The DAG
# needs a few rounds for worker batches to propagate into committed sub-dags.
BATCH_SETTLE=$((LOAD_START + 15))
wait_for 120 reached "$REFERENCE" "$BATCH_SETTLE" || fail "chain stalled waiting for batches to settle"
log "chain at block $(height "$REFERENCE"); blocks from $BATCH_SETTLE onward should carry batches"

# Let the observer follow until it is past the batch-settle point, then stop it fast. The crash
# fires on the first block the forward streamer processes, so the observer's anchor must be at a
# block where anchor+1 has batches in the consensus layer (num_primary_blocks > 0).
A=""
for _ in $(seq 1 180); do
    h=$(height "$VICTIM"); [[ -n $h ]] || { sleep 1; continue; }
    if [[ $h -ge $BATCH_SETTLE ]]; then A=$h; break; fi
    sleep 1
done
[[ -n $A ]] || fail "observer never reached the batch-settle point"
# Fast kill so the anchor does not advance past A between here and shutdown.
kill -KILL "${PIDS[$VICTIM]}" 2>/dev/null; wait "${PIDS[$VICTIM]}" 2>/dev/null; unset "PIDS[$VICTIM]"
PROBE_NUMBER=$((A + 1))
log "observer stopped at block $A; it will be served poison at block $PROBE_NUMBER or the first block above it that carries batches"
if [[ $ECLIPSE == 1 ]]; then
    eclipse_observer_committee || fail "could not route the observer to the attacker"
    log "routed the observer's committee connections to $ATTACKER (eclipse)"
else
    log "no eclipse: the observer keeps its honest connections; only its probe goes to $ATTACKER"
fi

log "staking node5 through the registry"
stake_node5 || fail "staking node5 failed"
wait_for $((EPOCH_SECS * 3 + 60)) scheduled || fail "node5 never entered a scheduled committee"
SEAT_EPOCH=$(seat_epoch)
log "node5 is scheduled for epoch $SEAT_EPOCH"
wait_for $((EPOCH_SECS * 3 + 60)) seated || fail "node5 was never seated; check staking and epoch timing"
seat=$(height "$REFERENCE")
log "node5 seated (chain at block $seat)"
if [[ $ADD != off ]]; then
    wait_for 120 cert_saved || fail "node5 never saved a certificate to add; see node5.log"
    log "node5 saved the certificate it adds: $(grep -aom1 'BYZANTINE EXTRA CERT: saved.*' "$RUN/node5.log")"
fi
wait_for 240 reached "$REFERENCE" $((seat + SEATED_BLOCKS)) || fail "chain stalled after node5 was seated"

# Confirm the target block is older than node5's epoch, so its committee excludes node5.
if [[ -n $SEAT_EPOCH ]]; then
    log "target block $PROBE_NUMBER is in epoch 1, before node5's epoch $SEAT_EPOCH"
fi

# Arm the attacker now that the certificate exists and the target number is known.
if [[ $ADD != off ]]; then
    log "arming $ATTACKER to serve poison on the probe (from block $PROBE_NUMBER up, plus the certificate)"
    stop_node "$ATTACKER"
    start_node "$ATTACKER" armed
    sleep 3
    kill -0 "${PIDS[$ATTACKER]}" 2>/dev/null || fail "$ATTACKER did not restart armed; see its log"
fi

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
        log "restart $attempt: the observer stopped: $(grep -aom1 "subscriber [a-z -]*$PANIC [A-Za-z0-9]*" "$restart_log")"
    fi
    case "$OUTCOME" in
        crashed)
            [[ $panicked -eq 1 ]] || fail "the observer exited for another reason; see $restart_log"
            crashes=$((crashes + 1))
            log "restart $attempt: the observer exited"
            ;;
        synced)
            log "restart $attempt: the observer caught up (block $(height "$VICTIM"))"
            break
            ;;
        *)
            # After a crash, the observer may hold the part of the poisoned block it executed. With
            # honest peers (-n) it then gets the real block, which does not match, and stays stuck.
            if [[ $panics -gt 0 ]]; then
                log "restart $attempt: the observer neither stopped nor caught up within ${SYNC_SECS}s after the earlier crash"
                break
            fi
            fail "the observer neither stopped nor caught up within ${SYNC_SECS}s"
            ;;
    esac
done
after=$(height "$REFERENCE")
log "chain at block $after (the validators kept producing)"
if [[ $ADD != off ]]; then
    log "poison served by $ATTACKER on the probe: $(grep -ac 'serving crash-probe poison on the latest probe' "$RUN/$ATTACKER.log")"
    log "poison served by $ATTACKER by hash: $(grep -ac 'serving crash-probe poison on a by-hash request' "$RUN/$ATTACKER.log")"
fi

if [[ $panics -gt 0 ]]; then
    if [[ $crashes -eq $attempt && $OUTCOME == crashed ]]; then
        log "REPRODUCED: the observer crashed on the walk-delivered certificate in all $crashes restarts"
    else
        log "REPRODUCED: the observer crashed on the walk-delivered certificate $crashes time(s)"
    fi
    echo "logs and datadirs: $RUN"
    exit 1
fi
log "HEALTHY: the observer caught up without stopping"
echo "logs and datadirs: $RUN"
exit 0
