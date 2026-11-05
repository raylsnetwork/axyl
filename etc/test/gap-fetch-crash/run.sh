#!/usr/bin/env bash
#
# Live reproduction of the crash form of the gap-fetch issue. See README.md in this directory.
#
# A node that follows the chain fetches a missing consensus block from a connected peer by its
# number. The reply is checked only for its number and parent link, not that it is the real block.
# This harness shows one consequence: a connected peer can make the follower stop with a fatal
# error that a restart repeats.
#
# The peer is node5, which joins the committee after genesis. Once it is a member of epoch N, it
# certifies one of its own blocks. From the other members' votes for that block it assembles a
# second certificate that leaves out its own vote, so the certificate is signed only by members of
# epoch N-1. When the follower asks node5 for an older block (from an epoch before node5 joined),
# node5 returns the real block with that certificate added. The follower checks every certificate
# against the older epoch's keys, where it passes, then looks up each author in that same older
# committee to pay block rewards. node5 is not in it, so the lookup fails and the follower's
# subscriber stops with "Consensus referenced unexpected authority". A release build exits; on
# restart the follower reaches the same block and exits again.
#
# node5's behaviour is compiled only with the byzantine-test-hooks feature and is armed by
# environment, so node5 misbehaves while the other nodes stay honest. With -a off node5 is honest
# and the follower catches up normally.
#
# Exit status: 1 the follower stopped on the added certificate (reproduced), 0 it caught up
# without stopping, 2 the run was inconclusive or could not start.

set -uo pipefail

usage() {
    cat <<EOF
usage: $0 [-a selfless|honest|off] [-k] [-t ms] [-s rounds] [-l senders] [-b binary] [-d workdir] [-r restarts] [-e epoch-secs] [-i instance]
  -a  what node5 adds to the older blocks it serves (default selfless):
      selfless  a certificate of its own built from the other members' votes only (the issue)
      honest    its real certificate, which carries its own vote as every real one does (control)
      off       nothing; node5 serves honest replies (control)
  -k  keep the backward walk's stored blocks; the observer skips only the odd-numbered stored older
      blocks, so it still fetches those from node5 by number. Use this to test a fix that accepts a
      block fetched by number only once a stored block links onto it (default: the walk stores
      no older blocks and the observer fetches every older block by number)
  -t  milliseconds the observer waits at each epoch boundary before stopping the epoch's consensus
      tasks (default 15000), so it stays on the older epoch while it fetches its blocks; 0 for none
  -s  rounds at the start of each epoch whose headers the other validators anchor on an older block
      (default 0, honest); used only to widen the window, like the committee-change-sync-crash test
  -l  parallel transfer senders, so blocks carry batches (default 16)
  -b  rayls-network binary built with --features byzantine-test-hooks and panic=abort, as in
      release (default: build target/panic-abort/debug/rayls-network from this checkout)
  -d  empty directory for keys, genesis, datadirs and logs
      (default: target/gap-fetch-crash/<timestamp>)
  -r  restarts of the follower after node5 is seated (default 3)
  -e  epoch duration in seconds (default 60)
  -i  first --instance number, which sets the RPC ports (default 50)
EOF
    exit 2
}

ADD=selfless
KEEP_WALK=0
TEARDOWN_DELAY_MS=15000
STALE_ROUNDS=0
LOAD_SENDERS=16
BIN=""
WORKDIR=""
RESTARTS=3
EPOCH_SECS=60
INSTANCE_BASE=50
while getopts "a:kt:s:l:b:d:r:e:i:h" opt; do
    case "$opt" in
        a) ADD=$OPTARG ;;
        k) KEEP_WALK=1 ;;
        t) TEARDOWN_DELAY_MS=$OPTARG ;;
        s) STALE_ROUNDS=$OPTARG ;;
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
for value in "$TEARDOWN_DELAY_MS" "$STALE_ROUNDS" "$LOAD_SENDERS" "$RESTARTS" "$EPOCH_SECS" "$INSTANCE_BASE"; do
    [[ $value =~ ^[0-9]+$ ]] || { echo "not a number: $value"; usage; }
done
[[ $RESTARTS -ge 1 ]] || { echo "need at least one restart"; usage; }

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
WORKDIR=${WORKDIR:-$REPO/target/gap-fetch-crash/$(date -u +%Y%m%dT%H%M%SZ)}
if [[ -d $WORKDIR && -n $(ls -A "$WORKDIR" 2>/dev/null) ]]; then
    echo "not empty: $WORKDIR"; exit 2
fi
RUN=$WORKDIR

# Nodes, by name: validator-1..4 (genesis committee), node5 (staked after genesis), observer.
N=4
REFERENCE=validator-1
VICTIM=observer
CERT_FILE=$RUN/extra-cert.bcs
WARMUP_BLOCKS=10
# blocks the chain must commit after node5 is seated, before the follower restarts
SEATED_BLOCKS=20
# seconds a restarted follower gets to either stop or catch up
SYNC_SECS=420
CHAIN_ID=0x1e7
ADMIN_ADDR=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266   # anvil #0, local only
ADMIN_KEY=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
OPERATOR_KEY=0x$(printf '%064x' 1005)
STAKE_AMOUNT=5000000000000000000000000
RLS_ADDRESS=0x07E17e17E17e17E17e17E17E17E17e17e17E17eA
REGISTRY=0x07E17e17E17e17E17e17E17E17E17e17e17E17e1
GAS_FUND=1000000000000000000
ERC1967_IMPL_SLOT=0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc
export RL_BLS_PASSPHRASE="gap-fetch-crash-local-only"

# the follower aborts on purpose; keep its multi-gigabyte debug core dumps off the disk
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
    echo "building rayls-network (debug, panic=abort, byzantine + fault-injection hooks)"
    (cd "$REPO" && CARGO_PROFILE_DEV_PANIC=abort cargo build -p rayls-network \
        --features byzantine-test-hooks,fault-injection-test-hooks --target-dir target/panic-abort) || exit 2
    BIN=$REPO/target/panic-abort/debug/rayls-network
fi
[[ -x $BIN ]] || { echo "not executable: $BIN"; exit 2; }
if [[ $ADD != off ]] && ! grep -qa 'BYZANTINE GAP-FETCH' "$BIN"; then
    echo "$BIN was built without --features byzantine-test-hooks"; exit 2
fi
if ! grep -qa 'FAULT TEARDOWN DELAY' "$BIN"; then
    echo "$BIN was built without --features fault-injection-test-hooks"; exit 2
fi

instance_of() {
    case "$1" in
        validator-*) echo $((INSTANCE_BASE + ${1#validator-})) ;;
        node5) echo $((INSTANCE_BASE + 5)) ;;
        observer) echo $((INSTANCE_BASE + 6)) ;;
        helper) echo $((INSTANCE_BASE + 7)) ;;
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
    [[ $name == observer || $name == helper ]] && extra=(--observer)
    # the follower's subscriber logs each block it takes at debug
    [[ $name == "$VICTIM" ]] && extra+=(--log.stdout.filter info,subscriber=debug)
    # node5 is armed to add a certificate to the older blocks it serves by number. A wide target
    # range is fine: the hook only acts once it has saved a certificate (after node5 is seated) and
    # only on older-epoch blocks that carry batches.
    if [[ $name == node5 && $ADD != off ]]; then
        env=(RL_BYZ_EXTRA_CERT="$ADD" RL_BYZ_EXTRA_CERT_OUT="$CERT_FILE")
    elif [[ $name == helper && $ADD != off ]]; then
        env=(RL_BYZ_GAP_TARGET="1-4294967295" RL_BYZ_GAP_MODE=crash RL_BYZ_EXTRA_CERT_IN="$CERT_FILE")
    # the observer catches up across the committee change, fetching older-epoch blocks by number from
    # node5. The backward walk normally fills those blocks by hash first (verified, so the attack
    # can't land). This hook makes the walk skip the DB store for blocks in pre-join epochs: the
    # walk still proceeds and publishes a sync target, but the forward streamer must fetch
    # older-epoch blocks by number instead (the path node5 answers with the poisoned certificate).
    elif [[ $name == "$VICTIM" ]]; then
        # The backward walk fills the DB faster than the forward streamer reads it, so the
        # forward streamer never falls through to by-number peer fetch. These hooks force it:
        # (1) skip storing backward-walked blocks for pre-join epochs in the DB, and
        # (2) skip reading cached blocks for pre-join epochs in the forward streamer.
        # With -k, (1) is off and (2) skips only odd numbers, so stored even blocks remain.
        env=(RL_FAULT_SKIP_CACHED_BELOW_EPOCH="${SEAT_EPOCH:-999}")
        if [[ $KEEP_WALK == 1 ]]; then
            env+=(RL_FAULT_SKIP_CACHED_ODD_ONLY=1)
        else
            env+=(RL_FAULT_SKIP_BACKWARD_STORE_BELOW_EPOCH="${SEAT_EPOCH:-999}")
        fi
        [[ $TEARDOWN_DELAY_MS -gt 0 ]] && env+=(RL_FAULT_TEARDOWN_DELAY_MS="$TEARDOWN_DELAY_MS")
    # other validators can anchor early-epoch headers on an older block, which widens the window.
    elif [[ $STALE_ROUNDS -gt 0 && $name == validator-* ]]; then
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
    for name in $(seq -f 'validator-%g' 1 "$N") node5 observer helper; do
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
    for name in observer helper; do
        "$BIN" keytool generate observer --datadir "$RUN/$name" \
            --address 0x0000000000000000000000000000000000000000 >/dev/null 2>&1 || return 1
    done
    for name in $(seq -f 'validator-%g' 1 "$N") node5 observer helper; do
        mkdir -p "$RUN/$name/genesis"
        cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$RUN/$name/genesis/"
        cp "$RUN/parameters.yaml" "$RUN/$name/"
    done
    # Add node5 as a bootstrap server in the observer's committee file, so the observer dials node5
    # from boot rather than only after it syncs into the epoch where node5 joins. node5 is not an
    # authority here: this only puts its address in the dial set, which any peer can advertise.
    append_bootstrap "$RUN/helper" "$RUN/observer/genesis/committee.yaml"
}

# Append node5's bootstrap_servers entry (its own node-info p2p addresses) to a committee file.
append_bootstrap() {
    local n5=$1/node-info.yaml committee=$2 bls pri_addr pri_key wrk_addr wrk_key
    bls=$(grep '^bls_public_key:' "$n5" | awk '{print $2}')
    pri_addr=$(awk '/^  primary:/{p=1} p&&/network_address:/{print $2; exit}' "$n5")
    pri_key=$(awk '/^  primary:/{p=1} p&&/network_key:/{print $2; exit}' "$n5")
    wrk_addr=$(awk '/^  worker:/{w=1} w&&/network_address:/{print $2; exit}' "$n5")
    wrk_key=$(awk '/^  worker:/{w=1} w&&/network_key:/{print $2; exit}' "$n5")
    [[ -n $bls && -n $pri_addr && -n $wrk_addr ]] || { log "could not read p2p info from $n5"; return 1; }
    cat >>"$committee" <<EOF
  $bls:
    primary:
      network_address: $pri_addr
      network_key: $pri_key
    worker:
      network_address: $wrk_addr
      network_key: $wrk_key
EOF
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

# The reference validator builds an epoch's committee from the registry at the epoch's start.
seated() { grep -a 'Created committee from state' "$RUN/$REFERENCE.log" | grep -qa "$NODE5_BLS"; }
# node5 has saved the certificate it adds to replies.
cert_saved() { grep -qa 'BYZANTINE EXTRA CERT: saved' "$RUN/node5.log"; }
# The first epoch whose committee includes node5.
seat_epoch() {
    grep -a 'validators_for_epoch: contract returned validators' "$RUN/$REFERENCE.log" \
        | grep -a "validator_count=$((N + 1))" | grep -aom1 ' epoch=[0-9]*' | cut -d= -f2
}
scheduled() { [[ -n $(seat_epoch) ]]; }

PANIC='failed fatally: Consensus referenced unexpected authority'

# One restart of the follower. Sets OUTCOME to "crashed", "synced" or "stuck", and saves the log
# lines it wrote to <follower>.restart-<n>.log.
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
log "network: $N genesis validators, node5 joins later (bls ${NODE5_BLS:0:16}...); follower is the observer; epochs of ${EPOCH_SECS}s; node5 adds: $ADD"
# Start validators and node5, but NOT the observer. The observer starts fresh later.
for name in $(seq -f 'validator-%g' 1 "$N") node5 helper; do start_node "$name"; done
sleep 5
for name in "${!PIDS[@]}"; do
    kill -0 "${PIDS[$name]}" 2>/dev/null || fail "$name did not start; see its log"
done
wait_for 240 reached "$REFERENCE" "$WARMUP_BLOCKS" || fail "chain did not reach block $WARMUP_BLOCKS"
log "warm-up done at block $(height "$REFERENCE")"

# Start the load BEFORE staking so that blocks from all epochs carry batches. The crash form
# only fires on blocks with batches (the subscriber looks up authors for block rewards only
# when the block carries batch certificates).
start_load || fail "could not fund the transaction load"
log "sending transfers through $REFERENCE so blocks carry batches"
log "staking node5 through the registry"
stake_node5 || fail "staking node5 failed"
wait_for $((EPOCH_SECS * 3 + 60)) scheduled || fail "node5 never entered a scheduled committee"
SEAT_EPOCH=$(seat_epoch)
log "node5 is scheduled for epoch $SEAT_EPOCH"

log "waiting for node5 to be seated in epoch $SEAT_EPOCH"
wait_for $((EPOCH_SECS * 3 + 60)) seated || fail "node5 was never seated; check staking and epoch timing"
seat=$(height "$REFERENCE")
log "node5 seated (chain at block $seat)"
if [[ $ADD != off ]]; then
    wait_for 120 cert_saved || fail "node5 never saved a certificate to add; see node5.log"
    log "node5 saved the certificate it adds: $(grep -aom1 'BYZANTINE EXTRA CERT: saved.*' "$RUN/node5.log")"
fi
wait_for 240 reached "$REFERENCE" $((seat + SEATED_BLOCKS)) || fail "chain stalled after node5 was seated"

# The observer starts fresh: its consensus DB is empty, so the forward streamer must fetch every
# block by number from peers (including node5, which serves poisoned older-epoch blocks).
crashes=0
panics=0
for attempt in $(seq 1 "$RESTARTS"); do
    target=$(height "$REFERENCE")
    # On first attempt the observer hasn't run yet; on subsequent attempts it crashed and we restart.
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
            fail "the observer neither stopped nor caught up within ${SYNC_SECS}s"
            ;;
    esac
done
after=$(height "$REFERENCE")
log "chain at block $after (the validators kept producing)"
if [[ $ADD != off ]]; then
    log "poisoned replies node5 served: $(grep -ac 'serving crash header' "$RUN/helper.log")"
fi

if [[ $panics -gt 0 ]]; then
    if [[ $crashes -eq 0 ]]; then
        log "REPRODUCED: the observer's subscriber stopped on the added certificate $panics time(s); the process survived (built with panic=unwind, unlike release)"
    elif [[ $crashes -eq $attempt && $OUTCOME == crashed ]]; then
        log "REPRODUCED: the observer crashed on the added certificate in all $crashes restarts"
    else
        log "REPRODUCED: the observer crashed on the added certificate $crashes time(s), then caught up"
    fi
    echo "logs and datadirs: $RUN"
    exit 1
fi
log "HEALTHY: the observer caught up without stopping"
echo "logs and datadirs: $RUN"
exit 0
