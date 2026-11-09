#!/usr/bin/env bash
#
# Live reproduction of the probe-tip fork issue: a committee member forks a following node through
# the follower's "latest block" probe. See README.md in this directory.
#
# A follower that is behind (an observer / an inactive validator, without gossip) learns the chain
# tip by probing every committee member for its latest consensus header and keeping the highest
# number. That reply is checked only for BLS signatures. The commit timestamp is hashed into the
# header digest but is not covered by any signature, so one committee member can take its own real
# latest block, raise the commit timestamp, and serve that on the probe (and on the by-hash request
# that follows). The follower executes the forged block, forks, and stalls. A restart does not
# heal it, because the forged block is persisted canonically on the follower.
#
# Delivery models the startup window in which the attacker answers the follower's first probe
# before honest peers do. A feature-gated fault on the follower restricts its probe to the
# attacker; with the attacker honest (-a off) the follower syncs normally: that is the control.
#
# The attacker's behaviour is compiled only with byzantine-test-hooks and is armed by environment,
# so the one armed validator lies while the rest stay honest.
#
# Exit status: 1 the follower executed a forged block and stalled (bug reproduced),
# 0 the follower caught up to the canonical tip (healthy; bug absent or fixed),
# 2 the run was inconclusive or could not start.

set -uo pipefail

usage() {
    cat <<EOF
usage: $0 [-a on|off] [-n] [-l senders] [-b binary] [-d workdir] [-w watch-secs] [-e epoch-secs] [-i instance]
  -a  on  the attacker serves the fork forgery on the probe (default; the bug)
      off the attacker serves honest replies (control)
  -n  no eclipse: the observer keeps its connections to the honest validators, and only its probe
      goes to the attacker (default: eclipse, so every peer request goes to the attacker)
  -l  parallel transfer senders, so blocks carry batches (default 8); a poison target must carry
      batches so the bumped commit timestamp executes to a distinct EVM block
  -b  rayls-network binary built with --features byzantine-test-hooks,fault-injection-test-hooks
      (default: build target/panic-abort/debug/rayls-network from here)
  -d  empty directory for keys, genesis, datadirs and logs
      (default: target/probe-tip-fork/<timestamp>)
  -w  seconds to watch the observer for the fork after it restarts (default 180)
  -e  epoch duration in seconds (default 3600; one epoch for the whole run)
  -i  first --instance number, which sets the RPC ports (default 70)
EOF
    exit 2
}

ARM=on
ECLIPSE=1
LOAD_SENDERS=8
BIN=""
WORKDIR=""
WATCH=180
EPOCH_SECS=3600
INSTANCE_BASE=70
while getopts "a:nl:b:d:w:e:i:h" opt; do
    case "$opt" in
        a) ARM=$OPTARG ;;
        n) ECLIPSE=0 ;;
        l) LOAD_SENDERS=$OPTARG ;;
        b) BIN=$OPTARG ;;
        d) WORKDIR=$OPTARG ;;
        w) WATCH=$OPTARG ;;
        e) EPOCH_SECS=$OPTARG ;;
        i) INSTANCE_BASE=$OPTARG ;;
        *) usage ;;
    esac
done
[[ $ARM == on || $ARM == off ]] || { echo "unknown -a: $ARM"; usage; }
for value in "$LOAD_SENDERS" "$WATCH" "$EPOCH_SECS" "$INSTANCE_BASE"; do
    [[ $value =~ ^[0-9]+$ ]] || { echo "not a number: $value"; usage; }
done

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
WORKDIR=${WORKDIR:-$REPO/target/probe-tip-fork/$(date -u +%Y%m%dT%H%M%SZ)}
if [[ -d $WORKDIR && -n $(ls -A "$WORKDIR" 2>/dev/null) ]]; then
    echo "not empty: $WORKDIR"; exit 2
fi
RUN=$WORKDIR

# Nodes: validator-1..4 (genesis committee), observer (the follower). No extra validator joins,
# so no epoch change, no certificate machinery: the fork forgery needs none of those.
N=4
REFERENCE=validator-1
CANONICAL=validator-2
ATTACKER=validator-4
VICTIM=observer
WARMUP_BLOCKS=10
GAP=20
# seconds the disarmed restart gets to show that the fork persists
HEAL_WATCH=90
CHAIN_ID=0x1e7
ADMIN_ADDR=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266   # anvil #0, local only
ADMIN_KEY=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
GAS_FUND=1000000000000000000
BUMP=1000000
export RL_BLS_PASSPHRASE="probe-tip-fork-local-only"

# keep multi-gigabyte core dumps off the disk on a hard kill
ulimit -c 0

declare -A PIDS
LOAD_PIDS=()
PROBE_NUMBER=""
ATTACKER_BLS=""
FORKED_AT=""
log() { echo "$(date -u +%T) $*" | tee -a "$RUN/summary.txt"; }
fail() { log "INCONCLUSIVE: $*"; echo "logs and datadirs: $RUN"; exit 2; }

for tool in curl cast; do
    command -v "$tool" >/dev/null || { echo "missing required tool: $tool (cast is part of Foundry)"; exit 2; }
done

if [[ -z $BIN ]]; then
    echo "building rayls-network (debug, panic=abort, byzantine + fault-injection hooks)"
    (cd "$REPO" && CARGO_PROFILE_DEV_PANIC=abort CARGO_INCREMENTAL=0 flock /tmp/axyl-cargo.lock \
        nice -n 19 cargo build -j 4 -p rayls-network \
        --features byzantine-test-hooks,fault-injection-test-hooks --target-dir target/panic-abort) || exit 2
    BIN=$REPO/target/panic-abort/debug/rayls-network
fi
[[ -x $BIN ]] || { echo "not executable: $BIN"; exit 2; }
if [[ $ARM == on ]] && ! grep -qa 'BYZANTINE FORK-TIP' "$BIN"; then
    echo "$BIN was built without the fork-probe hook (--features byzantine-test-hooks)"; exit 2
fi
if ! grep -qa 'RL_FAULT_PROBE_ONLY_PEER' "$BIN"; then
    echo "$BIN was built without --features fault-injection-test-hooks"; exit 2
fi

instance_of() {
    case "$1" in
        validator-*) echo $((INSTANCE_BASE + ${1#validator-})) ;;
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

# Parse the observer's durable consensus tip from its log: the last "stream handoff" line
# reports the number the state-sync streamer handed over, which is the consensus tip it held.
consensus_tip() {
    # Prefer the running streamer's progress: 'catch-up complete result=N' is emitted after every
    # header streams through, so it tracks the live tip. 'stream handoff' only logs at startup.
    local tip
    tip=$(grep -ao 'catch-up complete" result=[0-9]*' "$RUN/$1.log" 2>/dev/null | tail -1 | sed -n 's/.*result=\([0-9]*\)/\1/p')
    if [[ -n $tip ]]; then echo "$tip"; return; fi
    grep -a 'stream handoff: starting forward streamer' "$RUN/$1.log" 2>/dev/null \
        | tail -1 | sed -n 's/.*last_streamed_number=\([0-9]*\).*/\1/p'
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

# Arming. The attacker is armed only once PROBE_NUMBER is known (after the follower is stopped).
# The follower carries the probe fault on every (re)start while it is the victim.
start_node() {
    local name=$1 armed=${2:-} ; local -a extra=() env=()
    [[ $name == observer ]] && extra=(--observer --log.stdout.filter info,subscriber=debug,state-sync=debug)
    if [[ $name == "$ATTACKER" && $armed == armed && $ARM == on ]]; then
        env=(RL_BYZ_FORK_TIP=1 RL_BYZ_FORK_NUMBER="$PROBE_NUMBER" RL_BYZ_FORK_BUMP="$BUMP")
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

# Route the observer's committee connections to the attacker, keeping every committee member's BLS
# key (so the observer still verifies consensus against the real keys) but pointing each member's
# network address at the attacker. This makes the attacker the peer that answers the observer's
# probe and every follow-up request, which models an eclipse position; in a real network the
# attacker instead only needs to answer the observer's first probe before honest peers do.
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
    for name in $(seq -f 'validator-%g' 1 "$N") observer; do
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
    "$BIN" keytool generate observer --datadir "$RUN/observer" \
        --address 0x0000000000000000000000000000000000000000 >/dev/null 2>&1 || return 1
    for name in $(seq -f 'validator-%g' 1 "$N") observer; do
        mkdir -p "$RUN/$name/genesis"
        cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$RUN/$name/genesis/"
        cp "$RUN/parameters.yaml" "$RUN/$name/"
    done
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
                sleep 0.2
            done
        ) &
        LOAD_PIDS+=("$!")
    done
}

setup_network || fail "network setup failed"
ATTACKER_BLS=$(bls_of "$RUN/$ATTACKER")
log "network: $N genesis validators (attacker is $ATTACKER, bls ${ATTACKER_BLS:0:16}...); follower is the observer; epochs of ${EPOCH_SECS}s; attacker: $ARM"

for name in $(seq -f 'validator-%g' 1 "$N") "$VICTIM"; do start_node "$name"; done
sleep 5
for name in "${!PIDS[@]}"; do
    kill -0 "${PIDS[$name]}" 2>/dev/null || fail "$name did not start; see its log"
done
wait_for 240 reached "$REFERENCE" "$WARMUP_BLOCKS" || fail "chain did not reach block $WARMUP_BLOCKS"
log "warm-up done at block $(height "$REFERENCE")"

start_load || fail "could not fund the transaction load"
LOAD_START=$(height "$REFERENCE")
log "sending transfers through $REFERENCE so blocks carry batches (chain at block $LOAD_START)"

BATCH_SETTLE=$((LOAD_START + 15))
wait_for 120 reached "$REFERENCE" "$BATCH_SETTLE" || fail "chain stalled waiting for batches to settle"
log "chain at block $(height "$REFERENCE"); blocks from $BATCH_SETTLE onward should carry batches"

# Let the observer follow until it is past the batch-settle point, then stop it. The poison wraps
# the consensus header just above the observer's durable consensus tip, so the number must come
# from the observer's consensus tip (its EVM height lags: several consensus headers can execute to
# one EVM block), and the target header must carry batches so the bumped commit timestamp executes
# to a distinct EVM block.
A=""
for _ in $(seq 1 240); do
    tip=$(consensus_tip "$VICTIM"); [[ -z $tip ]] && { sleep 1; continue; }
    h=$(height "$VICTIM")
    if [[ ${tip:-0} -ge $BATCH_SETTLE && -n $h && $h -ge $BATCH_SETTLE ]]; then A=$tip; break; fi
    sleep 1
done
[[ -n $A ]] || fail "observer never reached the batch-settle point"
stop_node "$VICTIM"
PROBE_NUMBER=$((A + 1))
log "observer stopped with consensus tip $A (execution block $(height "$VICTIM")); its probe will be served a poison wrapped around consensus header $PROBE_NUMBER (or the first batch header above it)"

# Open the gap: let the chain advance so the probe answer is clearly above the observer's tip.
TOP=$((A + GAP))
wait_for 300 reached "$REFERENCE" "$TOP" || fail "chain did not reach block $TOP"
log "chain at consensus $(height "$REFERENCE"); the observer is behind by $(( $(height "$REFERENCE") - A )) consensus numbers"

if [[ $ECLIPSE == 1 ]]; then
    eclipse_observer_committee || fail "could not route the observer to the attacker"
    log "routed the observer's committee connections to $ATTACKER (eclipse)"
else
    log "no eclipse: the observer keeps its honest connections; only its probe goes to $ATTACKER"
fi

if [[ $ARM == on ]]; then
    log "arming $ATTACKER to serve the fork forgery on the probe (bump $BUMP secs)"
    stop_node "$ATTACKER"
    start_node "$ATTACKER" armed
    sleep 3
    kill -0 "${PIDS[$ATTACKER]}" 2>/dev/null || fail "$ATTACKER did not restart armed; see its log"
fi

mark=$(stat -c %s "$RUN/$VICTIM.log" 2>/dev/null || echo 0)
log "starting $VICTIM (probe restricted to $ATTACKER); watching ${WATCH}s for a fork"
start_node "$VICTIM" armed
pid=${PIDS[$VICTIM]}
waited=0
while [[ $waited -lt $WATCH ]]; do
    sleep 5; waited=$((waited + 5))
    if ! kill -0 "$pid" 2>/dev/null; then
        tail -c +"$((mark + 1))" "$RUN/$VICTIM.log" >"$RUN/$VICTIM.armed.log"
        fail "the observer exited unexpectedly; see $RUN/$VICTIM.armed.log"
    fi
    h=$(height "$VICTIM")
    if [[ -n $h && $h -gt $A ]]; then
        tip=$(consensus_tip "$VICTIM")
        obs_ts=$(block_field "$VICTIM" "$h" timestamp)
        can_ts=$(block_field "$CANONICAL" "$h" timestamp)
        if [[ -n $obs_ts && -n $can_ts && $obs_ts -ne $can_ts && $((obs_ts - can_ts)) -eq $BUMP ]]; then
            FORKED_AT=$h; break
        fi
        [[ $((waited % 20)) -eq 0 ]] && log "watching: observer at ${h:-?} (was $A), tip ${tip:-?}, chain at $(height "$REFERENCE")"
    elif [[ $((waited % 20)) -eq 0 ]]; then
        log "watching: observer still at ${h:-?} (was $A), chain at $(height "$REFERENCE")"
    fi
done
tail -c +"$((mark + 1))" "$RUN/$VICTIM.log" >"$RUN/$VICTIM.armed.log"

if [[ -n $FORKED_AT ]]; then
    obs_hash=$(block_field "$VICTIM" "$FORKED_AT" hash)
    can_hash=$(block_field "$CANONICAL" "$FORKED_AT" hash)
    obs_ts=$(block_field "$VICTIM" "$FORKED_AT" timestamp)
    can_ts=$(block_field "$CANONICAL" "$FORKED_AT" timestamp)
    sleep 10; stalled=$(height "$VICTIM")
    log "ISSUE REPRODUCED: observer forked at block $FORKED_AT"
    log "  observer  block $FORKED_AT: hash $obs_hash timestamp $obs_ts"
    log "  canonical block $FORKED_AT: hash $can_hash timestamp $can_ts (difference $((obs_ts - can_ts)) = bump)"
    log "  observer executed tip stalled at $stalled while the chain is at $(height "$REFERENCE")"
else
    log "no fork in ${WATCH}s: observer at $(height "$VICTIM"), chain at $(height "$REFERENCE")"
fi

# Proof of delivery: count the poison replies the attacker actually served.
served_probe=0; served_byhash=0
if [[ $ARM == on ]]; then
    served_probe=$(grep -ac 'BYZANTINE FORK-TIP: serving forged copy on the latest probe' "$RUN/$ATTACKER.log" || true)
    served_byhash=$(grep -ac 'BYZANTINE FORK-TIP: serving forged copy on a by-hash request' "$RUN/$ATTACKER.log" || true)
    log "poison served by $ATTACKER on the probe: $served_probe"
    log "poison served by $ATTACKER by hash: $served_byhash"
fi

if [[ -z $FORKED_AT ]]; then
    if [[ $ARM == on && $served_probe -eq 0 && $served_byhash -eq 0 ]]; then
        fail "the attacker served no poison; the run proves nothing"
    fi
    log "HEALTHY: the observer caught up without forking"
    echo "logs and datadirs: $RUN"
    exit 0
fi

# Restart-no-heal: with the attacker disarmed, the observer must still hold the forged block (it
# executed and persisted it) and must stay stalled below the canonical chain. A restart cannot
# heal it, because the forged header is canonical on the follower and every real successor fails
# to link (and stream_missing_consensus replays the poisoned row from the DB).
log "restart-no-heal: disarming $ATTACKER and restarting the observer"
stop_node "$VICTIM"
stop_node "$ATTACKER"
if [[ $ARM == on ]]; then
    start_node "$ATTACKER"
    sleep 3
    kill -0 "${PIDS[$ATTACKER]}" 2>/dev/null || fail "$ATTACKER did not restart honest; see its log"
fi
mark=$(stat -c %s "$RUN/$VICTIM.log" 2>/dev/null || echo 0)
start_node "$VICTIM" armed
pid=${PIDS[$VICTIM]}
kill -0 "$pid" 2>/dev/null || fail "the observer did not restart for the no-heal check"
healed="no"
for _ in $(seq 1 "$HEAL_WATCH"); do
    h=$(height "$VICTIM")
    if [[ -n $h && $h -gt $FORKED_AT ]]; then healed="yes"; break; fi
    sleep 1
done
tail -c +"$((mark + 1))" "$RUN/$VICTIM.log" >"$RUN/$VICTIM.no-heal.log"
final_obs=$(height "$VICTIM")
final_chain=$(height "$REFERENCE")
if [[ $healed == yes ]]; then
    log "NOTE: the observer advanced past the forged block after the disarmed restart (at $final_obs, chain at $final_chain)"
else
    log "  observer tip after the disarmed restart: $final_obs (forged block $FORKED_AT; chain at $final_chain): restart did not heal it"
fi

if [[ $healed == yes ]]; then
    log "INCONCLUSIVE: the fork was reproduced but the disarmed restart healed it, which contradicts the issue"
    echo "logs and datadirs: $RUN"
    exit 2
fi
log "REPRODUCED (confirmed): the observer executed the forged block, stalled, and a restart did not heal it"
echo "logs and datadirs: $RUN"
exit 1
