#!/usr/bin/env bash
#
# Live reproduction: a validator that catches up across an epoch boundary stays inactive, halting
# a network that has exactly a quorum up. See README.md in this directory.
#
# Starts five validators with short epochs. Each fault kills validator 3, keeps it down past at
# least one epoch boundary, restarts it, and stops validator 5 as soon as validator 3 serves RPC,
# leaving exactly a quorum (4 of 5) that includes the returning validator. The chain must then
# commit more headers. The bug is a stall where validator 3 stays inactive_cvv and its log shows a
# state-sync walk that started at its covered anchor and published nothing.
#
# Exit status: 0 no fault showed the bug (healthy; bug absent or fixed),
# 1 a fault halted the network with the bug's signature (issue reproduced),
# 2 the run was inconclusive or could not start.

set -uo pipefail

usage() {
    cat <<EOF
Usage: $0 [-f FAULTS] [-o OFFLINE_SECS] [-e EPOCH_SECS] [-w WATCH_SECS] [-k HEADERS]
          [-b BINARY] [-d WORKDIR] [-i INSTANCE_BASE]

  -f  faults to inject; stops at the first one that reproduces (default 6)
  -o  seconds validator 3 stays down; it also waits for an epoch boundary (default 40)
  -e  epoch duration in seconds (default 30)
  -w  seconds the chain has to commit HEADERS more headers after each fault (default 120)
  -k  headers the chain must commit after each fault (default 20)
  -b  node binary (default: build target/debug/rayls-network from this checkout)
  -d  directory for datadirs and logs (default: target/statesync-anchor-republish/<timestamp>)
  -i  first --instance number, which sets the RPC ports (default 50)
EOF
    exit 2
}

FAULTS=6
OFFLINE=40
EPOCH_SECS=30
WATCH=120
HEADERS=20
BIN=""
WORKDIR=""
INSTANCE_BASE=50
while getopts "f:o:e:w:k:b:d:i:h" opt; do
    case "$opt" in
        f) FAULTS=$OPTARG ;;
        o) OFFLINE=$OPTARG ;;
        e) EPOCH_SECS=$OPTARG ;;
        w) WATCH=$OPTARG ;;
        k) HEADERS=$OPTARG ;;
        b) BIN=$OPTARG ;;
        d) WORKDIR=$OPTARG ;;
        i) INSTANCE_BASE=$OPTARG ;;
        *) usage ;;
    esac
done
for value in "$FAULTS" "$OFFLINE" "$EPOCH_SECS" "$WATCH" "$HEADERS" "$INSTANCE_BASE"; do
    [[ $value =~ ^[0-9]+$ ]] || { echo "not a number: $value"; usage; }
done

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
WORKDIR=${WORKDIR:-$REPO/target/statesync-anchor-republish/$(date -u +%Y%m%dT%H%M%SZ)}
if [[ -d $WORKDIR && -n $(ls -A "$WORKDIR" 2>/dev/null) ]]; then
    echo "not empty: $WORKDIR"; exit 2
fi
RUN=$WORKDIR

# Five validators: exactly a quorum (4) is left when the victim is back and OTHER is stopped.
N=5
VICTIM=3
OTHER=5
REFERENCE=1
CHAIN_ID=487
DEV_ACCOUNT=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
export RL_BLS_PASSPHRASE="statesync-anchor-republish-local-only"

declare -A PIDS
log() { echo "$(date -u +%T) $*" | tee -a "$RUN/summary.txt"; }
fail() { log "INCONCLUSIVE: $*"; echo "logs and datadirs: $RUN"; exit 2; }

for tool in curl jq; do
    command -v "$tool" >/dev/null || { echo "missing required tool: $tool"; exit 2; }
done

if [[ -z $BIN ]]; then
    echo "building rayls-network (debug)"
    (cd "$REPO" && cargo build -p rayls-network) || exit 2
    BIN=$REPO/target/debug/rayls-network
fi
[[ -x $BIN ]] || { echo "not executable: $BIN"; exit 2; }

rpc_port() { echo $((8546 - (INSTANCE_BASE + $1))); }

# JSON-RPC call to validator $1, method $2, printing jq filter $3 of the response ("" on error).
rpc() {
    curl -s -m 3 -H 'content-type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$2\",\"params\":[]}" \
        "http://127.0.0.1:$(rpc_port "$1")" 2>/dev/null | jq -r "$3 // empty" 2>/dev/null
}
header_number() { rpc "$1" rayls_latestHeader .result.number; }
epoch() { rpc "$1" rayls_nodeStatus .result.epoch; }
role() { rpc "$1" rayls_nodeStatus .result.role; }
status() {
    local out
    out=$(rpc "$1" rayls_nodeStatus '.result | "role=\(.role) epoch=\(.epoch) committed_round=\(.committed_round) primary_round=\(.primary_round) last_canonical_block=\(.last_canonical_block)"')
    echo "${out:-no answer}"
}
serves_rpc() { [[ -n $(rpc "$1" eth_chainId .result) ]]; }

start_validator() {
    local seq=$1
    "$BIN" node --datadir "$RUN/validator-$seq" --network local \
        --instance $((INSTANCE_BASE + seq)) --http --storage.v2 \
        >>"$RUN/validator-$seq.log" 2>&1 &
    PIDS[$seq]=$!
}

# Graceful stop (SIGTERM), SIGKILL after 40 s.
stop_validator() {
    local pid=${PIDS[$1]:-}
    unset "PIDS[$1]"
    [[ -z $pid ]] && return 0
    kill -TERM "$pid" 2>/dev/null
    for _ in $(seq 1 40); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
    kill -KILL "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
    return 0
}

# Crash: SIGKILL with no shutdown.
kill_validator() {
    local pid=${PIDS[$1]:-}
    unset "PIDS[$1]"
    [[ -z $pid ]] && return 0
    kill -KILL "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
    return 0
}

stop_all() {
    local seq
    for seq in "${!PIDS[@]}"; do kill -TERM "${PIDS[$seq]}" 2>/dev/null; done
    for seq in "${!PIDS[@]}"; do stop_validator "$seq"; done
    return 0
}
trap stop_all EXIT
trap 'exit 2' INT TERM

wait_for() {
    local secs=$1; shift
    for _ in $(seq 1 "$secs"); do "$@" && return 0; sleep 1; done
    return 1
}
at_least() { local h; h=$(header_number "$1"); [[ -n $h && $h -ge $2 ]]; }
epoch_after() { local e; e=$(epoch "$REFERENCE"); [[ -n $e && $e -gt $1 ]]; }

# Validator $1 reaches the header the reference holds now, within $2 seconds.
caught_up() {
    local target; target=$(header_number "$REFERENCE")
    [[ -n $target ]] || return 1
    wait_for "$2" at_least "$1" "$target"
}

# The bug's signature in the victim log after byte offset $1: the last walk derived its coverage
# from cached headers at anchor N, a later probe with the watch at 0 reported the same N as the
# network tip, and nothing was published to the watch after that walk. Prints N when found.
anchor_signature() {
    local since line at anchor after
    since=$(tail -c +"$(($1 + 1))" "$RUN/validator-$VICTIM.log" | sed 's/\x1b\[[0-9;]*m//g')
    line=$(grep -n 'derived walk coverage from cached headers' <<<"$since" | tail -1)
    [[ $line =~ ^([0-9]+):.*anchor=([0-9]+) ]] || return 1
    at=${BASH_REMATCH[1]}; anchor=${BASH_REMATCH[2]}
    after=$(tail -n +"$((at + 1))" <<<"$since")
    grep 'probe discovered' <<<"$after" | grep -E '(^|[^_[:alnum:]])current=0([^0-9]|$)' \
        | grep -qE "peer_latest=$anchor([^0-9]|$)" || return 1
    grep -q 'notifying watchers' <<<"$after" && return 1
    echo "$anchor"
}

setup_network() {
    rm -rf "$RUN"; mkdir -p "$RUN/genesis/validators"
    local seq port
    for seq in $(seq 1 "$N"); do
        port=$(rpc_port "$seq")
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
log "network: $N validators, ${EPOCH_SECS}s epochs; victim is validator $VICTIM, validator $OTHER stops when it returns"

for seq in $(seq 1 "$N"); do start_validator "$seq"; done
sleep 5
for seq in $(seq 1 "$N"); do
    kill -0 "${PIDS[$seq]}" 2>/dev/null || fail "validator $seq did not start; see its log"
    wait_for 60 serves_rpc "$seq" || fail "validator $seq does not serve RPC"
done
wait_for 120 at_least "$REFERENCE" 10 || fail "chain did not reach header 10"
log "warm-up done: reference at header $(header_number "$REFERENCE"), epoch $(epoch "$REFERENCE")"

other_stalls=0
for fault in $(seq 1 "$FAULTS"); do
    down_epoch=$(epoch "$REFERENCE")
    [[ -n $down_epoch ]] || fail "reference does not answer rayls_nodeStatus"
    kill_validator "$VICTIM"
    log "fault $fault/$FAULTS: validator $VICTIM killed in epoch $down_epoch, down for ${OFFLINE}s and past an epoch boundary"
    sleep "$OFFLINE"
    wait_for 120 epoch_after "$down_epoch" || fail "reference did not leave epoch $down_epoch"

    log_offset=$(stat -c %s "$RUN/validator-$VICTIM.log")
    start_validator "$VICTIM"
    wait_for 60 serves_rpc "$VICTIM" || fail "validator $VICTIM does not serve RPC after restart"
    stop_validator "$OTHER"
    start=$(header_number "$REFERENCE")
    [[ -n $start ]] || fail "reference does not answer rayls_latestHeader"
    log "fault $fault/$FAULTS: validator $VICTIM back, validator $OTHER stopped, reference in epoch $(epoch "$REFERENCE") at header $start"

    began=$SECONDS
    if wait_for "$WATCH" at_least "$REFERENCE" $((start + HEADERS)); then
        log "fault $fault/$FAULTS: PROGRESSED, $HEADERS headers in $((SECONDS - began))s"
    else
        log "fault $fault/$FAULTS: STALLED, reference at header $(header_number "$REFERENCE") (from $start) after $((SECONDS - began))s"
        for seq in 1 2 "$VICTIM" 4; do log "  validator $seq: $(status "$seq")"; done
        victim_role=$(role "$VICTIM")
        if anchor=$(anchor_signature "$log_offset") && [[ $victim_role == inactive_cvv ]]; then
            log "ISSUE REPRODUCED: fault $fault halted the network with exactly a quorum up"
            log "  validator $VICTIM stayed inactive_cvv; its walk started at the covered anchor $anchor and published nothing"
            since=$(tail -c +"$((log_offset + 1))" "$RUN/validator-$VICTIM.log" | sed 's/\x1b\[[0-9;]*m//g')
            { grep 'derived walk coverage from cached headers' <<<"$since" | tail -1
              grep -E "probe discovered.*peer_latest=$anchor([^0-9]|$)" <<<"$since" | tail -1
            } | cut -c1-220 | sed 's/^/  victim log: /' | tee -a "$RUN/summary.txt"
            echo "logs and datadirs: $RUN"
            exit 1
        fi
        other_stalls=$((other_stalls + 1))
        log "fault $fault/$FAULTS: stalled without the anchor-republish signature (victim role ${victim_role:-unknown}); another defect, not counted"
    fi

    # Recover before the next fault: bring OTHER back and let both returned nodes catch up.
    start_validator "$OTHER"
    wait_for 60 serves_rpc "$OTHER" || fail "validator $OTHER does not serve RPC after restart"
    caught_up "$OTHER" 180 || fail "validator $OTHER did not catch up"
    caught_up "$VICTIM" 180 || fail "validator $VICTIM did not catch up"
done

log "HEALTHY: $FAULTS faults, no anchor-republish stall ($other_stalls stalls without the signature)"
echo "logs and datadirs: $RUN"
exit 0
