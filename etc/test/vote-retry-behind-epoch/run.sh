#!/usr/bin/env bash
#
# Live reproduction: a header vote refused by a peer still in the previous epoch is never asked
# again, so with one validator down the network stops committing at the epoch boundary for about
# four minutes. See README.md in this directory.
#
# Starts a local network of validators with short epochs. One validator (the late one) closes
# every epoch a few seconds after its peers, through the RL_TEST_LATE_EPOCH_CLOSE_MS test hook:
# in that window it still runs the old epoch and refuses the new epoch's vote requests as a peer
# one epoch behind. Each trial kills another validator ten seconds into an epoch, so exactly a
# quorum is left and the late validator's vote is needed, then requires the chain to commit a few
# headers of the next epoch within a deadline.
#
# The hook is compiled only with the `test-hooks` cargo feature and is armed per process by
# environment, so only the late validator closes its epochs late.
#
# Exit status: 0 the chain kept committing across every boundary (healthy; bug absent or fixed),
# 1 the chain stalled at a boundary after the proposers gave up on the late validator's refusal
# (bug present, issue reproduced), 2 the run was inconclusive or could not start.

set -uo pipefail

usage() {
    cat <<EOF
usage: $0 [-n validators] [-b binary] [-d workdir] [-t trials] [-l late_ms] [-e epoch_secs] [-p deadline] [-i instance]
  -n  number of validators, at least 5 (default 5)
  -b  rayls-network binary built with --features test-hooks
      (default: build target/debug/rayls-network from this checkout with the feature)
  -d  empty directory for keys, genesis, datadirs and logs
      (default: target/vote-retry-behind-epoch/<timestamp>)
  -t  epoch boundaries to cross with a validator down (default 1)
  -l  milliseconds the late validator closes each epoch after its peers (default 5000)
  -e  epoch duration in seconds (default 30)
  -p  seconds after the next epoch starts by which it must commit $HEADERS headers (default 90)
  -i  first --instance number, which sets the RPC ports (default 30)
EOF
    exit 2
}

HEADERS=10
N=5
BIN=""
WORKDIR=""
TRIALS=1
LATE_MS=5000
EPOCH_SECS=30
DEADLINE=90
INSTANCE_BASE=30
while getopts "n:b:d:t:l:e:p:i:h" opt; do
    case "$opt" in
        n) N=$OPTARG ;;
        b) BIN=$OPTARG ;;
        d) WORKDIR=$OPTARG ;;
        t) TRIALS=$OPTARG ;;
        l) LATE_MS=$OPTARG ;;
        e) EPOCH_SECS=$OPTARG ;;
        p) DEADLINE=$OPTARG ;;
        i) INSTANCE_BASE=$OPTARG ;;
        *) usage ;;
    esac
done
[[ $N =~ ^[0-9]+$ && $N -ge 5 ]] || { echo "need at least 5 validators"; usage; }
for value in "$TRIALS" "$LATE_MS" "$EPOCH_SECS" "$DEADLINE" "$INSTANCE_BASE"; do
    [[ $value =~ ^[0-9]+$ ]] || { echo "not a number: $value"; usage; }
done
[[ $EPOCH_SECS -ge 20 ]] || { echo "epochs must last at least 20 s"; usage; }

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
WORKDIR=${WORKDIR:-$REPO/target/vote-retry-behind-epoch/$(date -u +%Y%m%dT%H%M%SZ)}
if [[ -d $WORKDIR && -n $(ls -A "$WORKDIR" 2>/dev/null) ]]; then
    echo "not empty: $WORKDIR"; exit 2
fi
RUN=$WORKDIR

# The validator that closes every epoch late, the one killed in each trial to leave exactly a
# quorum, and the one the chain is read from.
LATE=3
OTHER=$N
REFERENCE=1
KILL_AFTER=10
WARMUP_HEADERS=10
CHAIN_ID=0x1e7
DEV_ACCOUNT=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
export RL_BLS_PASSPHRASE="vote-retry-behind-epoch-local-only"
# what a proposer on current code logs when it gives up on a refusal from a peer behind its epoch
# (one epoch behind, then two or more), and what it logs with the fix
GAVE_UP='stopping vote requests for stale header|ignoring epoch rejection from stale peer'
RETRIED='retrying vote request'

declare -A PIDS
mkdir -p "$RUN"
log() { echo "$(date -u +%T) $*" | tee -a "$RUN/summary.txt"; }
fail() { log "INCONCLUSIVE: $*"; echo "logs and datadirs: $RUN"; exit 2; }

for tool in curl; do
    command -v "$tool" >/dev/null || { echo "missing required tool: $tool"; exit 2; }
done

if [[ -z $BIN ]]; then
    echo "building rayls-network (debug) with --features test-hooks"
    (cd "$REPO" && cargo build -p rayls-network --features test-hooks) || exit 2
    BIN=$REPO/target/debug/rayls-network
fi
[[ -x $BIN ]] || { echo "not executable: $BIN"; exit 2; }

rpc_port() { echo $((8546 - (INSTANCE_BASE + $1))); }

rpc() {
    curl -s -m 2 -H 'content-type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$2\",\"params\":[]}" \
        "http://127.0.0.1:$(rpc_port "$1")" 2>/dev/null
}

# The node's epoch (rayls_nodeStatus), empty when it does not answer.
epoch() {
    local out; out=$(rpc "$1" rayls_nodeStatus) || return 0
    [[ $out =~ \"epoch\":([0-9]+) ]] && echo "${BASH_REMATCH[1]}"
}

# The number of the node's latest consensus header (rayls_latestHeader), empty when it does not
# answer. The header's own number is the last "number" field (the sub-dag before it has others).
header() {
    local out; out=$(rpc "$1" rayls_latestHeader) || return 0
    [[ $out =~ .*\"number\":([0-9]+) ]] && echo "${BASH_REMATCH[1]}"
}

start_validator() {
    local seq=$1
    local -a env=()
    [[ $seq -eq $LATE ]] && env=(RL_TEST_LATE_EPOCH_CLOSE_MS="$LATE_MS")
    env "${env[@]}" "$BIN" node --datadir "$RUN/validator-$seq" --network local \
        --instance $((INSTANCE_BASE + seq)) --log.stdout.format log-fmt --full --storage.v2 \
        --db.growth-step 1MB --consensus-db.growth-step 1MB --txpool.minimal-protocol-fee 0 \
        --gpo.default-suggested-fee 0 -vvv --http --http.addr 127.0.0.1 --http.api all \
        >>"$RUN/validator-$seq.log" 2>&1 &
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
reached() { local h; h=$(header "$1"); [[ -n $h && $h -ge $2 ]]; }
in_epoch_after() { local e; e=$(epoch "$1"); [[ -n $e && $e -gt $2 ]]; }

setup_network() {
    mkdir -p "$RUN/genesis/validators"
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

# Log lines matching $1 that the validators still running wrote after the byte offsets in $2.
count_since() {
    local pattern=$1 marks=$2 seq total=0 mark n
    for seq in $(seq 1 "$N"); do
        [[ $seq -eq $OTHER ]] && continue
        mark=$(grep "^$seq " "$marks" | cut -d' ' -f2)
        n=$(tail -c +"$((mark + 1))" "$RUN/validator-$seq.log" | grep -cE "$pattern")
        total=$((total + n))
    done
    echo "$total"
}

# One boundary. Writes "<stalled 0|1> <refusals given up> <refusals retried>" to trial-<n>.txt.
one_trial() {
    local trial=$1 current next_first="" tip_epoch tip killed waited limit stalled=1 seq _
    # Two epoch changes, then kill. After a stall the epoch it held is already past its duration
    # and closes as soon as the chain moves again, so the first change seen can be that epoch's
    # late commits; the second is a fresh boundary, seen right after it starts.
    for _ in 1 2; do
        current=$(epoch "$REFERENCE")
        [[ -n $current ]] || fail "validator $REFERENCE does not answer rayls_nodeStatus"
        wait_for $((EPOCH_SECS * 3)) in_epoch_after "$REFERENCE" "$current" || fail "no new epoch"
    done
    current=$(epoch "$REFERENCE")
    sleep "$KILL_AFTER"
    for seq in $(seq 1 "$N"); do echo "$seq $(stat -c %s "$RUN/validator-$seq.log")"; done >"$RUN/marks-$trial.txt"
    kill -KILL "${PIDS[$OTHER]}" 2>/dev/null
    wait "${PIDS[$OTHER]}" 2>/dev/null
    unset "PIDS[$OTHER]"
    killed=$SECONDS
    log "trial $trial: epoch $current, killed validator $OTHER (SIGKILL); exactly a quorum is left, including late validator $LATE"

    # the first header of the next epoch, then $HEADERS more, within the deadline
    limit=$((EPOCH_SECS - KILL_AFTER + DEADLINE))
    waited=0
    while [[ $waited -lt $limit ]]; do
        sleep 1; waited=$((SECONDS - killed))
        tip_epoch=$(epoch "$REFERENCE"); tip=$(header "$REFERENCE")
        [[ -n $tip_epoch && -n $tip && $tip_epoch -gt $current ]] || continue
        [[ -z $next_first ]] && next_first=$tip
        if [[ $tip -ge $((next_first + HEADERS)) ]]; then stalled=0; break; fi
    done
    tip_epoch=$(epoch "$REFERENCE"); tip=$(header "$REFERENCE")
    local gave_up retried
    gave_up=$(count_since "$GAVE_UP" "$RUN/marks-$trial.txt")
    retried=$(count_since "$RETRIED" "$RUN/marks-$trial.txt")
    if [[ $stalled -eq 1 ]]; then
        log "trial $trial: STALLED: no $HEADERS headers of epoch $((current + 1)) within ${limit}s of the kill; reference at header ${tip:-?} of epoch ${tip_epoch:-?}"
    else
        log "trial $trial: PROGRESSED: $HEADERS headers of epoch $((current + 1)) committed $((SECONDS - killed))s after the kill"
    fi
    log "trial $trial: proposers gave up on a refusal $gave_up times, retried a refusal $retried times"
    echo "$stalled $gave_up $retried" >"$RUN/trial-$trial.txt"

    # restarted only past the 30 s idle timeout, so its peers hold no stale connection to it
    [[ $((SECONDS - killed)) -lt 35 ]] && sleep $((35 - (SECONDS - killed)))
    start_validator "$OTHER"
    tip=$(header "$REFERENCE")
    wait_for 400 reached "$OTHER" "$tip" || fail "validator $OTHER did not catch up"
    wait_for 180 reached "$LATE" "$tip" || fail "validator $LATE did not catch up"
}

setup_network || fail "network setup failed"
log "network: $N validators, ${EPOCH_SECS}s epochs; validator $LATE closes each epoch ${LATE_MS}ms late, validator $OTHER is killed in each trial"
for seq in $(seq 1 "$N"); do start_validator "$seq"; done
sleep 5
for seq in $(seq 1 "$N"); do
    kill -0 "${PIDS[$seq]}" 2>/dev/null || fail "validator $seq did not start; see its log"
done
wait_for 240 reached "$REFERENCE" "$WARMUP_HEADERS" || fail "chain did not reach header $WARMUP_HEADERS"
log "warm-up done at header $(header "$REFERENCE"), epoch $(epoch "$REFERENCE")"

stalled_trials=0
gave_up_total=0
for trial in $(seq 1 "$TRIALS"); do
    one_trial "$trial"
    read -r stalled gave_up _ <"$RUN/trial-$trial.txt"
    stalled_trials=$((stalled_trials + stalled))
    gave_up_total=$((gave_up_total + gave_up))
done
hook=$(grep -c 'test hook: closing the epoch late' "$RUN/validator-$LATE.log")
log "late validator $LATE closed $hook epochs late"
[[ $hook -gt 0 ]] || fail "the hook never fired: is the binary built with --features test-hooks?"

if [[ $stalled_trials -gt 0 && $gave_up_total -gt 0 ]]; then
    grep -ahE "$GAVE_UP" "$RUN"/validator-*.log | tail -1 | sed 's/\x1b\[[0-9;]*m//g' | cut -c1-220 \
        | sed 's/^/  proposer log: /' | tee -a "$RUN/summary.txt"
    log "REPRODUCED: the chain stalled in $stalled_trials of $TRIALS trials after the proposers gave up on the late validator's refusals"
    echo "logs and datadirs: $RUN"
    exit 1
fi
if [[ $stalled_trials -gt 0 ]]; then
    fail "the chain stalled in $stalled_trials of $TRIALS trials, but no proposer gave up on a refusal"
fi
log "HEALTHY: the chain kept committing across all $TRIALS boundaries with validator $OTHER down"
echo "logs and datadirs: $RUN"
exit 0
