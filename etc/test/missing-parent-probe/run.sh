#!/usr/bin/env bash
#
# Live reproduction: a parent certificate lost from gossip is fetched only by the certificate
# fetcher's idle probe, about once a minute, so the chain stops committing for up to a minute.
# See README.md in this directory.
#
# Starts a local network of validators and stops one, so exactly a quorum is up. One validator
# (the victim) then loses the certificate gossip of one round in both directions, through the
# cert-gossip-drop-hook test hook: its own certificate reaches nobody and it receives no one
# else's. No live node then holds a quorum of that round's certificates, so only the certificate
# fetcher can get them. Each loss must be followed by the chain committing again within a few max
# header delays.
#
# The hook is compiled only with the `cert-gossip-drop-hook` cargo feature and is armed per
# process by environment (RAYLS_TEST_CERT_GOSSIP_DROP_FILE), so only the victim drops gossip.
#
# Exit status: 0 the chain resumed quickly after every loss (healthy; bug absent or fixed),
# 1 the chain went more than the allowed gap without a new header after a loss (bug present,
# issue reproduced), 2 the run was inconclusive or could not start.

set -uo pipefail

usage() {
    cat <<EOF
usage: $0 [-n validators] [-b binary] [-d workdir] [-f losses] [-g max_gap] [-i instance]
  -n  number of validators, at least 5 (default 5)
  -b  rayls-network binary built with --features cert-gossip-drop-hook
      (default: build target/debug/rayls-network from this checkout with the feature)
  -d  empty directory for keys, genesis, datadirs and logs
      (default: target/missing-parent-probe/<timestamp>)
  -f  gossip losses to inject (default 3)
  -g  longest allowed time in seconds without a new header after a loss (default 30)
  -i  first --instance number, which sets the RPC ports (default 40)
EOF
    exit 2
}

HEADERS=10
N=5
BIN=""
WORKDIR=""
FAULTS=3
MAX_GAP=30
INSTANCE_BASE=40
while getopts "n:b:d:f:g:i:h" opt; do
    case "$opt" in
        n) N=$OPTARG ;;
        b) BIN=$OPTARG ;;
        d) WORKDIR=$OPTARG ;;
        f) FAULTS=$OPTARG ;;
        g) MAX_GAP=$OPTARG ;;
        i) INSTANCE_BASE=$OPTARG ;;
        *) usage ;;
    esac
done
[[ $N =~ ^[0-9]+$ && $N -ge 5 ]] || { echo "need at least 5 validators"; usage; }
for value in "$FAULTS" "$MAX_GAP" "$INSTANCE_BASE"; do
    [[ $value =~ ^[0-9]+$ ]] || { echo "not a number: $value"; usage; }
done
[[ $FAULTS -ge 1 ]] || { echo "need at least one loss"; usage; }

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
WORKDIR=${WORKDIR:-$REPO/target/missing-parent-probe/$(date -u +%Y%m%dT%H%M%SZ)}
if [[ -d $WORKDIR && -n $(ls -A "$WORKDIR" 2>/dev/null) ]]; then
    echo "not empty: $WORKDIR"; exit 2
fi
RUN=$WORKDIR

# The validator that loses one round of certificate gossip, the one kept stopped so exactly a
# quorum is up, and the one the chain is read from.
VICTIM=3
STOPPED=$N
REFERENCE=1
WARMUP_HEADERS=10
# a loss must be followed by $HEADERS headers within this many seconds, or the run is
# inconclusive (the fetcher's idle probe runs once a minute)
LOSS_DEADLINE=180
CHAIN_ID=0x1e7
DEV_ACCOUNT=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
EPOCH_SECS=86400
export RL_BLS_PASSPHRASE="missing-parent-probe-local-only"
DROP_LINE='test hook: dropping certificate gossip'
TRIGGER=$RUN/drop-cert-gossip

declare -A PIDS
mkdir -p "$RUN"
log() { echo "$(date -u +%T) $*" | tee -a "$RUN/summary.txt"; }
fail() { log "INCONCLUSIVE: $*"; echo "logs and datadirs: $RUN"; exit 2; }

for tool in curl bc; do
    command -v "$tool" >/dev/null || { echo "missing required tool: $tool"; exit 2; }
done

if [[ -z $BIN ]]; then
    echo "building rayls-network (debug) with --features cert-gossip-drop-hook"
    (cd "$REPO" && cargo build -p rayls-network --features cert-gossip-drop-hook) || exit 2
    BIN=$REPO/target/debug/rayls-network
fi
[[ -x $BIN ]] || { echo "not executable: $BIN"; exit 2; }

rpc_port() { echo $((8546 - (INSTANCE_BASE + $1))); }

rpc() {
    curl -s -m 2 -H 'content-type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$2\",\"params\":[]}" \
        "http://127.0.0.1:$(rpc_port "$1")" 2>/dev/null
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
    [[ $seq -eq $VICTIM ]] && env=(RAYLS_TEST_CERT_GOSSIP_DROP_FILE="$TRIGGER")
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
armed() { [[ ! -e $TRIGGER ]]; }

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
    "$BIN" genesis --datadir "$RUN" --chain-id $CHAIN_ID --epoch-duration-in-secs $EPOCH_SECS \
        --dev-funded-account $DEV_ACCOUNT --max-header-delay-ms 1000 --min-header-delay-ms 500 \
        --consensus-registry-owner $DEV_ACCOUNT --network-admin $DEV_ACCOUNT >/dev/null 2>&1 || return 1
    for seq in $(seq 1 "$N"); do
        mkdir -p "$RUN/validator-$seq/genesis"
        cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$RUN/validator-$seq/genesis/"
        cp "$RUN/parameters.yaml" "$RUN/validator-$seq/"
    done
}

drops() { grep -c "$DROP_LINE" "$RUN/validator-$VICTIM.log"; }

# One loss. Writes "<longest gap in seconds> <certificates dropped>" to loss-<n>.txt.
one_loss() {
    local loss=$1 before start last now changed longest=0 gap deadline
    before=$(drops)
    : >"$TRIGGER"
    wait_for 30 armed || fail "the hook did not arm: is the binary built with --features cert-gossip-drop-hook?"
    start=$(header "$REFERENCE")
    [[ -n $start ]] || fail "validator $REFERENCE does not answer rayls_latestHeader"
    log "loss $loss: armed at header $start"
    last=$start
    changed=$(date +%s.%N)
    deadline=$((SECONDS + LOSS_DEADLINE))
    while [[ $last -lt $((start + HEADERS)) ]]; do
        if [[ $SECONDS -ge $deadline ]]; then
            fail "chain stuck at header $last (from $start) ${LOSS_DEADLINE}s after loss $loss"
        fi
        sleep 0.2
        now=$(header "$REFERENCE")
        gap=$(echo "$(date +%s.%N) - $changed" | bc)
        (($(echo "$gap > $longest" | bc))) && longest=$gap
        if [[ -n $now && $now -gt $last ]]; then
            last=$now
            changed=$(date +%s.%N)
        fi
    done
    local dropped=$(($(drops) - before))
    longest=$(printf '%.1f' "$longest")
    log "loss $loss: dropped $dropped certificates, longest gap without a new header ${longest}s"
    echo "$longest $dropped" >"$RUN/loss-$loss.txt"
}

setup_network || fail "network setup failed"
log "network: $N validators; validator $VICTIM can drop one round of certificate gossip, validator $STOPPED is stopped"
for seq in $(seq 1 "$N"); do start_validator "$seq"; done
sleep 5
for seq in $(seq 1 "$N"); do
    kill -0 "${PIDS[$seq]}" 2>/dev/null || fail "validator $seq did not start; see its log"
done
wait_for 240 reached "$REFERENCE" "$WARMUP_HEADERS" || fail "chain did not reach header $WARMUP_HEADERS"
log "warm-up done at header $(header "$REFERENCE")"

# exactly a quorum left: every round needs the certificate of every live validator
stop_pid "${PIDS[$STOPPED]}"
unset "PIDS[$STOPPED]"
now=$(header "$REFERENCE")
wait_for 60 reached "$REFERENCE" $((now + 5)) || fail "chain did not commit with validator $STOPPED stopped"
log "validator $STOPPED stopped; chain at header $(header "$REFERENCE") with exactly a quorum"

slow=0
worst=0
for loss in $(seq 1 "$FAULTS"); do
    one_loss "$loss"
    read -r gap dropped <"$RUN/loss-$loss.txt"
    [[ $dropped -gt 0 ]] || fail "the hook dropped no certificate in loss $loss"
    (($(echo "$gap > $MAX_GAP" | bc))) && slow=$((slow + 1))
    (($(echo "$gap > $worst" | bc))) && worst=$gap
done

if [[ $slow -gt 0 ]]; then
    # on current code the lost certificates arrive only after the fetcher's periodic probe
    grep -ahE 'periodic probe to discover missing certificates|processing [0-9]+ fetched certificates' \
        "$RUN/validator-$VICTIM.log" | tail -2 | sed 's/\x1b\[[0-9;]*m//g' | cut -c1-220 \
        | sed 's/^/  victim log: /' | tee -a "$RUN/summary.txt"
    log "REPRODUCED: in $slow of $FAULTS losses the chain went more than ${MAX_GAP}s without a new header (worst ${worst}s)"
    echo "logs and datadirs: $RUN"
    exit 1
fi
log "HEALTHY: the chain resumed within ${MAX_GAP}s after all $FAULTS losses (worst ${worst}s)"
echo "logs and datadirs: $RUN"
exit 0
