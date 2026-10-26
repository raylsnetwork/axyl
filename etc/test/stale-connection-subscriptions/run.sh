#!/usr/bin/env bash
#
# Live reproduction: a validator killed and restarted within the 30 s QUIC idle timeout cannot
# publish gossip afterwards, because its peers keep the dead connection next to the new one and
# never send it their topic subscriptions. See README.md in this directory.
#
# Starts a local network of validators and puts one of them (the victim) behind a stale connection
# in one of two ways, lets it reconnect, then stops another validator gracefully so exactly a
# quorum is left, and counts the victim's failed gossip publishes:
#
# - crash (default): the victim is killed with SIGKILL (no QUIC close reaches its peers) and
#   restarted at once. No test hook is needed.
# - oneway: no crash. An on-path attacker drops every datagram the victim receives for a little
#   over the 30 s idle timeout, and stops when the victim starts redialing. Played by the
#   LD_PRELOAD shim udp_drop.c in this directory, built by this script and loaded only into the
#   victim; the node binary is unchanged.
#
# Exit status: 0 the restarted validator gossips again and the chain kept committing (healthy;
# bug absent or fixed), 1 the restarted validator's publishes failed (bug present, issue
# reproduced), 2 the run was inconclusive or could not start.

set -uo pipefail

usage() {
    cat <<EOF
usage: $0 [-m crash|oneway] [-n validators] [-b binary] [-d workdir] [-r rounds] [-s settle] [-w watch] [-i instance]
  -m  how the victim ends up behind a stale connection (default crash)
  -n  number of validators, at least 5 (default 5)
  -b  rayls-network binary to run
      (default: build target/debug/rayls-network from this checkout)
  -d  empty directory for keys, genesis, datadirs and logs
      (default: target/stale-connection-subscriptions/<timestamp>)
  -r  restart rounds on the same network (default 2)
  -s  seconds the restarted validator gets to reconnect before it is checked (default 20)
  -w  seconds the restarted validator's publishes and the chain are watched (default 60)
  -i  first --instance number, which sets the RPC ports (default 30)
EOF
    exit 2
}

MODE=crash
N=5
BIN=""
WORKDIR=""
ROUNDS=2
SETTLE=20
WATCH=60
INSTANCE_BASE=30
while getopts "m:n:b:d:r:s:w:i:h" opt; do
    case "$opt" in
        m) MODE=$OPTARG ;;
        n) N=$OPTARG ;;
        b) BIN=$OPTARG ;;
        d) WORKDIR=$OPTARG ;;
        r) ROUNDS=$OPTARG ;;
        s) SETTLE=$OPTARG ;;
        w) WATCH=$OPTARG ;;
        i) INSTANCE_BASE=$OPTARG ;;
        *) usage ;;
    esac
done
[[ $MODE == crash || $MODE == oneway ]] || { echo "unknown mode: $MODE"; usage; }
[[ $N =~ ^[0-9]+$ && $N -ge 5 ]] || { echo "need at least 5 validators"; usage; }
for value in "$ROUNDS" "$SETTLE" "$WATCH" "$INSTANCE_BASE"; do
    [[ $value =~ ^[0-9]+$ ]] || { echo "not a number: $value"; usage; }
done

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
WORKDIR=${WORKDIR:-$REPO/target/stale-connection-subscriptions/$(date -u +%Y%m%dT%H%M%SZ)}
if [[ -d $WORKDIR && -n $(ls -A "$WORKDIR" 2>/dev/null) ]]; then
    echo "not empty: $WORKDIR"; exit 2
fi
RUN=$WORKDIR

# The validator that crashes and restarts, the one stopped afterwards to leave exactly a quorum,
# and the one the chain height is read from.
VICTIM=3
OTHER=$N
REFERENCE=1
WARMUP_BLOCKS=20
MIN_BLOCKS=20
CHAIN_ID=0x1e7
EPOCH_SECS=3600
DEV_ACCOUNT=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
export RL_BLS_PASSPHRASE="stale-connection-local-only"

declare -A PIDS
log() { echo "$(date -u +%T) $*" | tee -a "$RUN/summary.txt"; }
fail() { log "INCONCLUSIVE: $*"; echo "logs and datadirs: $RUN"; exit 2; }

DROP_FILE=$RUN/drop-victim
LIFT_FILE=$RUN/lift-victim
# the drop must outlast the victim's 30 s idle timeout before the attacker lets its redial through
DROP_MIN_SECS=32

for tool in curl; do
    command -v "$tool" >/dev/null || { echo "missing required tool: $tool"; exit 2; }
done

if [[ -z $BIN ]]; then
    echo "building rayls-network (debug)"
    (cd "$REPO" && cargo build -p rayls-network) || exit 2
    BIN=$REPO/target/debug/rayls-network
fi
[[ -x $BIN ]] || { echo "not executable: $BIN"; exit 2; }
SHIM=""
if [[ $MODE == oneway ]]; then
    command -v cc >/dev/null || { echo "missing required tool: cc"; exit 2; }
    mkdir -p "$RUN"
    SHIM=$RUN/udp_drop.so
    cc -shared -fPIC -O2 -o "$SHIM" "$REPO/etc/test/stale-connection-subscriptions/udp_drop.c" -ldl || exit 2
fi

rpc_port() { echo $((8546 - (INSTANCE_BASE + $1))); }

height() {
    local out
    out=$(curl -s -m 2 -H 'content-type: application/json' \
        -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
        "http://127.0.0.1:$(rpc_port "$1")" 2>/dev/null) || return 0
    [[ $out =~ \"result\":\"0x([0-9a-fA-F]+)\" ]] && echo $((16#${BASH_REMATCH[1]}))
}

start_validator() {
    local seq=$1
    local -a env=()
    if [[ -n $SHIM && $seq -eq $VICTIM ]]; then
        env=(LD_PRELOAD="$SHIM" RL_UDP_DROP_FILE="$DROP_FILE" RL_UDP_LIFT_FILE="$LIFT_FILE")
    fi
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
reached() { local h; h=$(height "$1"); [[ -n $h && $h -ge $2 ]]; }
serves() { [[ -n $(height "$1") ]]; }

setup_network() {
    mkdir -p "$RUN"; find "$RUN" -mindepth 1 -maxdepth 1 ! -name udp_drop.so -exec rm -rf {} +
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

crash_victim() {
    log "round $1: killing validator $VICTIM (SIGKILL) and restarting it at once"
    kill -KILL "${PIDS[$VICTIM]}" 2>/dev/null
    wait "${PIDS[$VICTIM]}" 2>/dev/null
    start_validator "$VICTIM"
    wait_for 120 serves "$VICTIM" || fail "validator $VICTIM did not come back"
}

drop_victim_inbound() {
    local started=$SECONDS
    log "round $1: dropping everything validator $VICTIM receives (it still sends)"
    touch "$DROP_FILE"
    sleep "$DROP_MIN_SECS"
    touch "$LIFT_FILE"
    wait_for 90 test ! -e "$DROP_FILE" || { rm -f "$DROP_FILE" "$LIFT_FILE"; log "round $1: no redial seen, drop lifted anyway"; }
    log "round $1: drop ended after $((SECONDS - started))s"
    kill -0 "${PIDS[$VICTIM]}" 2>/dev/null || fail "validator $VICTIM died"
}

# One round: put the victim behind a stale connection, let it reconnect, leave exactly a quorum,
# watch. Writes "<publish failures> <blocks committed while watched>" to round-<n>.txt.
one_round() {
    local round=$1 mark start end failures
    if [[ $MODE == crash ]]; then crash_victim "$round"; else drop_victim_inbound "$round"; fi
    sleep "$SETTLE"
    mark=$(stat -c %s "$RUN/validator-$VICTIM.log")
    log "round $round: stopping validator $OTHER so exactly a quorum is left"
    stop_pid "${PIDS[$OTHER]}"; unset "PIDS[$OTHER]"
    start=$(height "$REFERENCE")
    sleep "$WATCH"
    end=$(height "$REFERENCE")
    [[ -n $start && -n $end ]] || fail "validator $REFERENCE stopped serving RPC"
    failures=$(tail -c +"$((mark + 1))" "$RUN/validator-$VICTIM.log" | grep -c 'gossipsub PUBLISH FAILED')
    log "round $round: validator $VICTIM failed $failures publishes; the chain committed $((end - start)) blocks in ${WATCH}s"
    start_validator "$OTHER"
    wait_for 120 serves "$OTHER" || fail "validator $OTHER did not come back"
    wait_for 180 reached "$OTHER" "$end" || fail "validator $OTHER did not catch up"
    wait_for 180 reached "$VICTIM" "$end" || fail "validator $VICTIM did not catch up"
    echo "$failures $((end - start))" >"$RUN/round-$round.txt"
}

setup_network || fail "network setup failed"
log "network: $N validators; mode $MODE; victim is validator $VICTIM, validator $OTHER is stopped in each round"
for seq in $(seq 1 "$N"); do start_validator "$seq"; done
sleep 5
for seq in $(seq 1 "$N"); do
    kill -0 "${PIDS[$seq]}" 2>/dev/null || fail "validator $seq did not start; see its log"
done
wait_for 240 reached "$REFERENCE" "$WARMUP_BLOCKS" || fail "chain did not reach block $WARMUP_BLOCKS"
log "warm-up done at block $(height "$REFERENCE")"

broken=0
stalled=0
for round in $(seq 1 "$ROUNDS"); do
    one_round "$round"
    read -r failures blocks <"$RUN/round-$round.txt"
    [[ $failures -gt 0 ]] && broken=$((broken + 1))
    [[ $blocks -lt $MIN_BLOCKS ]] && stalled=$((stalled + 1))
done

if [[ $broken -gt 0 ]]; then
    log "REPRODUCED ($MODE): validator $VICTIM could not publish in $broken of $ROUNDS rounds"
    echo "logs and datadirs: $RUN"
    exit 1
fi
if [[ $stalled -gt 0 ]]; then
    fail "validator $VICTIM gossips, but the chain committed fewer than $MIN_BLOCKS blocks in $stalled of $ROUNDS rounds"
fi
log "HEALTHY ($MODE): validator $VICTIM gossips again and the chain kept committing in all $ROUNDS rounds"
echo "logs and datadirs: $RUN"
exit 0
