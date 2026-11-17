#!/usr/bin/env bash
#
# Live reproduction: the forward streamer deletes the by-hash index entry of a block the
# subscriber has just saved. See README.md in this directory.
#
# Starts a local network of validators plus one observer, lets a following node (the victim) fall
# behind, arms one validator to answer by-number consensus-header requests with a header that does
# not link to the real one below, and restarts the victim so it catches up through the gap. Each
# such reply sends the victim's streamer into its mismatch branch, which checks that the row below
# is not saved and then deletes it with its by-hash index entry. The victim's subscriber saves that
# same row from another task. A save that lands between the check and the delete leaves a canonical
# block with no by-hash index entry. The victim is then stopped and its consensus DB is checked with
# rayls-db-inspect.
#
# Test-only behaviour comes from two cargo features, both armed per process by environment:
# `byzantine-test-hooks` (the attacker's reply) and `fault-injection-test-hooks` (on the victim:
# delays that widen the race window, and switches that make it fetch rows by number).
#
# Modes (-m):
#   race      victim is the observer, attacker armed, delay on (default): expect REPRODUCED
#   nodelay   as race with the delay off: how often the natural window is hit
#   honest    as race with the attacker not armed: the branch never runs, expect HEALTHY
#   restart   victim is the observer. The attacker is the only peer it fetches by number from, so
#             it stalls right after the race, with the block that lost its entry as its last
#             executed block. It is then stopped, the attacker disarmed, and the victim restarted
#             with no switches on an honest network, to see what boot does without the entry.
#   vrestart  as restart, with committee validator N as the victim: it catches up as an inactive
#             validator, then restarts.
#   With -C, the restart modes run as a control: no delays, so the race does not land, and the
#   same stall and restart must recover.
#
# Exit status: 0 no canonical block lost its index entry (healthy; bug absent or fixed),
# 1 a canonical block lost its by-hash index entry (issue reproduced),
# 2 the run was inconclusive or could not start.

set -uo pipefail

usage() {
    cat <<EOF
Usage: $0 [-m MODE] [-n VALIDATORS] [-b BINARY] [-t INSPECT] [-d WORKDIR] [-w WATCH_SECS]
          [-g GAP] [-D DELAY_MS] [-S SAVE_DELAY_MS] [-e EPOCH_SECS] [-E] [-C] [-i INSTANCE_BASE]

  -m  race | nodelay | honest | restart | vrestart (default race)
  -n  validators in the committee, at least 4 (default 4)
  -b  node binary built with --features byzantine-test-hooks,fault-injection-test-hooks
      (default: build target/debug/rayls-network from this checkout with both features)
  -t  rayls-db-inspect binary (default: build target/debug/rayls-db-inspect from this checkout)
  -d  directory for datadirs and logs (default: target/catchup-clear-index-race/<timestamp>-<mode>)
  -w  seconds to watch the victim catch up, and after its restart (default 120)
  -g  how many blocks the victim falls behind before catching up (default 60)
  -D  victim's delay between the check and the delete, in ms
      (default 200, 2000 in the restart modes; forced to 0 in nodelay mode)
  -S  victim's delay before each catch-up save, in ms (default 0, 300 in the restart modes)
  -e  epoch length in seconds (default 3600, so the whole run stays in epoch 0)
  -E  keep the victim following until the first epoch boundary has passed, so it falls behind,
      races and restarts in a later epoch (use with a short -e, e.g. 120)
  -C  control for the restart modes: no delays, the victim stalls for 40s without the race, and
      the restart must recover (exit 0); any other outcome exits 1
  -i  first --instance number, which sets the RPC ports (default 30)
EOF
    exit 2
}

MODE=race
N=4
BIN=""
INSPECT=""
WORKDIR=""
WATCH=120
GAP=60
DELAY_MS=""
SAVE_DELAY_MS=""
EPOCH_SECS=3600
PAST_EPOCH=0
CONTROL=0
INSTANCE_BASE=30
while getopts "m:n:b:t:d:w:g:D:S:e:ECi:h" opt; do
    case "$opt" in
        m) MODE=$OPTARG ;;
        n) N=$OPTARG ;;
        b) BIN=$OPTARG ;;
        t) INSPECT=$OPTARG ;;
        d) WORKDIR=$OPTARG ;;
        w) WATCH=$OPTARG ;;
        g) GAP=$OPTARG ;;
        D) DELAY_MS=$OPTARG ;;
        S) SAVE_DELAY_MS=$OPTARG ;;
        e) EPOCH_SECS=$OPTARG ;;
        E) PAST_EPOCH=1 ;;
        C) CONTROL=1 ;;
        i) INSTANCE_BASE=$OPTARG ;;
        *) usage ;;
    esac
done
case "$MODE" in
    race | nodelay | honest) RESTART=0; DELAY_MS=${DELAY_MS:-200}; SAVE_DELAY_MS=${SAVE_DELAY_MS:-0} ;;
    restart | vrestart) RESTART=1; DELAY_MS=${DELAY_MS:-2000}; SAVE_DELAY_MS=${SAVE_DELAY_MS:-300} ;;
    *) echo "unknown mode: $MODE"; usage ;;
esac
[[ $MODE == nodelay ]] && DELAY_MS=0
if [[ $CONTROL -eq 1 ]]; then
    [[ $RESTART -eq 1 ]] || { echo "-C applies to the restart modes"; usage; }
    DELAY_MS=0; SAVE_DELAY_MS=0
fi
[[ $N =~ ^[0-9]+$ && $N -ge 4 ]] || { echo "need at least 4 validators"; usage; }
for value in "$WATCH" "$GAP" "$DELAY_MS" "$SAVE_DELAY_MS" "$EPOCH_SECS" "$INSTANCE_BASE"; do
    [[ $value =~ ^[0-9]+$ ]] || { echo "not a number: $value"; usage; }
done

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
SUFFIX=$MODE; [[ $CONTROL -eq 1 ]] && SUFFIX=$MODE-control
WORKDIR=${WORKDIR:-$REPO/target/catchup-clear-index-race/$(date -u +%Y%m%dT%H%M%SZ)-$SUFFIX}
if [[ -d $WORKDIR && -n $(ls -A "$WORKDIR" 2>/dev/null) ]]; then
    echo "not empty: $WORKDIR"; exit 2
fi
RUN=$WORKDIR
mkdir -p "$RUN"

# The attacker is validator 1. The victim is the observer, or validator N in vrestart mode.
ATTACKER=1
if [[ $MODE == vrestart ]]; then VICTIM=$N; else VICTIM=obs; fi
OBS_INSTANCE=$((INSTANCE_BASE + N + 1))
WARMUP_BLOCKS=20
CHAIN_ID=0x1e7
DEV_ACCOUNT=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
# The attacker lies about every consensus number while armed; only followers ask by number.
ARM_RANGE="1-1000000000"
export RL_BLS_PASSPHRASE="catchup-clear-index-race-local-only"

declare -A PIDS
log() { echo "$(date -u +%T) $*" | tee -a "$RUN/summary.txt"; }
fail() { log "INCONCLUSIVE: $*"; echo "logs and datadirs: $RUN"; exit 2; }
strip() { sed 's/\x1b\[[0-9;]*m//g'; }

command -v curl >/dev/null || { echo "missing required tool: curl"; exit 2; }

if [[ -z $BIN ]]; then
    echo "building rayls-network (debug) with --features byzantine-test-hooks,fault-injection-test-hooks"
    (cd "$REPO" && cargo build -p rayls-network \
        --features byzantine-test-hooks,fault-injection-test-hooks) || exit 2
    BIN=$REPO/target/debug/rayls-network
fi
if [[ -z $INSPECT ]]; then
    echo "building rayls-db-inspect (debug)"
    (cd "$REPO" && cargo build -p rayls-db-inspect) || exit 2
    INSPECT=$REPO/target/debug/rayls-db-inspect
fi
[[ -x $BIN ]] || { echo "not executable: $BIN"; exit 2; }
[[ -x $INSPECT ]] || { echo "not executable: $INSPECT"; exit 2; }

# RPC port for a validator seq (1..N) or for the observer when given "obs".
rpc_port() {
    if [[ $1 == obs ]]; then echo $((8546 - OBS_INSTANCE))
    else echo $((8546 - (INSTANCE_BASE + $1))); fi
}

rpc() {
    curl -s -m 3 -H 'content-type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$2\",\"params\":$3}" \
        "http://127.0.0.1:$(rpc_port "$1")" 2>/dev/null
}

# eth_blockNumber as a decimal, empty when the node does not answer.
height() {
    local out
    out=$(rpc "$1" eth_blockNumber '[]') || return 0
    [[ $out =~ \"result\":\"0x([0-9a-fA-F]+)\" ]] && echo $((16#${BASH_REMATCH[1]}))
}

# Hash of EVM block $2 (decimal) on node $1.
block_hash() {
    local num_hex out
    printf -v num_hex '0x%x' "$2"
    out=$(rpc "$1" eth_getBlockByNumber "[\"$num_hex\",false]") || return 0
    [[ $out =~ \"hash\":\"(0x[0-9a-fA-F]+)\" ]] && echo "${BASH_REMATCH[1]}"
}

# Highest block among the validators other than the victim.
max_val_height() {
    local best=-1 h v
    for v in $(seq 1 "$N"); do
        [[ $v == "$VICTIM" ]] && continue
        h=$(height "$v"); [[ -n $h && $h -gt $best ]] && best=$h
    done
    echo "$best"
}

# A healthy validator that is neither the attacker nor the victim, for canonical comparison.
HEALTHY=2

node_log() { if [[ $1 == obs ]]; then echo "$RUN/observer.log"; else echo "$RUN/validator-$1.log"; fi; }

# Start node $1 (a validator seq or "obs"), logging to $2. Extra environment (the attacker's arming
# or the victim's fault switches) is passed as VAR=value arguments after that.
start_node() {
    local node=$1 logfile=$2; shift 2
    if [[ $node == obs ]]; then
        env "$@" \
        PRIMARY_LISTENER_MULTIADDR="/ip4/0.0.0.0/udp/$((49000 + OBS_INSTANCE))/quic-v1" \
        WORKER_LISTENER_MULTIADDR="/ip4/0.0.0.0/udp/$((49100 + OBS_INSTANCE))/quic-v1" \
            "$BIN" node --datadir "$RUN/observer" --network local --observer \
            --instance "$OBS_INSTANCE" --log.stdout.format log-fmt --full --storage.v2 \
            --db.growth-step 1MB --consensus-db.growth-step 1MB --txpool.minimal-protocol-fee 0 \
            --gpo.default-suggested-fee 0 -vvv --http --http.addr 127.0.0.1 --http.api all \
            >>"$logfile" 2>&1 &
    else
        env "$@" "$BIN" node --datadir "$RUN/validator-$node" --network local \
            --instance $((INSTANCE_BASE + node)) --log.stdout.format log-fmt --full --storage.v2 \
            --db.growth-step 1MB --consensus-db.growth-step 1MB --txpool.minimal-protocol-fee 0 \
            --gpo.default-suggested-fee 0 -vvv --http --http.addr 127.0.0.1 --http.api all \
            >>"$logfile" 2>&1 &
    fi
    PIDS[$node]=$!
}

stop_node() {
    local pid=${PIDS[$1]:-}
    [[ -z $pid ]] && return 0
    kill -TERM "$pid" 2>/dev/null
    for _ in $(seq 1 60); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
    kill -KILL "$pid" 2>/dev/null
    unset "PIDS[$1]"
    return 0
}

alive() { local pid=${PIDS[$1]:-}; [[ -n $pid ]] && kill -0 "$pid" 2>/dev/null; }

stop_all() {
    local node
    for node in "${!PIDS[@]}"; do kill -TERM "${PIDS[$node]}" 2>/dev/null; done
    for node in "${!PIDS[@]}"; do stop_node "$node"; done
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
victim_reached() { local h; h=$(height "$VICTIM"); [[ -n $h && $h -ge $1 ]]; }

setup_network() {
    mkdir -p "$RUN/genesis/validators"
    local seq
    for seq in $(seq 1 "$N") obs; do
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
    # The observer is not a committee member. It is generated after genesis and only follows.
    "$BIN" keytool generate observer --datadir "$RUN/observer" \
        --address 0x0000000000000000000000000000000000000000 --advertise-identity-only >/dev/null 2>&1 || return 1
    mkdir -p "$RUN/observer/genesis"
    cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$RUN/observer/genesis/"
    cp "$RUN/parameters.yaml" "$RUN/observer/"
}

victim_datadir() { if [[ $VICTIM == obs ]]; then echo "$RUN/observer"; else echo "$RUN/validator-$VICTIM"; fi; }

# Copy the victim's consensus DB (the victim must be stopped) to $RUN/backup-$1.
backup_victim_db() {
    local src
    src=$(dirname "$(find "$(victim_datadir)" -path '*consensus-db*' -name mdbx.dat | head -1)")
    [[ -d $src ]] || { log "no victim consensus-db found to back up"; return 0; }
    mkdir -p "$RUN/backup-$1"
    cp -a "$src" "$RUN/backup-$1/victim-consensus-db"
    log "victim consensus-db copied to $RUN/backup-$1/victim-consensus-db"
}

# Walk the victim's stored header chain with rayls-db-inspect (victim stopped) and print the lines
# that report a header whose parent is stored but not indexed by hash.
inspect_lost_index() {
    local db=$1 tip
    "$INSPECT" --json --db "$db" summary >"$RUN/inspect-summary-$2.json" 2>&1
    # The highest stored header, canonical or cache: start the walk there so a lost entry at
    # the canonical tip is still checked from the row above it.
    tip=$(grep -aoE '"latest_(cached_)?consensus_number": ?[0-9]+' "$RUN/inspect-summary-$2.json" \
        | grep -aoE '[0-9]+$' | sort -n | tail -1)
    [[ -n $tip ]] || { echo "could not read the stored tip from rayls-db-inspect summary" >&2; return 0; }
    "$INSPECT" --db "$db" header-check "$tip" --back 1000000 2>&1 | strip >"$RUN/inspect-header-check-$2.txt"
    grep -a 'INDEX MISMATCH' "$RUN/inspect-header-check-$2.txt"
}

# --- run -------------------------------------------------------------------------------------

setup_network || fail "network setup failed"
log "mode $SUFFIX: $N validators + 1 observer; attacker is validator $ATTACKER, victim is $VICTIM"
log "victim delays: ${DELAY_MS} ms between the check and the delete, ${SAVE_DELAY_MS} ms before each save; epoch length ${EPOCH_SECS}s"

START=$(date +%s)
for seq in $(seq 1 "$N"); do start_node "$seq" "$(node_log "$seq")"; done
[[ $VICTIM == obs ]] && start_node obs "$(node_log obs)"
sleep 5
for node in "${!PIDS[@]}"; do alive "$node" || fail "node $node did not start; see its log"; done

wait_for 240 val_reached "$WARMUP_BLOCKS" || fail "chain did not reach block $WARMUP_BLOCKS"
wait_for 120 victim_reached "$WARMUP_BLOCKS" || fail "victim did not follow to block $WARMUP_BLOCKS"
if [[ $PAST_EPOCH -eq 1 ]]; then
    left=$((START + EPOCH_SECS + 20 - $(date +%s)))
    if [[ $left -gt 0 ]]; then log "following past the first epoch boundary (${left}s)"; sleep "$left"; fi
fi
VICTIM_STOP=$(height "$VICTIM")
log "warm-up done: validators at $(max_val_height), victim following at $VICTIM_STOP"

# Stop the victim to open a gap, then let the validators build $GAP more blocks.
stop_node "$VICTIM"
log "victim stopped at block $VICTIM_STOP"
TARGET_TOP=$((VICTIM_STOP + GAP))
wait_for 300 val_reached "$TARGET_TOP" || fail "validators did not reach block $TARGET_TOP"
log "validators advanced to $(max_val_height); the victim is $GAP blocks behind"

if [[ $MODE != honest ]]; then
    stop_node "$ATTACKER"
    start_node "$ATTACKER" "$(node_log "$ATTACKER")" RL_BYZ_UNLINK_TARGET="$ARM_RANGE"
    log "validator $ATTACKER restarted and armed: by-number replies do not link to the block below"
    wait_for 180 val_reached "$((TARGET_TOP + 5))" || fail "chain did not move on after arming validator $ATTACKER"
fi

VICTIM_ENV=(RL_FAULT_CLEAR_RACE_DELAY_MS="$DELAY_MS" RL_FAULT_SUBSCRIBER_SAVE_DELAY_MS="$SAVE_DELAY_MS")
if [[ $RESTART -eq 1 ]]; then
    ATTACKER_KEY=$(grep -aE '^bls_public_key:' "$RUN/validator-$ATTACKER/node-info.yaml" \
        | awk '{print $2}' | tr -d '"')
    [[ -n $ATTACKER_KEY ]] || fail "could not read validator $ATTACKER's BLS key"
    VICTIM_ENV+=(RL_FAULT_GAP_FETCH=after:8 RL_FAULT_GAP_FETCH_PEER="$ATTACKER_KEY")
else
    VICTIM_ENV+=(RL_FAULT_GAP_FETCH=odd)
fi
log "victim switches: ${VICTIM_ENV[*]}"

VLOG=$RUN/victim-catchup.log
start_node "$VICTIM" "$VLOG" "${VICTIM_ENV[@]}"
sleep 2
alive "$VICTIM" || fail "victim did not restart; see $VLOG"
log "victim restarted, catching up through the gap for up to ${WATCH}s"

waited=0
while [[ $waited -lt $WATCH ]]; do
    sleep 5; waited=$((waited + 5))
    alive "$VICTIM" || fail "victim exited during catch-up; see $VLOG"
    if [[ $RESTART -eq 1 && $CONTROL -eq 0 ]] && grep -qa 'RACE HIT' "$VLOG"; then
        # Let the subscriber execute the saved block so it is the last executed block on disk.
        sleep 10
        break
    fi
    # Control: the victim is held at the attacker the same way, without the race.
    [[ $CONTROL -eq 1 && $waited -ge 40 ]] && break
    h=$(height "$VICTIM")
    if [[ $RESTART -eq 0 && -n $h && $h -ge $(max_val_height) && $waited -ge 60 ]]; then
        break
    fi
    [[ $((waited % 20)) -eq 0 ]] && log "victim at ${h:-?}, validators at $(max_val_height)"
done

VICTIM_AT=$(height "$VICTIM")
stop_node "$VICTIM"
BRANCH=$(grep -ac 'FAULT HOOK: mismatch branch checked' "$VLOG")
HITS=$(grep -ac 'RACE HIT' "$VLOG")
REPLIES=$(grep -ac 'BYZANTINE BY-NUMBER' "$(node_log "$ATTACKER")")
log "victim stopped at block ${VICTIM_AT:-?} (validators at $(max_val_height))"
log "attacker unlinked replies: $REPLIES; victim delete-branch runs: $BRANCH; RACE HIT log lines: $HITS"
grep -a 'RACE HIT' "$VLOG" | strip | cut -c1-220 | sed 's/^/  victim log: /' | tee -a "$RUN/summary.txt"

backup_victim_db after-catch-up
LOST=$(inspect_lost_index "$RUN/backup-after-catch-up/victim-consensus-db" after-catch-up)
if [[ -n $LOST ]]; then
    log "rayls-db-inspect header-check on the victim:"
    echo "$LOST" | cut -c1-220 | sed 's/^/  /' | tee -a "$RUN/summary.txt"
else
    log "rayls-db-inspect header-check on the victim: no header with an unindexed parent"
fi

if [[ $RESTART -eq 0 ]]; then
    if [[ -n $LOST ]]; then
        log "ISSUE REPRODUCED: a canonical block on the victim has no by-hash index entry"
        echo "logs and datadirs: $RUN"; exit 1
    fi
    log "HEALTHY: every stored header on the victim is indexed by hash"
    echo "logs and datadirs: $RUN"; exit 0
fi

# --- restart modes: what boot does when the last executed block has no index entry -----------

if [[ $CONTROL -eq 1 ]]; then
    [[ -z $LOST ]] || fail "control: the race landed without delays; rerun"
    log "control: the victim is held at the attacker with every stored header indexed"
else
    [[ $HITS -gt 0 && -n $LOST ]] || fail "the race did not land in ${WATCH}s; rerun, or raise -D/-S"
fi

stop_node "$ATTACKER"
start_node "$ATTACKER" "$(node_log "$ATTACKER")"
log "validator $ATTACKER restarted honest"
wait_for 120 val_reached "$(( $(max_val_height) + 5 ))" || log "chain did not move on after disarming the attacker"

# Restart the victim with no switches, logging to $1, and set OUTCOME.
watch_restart() {
RLOG=$1
VALS_BEFORE=$(max_val_height)
start_node "$VICTIM" "$RLOG"
log "victim restarted with no switches, watching ${WATCH}s"
OUTCOME=""; waited=0; last_h=-1; still=0
while [[ $waited -lt $WATCH ]]; do
    sleep 5; waited=$((waited + 5))
    if ! alive "$VICTIM"; then OUTCOME=CRASHED; break; fi
    h=$(height "$VICTIM")
    [[ -z $h ]] && continue
    obs_hash=$(block_hash "$VICTIM" "$h"); can_hash=$(block_hash "$HEALTHY" "$h")
    if [[ -n $obs_hash && -n $can_hash && $obs_hash != "$can_hash" ]]; then
        OUTCOME="FORKED at block $h"; break
    fi
    if [[ $h -le $last_h ]]; then still=$((still + 5)); else still=0; fi
    last_h=$h
    [[ $((waited % 20)) -eq 0 ]] && log "victim at $h, validators at $(max_val_height)"
done
FINAL=$(height "$VICTIM"); VALS=$(max_val_height)
if [[ -z $OUTCOME ]]; then
    if [[ -n $FINAL && $FINAL -ge $((VALS - 5)) ]]; then OUTCOME=RECOVERED
    else OUTCOME="STALLED at block ${FINAL:-?} (no progress for ${still}s)"; fi
fi
log "after restart: $OUTCOME (victim at ${FINAL:-?}, validators at $VALS, were at $VALS_BEFORE)"
grep -aE 'consensus tip vs execution anchor|prime_consensus: begin|stream_missing_consensus|stream handoff|identify_node_mode: decided|catch_up_from_to: starting|could not confirm execution|halting forward-streamer|digest mismatch at|panicked|level=error' \
    "$RLOG" | strip | sed -E 's/^ts=[^ ]+ //' | cut -c1-240 | sort | uniq -c | sort -rn | head -20 \
    | sed 's/^/  restart log: /' | tee -a "$RUN/summary.txt"
stop_node "$VICTIM"
}

watch_restart "$RUN/victim-restart.log"
backup_victim_db after-restart
FIRST=$OUTCOME
if [[ $OUTCOME == STALLED* ]]; then
    log "restarting the stalled victim once more"
    watch_restart "$RUN/victim-restart-2.log"
    backup_victim_db after-restart-2
    FIRST="$FIRST; after a second restart: $OUTCOME"
fi
if [[ $CONTROL -eq 1 ]]; then
    if [[ $FIRST == RECOVERED ]]; then
        log "CONTROL HEALTHY: with every header indexed, the same stall and restart recover"
        echo "logs and datadirs: $RUN"; exit 0
    fi
    log "CONTROL FAILED: the restart did not recover without the race: $FIRST"
    echo "logs and datadirs: $RUN"; exit 1
fi
log "ISSUE REPRODUCED: the victim's last executed block had no by-hash index entry; after restart: $FIRST"
echo "logs and datadirs: $RUN"
exit 1
