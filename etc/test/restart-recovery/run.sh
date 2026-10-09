#!/usr/bin/env bash
#
# Live reproduction: a validator that restarts one block behind never catches up when no new block
# arrives. See README.md in this directory.
#
# Starts a local network of N validators with only a bare quorum running, kills one validator with
# SIGKILL just before it saves a block, restarts it, and checks whether the chain moves again.
#
# Exit status: 0 the restarted validator caught up, 1 the chain stayed stopped (bug present),
# 2 the run was inconclusive or could not start.

set -uo pipefail

usage() {
    cat <<EOF
Usage: $0 [-n VALIDATORS] [-b BINARY] [-d WORKDIR] [-w WATCH_SECS] [-a ATTEMPTS] [-i INSTANCE_BASE]

  -n  validators in the committee, at least 4 (default 5)
  -b  node binary (default: build target/release/rayls-network from this checkout)
  -d  directory for datadirs and logs (default: target/restart-recovery/<timestamp>)
  -w  seconds to watch the restarted validator (default 120)
  -a  attempts when the kill misses the window (default 3)
  -i  first --instance number, which sets the RPC ports (default 20)
EOF
    exit 2
}

N=5
BIN=""
WORKDIR=""
WATCH=120
ATTEMPTS=3
INSTANCE_BASE=20
while getopts "n:b:d:w:a:i:h" opt; do
    case "$opt" in
        n) N=$OPTARG ;;
        b) BIN=$OPTARG ;;
        d) WORKDIR=$OPTARG ;;
        w) WATCH=$OPTARG ;;
        a) ATTEMPTS=$OPTARG ;;
        i) INSTANCE_BASE=$OPTARG ;;
        *) usage ;;
    esac
done
[[ $N =~ ^[0-9]+$ && $N -ge 4 ]] || { echo "need at least 4 validators"; usage; }
for value in "$WATCH" "$ATTEMPTS" "$INSTANCE_BASE"; do
    [[ $value =~ ^[0-9]+$ ]] || { echo "not a number: $value"; usage; }
done
[[ $ATTEMPTS -ge 1 ]] || { echo "need at least 1 attempt"; usage; }

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
WORKDIR=${WORKDIR:-$REPO/target/restart-recovery/$(date -u +%Y%m%dT%H%M%SZ)}
if [[ -d $WORKDIR && -n $(ls -A "$WORKDIR") ]]; then
    echo "not empty: $WORKDIR"; exit 2
fi
RUN=$WORKDIR

# Quorum for equal voting power, as in `quorum_threshold` (committee.rs).
QUORUM=$(((2 * N) / 3 + 1))
# Validator QUORUM is the one killed; validators above it stay down so the chain needs it.
TARGET=$QUORUM
WARMUP_BLOCKS=20
CHAIN_ID=0x1e7
EPOCH_SECS=3600
DEV_ACCOUNT=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
export RL_BLS_PASSPHRASE="restart-recovery-local-only"

PIDS=()
log() { echo "$(date -u +%T) $*" | tee -a "$RUN/summary.txt"; }
fail() { log "INCONCLUSIVE: $*"; echo "logs and datadirs: $RUN"; exit 2; }

for tool in gdb curl; do
    command -v "$tool" >/dev/null || { echo "missing required tool: $tool"; exit 2; }
done

if [[ -z $BIN ]]; then
    echo "building rayls-network (release)"
    (cd "$REPO" && cargo build --release -p rayls-network) || exit 2
    BIN=$REPO/target/release/rayls-network
fi
[[ -x $BIN ]] || { echo "not executable: $BIN"; exit 2; }
# gdb must have Python and be allowed to trace its own child process.
gdb_check=$(gdb -q -nx -batch -ex 'python print("python-ok")' -ex run --args /bin/true 2>&1)
[[ $gdb_check == *python-ok* && $gdb_check == *"exited normally"* ]] \
    || { echo "gdb cannot run a child process with Python support:"; echo "$gdb_check"; exit 2; }
symbols=$(gdb -q -nx -batch -ex 'info functions ^rayls_consensus_state_sync::save_consensus$' "$BIN" 2>&1)
[[ $symbols == *save_consensus* ]] || { echo "symbol save_consensus not found in $BIN"; exit 2; }

rpc_port() { echo $((8545 - (INSTANCE_BASE + $1) + 1)); }

# Block height of validator $1 from its RPC, or empty when it does not answer.
height() {
    local out
    out=$(curl -s -m 2 -H 'content-type: application/json' \
        -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
        "http://127.0.0.1:$(rpc_port "$1")" 2>/dev/null) || return 0
    [[ $out =~ \"result\":\"0x([0-9a-fA-F]+)\" ]] && echo $((16#${BASH_REMATCH[1]}))
}

# Highest height among the validators given as arguments.
max_height() {
    local best=-1 h v
    for v in "$@"; do h=$(height "$v"); [[ -n $h && $h -gt $best ]] && best=$h; done
    echo "$best"
}

# Lowest height among the validators given as arguments, or -1 if one does not answer.
min_height() {
    local low="" h v
    for v in "$@"; do
        h=$(height "$v"); [[ -z $h ]] && { echo -1; return; }
        [[ -z $low || $h -lt $low ]] && low=$h
    done
    echo "$low"
}

# Height of every validator, for the log.
heights() {
    local v h line=""
    for v in $(seq 1 "$N"); do h=$(height "$v"); line+=" v$v=${h:-down}"; done
    echo "${line# }"
}

# Set NODE_ARGS to the command line of validator $1.
node_args() {
    local v=$1
    NODE_ARGS=(node --datadir "$RUN/validator-$v" --network local
        --instance $((INSTANCE_BASE + v)) --log.stdout.format log-fmt --full --storage.v2
        --db.growth-step 1MB --consensus-db.growth-step 1MB --txpool.minimal-protocol-fee 0
        --gpo.default-suggested-fee 0 -vvv --http --http.addr 127.0.0.1 --http.api all)
}

start_node() {
    local v=$1 logfile=$2
    node_args "$v"
    "$BIN" "${NODE_ARGS[@]}" >>"$logfile" 2>&1 &
    PIDS[$v]=$!
}

# Run the target under gdb: a breakpoint at save_consensus sends SIGKILL once the arm file exists.
start_target_under_gdb() {
    local v=$1 logfile=$2
    cat >"$RUN/kill-at-save.gdb" <<EOF
set pagination off
set confirm off
set print thread-events off
set print inferior-events off
set startup-with-shell off
handle SIGPIPE SIGUSR1 SIGUSR2 SIGALRM SIGCHLD SIGTERM SIGINT nostop noprint pass
python
import os, signal, time
run_dir = os.environ["RESTART_RECOVERY_RUN"]
class KillAtSave(gdb.Breakpoint):
    def stop(self):
        pid = gdb.selected_inferior().pid
        with open(os.path.join(run_dir, "inferior.pid"), "w") as f:
            f.write(str(pid))
        if not os.path.exists(os.path.join(run_dir, "arm")):
            return False
        with open(os.path.join(run_dir, "killed"), "w") as f:
            f.write(time.strftime("%H:%M:%S", time.gmtime()))
        os.kill(pid, signal.SIGKILL)
        return False
KillAtSave("rayls_consensus_state_sync::save_consensus")
end
run
EOF
    node_args "$v"
    RESTART_RECOVERY_RUN=$RUN gdb -q -nx -batch -x "$RUN/kill-at-save.gdb" --args "$BIN" \
        "${NODE_ARGS[@]}" >>"$logfile" 2>&1 &
    PIDS[$v]=$!
}

stop_all() {
    local v pid
    for v in "${!PIDS[@]}"; do kill -TERM "${PIDS[$v]}" 2>/dev/null; done
    for v in "${!PIDS[@]}"; do
        pid=${PIDS[$v]}
        for _ in $(seq 1 60); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
        kill -KILL "$pid" 2>/dev/null
    done
    PIDS=()
    # The node under gdb keeps running if gdb itself dies first.
    [[ -f $RUN/inferior.pid ]] && kill -KILL "$(cat "$RUN/inferior.pid")" 2>/dev/null
    return 0
}
trap stop_all EXIT
trap 'exit 2' INT TERM

# Wait up to $1 seconds for the command in the remaining arguments to succeed.
wait_for() {
    local secs=$1; shift
    for _ in $(seq 1 "$secs"); do "$@" && return 0; sleep 1; done
    return 1
}

# True when every validator listed after the block number $1 is above that block.
all_above() { local h=$1; shift; [[ $(min_height "$@") -gt $h ]]; }

# "validator 4" or "validators 4-5" for the range $1 to $2.
range_name() { [[ $1 -eq $2 ]] && echo "validator $1" || echo "validators $1-$2"; }

# True when log $1 shows header $2 passed on before the forward streamer started.
published_before_streamer() {
    local published started
    published=$(grep -anE "notifying watchers\"? header_number=$2\b" "$1" | head -1 | cut -d: -f1)
    started=$(grep -an 'stream handoff: starting forward streamer' "$1" | head -1 | cut -d: -f1)
    [[ -n $published && -n $started && $published -lt $started ]]
}

# True when process $1 has exited.
gone() { ! kill -0 "$1" 2>/dev/null; }

setup_network() {
    rm -rf "$RUN"; mkdir -p "$RUN/genesis/validators"
    local v
    # Another local network on the same instance numbers would answer the height checks.
    for v in $(seq 1 "$N"); do
        if (echo >"/dev/tcp/127.0.0.1/$(rpc_port "$v")") 2>/dev/null; then
            fail "RPC port $(rpc_port "$v") is in use; pick other instance numbers with -i"
        fi
    done
    for v in $(seq 1 "$N"); do
        "$BIN" keytool generate validator --datadir "$RUN/validator-$v" \
            --address "$(printf '0x%040x' $((0x5eed0000 + v)))" >/dev/null || return 1
        cp "$RUN/validator-$v/node-info.yaml" "$RUN/genesis/validators/validator-$v.yaml"
    done
    "$BIN" genesis --datadir "$RUN" --chain-id $CHAIN_ID --epoch-duration-in-secs $EPOCH_SECS \
        --dev-funded-account $DEV_ACCOUNT --max-header-delay-ms 1000 --min-header-delay-ms 500 \
        --consensus-registry-owner $DEV_ACCOUNT --network-admin $DEV_ACCOUNT >/dev/null || return 1
    for v in $(seq 1 "$N"); do
        mkdir -p "$RUN/validator-$v/genesis"
        cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$RUN/validator-$v/genesis/"
        cp "$RUN/parameters.yaml" "$RUN/validator-$v/"
    done
}

# One attempt. Returns 0 healthy, 1 bug present, 2 inconclusive, 3 the kill missed the window.
attempt() {
    local running others spares v k after cached_anchor target_height cause f
    running=$(seq 1 "$QUORUM")
    others=$(seq 1 $((QUORUM - 1)))
    spares=$(seq $((QUORUM + 1)) "$N")

    setup_network || fail "network setup failed"
    log "network: $N validators, quorum $QUORUM; $(range_name 1 "$QUORUM") running, $(range_name $((QUORUM + 1)) "$N") down"
    for v in $others; do start_node "$v" "$RUN/validator-$v.log"; done
    start_target_under_gdb "$TARGET" "$RUN/validator-$TARGET.log"
    sleep 5
    gone "${PIDS[$TARGET]}" && fail "validator $TARGET did not start under gdb; see its log"

    # shellcheck disable=SC2086
    wait_for 240 all_above $((WARMUP_BLOCKS - 1)) $running || fail "chain did not reach block $WARMUP_BLOCKS"
    log "chain running: $(heights)"

    touch "$RUN/arm"
    wait_for 60 gone "${PIDS[$TARGET]}" || fail "validator $TARGET was not killed"
    unset "PIDS[$TARGET]"
    rm -f "$RUN/inferior.pid"
    log "validator $TARGET killed with SIGKILL at save_consensus ($(cat "$RUN/killed" 2>/dev/null))"

    sleep 10
    # shellcheck disable=SC2086
    k=$(max_height $others)
    sleep 15
    # shellcheck disable=SC2086
    [[ $(max_height $others) -eq $k ]] || fail "chain kept moving without validator $TARGET"
    # shellcheck disable=SC2086
    [[ $(min_height $others) -eq $k ]] || fail "not every other validator is at block $k: $(heights)"
    log "chain stopped at block $k: $(heights)"

    start_node "$TARGET" "$RUN/validator-$TARGET.restart.log"
    log "validator $TARGET restarted, watching ${WATCH}s"
    local waited=0
    while [[ $waited -lt $WATCH ]]; do
        sleep 5; waited=$((waited + 5))
        # shellcheck disable=SC2086
        [[ $(max_height $running) -gt $k ]] && break
    done
    # shellcheck disable=SC2086
    after=$(max_height $running)
    log "after ${waited}s: $(heights)"

    cached_anchor=$(grep -aoE 'derived walk coverage from cached headers"? anchor=[0-9]+' \
        "$RUN/validator-$TARGET.restart.log" | head -1 | grep -oE '[0-9]+$')
    target_height=$(height "$TARGET")
    log "validator $TARGET after restart: at block ${target_height:-?}, cached header tip ${cached_anchor:-none}"

    if [[ $after -gt $k ]]; then
        if [[ $cached_anchor == "$k" ]]; then
            log "HEALTHY: validator $TARGET restarted with block $k only in its header cache and the chain moved on"
            return 0
        fi
        log "MISSED: block $k was not left only in the header cache, so this run does not test the bug"
        return 3
    fi
    # shellcheck disable=SC2086
    if [[ $(min_height $others) -ne $k ]]; then
        fail "another validator stopped answering, so the stall is not shown to come from validator $TARGET"
    fi
    # A validator already at block k that still cannot move the chain points to a different fault.
    if [[ -z $target_height || $target_height -ge $k ]]; then
        fail "chain stayed at block $k, but validator $TARGET is at block ${target_height:-?}, not behind"
    fi
    f=$RUN/validator-$TARGET.restart.log
    if [[ $cached_anchor == "$k" ]]; then
        cause="block $k was only in its header cache, so the backwards walk never passed it on"
    elif published_before_streamer "$f" "$k"; then
        cause="the backwards walk passed block $k on before the forward streamer started listening"
    else
        fail "chain stayed at block $k with validator $TARGET behind, for a reason this script does not recognise"
    fi
    log "BUG: validator $TARGET stayed at block $target_height and the chain at block $k for ${WATCH}s: $cause"
    grep -aE 'derived walk coverage|notifying watchers|stream handoff|starting backwards walk|catch-up idle' "$f" \
        | head -6 | sed -E 's/^ts=([^ ]+) .*message=/  \1 /' | tee -a "$RUN/summary.txt"

    if [[ -n $spares ]]; then
        for v in $spares; do start_node "$v" "$RUN/validator-$v.log"; done
        log "started $(range_name $((QUORUM + 1)) "$N")"
        # shellcheck disable=SC2086
        if wait_for 180 all_above "$k" $running; then
            log "chain moved on and validator $TARGET caught up: $(heights)"
        else
            log "chain did not move on after starting the other validators: $(heights)"
        fi
    fi
    return 1
}

mkdir -p "$WORKDIR"
result=2
for try in $(seq 1 "$ATTEMPTS"); do
    RUN=$WORKDIR/attempt-$try
    mkdir -p "$RUN"
    attempt; result=$?
    stop_all
    [[ $result -ne 3 ]] && break
done
[[ $result -eq 3 ]] && { log "INCONCLUSIVE: every attempt missed the window"; result=2; }
echo "logs and stopped datadirs: $RUN"
exit $result
