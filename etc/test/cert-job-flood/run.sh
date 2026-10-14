#!/usr/bin/env bash
#
# Live reproduction: a single connected peer floods a validator with ordinary, valid
# missing-certificates requests and makes its storage jobs pile up. See README.md here.
#
# Starts a local network of validators plus one observer, warms up the chain, then arms one
# validator to send valid missing-certificates requests to some of its peers on many concurrent
# streams. It samples each validator's peak memory and checks that the chain keeps producing
# blocks, so the cost of the pile-up on the targeted nodes is visible against the untargeted ones.
#
# The load behaviour is compiled only with the `cert-job-flood-hook` cargo feature and is armed
# per process by environment, so the one armed validator generates load while the rest stay honest.
#
# Exit status: 0 targeted nodes stayed close to the others (healthy; bug absent or fixed),
# 1 targeted nodes used far more memory than the others (bug present, issue reproduced),
# 2 the run was inconclusive or could not start.

set -uo pipefail

usage() {
    cat <<EOF
Usage: $0 [-n VALIDATORS] [-b BINARY] [-d WORKDIR] [-w FLOOD_SECS] [-i INSTANCE_BASE]

  -n  validators in the committee, at least 5 (default 5)
  -b  node binary built with --features cert-job-flood-hook
      (default: build target/debug/rayls-network from this checkout with the feature)
  -d  directory for datadirs and logs (default: target/cert-job-flood/<timestamp>)
  -w  seconds to run the flood while sampling memory (default 120)
  -i  first --instance number, which sets the RPC ports (default 40)
EOF
    exit 2
}

N=5; BIN=""; WORKDIR=""; FLOOD_SECS=180; INSTANCE_BASE=70
while getopts "n:b:d:w:i:h" opt; do
    case "$opt" in
        n) N=$OPTARG ;; b) BIN=$OPTARG ;; d) WORKDIR=$OPTARG ;;
        w) FLOOD_SECS=$OPTARG ;; i) INSTANCE_BASE=$OPTARG ;; *) usage ;;
    esac
done
[[ $N =~ ^[0-9]+$ && $N -ge 5 ]] || { echo "need at least 5 validators"; usage; }
for v in "$FLOOD_SECS" "$INSTANCE_BASE"; do
    [[ $v =~ ^[0-9]+$ ]] || { echo "not a number: $v"; usage; }
done

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
WORKDIR=${WORKDIR:-$REPO/target/cert-job-flood/$(date -u +%Y%m%dT%H%M%SZ)}
RUN=$WORKDIR

# The last validator is the victim, the one before it is the control, and the rest are flooders
# that all target the victim at once. A single peer is not enough: libp2p caps the concurrent
# requests one connection can have in flight, so the jobs only pile up when several peers add to
# them together.
VICTIM=$N
CONTROL=$((N - 1))
N_OBS=8
STREAMS=64
GIVEUP_MS=50
WARMUP_BLOCKS=20
CHAIN_ID=0x1e7
EPOCH_SECS=3600
DEV_ACCOUNT=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
# A targeted node counts as reproduced if its peak memory is both this multiple of the best
# untargeted node and this many MiB above it.
GROWTH_RATIO=150   # percent
GROWTH_MIB=150
export RL_BLS_PASSPHRASE="cert-job-flood-local-only"

declare -A PIDS; declare -A OBS_PIDS
log() { echo "$(date -u +%T) $*" | tee -a "$RUN/summary.txt"; }
fail() { log "INCONCLUSIVE: $*"; echo "logs and datadirs: $RUN"; exit 2; }
command -v curl >/dev/null || { echo "missing required tool: curl"; exit 2; }

if [[ -z $BIN ]]; then
    echo "building rayls-network (debug) with --features cert-job-flood-hook"
    (cd "$REPO" && cargo build -p rayls-network --features cert-job-flood-hook) || exit 2
    BIN=$REPO/target/debug/rayls-network
fi
[[ -x $BIN ]] || { echo "not executable: $BIN"; exit 2; }

rpc_port() { if [[ $1 == obs* ]]; then echo $((8546 - (INSTANCE_BASE + N + ${1#obs})))
    else echo $((8546 - (INSTANCE_BASE + $1))); fi; }
height() {
    local out
    out=$(curl -s -m 2 -H 'content-type: application/json' \
        -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
        "http://127.0.0.1:$(rpc_port "$1")" 2>/dev/null) || return 0
    [[ $out =~ \"result\":\"0x([0-9a-fA-F]+)\" ]] && echo $((16#${BASH_REMATCH[1]}))
}
max_val_height() { local best=-1 h v
    for v in $(seq 1 "$N"); do h=$(height "$v"); [[ -n $h && $h -gt $best ]] && best=$h; done; echo "$best"; }
# Peak resident memory (VmHWM) of a pid, in MiB; empty if gone.
peak_mib() { local kib; kib=$(awk '/^VmHWM:/{print $2}' "/proc/$1/status" 2>/dev/null) || return 0
    [[ -n $kib ]] && echo $((kib / 1024)); }

node_args() { local seq=$1
    NODE_ARGS=(node --datadir "$RUN/validator-$seq" --network local
        --instance $((INSTANCE_BASE + seq)) --log.stdout.format log-fmt --full --storage.v2
        --db.growth-step 1MB --consensus-db.growth-step 1MB --txpool.minimal-protocol-fee 0
        --gpo.default-suggested-fee 0 -vvv --http --http.addr 127.0.0.1 --http.api all); }
start_validator() { local seq=$1 logfile=$2; node_args "$seq"
    if [[ " ${ARMED_FLOODERS:-} " == *" $seq "* ]]; then
        RL_CJFLOOD_TARGETS=1 RL_CJFLOOD_TARGET_BLS="$VICT_BLS" RL_CJFLOOD_STREAMS="$STREAMS" \
            RL_CJFLOOD_GIVEUP_MS="$GIVEUP_MS" "$BIN" "${NODE_ARGS[@]}" >>"$logfile" 2>&1 &
    else "$BIN" "${NODE_ARGS[@]}" >>"$logfile" 2>&1 & fi
    PIDS[$seq]=$!; }
start_observer() { local idx=$1 arm=$2 logfile=$3
    local inst=$((INSTANCE_BASE + N + idx)) env=()
    [[ -n $arm ]] && env=(RL_CJFLOOD_TARGETS=1 RL_CJFLOOD_TARGET_BLS="$VICT_BLS"
        RL_CJFLOOD_STREAMS="$STREAMS" RL_CJFLOOD_GIVEUP_MS="$GIVEUP_MS")
    PRIMARY_LISTENER_MULTIADDR="/ip4/0.0.0.0/udp/$((49000 + inst))/quic-v1" \
    WORKER_LISTENER_MULTIADDR="/ip4/0.0.0.0/udp/$((49100 + inst))/quic-v1" \
        env "${env[@]}" "$BIN" node --datadir "$RUN/observer-$idx" --network local --observer \
        --instance "$inst" --log.stdout.format log-fmt --full --storage.v2 \
        --db.growth-step 1MB --consensus-db.growth-step 1MB --txpool.minimal-protocol-fee 0 \
        --gpo.default-suggested-fee 0 -vvv --http --http.addr 127.0.0.1 --http.api all \
        >>"$logfile" 2>&1 &
    OBS_PIDS[$idx]=$!; }
stop_pid() { local pid=$1; [[ -z $pid ]] && return 0; kill -TERM "$pid" 2>/dev/null
    for _ in $(seq 1 40); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
    kill -KILL "$pid" 2>/dev/null; return 0; }
stop_all() { local seq
    for seq in "${!PIDS[@]}"; do kill -TERM "${PIDS[$seq]}" 2>/dev/null; done
    for i in "${!OBS_PIDS[@]}"; do kill -TERM "${OBS_PIDS[$i]}" 2>/dev/null; done
    for seq in "${!PIDS[@]}"; do stop_pid "${PIDS[$seq]}"; done
    for i in "${!OBS_PIDS[@]}"; do stop_pid "${OBS_PIDS[$i]}"; done
    PIDS=(); OBS_PIDS=(); return 0; }
trap stop_all EXIT; trap 'exit 2' INT TERM
wait_for() { local secs=$1; shift; for _ in $(seq 1 "$secs"); do "$@" && return 0; sleep 1; done; return 1; }
val_reached() { [[ $(max_val_height) -ge $1 ]]; }

setup_network() { rm -rf "$RUN"; mkdir -p "$RUN/genesis/validators"; local seq port
    for seq in $(seq 1 "$N") $(seq 1 "$N_OBS" | sed "s/^/obs/"); do port=$(rpc_port "$seq")
        if (echo >"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
            fail "RPC port $port is in use; pick other instance numbers with -i"; fi; done
    for seq in $(seq 1 "$N"); do
        "$BIN" keytool generate validator --datadir "$RUN/validator-$seq" \
            --address "$(printf '0x%040x' $((0x5eed0000 + seq)))" >/dev/null 2>&1 || return 1
        cp "$RUN/validator-$seq/node-info.yaml" "$RUN/genesis/validators/validator-$seq.yaml"; done
    "$BIN" genesis --datadir "$RUN" --chain-id $CHAIN_ID --epoch-duration-in-secs $EPOCH_SECS \
        --dev-funded-account $DEV_ACCOUNT --max-header-delay-ms 1000 --min-header-delay-ms 500 \
        --consensus-registry-owner $DEV_ACCOUNT --network-admin $DEV_ACCOUNT >/dev/null 2>&1 || return 1
    for seq in $(seq 1 "$N"); do mkdir -p "$RUN/validator-$seq/genesis"
        cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$RUN/validator-$seq/genesis/"
        cp "$RUN/parameters.yaml" "$RUN/validator-$seq/"; done
    local i
    for i in $(seq 1 "$N_OBS"); do
        "$BIN" keytool generate observer --datadir "$RUN/observer-$i" \
            --address 0x0000000000000000000000000000000000000000 --advertise-identity-only >/dev/null 2>&1 || return 1
        mkdir -p "$RUN/observer-$i/genesis"
        cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$RUN/observer-$i/genesis/"
        cp "$RUN/parameters.yaml" "$RUN/observer-$i/"
    done; }

# --- run -------------------------------------------------------------------------------------
setup_network || fail "network setup failed"
log "network: $N validators + $N_OBS observers; victim=v$VICTIM control=v$CONTROL"
for seq in $(seq 1 "$N"); do start_validator "$seq" "$RUN/validator-$seq.log"; done
sleep 5
for seq in $(seq 1 "$N"); do kill -0 "${PIDS[$seq]}" 2>/dev/null || fail "validator $seq did not start"; done
wait_for 240 val_reached "$WARMUP_BLOCKS" || fail "chain did not reach block $WARMUP_BLOCKS"
log "warm-up done: validators at $(max_val_height)"

# The victim's BLS key, so every observer points at the same validator.
VICT_BLS=$(awk '/^bls_public_key:/{print $2}' "$RUN/validator-$VICTIM/node-info.yaml")
[[ -n $VICT_BLS ]] || fail "could not read victim BLS key"
log "victim is validator $VICTIM (bls ${VICT_BLS:0:12}...), control is validator $CONTROL, flooders are $N_OBS observers"

# Start the armed observers; they join, follow, and flood the victim.
for i in $(seq 1 "$N_OBS"); do start_observer "$i" armed "$RUN/observer-$i.log"; done
log "$N_OBS observers started and armed: $STREAMS streams each at validator $VICTIM, ${GIVEUP_MS}ms give-up"
sleep 10

# Sample peak memory while the flood runs.
START_H=$(max_val_height); waited=0
while [[ $waited -lt $FLOOD_SECS ]]; do sleep 10; waited=$((waited + 10))
    line="t=${waited}s height=$(max_val_height)"
    for seq in $(seq 1 "$N"); do line+=" v$seq=$(peak_mib "${PIDS[$seq]:-0}")MiB"; done
    log "$line"; done
END_H=$(max_val_height)

declare -A PEAK; for seq in $(seq 1 "$N"); do PEAK[$seq]=$(peak_mib "${PIDS[$seq]:-0}"); PEAK[$seq]=${PEAK[$seq]:-0}; done
log "peak memory (MiB): $(for seq in $(seq 1 "$N"); do printf 'v%s=%s ' "$seq" "${PEAK[$seq]}"; done)(victim=v$VICTIM control=v$CONTROL)"
log "chain moved from block $START_H to $END_H during the ${FLOOD_SECS}s flood"
vict=${PEAK[$VICTIM]}; ctrl=${PEAK[$CONTROL]}
reproduced=0
if [[ $vict -ge $((ctrl * GROWTH_RATIO / 100)) && $vict -ge $((ctrl + GROWTH_MIB)) ]]; then
    reproduced=1; log "  victim v$VICTIM peaked at ${vict} MiB vs ${ctrl} MiB for the control v$CONTROL"; fi
# The victim falling behind the chain while the control keeps up also shows the flood hurting it.
vh=$(height "$VICTIM"); ch=$(height "$CONTROL"); top=$(max_val_height)
log "final height: victim v$VICTIM=$vh control v$CONTROL=$ch max=$top"
if [[ -n $vh && -n $top && $((top - vh)) -ge 20 && -n $ch && $((top - ch)) -lt 10 ]]; then
    reproduced=1; log "  victim v$VICTIM fell to block $vh while the control v$CONTROL kept up at $ch (max $top)"; fi
if [[ $END_H -le $START_H ]]; then log "chain stopped producing blocks during the flood"; reproduced=1; fi

echo "logs and datadirs: $RUN"
if [[ $reproduced -eq 1 ]]; then
    log "ISSUE REPRODUCED: targeted validators used far more memory (jobs piled up with no limit)"
    exit 1
fi
log "HEALTHY: all validators stayed comparable and the chain kept producing (bug absent or fixed)"
exit 0
