#!/usr/bin/env bash
#
# Live reproduction: a peer floods the transaction gossip topic with oversized messages that
# validators accept, forward, and process without penalty. See README.md in this directory.
#
# Starts a local committee, lets it reach steady block production, records a baseline, then arms
# one node to publish a roughly 2 MiB message on the transaction topic (`rayls-batch`) in a tight
# loop with no rate limit. It measures per-node CPU, loopback bandwidth, and block production during
# the flood, and checks each validator's log for any penalty, ban, or score change against the
# attacker.
#
# The flood behaviour is compiled only with the `txn-flood-hook` cargo feature and is armed
# per process by environment, so the one armed node floods while the rest stay honest.
#
# Exit status: 0 the flood had no measurable effect (healthy; bug absent or fixed),
# 1 the flood was accepted, forwarded, and processed with no penalty and a measurable cost
#   (bug present, issue reproduced),
# 2 the run was inconclusive or could not start.

set -uo pipefail

usage() {
    cat <<EOF
Usage: $0 [-n VALIDATORS] [-a ATTACKER] [-x] [-p PAYLOAD] [-b BINARY] [-d WORKDIR] [-w WINDOW] [-r RATE_MS] [-i INSTANCE_BASE]

  -n  validators in the committee, at least 4 (default 5)
  -a  attacker: validator (the last committee validator floods) or observer (an extra,
      non-registered observer node joins and floods) (default validator)
  -x  observer attacker only: validators 2..N are held at their peer limit by the committee
      alone, so they refuse the observer's connections and it reaches only validator 1 itself
  -p  flood payload: txns (valid signed txns, CPU path) or filler (undecodable, bandwidth path)
      (default txns)
  -b  node binary built with --features txn-flood-hook
      (default: build target/debug/rayls-network from this checkout with the feature)
  -d  directory for datadirs and logs (default: target/rogue-member-flood/<timestamp>)
  -w  seconds to measure each of the baseline and flood windows (default 60)
  -r  milliseconds between flood publishes (default 0, i.e. as fast as possible)
  -i  first --instance number, which sets the RPC ports (default 40)
EOF
    exit 2
}

N=5
ATTACKER_KIND=validator
ISOLATE=0
PAYLOAD=txns
BIN=""
WORKDIR=""
WINDOW=60
RATE_MS=0
INSTANCE_BASE=40
while getopts "n:a:xp:b:d:w:r:i:h" opt; do
    case "$opt" in
        n) N=$OPTARG ;;
        a) ATTACKER_KIND=$OPTARG ;;
        x) ISOLATE=1 ;;
        p) PAYLOAD=$OPTARG ;;
        b) BIN=$OPTARG ;;
        d) WORKDIR=$OPTARG ;;
        w) WINDOW=$OPTARG ;;
        r) RATE_MS=$OPTARG ;;
        i) INSTANCE_BASE=$OPTARG ;;
        *) usage ;;
    esac
done
[[ $N =~ ^[0-9]+$ && $N -ge 4 ]] || { echo "need at least 4 validators"; usage; }
[[ $PAYLOAD == txns || $PAYLOAD == filler ]] || { echo "payload must be txns or filler"; usage; }
[[ $ATTACKER_KIND == validator || $ATTACKER_KIND == observer ]] \
    || { echo "attacker must be validator or observer"; usage; }
[[ $ISOLATE -eq 0 || $ATTACKER_KIND == observer ]] || { echo "-x needs -a observer"; usage; }
for value in "$WINDOW" "$RATE_MS" "$INSTANCE_BASE"; do
    [[ $value =~ ^[0-9]+$ ]] || { echo "not a number: $value"; usage; }
done

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
WORKDIR=${WORKDIR:-$REPO/target/rogue-member-flood/$(date -u +%Y%m%dT%H%M%SZ)}
RUN=$WORKDIR

# Nodes 1..N are the committee validators. In observer mode node N+1 is the attacking observer: it
# is not in the registry, the committee, or any trusted list.
if [[ $ATTACKER_KIND == observer ]]; then TOTAL=$((N + 1)); else TOTAL=$N; fi
ATTACKER=$TOTAL
WARMUP_BLOCKS=20
CHAIN_ID=0x1e7
EPOCH_SECS=3600
DEV_ACCOUNT=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
CLK_TCK=$(getconf CLK_TCK)
export RL_BLS_PASSPHRASE="rogue-member-flood-local-only"

declare -A PIDS
log() { echo "$(date -u +%T) $*" | tee -a "$RUN/summary.txt"; }
fail() { log "INCONCLUSIVE: $*"; echo "logs and datadirs: $RUN"; exit 2; }

command -v curl >/dev/null || { echo "missing required tool: curl"; exit 2; }

if [[ -z $BIN ]]; then
    echo "building rayls-network (debug) with --features txn-flood-hook"
    (cd "$REPO" && flock /tmp/axyl-cargo.lock nice -n 19 cargo build -p rayls-network \
        --features txn-flood-hook -j 4) || exit 2
    BIN=$REPO/target/debug/rayls-network
fi
[[ -x $BIN ]] || { echo "not executable: $BIN"; exit 2; }

rpc_port() { echo $((8546 - (INSTANCE_BASE + $1))); }

height() {
    local out
    out=$(curl -s -m 2 -H 'content-type: application/json' \
        -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
        "http://127.0.0.1:$(rpc_port "$1")" 2>/dev/null) || return 0
    [[ $out =~ \"result\":\"0x([0-9a-fA-F]+)\" ]] && echo $((16#${BASH_REMATCH[1]}))
}

max_val_height() {
    local best=-1 h v
    for v in $(seq 1 "$N"); do h=$(height "$v"); [[ -n $h && $h -gt $best ]] && best=$h; done
    echo "$best"
}

# CPU ticks (utime+stime) consumed by a pid so far, from /proc/<pid>/stat field 14 and 15.
cpu_ticks() {
    local pid=$1 stat
    stat=$(cat "/proc/$pid/stat" 2>/dev/null) || { echo 0; return; }
    # Fields after the (comm) parenthesis: strip up to and including ") ".
    local rest=${stat#*) }
    # shellcheck disable=SC2206
    local f=($rest)
    # rest[0] is field 3 (state); utime is field 14 => index 11, stime field 15 => index 12.
    echo $(( ${f[11]:-0} + ${f[12]:-0} ))
}

# Resident set size of a pid in MiB, from /proc/<pid>/status VmRSS.
rss_mb() {
    awk '/^VmRSS:/ {printf "%.0f", $2/1024; exit}' "/proc/$1/status" 2>/dev/null
}

# Total bytes received on the loopback interface, which carries all local node traffic.
lo_rx_bytes() {
    awk '/lo:/ {sub(/^.*lo:[[:space:]]*/, ""); print $1; exit}' /proc/net/dev 2>/dev/null
}

is_observer() { [[ $1 -gt $N ]]; }
node_dir() { if is_observer "$1"; then echo "$RUN/observer"; else echo "$RUN/validator-$1"; fi; }
node_log() { if is_observer "$1"; then echo "$RUN/observer.log"; else echo "$RUN/validator-$1.log"; fi; }
node_name() { if is_observer "$1"; then echo "observer"; else echo "validator-$1"; fi; }

# A UDP port nothing on this host is bound to, at or above $1.
free_udp_port() {
    local p
    for p in $(seq "$1" $(($1 + 999))); do
        ss -Hlun "sport = :$p" 2>/dev/null | grep -q . || { echo "$p"; return 0; }
    done
    return 1
}

node_args() {
    local seq=$1
    NODE_ARGS=(node --datadir "$(node_dir "$seq")" --network local
        --instance $((INSTANCE_BASE + seq)) --log.stdout.format log-fmt --full --storage.v2
        --db.growth-step 1MB --consensus-db.growth-step 1MB --txpool.minimal-protocol-fee 0
        --gpo.default-suggested-fee 0 -vvv --http --http.addr 127.0.0.1 --http.api all)
    if [[ $ATTACKER_KIND == observer ]]; then
        # Shows, per validator, the attacker's messages it forwarded but could not attribute.
        NODE_ARGS+=(--log.stdout.filter "network::gossip=debug")
    fi
    if is_observer "$seq"; then NODE_ARGS+=(--observer); fi
}

start_node() {
    local seq=$1 logfile=$2 flood=${3:-no}
    local env_args=()
    node_args "$seq"
    if is_observer "$seq"; then
        # The observer advertises only its identity, so validators cannot dial it: every
        # connection it has is one it opened.
        env_args+=(PRIMARY_LISTENER_MULTIADDR="/ip4/127.0.0.1/udp/$OBS_PRIMARY_PORT/quic-v1"
            WORKER_LISTENER_MULTIADDR="/ip4/127.0.0.1/udp/$OBS_WORKER_PORT/quic-v1")
    fi
    if [[ $flood == flood ]]; then
        env_args+=(RL_FLOOD=1 RL_FLOOD_PAYLOAD="$RUN/payloads/$PAYLOAD.bin"
            RL_FLOOD_INTERVAL_MS="$RATE_MS" RL_FLOOD_WARMUP_SECS=2)
    fi
    env "${env_args[@]}" "$BIN" "${NODE_ARGS[@]}" >>"$logfile" 2>&1 &
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
val_reached() { [[ $(max_val_height) -ge $1 ]]; }
node_reached() { local h; h=$(height "$1"); [[ -n $h && $h -ge $2 ]]; }

generate_payloads() {
    mkdir -p "$RUN/payloads"
    if [[ -f "$RUN/payloads/$PAYLOAD.bin" ]]; then return 0; fi
    # Reuse prebuilt payloads when RL_PREBUILT_PAYLOADS points at a directory holding them.
    if [[ -n ${RL_PREBUILT_PAYLOADS:-} && -f "$RL_PREBUILT_PAYLOADS/$PAYLOAD.bin" ]]; then
        cp "$RL_PREBUILT_PAYLOADS"/*.bin "$RUN/payloads/" 2>/dev/null
        log "reusing prebuilt payload $PAYLOAD.bin ($(stat -c%s "$RUN/payloads/$PAYLOAD.bin") bytes)"
        return 0
    fi
    log "generating flood payloads (filler.bin, txns.bin)"
    (cd "$REPO" && RL_GEN_OUT="$RUN/payloads" flock /tmp/axyl-cargo.lock nice -n 19 \
        cargo test -p rayls-consensus-worker --lib -j 4 -- \
        --ignored --exact network::tests::generate_flood_payloads >>"$RUN/gen.log" 2>&1) \
        || { cat "$RUN/gen.log" | tail -20; return 1; }
    [[ -f "$RUN/payloads/$PAYLOAD.bin" ]] || return 1
    log "payload $PAYLOAD.bin is $(stat -c%s "$RUN/payloads/$PAYLOAD.bin") bytes"
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
    "$BIN" genesis --datadir "$RUN" --chain-id $CHAIN_ID --epoch-duration-in-secs $EPOCH_SECS \
        --dev-funded-account $DEV_ACCOUNT --max-header-delay-ms 1000 --min-header-delay-ms 500 \
        --consensus-registry-owner $DEV_ACCOUNT --network-admin $DEV_ACCOUNT >/dev/null 2>&1 || return 1
    for seq in $(seq 1 "$N"); do
        mkdir -p "$RUN/validator-$seq/genesis"
        cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$RUN/validator-$seq/genesis/"
        cp "$RUN/parameters.yaml" "$RUN/validator-$seq/"
    done
    if [[ $ATTACKER_KIND == observer ]]; then
        local obs=$RUN/observer
        "$BIN" keytool generate observer --datadir "$obs" \
            --address 0x0000000000000000000000000000000000000000 --advertise-identity-only \
            >/dev/null 2>&1 || return 1
        mkdir -p "$obs/genesis"
        cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$obs/genesis/"
        cp "$RUN/parameters.yaml" "$obs/"
        OBS_PRIMARY_PORT=$(free_udp_port 47000) || return 1
        OBS_WORKER_PORT=$(free_udp_port $((OBS_PRIMARY_PORT + 1))) || return 1
    fi
}

# Measure a window: samples CPU per validator and loopback bytes, and the block-production rate.
# Writes a "$1" labelled block into summary.txt and sets globals RATE_$1 and CPU_ATT_$1.
measure_window() {
    local label=$1 secs=$2
    local h0 h1 lo0 lo1 seq
    declare -A t0 t1
    h0=$(max_val_height); lo0=$(lo_rx_bytes)
    for seq in $(seq 1 "$TOTAL"); do t0[$seq]=$(cpu_ticks "${PIDS[$seq]}"); done
    sleep "$secs"
    h1=$(max_val_height); lo1=$(lo_rx_bytes)
    for seq in $(seq 1 "$TOTAL"); do t1[$seq]=$(cpu_ticks "${PIDS[$seq]}"); done

    local blocks=$((h1 - h0))
    local rate; rate=$(awk "BEGIN{printf \"%.3f\", $blocks/$secs}")
    local lo_mb; lo_mb=$(awk "BEGIN{printf \"%.1f\", ($lo1-$lo0)/1048576}")
    log "[$label] blocks $h0->$h1 = $blocks in ${secs}s (${rate} blk/s); loopback rx ${lo_mb} MiB"
    local max_cpu=0 max_seq=0
    for seq in $(seq 1 "$TOTAL"); do
        local dt=$(( ${t1[$seq]} - ${t0[$seq]} ))
        local pct; pct=$(awk "BEGIN{printf \"%.0f\", $dt*100.0/$CLK_TCK/$secs}")
        local who; who=$(node_name "$seq"); [[ $seq -eq $ATTACKER ]] && who="$who(attacker)"
        local rss; rss=$(rss_mb "${PIDS[$seq]}")
        log "[$label]   $who CPU ${pct}% (${dt} ticks), RSS ${rss:-?} MiB"
        if [[ $seq -ne $ATTACKER && $pct -gt $max_cpu ]]; then max_cpu=$pct; max_seq=$seq; fi
    done
    printf -v "RATE_$label" '%s' "$rate"
    printf -v "VICTIM_CPU_$label" '%s' "$max_cpu"
    printf -v "VICTIM_SEQ_$label" '%s' "$max_seq"
    printf -v "LO_$label" '%s' "$lo_mb"
}

# --- run -------------------------------------------------------------------------------------

setup_network || fail "network setup failed"
generate_payloads || fail "payload generation failed"
if [[ $ATTACKER_KIND == observer ]]; then
    log "network: $N validators; attacker is a non-registered observer$([[ $ISOLATE -eq 1 ]] && echo " (isolated: only validator 1 accepts its connections)"); payload=$PAYLOAD rate=${RATE_MS}ms"
else
    log "network: $N validators; attacker is validator $ATTACKER; payload=$PAYLOAD rate=${RATE_MS}ms"
fi

for seq in $(seq 1 "$N"); do start_node "$seq" "$(node_log "$seq")"; done
sleep 5

if [[ $ISOLATE -eq 1 ]]; then
    # A node writes its network config on its first start. Lower the target peer count of
    # validators 2..N so their peer limit, ceil(target * (1 + excess factor)), is at most N-1:
    # the other committee members, which are always admitted, fill it, so the observer is refused
    # (and would be pruned, as a non-validator over target). Then restart them on the new config.
    # Stop only past block 10: a `--full` node stopped earlier (mid-initialization, or before
    # block 5) can fail to restart with a storage consistency panic.
    wait_for 120 val_reached 10 || fail "chain did not reach block 10 before the isolate restart"
    for seq in $(seq 1 "$N"); do
        [[ -f "$RUN/validator-$seq/network-config" ]] || fail "validator $seq wrote no network config"
    done
    stop_all
    for seq in $(seq 2 "$N"); do
        cfg=$RUN/validator-$seq/network-config
        excess=$(awk '/^  peer_excess_factor:/ {print $2}' "$cfg")
        target=$(awk "BEGIN{t=int(($N - 1) / (1 + $excess)); while (t > 1 && int(t * (1 + $excess) + 0.999999) > $N - 1) t--; print t}")
        sed -i "s/^  target_num_peers: .*/  target_num_peers: $target/" "$cfg"
        grep -q "^  target_num_peers: $target\$" "$cfg" || fail "could not cap validator $seq"
    done
    log "isolate: validators 2..$N capped at target peer count $target; they refuse the observer's connections"
    for seq in $(seq 1 "$N"); do start_node "$seq" "$(node_log "$seq")"; done
    sleep 5
    for seq in $(seq 1 "$N"); do
        kill -0 "${PIDS[$seq]}" 2>/dev/null || fail "validator $seq did not restart; see its log"
    done
fi
for seq in $(seq 1 "$N"); do
    kill -0 "${PIDS[$seq]}" 2>/dev/null || fail "validator $seq did not start; see its log"
done

wait_for 300 val_reached "$WARMUP_BLOCKS" || fail "chain did not reach block $WARMUP_BLOCKS"
log "warm-up done: validators at $(max_val_height)"

if [[ $ATTACKER_KIND == observer ]]; then
    # Join the observer unarmed and let it follow the chain, so the baseline includes it.
    start_node "$ATTACKER" "$(node_log "$ATTACKER")"
    wait_for 300 node_reached "$ATTACKER" "$(max_val_height)" \
        || fail "observer did not catch up; see $(node_log "$ATTACKER")"
    log "observer joined and following at $(height "$ATTACKER")"
fi

# Baseline window with no flood.
measure_window baseline "$WINDOW"

# Restart the attacker armed to flood, so it rejoins and begins publishing.
stop_pid "${PIDS[$ATTACKER]}"; unset "PIDS[$ATTACKER]"
: >"$(node_log "$ATTACKER")"
start_node "$ATTACKER" "$(node_log "$ATTACKER")" flood
log "attacker ($(node_name "$ATTACKER")) restarted and armed to flood the transaction topic"
sleep 20  # let the attacker rejoin the mesh and the flood reach steady state

# Flood window.
measure_window flood "$WINDOW"

# Accountability: look for any applied penalty, ban, or graylist against a peer in victim logs.
# Peer scoring is off and committee members are never banned, so we expect none.
PENALTIES=0
for seq in $(seq 1 "$N"); do
    [[ $seq -eq $ATTACKER ]] && continue
    c=$(grep -aiE 'penaliz|banning peer|ban peer|banned peer|graylist|blacklist' \
        "$RUN/validator-$seq.log" 2>/dev/null | wc -l)
    PENALTIES=$((PENALTIES + c))
done
if [[ $ATTACKER_KIND == observer ]]; then
    # Per validator: its sessions with the observer (either direction; the observer's own record
    # binds its key on each), how many of those the validator dialed itself, and the observer's
    # messages it forwarded but dropped for lack of a BLS mapping (logged, no penalty).
    obs_ids=$(grep -o '12D3KooW[A-Za-z0-9]*' "$RUN/observer/node-info.yaml" | paste -sd '|')
    if [[ -z $obs_ids ]]; then
        log "observer peer ids not found in its node-info.yaml; skipping the per-validator report"
    fi
    for seq in $(seq 1 "$N"); do
        [[ -n $obs_ids ]] || break
        vlog=$RUN/validator-$seq.log
        sessions=$(grep -a 'peer CONNECTED' "$vlog" | grep -a -c -E "$obs_ids")
        dialed=$(grep -a 'new connection established' "$vlog" | grep -a 'Dialer' \
            | grep -a -c -E "$obs_ids")
        dropped=$(grep -a 'no BLS mapping for propagation source' "$vlog" \
            | grep -a -c 'topic=rayls-batch')
        log "validator-$seq: sessions with the observer $sessions (dialed by the validator $dialed); its messages dropped unattributed $dropped"
    done
fi
FLOOD_SENT=$(grep -ac 'TXN FLOOD progress' "$(node_log "$ATTACKER")" 2>/dev/null)
FLOOD_SENT=${FLOOD_SENT:-0}
log "attacker flood progress log lines: $FLOOD_SENT; applied penalty/ban lines in victim logs: $PENALTIES"

# --- verdict ---------------------------------------------------------------------------------

rate_base=$(eval echo "\$RATE_baseline"); rate_flood=$(eval echo "\$RATE_flood")
victim_cpu_base=$(eval echo "\$VICTIM_CPU_baseline"); victim_cpu_flood=$(eval echo "\$VICTIM_CPU_flood")
victim_seq=$(eval echo "\$VICTIM_SEQ_flood")
lo_base=$(eval echo "\$LO_baseline"); lo_flood=$(eval echo "\$LO_flood")

log "SUMMARY ($PAYLOAD): block rate ${rate_base} -> ${rate_flood} blk/s; victim(validator $victim_seq) CPU ${victim_cpu_base}% -> ${victim_cpu_flood}%; loopback ${lo_base} -> ${lo_flood} MiB; penalties $PENALTIES"

# The flood must actually be running (progress lines) for the run to be conclusive.
if [[ ${FLOOD_SENT:-0} -eq 0 ]]; then
    fail "attacker never reported flood progress; the hook may not be armed (check RL_FLOOD and the feature build)"
fi

cpu_rose=$(awk "BEGIN{print ($victim_cpu_flood > $victim_cpu_base + 20) ? 1 : 0}")
bw_rose=$(awk "BEGIN{print ($lo_flood > $lo_base * 2) ? 1 : 0}")
rate_fell=$(awk "BEGIN{print ($rate_flood < $rate_base * 0.8) ? 1 : 0}")

if [[ $PENALTIES -eq 0 && ( $cpu_rose -eq 1 || $bw_rose -eq 1 || $rate_fell -eq 1 ) ]]; then
    log "ISSUE REPRODUCED: the flood was accepted, forwarded, and processed with no penalty"
    [[ $cpu_rose -eq 1 ]] && log "  victim validator CPU rose from ${victim_cpu_base}% to ${victim_cpu_flood}% under attacker-chosen content"
    [[ $bw_rose -eq 1 ]] && log "  loopback traffic rose from ${lo_base} to ${lo_flood} MiB (mesh forwarding amplification)"
    [[ $rate_fell -eq 1 ]] && log "  block production fell from ${rate_base} to ${rate_flood} blk/s"
    echo "logs and datadirs: $RUN"
    exit 1
fi

if [[ $PENALTIES -gt 0 ]]; then
    log "HEALTHY: the flooding peer was penalized ($PENALTIES lines); see victim logs"
else
    log "HEALTHY or INCONCLUSIVE: no measurable CPU, bandwidth, or block-rate effect from the flood"
fi
echo "logs and datadirs: $RUN"
exit 0
