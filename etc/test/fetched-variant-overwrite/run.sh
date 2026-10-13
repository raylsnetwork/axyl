#!/usr/bin/env bash
#
# Live reproduction: the certificate fetcher's accept path overwrites a stored certificate with a
# peer-supplied valid variant (same digest, different signer subset). See README.md in this
# directory.
#
# Starts a local network of validators, arms a single validator to, on a target round, store its own
# canonical certificate through the guarded own/gossip path and then push a second valid certificate
# for the same header (a different 2f+1 signer subset) through the production fetch accept path
# (ProcessVerifiedCertificates -> CertificateManager::process_verified_certificates). That path has
# no "already stored" guard, so it overwrites the stored canonical last-writer-wins.
#
# The attacker behaviour is compiled only with the `byzantine-test-hooks` cargo feature and is armed
# per process by environment, so the one armed validator forges while the rest stay honest.
#
# Exit status: 0 the stored certificate was NOT overwritten (healthy; bug absent or fixed),
# 1 the stored certificate was overwritten through the fetch path (bug present, issue reproduced),
# 2 the run was inconclusive or could not start.

set -uo pipefail

usage() {
    cat <<EOF
Usage: $0 [-n VALIDATORS] [-b BINARY] [-d WORKDIR] [-r ROUND] [-e EPOCH_SECS] [-w WATCH_SECS] [-i INSTANCE_BASE]

  -n  validators in the committee, at least 4 (default 4)
  -b  node binary built with --features byzantine-test-hooks
      (default: build target/debug/rayls-network from this checkout with the feature)
  -d  directory for datadirs and logs (default: target/fetched-variant-overwrite/<timestamp>)
  -r  epoch-1 round the armed node forges a variant for (default 6)
  -e  epoch duration in seconds; keep it short so epoch 1 is reached quickly (default 20)
  -w  seconds to wait for the armed node to forge and inject the variant (default 240)
  -i  first --instance number, which sets the RPC ports (default 50)
EOF
    exit 2
}

N=4
BIN=""
WORKDIR=""
ROUND=6
EPOCH_SECS=20
WATCH=240
INSTANCE_BASE=50
while getopts "n:b:d:r:e:w:i:h" opt; do
    case "$opt" in
        n) N=$OPTARG ;;
        b) BIN=$OPTARG ;;
        d) WORKDIR=$OPTARG ;;
        r) ROUND=$OPTARG ;;
        e) EPOCH_SECS=$OPTARG ;;
        w) WATCH=$OPTARG ;;
        i) INSTANCE_BASE=$OPTARG ;;
        *) usage ;;
    esac
done
[[ $N =~ ^[0-9]+$ && $N -ge 4 ]] || { echo "need at least 4 validators"; usage; }
for value in "$ROUND" "$EPOCH_SECS" "$WATCH" "$INSTANCE_BASE"; do
    [[ $value =~ ^[0-9]+$ ]] || { echo "not a number: $value"; usage; }
done

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
WORKDIR=${WORKDIR:-$REPO/target/fetched-variant-overwrite/$(date -u +%Y%m%dT%H%M%SZ)}
if [[ -d $WORKDIR && -n $(ls -A "$WORKDIR" 2>/dev/null) ]]; then
    echo "not empty: $WORKDIR"; exit 2
fi
RUN=$WORKDIR

# The armed node is validator 1. A healthy committee member for comparison is validator N.
ATTACKER=1
HEALTHY=$N
CHAIN_ID=0x1e7
DEV_ACCOUNT=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
export RL_BLS_PASSPHRASE="fetched-variant-overwrite-local-only"

declare -A PIDS
log() { echo "$(date -u +%T) $*" | tee -a "$RUN/summary.txt"; }
fail() { log "INCONCLUSIVE: $*"; echo "logs and datadirs: $RUN"; exit 2; }

command -v curl >/dev/null || { echo "missing required tool: curl"; exit 2; }

if [[ -z $BIN ]]; then
    echo "building rayls-network (debug) with --features byzantine-test-hooks"
    (cd "$REPO" && cargo build -p rayls-network --features byzantine-test-hooks) || exit 2
    BIN=$REPO/target/debug/rayls-network
fi
[[ -x $BIN ]] || { echo "not executable: $BIN"; exit 2; }

rpc_port() { echo $((8546 - (INSTANCE_BASE + $1))); }

# eth_blockNumber as a decimal, empty when the node does not answer.
height() {
    local out
    out=$(curl -s -m 2 -H 'content-type: application/json' \
        -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' \
        "http://127.0.0.1:$(rpc_port "$1")" 2>/dev/null) || return 0
    [[ $out =~ \"result\":\"0x([0-9a-fA-F]+)\" ]] && echo $((16#${BASH_REMATCH[1]}))
}

max_height() {
    local best=-1 h v
    for v in $(seq 1 "$N"); do h=$(height "$v"); [[ -n $h && $h -gt $best ]] && best=$h; done
    echo "$best"
}

node_args() {
    local seq=$1
    NODE_ARGS=(node --datadir "$RUN/validator-$seq" --network local
        --instance $((INSTANCE_BASE + seq)) --log.stdout.format log-fmt
        --log.stdout.filter "info,primary::byzantine=info" --full --storage.v2
        --db.growth-step 1MB --consensus-db.growth-step 1MB --txpool.minimal-protocol-fee 0
        --gpo.default-suggested-fee 0 -vvv --http --http.addr 127.0.0.1 --http.api all)
}

start_validator() {
    local seq=$1 logfile=$2
    node_args "$seq"
    if [[ $seq -eq $ATTACKER ]]; then
        RL_FVO_ROUND="$ROUND" "$BIN" "${NODE_ARGS[@]}" >>"$logfile" 2>&1 &
    else
        "$BIN" "${NODE_ARGS[@]}" >>"$logfile" 2>&1 &
    fi
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

setup_network() {
    rm -rf "$RUN"; mkdir -p "$RUN/genesis/validators"
    local seq
    for seq in $(seq 1 "$N"); do
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
    "$BIN" genesis --datadir "$RUN" --chain-id $CHAIN_ID --epoch-duration-in-secs "$EPOCH_SECS" \
        --dev-funded-account $DEV_ACCOUNT --max-header-delay-ms 1000 --min-header-delay-ms 500 \
        --consensus-registry-owner $DEV_ACCOUNT --network-admin $DEV_ACCOUNT >/dev/null 2>&1 || return 1
    for seq in $(seq 1 "$N"); do
        mkdir -p "$RUN/validator-$seq/genesis"
        cp "$RUN/genesis/genesis.yaml" "$RUN/genesis/committee.yaml" "$RUN/validator-$seq/genesis/"
        cp "$RUN/parameters.yaml" "$RUN/validator-$seq/"
    done
}

# Pull the roaring bitmap contents (e.g. "0,1,2,3") out of a stored_before=/stored_after= field.
# The log-fmt line renders the value as: <field>="Some(\"RoaringBitmap<[0, 1, 2, 3]>\")", so take
# the first [...] after the field name and drop the spaces. $1 field name, $2 log line.
bitmap() {
    sed -n "s/.*$1=[^[]*\[\([0-9, ]*\)\].*/\1/p" <<<"$2" | head -1 | tr -d ' '
}

# --- run -------------------------------------------------------------------------------------

setup_network || fail "network setup failed"
log "network: $N validators, chain $CHAIN_ID, epoch ${EPOCH_SECS}s; armed node is validator $ATTACKER (RL_FVO_ROUND=$ROUND, epoch>=1)"

for seq in $(seq 1 "$N"); do start_validator "$seq" "$RUN/validator-$seq.log"; done
sleep 5
for seq in $(seq 1 "$N"); do
    kill -0 "${PIDS[$seq]}" 2>/dev/null || fail "validator $seq did not start; see its log"
done
log "all $N validators started; waiting up to ${WATCH}s for the armed node to forge and inject a variant"

ATTACKER_LOG="$RUN/validator-$ATTACKER.log"
INJECT=""; waited=0
while [[ $waited -lt $WATCH ]]; do
    sleep 5; waited=$((waited + 5))
    INJECT=$(grep -a 'RL_FVO injected variant through the fetch accept path' "$ATTACKER_LOG" 2>/dev/null \
        | sed 's/\x1b\[[0-9;]*m//g' | head -1)
    [[ -n $INJECT ]] && break
    [[ $((waited % 20)) -eq 0 ]] && log "waiting: validators at $(max_height), no injection yet (${waited}s)"
done

[[ -n $INJECT ]] || fail "armed node did not forge/inject within ${WATCH}s (round $ROUND in epoch>=1 may not have been reached; try a lower -r or longer -w)"

BEFORE=$(bitmap stored_before "$INJECT")
AFTER=$(bitmap stored_after "$INJECT")
ACCEPT=$(sed -n 's/.*accept=\(Ok([^ ]*)\).*/\1/p' <<<"$INJECT" | head -1)
log "armed node injected a variant through the fetch accept path:"
log "  accept=$ACCEPT  stored_before=[$BEFORE]  stored_after=[$AFTER]"

[[ -n $BEFORE && -n $AFTER ]] || fail "could not parse stored signer sets from the injection log line"

# Liveness check: the overwrite must not halt the network. Confirm blocks still advance.
H1=$(max_height); sleep 10; H2=$(max_height)
log "liveness: max block $H1 -> $H2 after the overwrite"

if [[ $BEFORE != "$AFTER" ]]; then
    log "ISSUE REPRODUCED: the stored certificate was overwritten through the fetch accept path"
    log "  signer set changed [$BEFORE] -> [$AFTER] for the same certificate digest, accept=$ACCEPT"
    log "  node $HEALTHY (honest) still holds the canonical [$BEFORE] for that digest"
    [[ $H2 -gt $H1 ]] && log "  network stayed live (no halt): blocks advanced $H1 -> $H2" \
                      || log "  WARNING: blocks did not advance in the 10s sample ($H1 -> $H2); inspect logs"
    echo "logs and datadirs: $RUN"
    log "cross-check (optional): stop the nodes and compare the stored cert across validator-*/consensus-db with rayls-db-inspect"
    exit 1
fi

log "HEALTHY: the fetch accept path did NOT overwrite the stored certificate (stored_before == stored_after)"
log "  the variant was built and submitted but rejected/ignored; bug absent or fixed"
echo "logs and datadirs: $RUN"
exit 0
