#!/bin/bash
#
# Add one extra OBSERVER to an already-running testnet -- with NO relay in front of it.
#
# This is the typical observer setup: the observer reaches the committee the way an external client
# does. If the committee is advertised as /dnsaddr (--relay-dns), it resolves each validator's
# /dnsaddr against the local dnsmasq, follows the TXT records to the validators' relay circuits, and
# dials them THROUGH their relays. If the committee uses plain direct or concrete-circuit addresses,
# it just dials those. Either way the observer makes no reservation of its own (nobody dials it back
# -- gossip/req-res flow over the connections it opens), so it needs no relay.
#
# Difference from add-relay-node.sh: that stands up a relay in front of the node and bakes a
# /p2p-circuit advertise address (so it is reachable via its own relay and is stakeable into the
# committee). Here we want the plain "observer through the committee's relays" path.
#
# Runs with --observer, so the node is pinned out of the committee permanently (never counted toward
# quorum) -- injecting it into a live chain is safe and cannot wedge consensus.
#
# The network files (genesis.yaml, committee.yaml, parameters.yaml) identify the specific network
# this observer joins. They are NOT fabricated here: fetch them from a committee member and place
# them in the observer's datadir (paths printed below). The script only generates the observer's own
# node keys.
#
# Restart-safe: re-running with an existing datadir just (re)starts the node. So `add-observer.sh 7`
# both adds the observer the first time and brings it back after you kill it.
#
# Usage:
#   ./add-observer.sh <INDEX>                        # INDEX is required, must be unique across nodes
#   DNSMASQ_PORT=5354 ./add-observer.sh <INDEX>      # resolve committee via the PUBLIC relay view (MULTI_LISTEN)
#   DNSMASQ_HOST=10.0.0.5 ./add-observer.sh <INDEX>  # join from another host: point at that resolver's address
#   LISTEN_HOST=10.0.0.10 ./add-observer.sh <INDEX>  # bind p2p to this IP instead of 0.0.0.0 (default binds all ifaces)
#   DISABLE_PRUNING=1 ./add-observer.sh <INDEX>      # run as a full archive (no --full)

set -e

directory=$(dirname "${BASH_SOURCE[0]}")
scriptDir=$(cd "$directory" && pwd)
envPath="$scriptDir/.env"
[[ -e "$envPath" ]] || { echo "Error: .env not found at $envPath"; exit 1; }
. "$envPath"
export RL_BLS_PASSPHRASE="$RL_BLS_PASSPHRASE"
export RAYLS_NETWORK="$RAYLS_NETWORK"
cd "$scriptDir/../.."

NODE_NUM="$1"
[[ -n "$NODE_NUM" ]] || { echo "Error: INDEX required.  Usage: ./add-observer.sh <INDEX>  (e.g. ./add-observer.sh 7, must be unique across nodes)"; exit 1; }
BUILD_CONFIG="${BUILD_CONFIG:-release}"
LOG_LEVEL="${LOG_LEVEL:-vvv}"
# Local dnsmasq the observer points RAYLS_DNS_SERVER at, to resolve the committee's /dnsaddr records.
# A plain --relay-dns network serves everything on 5353; a MULTI_LISTEN network's PUBLIC (relay) view
# is on 5354 -- an outsider observer wants the relay view, so override DNSMASQ_PORT=5354 there.
DNSMASQ_PORT="${DNSMASQ_PORT:-5353}"
# Host of that resolver. Default 127.0.0.1 (co-located, single-host testnet). Set to the resolver's
# address when joining from another machine (network must have started dnsmasq with DNSMASQ_BIND=0.0.0.0).
DNSMASQ_HOST="${DNSMASQ_HOST:-127.0.0.1}"

# `--full` prunes account/storage history to ~10k blocks (matches start-local-observer.sh). Set
# DISABLE_PRUNING=1 to run this observer as a full archive so its datadir can seed rayls-replay.
FULL_FLAG="--full"
if [[ "$DISABLE_PRUNING" == "1" || "$DISABLE_PRUNING" == "true" ]]; then
    FULL_FLAG=""
    echo "DISABLE_PRUNING set: running observer as a full archive (no --full)"
fi

# reth namespaces rpc/ws/authrpc/p2p ports by --instance (subtracts instance-1 from the defaults),
# so a unique index gives a unique port band -- same scheme start-local-observer.sh uses. The
# consensus prometheus --metrics port is NOT instance-derived, so set it explicitly. The p2p swarm
# port is auto-picked by keygen (get_available_udp_port), so it never collides.
INSTANCE="$NODE_NUM"
METRICS_PORT=$((9100 + NODE_NUM))
HTTP_PORT=$((8545 - (NODE_NUM - 1)))
WS_PORT=$((8546 - (NODE_NUM - 1)))
NODE_NAME="observer-${NODE_NUM}"

# p2p listener bind address. Default 0.0.0.0 (all interfaces). REQUIRED for an observer: keygen set
# its node-info network_address to an identity-only /p2p/<key> (undialable, so nothing ever dials it
# -- committee traffic flows back over the connection the observer itself opens). That address is not
# listenable, so the node errors at startup unless PRIMARY/WORKER_LISTENER_MULTIADDR gives it a real
# socket. Binding 0.0.0.0 (not 127.0.0.1) lets its outbound QUIC reach the committee's relays from any
# interface. Override the bind IP with LISTEN_HOST=<ip>, or replace the whole multiaddr with
# PRIMARY_LISTENER_MULTIADDR / WORKER_LISTENER_MULTIADDR (the env vars the node reads).
LISTEN_HOST="${LISTEN_HOST:-0.0.0.0}"
export PRIMARY_LISTENER_MULTIADDR="${PRIMARY_LISTENER_MULTIADDR:-/ip4/${LISTEN_HOST}/udp/$((49000 + NODE_NUM))/quic-v1}"
export WORKER_LISTENER_MULTIADDR="${WORKER_LISTENER_MULTIADDR:-/ip4/${LISTEN_HOST}/udp/$((49100 + NODE_NUM))/quic-v1}"

ROOTDIR="$scriptDir/local-validators"
BIN="$scriptDir/../../target/${BUILD_CONFIG}/rayls-network"
DATADIR="${ROOTDIR}/${NODE_NAME}"
NODE_LOG="${ROOTDIR}/${NODE_NAME}.log"
NODE_PID_FILE="${ROOTDIR}/${NODE_NAME}.pid"
COMMITTEE_YAML="${DATADIR}/genesis/committee.yaml"

# --- guards ---
[[ "$NODE_NUM" =~ ^[0-9]+$ && "$NODE_NUM" -ge 1 && "$NODE_NUM" -le 200 ]] || { echo "Error: INDEX must be an integer in 1..200 (reth --instance range)."; exit 1; }
[[ "$NODE_NUM" -gt "${NUM_VALIDATORS:-0}" ]] || { echo "Error: INDEX ($NODE_NUM) must be > NUM_VALIDATORS (${NUM_VALIDATORS}) to avoid an instance/port clash with a validator."; exit 1; }
[[ -d "${ROOTDIR}/genesis" ]] || { echo "Error: no genesis found at ${ROOTDIR}/genesis (run local-testnet.sh first, or place the network files there)."; exit 1; }
[[ -x "$BIN" ]] || { echo "Error: $BIN not built."; exit 1; }
alive() { [[ -f "$1" ]] && kill -0 "$(cat "$1" 2>/dev/null)" 2>/dev/null; }
if alive "$NODE_PID_FILE"; then
    echo "Error: ${NODE_NAME} already running (pid $(cat "$NODE_PID_FILE")). Kill it first, then re-run to restart."
    exit 1
fi

# --- 1. first add: generate this observer's own node keys (a plain direct advertise address -- NO
#        --relay, so it makes no reservation and sits behind no relay) and give it the genesis +
#        committee so it knows the bootstrap seeds (copied from the local testnet, same as
#        local-testnet.sh / start-local-observer.sh). Restart-safe: skip if the datadir exists. ---
if [[ ! -d "$DATADIR" ]]; then
    # The network files must already be present locally -- from local-testnet.sh, or a copied
    # --export-join-bundle. Bail clearly if any is missing so a forgotten bundle fails loudly
    # instead of starting a misconfigured node.
    for f in "${ROOTDIR}/genesis/genesis.yaml" "${ROOTDIR}/genesis/committee.yaml" "${ROOTDIR}/parameters.yaml"; do
        [[ -f "$f" ]] || { echo "Error: required network file not found: $f"; echo "Run local-testnet.sh first, or copy the join bundle (local-testnet.sh --export-join-bundle) into ${ROOTDIR}/ before adding an observer."; exit 1; }
    done
    echo "Generating observer keys for ${NODE_NAME} (no relay in front)..."
    mkdir -p "${DATADIR}/genesis"
    # Observer is outbound-only: it must follow consensus but nothing should dial it.
    #  - --advertise-identity-only: sets network_address = /p2p/<key> (undialable), so peers map its
    #    peer_id -> bls (accept its batch requests) but never try to dial it.
    # The listen socket is bound separately via PRIMARY/WORKER_LISTENER_MULTIADDR (exported above),
    # binding 0.0.0.0 so the observer can reach the committee's real relay IPs (a loopback bind
    # couldn't). That env is required: an identity-only network_address is not listenable on its own.
    "$BIN" keytool generate observer \
        --datadir "$DATADIR" \
        --address "0x0000000000000000000000000000000000000000" \
        --advertise-identity-only
    cp "${ROOTDIR}/genesis/genesis.yaml"   "${DATADIR}/genesis/"
    cp "${ROOTDIR}/genesis/committee.yaml" "${DATADIR}/genesis/"
    cp "${ROOTDIR}/parameters.yaml"        "${DATADIR}/"
else
    echo "Reusing existing datadir ${DATADIR} (skipping keygen + genesis copy)."
fi

# If the committee is advertised as /dnsaddr (--relay-dns), point the resolver at the local dnsmasq
# so the observer can follow those names to the validators' relay circuits and dial THROUGH the
# relays -- otherwise it queries the system/public resolver, gets NXDomain for the committee names,
# resolves no addresses, and never connects. With plain direct/concrete-circuit committee addresses
# (no /dnsaddr), no resolver is needed. Same conditional as add-relay-node.sh.
NODE_ENV=()
if grep -q '/dnsaddr/' "$COMMITTEE_YAML"; then
    echo "committee uses /dnsaddr -> resolving via dnsmasq at ${DNSMASQ_HOST}:${DNSMASQ_PORT} (observer dials validators through their relays)"
    NODE_ENV=("RAYLS_DNS_SERVER=${DNSMASQ_HOST}:${DNSMASQ_PORT}")
else
    echo "committee uses direct/concrete addresses (no /dnsaddr) -> no resolver needed"
fi

# --- 3. start the observer (args mirror start-local-observer.sh). --observer pins it out of the
#        committee (never counted toward quorum). Backgrounded + pid file so it is restartable. ---
echo "Starting ${NODE_NAME} (instance ${INSTANCE}, rpc http://localhost:${HTTP_PORT} ws ws://localhost:${WS_PORT}, metrics 127.0.0.1:${METRICS_PORT})..."
echo "  p2p listeners: primary ${PRIMARY_LISTENER_MULTIADDR}, worker ${WORKER_LISTENER_MULTIADDR}"
env "${NODE_ENV[@]}" "$BIN" node \
    --observer \
    --datadir "$DATADIR" \
    --instance "$INSTANCE" \
    --metrics "127.0.0.1:${METRICS_PORT}" \
    --log.stdout.format log-fmt \
    ${FULL_FLAG} \
    --storage.v2 \
    --db.growth-step 1MB \
    --consensus-db.growth-step 1MB \
    --txpool.pending-max-count 1000000 \
    --txpool.pending-max-size 1242880000 \
    --txpool.basefee-max-count 1000000 \
    --txpool.basefee-max-size 20971120000 \
    --txpool.queued-max-count 1000000 \
    --txpool.queued-max-size 20971120000 \
    --txpool.max-pending-txns 1000000 \
    --txpool.max-new-txns 1000000 \
    --txpool.minimal-protocol-fee 0 \
    --txpool.max-tx-input-bytes 999999999999 \
    --txpool.max-account-slots 410000006 \
    --http \
    --http.addr 0.0.0.0 \
    --http.api all \
    --ws \
    --ws.addr 0.0.0.0 \
    --ws.api all \
    -${LOG_LEVEL} \
    >> "$NODE_LOG" 2>&1 &
echo $! > "$NODE_PID_FILE"

echo
echo "Started ${NODE_NAME} (observer, no relay in front). It dials the committee and syncs."
echo "  node log: tail -f ${NODE_LOG}"
echo "  through relays?  grep -E 'p2p-circuit|relay|ConnectionEstablished' ${NODE_LOG}"
echo "Stop it with: kill \$(cat ${NODE_PID_FILE})   (then re-run this script to bring it back)"
