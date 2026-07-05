#!/bin/bash
#
# Add one extra relayed node to an already-running local relay testnet, with its own relay, and
# let it connect via the genesis bootstrap seeds. No edits to existing nodes' config.
#
# The node joins as an OBSERVER: it dials the committee (through their relays) and syncs. Becoming
# a voting validator additionally requires staking it in the on-chain ConsensusRegistry + an epoch
# transition -- not done here.
#
# Usage: ./add-relay-node.sh [INDEX]   (default INDEX=5; must not collide with existing nodes/ports)

set -e

directory=$(dirname "${BASH_SOURCE[0]}")
scriptDir=$(cd "$directory" && pwd)
envPath="$scriptDir/.env"
[[ -e "$envPath" ]] || { echo "Error: .env not found at $envPath"; exit 1; }
. "$envPath"
export RL_BLS_PASSPHRASE="$RL_BLS_PASSPHRASE"
export RAYLS_NETWORK="$RAYLS_NETWORK"
cd "$scriptDir/../.."

NODE_NUM="${1:-5}"
BUILD_CONFIG="${BUILD_CONFIG:-release}"
LOG_LEVEL="${LOG_LEVEL:-info}"
# Relay ip/port convention must match local-testnet.sh.
RELAY_HOST="127.0.0.1"
RELAY_BASE_PORT=50000

idx=$((NODE_NUM - 1))
RELAY_PORT=$((RELAY_BASE_PORT + idx))
# reth namespaces its rpc/ws/authrpc/p2p ports by --instance (it subtracts instance-1 from the
# defaults), so two nodes with the same instance collide. The base uses instances 1..NUM_VALIDATORS
# for validators and the next few for observers, so give this ad-hoc node a unique HIGH instance
# (well clear of the base, and <=200 as reth requires). Its reth ports then auto-shift into a free
# band. The consensus prometheus --metrics port is NOT instance-derived, so we set it explicitly.
INSTANCE=$((100 + NODE_NUM))
HTTP_PORT=$((8545 - (INSTANCE - 1)))
WS_PORT=$((8556 - idx + 10000))
METRICS_PORT=$((9100 + idx + 10000))
NODE_NAME="relay-node-${NODE_NUM}"

ROOTDIR="$scriptDir/local-validators"
BIN="$scriptDir/../../target/${BUILD_CONFIG}/rayls-network"
RELAY_BIN="$scriptDir/../../target/${BUILD_CONFIG}/rayls-relay"
DATADIR="${ROOTDIR}/${NODE_NAME}"
RELAY_LOG="${ROOTDIR}/relay-${NODE_NUM}.log"
NODE_LOG="${ROOTDIR}/${NODE_NAME}.log"

# Fixed relay identity seed for this index: byte NODE_NUM repeated 32x (same scheme as the validators).
byte=$(printf '%02x' "$NODE_NUM")
SEED=""
for ((c = 0; c < 32; c++)); do SEED="${SEED}${byte}"; done

# --- guards ---
[[ "$NODE_NUM" -gt "${NUM_VALIDATORS:-0}" ]] || { echo "Error: INDEX ($NODE_NUM) must be > NUM_VALIDATORS (${NUM_VALIDATORS}) so its relay port ($RELAY_PORT) and identity don't collide with a validator's."; exit 1; }
[[ -d "${ROOTDIR}/genesis" ]] || { echo "Error: run local-testnet.sh --relay first (no genesis found)."; exit 1; }
[[ -x "$BIN" ]] || { echo "Error: $BIN not built."; exit 1; }
[[ -d "$DATADIR" ]] && { echo "Error: $DATADIR already exists -- pick another INDEX or remove it."; exit 1; }
if [[ ! -x "$RELAY_BIN" ]]; then
    echo "Building rayls-relay..."
    cargo build --bin rayls-relay $([[ "$BUILD_CONFIG" == "release" ]] && echo --release)
fi

# --- 1. start this node's relay and read back its peer id ---
echo "Starting relay-${NODE_NUM} on ${RELAY_HOST}:${RELAY_PORT}..."
RELAY_SEED_HEX="$SEED" RELAY_PORT="$RELAY_PORT" "$RELAY_BIN" >> "$RELAY_LOG" 2>&1 &
echo $! > "${ROOTDIR}/relay-${NODE_NUM}.pid"

RELAY_PEER=""
for _ in $(seq 1 40); do
    RELAY_PEER=$(grep -ao '12D3KooW[A-Za-z0-9]*' "$RELAY_LOG" 2>/dev/null | head -1 || true)
    [[ -n "$RELAY_PEER" ]] && break
    sleep 0.25
done
[[ -n "$RELAY_PEER" ]] || { echo "Error: relay did not report a peer id (see $RELAY_LOG)."; exit 1; }
RELAY_ADDR="/ip4/${RELAY_HOST}/udp/${RELAY_PORT}/quic-v1/p2p/${RELAY_PEER}"
echo "relay-${NODE_NUM} up: ${RELAY_ADDR}"

# --- 2. generate node keys with a circuit address on that relay ---
mkdir -p "${DATADIR}/genesis"
"$BIN" keytool generate observer \
    --datadir "$DATADIR" \
    --address "0x0000000000000000000000000000000000000000" \
    --relay "$RELAY_ADDR"

# --- 3. give it the genesis + committee so it knows the bootstrap seeds ---
cp "${ROOTDIR}/genesis/genesis.yaml" "${DATADIR}/genesis/"
cp "${ROOTDIR}/genesis/committee.yaml" "${DATADIR}/genesis/"
cp "${ROOTDIR}/parameters.yaml" "${DATADIR}/"

# --- 4. start the node; it dials the committee (via their relays) and syncs ---
echo "Starting ${NODE_NAME} (instance ${INSTANCE}, rpc http://localhost:${HTTP_PORT} ws ws://localhost:${WS_PORT})..."
"$BIN" node \
    --datadir "$DATADIR" \
    --observer \
    --instance "$INSTANCE" \
    --metrics "127.0.0.1:${METRICS_PORT}" \
    --log.stdout.format log-fmt \
    --full \
    --storage.v2 \
    -${LOG_LEVEL} \
    --http \
    --http.api all \
    --ws.port "$WS_PORT" \
    --ws.api all \
    >> "$NODE_LOG" 2>&1 &
echo $! > "${ROOTDIR}/${NODE_NAME}.pid"

echo
echo "Started ${NODE_NAME}. It will dial the committee through their relays and sync."
echo "  node log:  tail -f ${NODE_LOG}"
echo "  relay log: tail -f ${RELAY_LOG}"
echo "Stop everything with: killall rayls-network rayls-relay"
