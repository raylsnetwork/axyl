#!/bin/bash
#
# Add one extra relayed node to an already-running local relay testnet, with its own relay, and
# let it connect via the genesis bootstrap seeds. No edits to existing nodes' config.
#
# The node is not in the genesis committee, so it boots following the committee (dials them through
# their relays and syncs) as an observer. It runs WITHOUT --observer, so once it is staked in the
# on-chain ConsensusRegistry it is promoted to a voting validator (CVV) at the next epoch boundary --
# and that mode persists across restarts.
#
# Staking is a separate, one-time step -- run stake-relay-node.sh after this. Because the node's
# proof-of-possession is bound to its operator address at keygen, pass ADDRESS=0x<operator> here on
# the FIRST run if you intend to stake it later (default is the zero address = pure observer).
#
# Restart-safe: if this node's datadir already exists, keygen + genesis copy are skipped and only the
# relay + node processes are (re)started. So the same script both adds the node the first time and
# restarts it later.
#
# Usage:
#   ./add-relay-node.sh [INDEX]                            # add as a pure observer (default INDEX=5)
#   ADDRESS=0x<operator> ./add-relay-node.sh [INDEX]       # add, stakeable later via stake-relay-node.sh
#   DNSMASQ_PORT=5354 ./add-relay-node.sh [INDEX]          # outsider: resolve committee via the public relay view
#   DNSMASQ_HOST=10.0.0.5 ./add-relay-node.sh [INDEX]      # join from another host: point at that resolver's address

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
# Local dnsmasq port used by --relay-dns. A newcomer is an outsider, so point it at the network's
# PUBLIC view (relay records): DNSMASQ_PORT=5354 when the network was started with MULTI_LISTEN; a
# plain --relay-dns network serves everything on 5353.
DNSMASQ_PORT="${DNSMASQ_PORT:-5353}"
# Host of the dnsmasq resolver this node points RAYLS_DNS_SERVER at. Default 127.0.0.1 (co-located
# with the committee, as in the single-host testnet). Set DNSMASQ_HOST to the resolver's address
# when joining from another machine (the network must have started dnsmasq with DNSMASQ_BIND=0.0.0.0).
DNSMASQ_HOST="${DNSMASQ_HOST:-127.0.0.1}"

# Operator identity for staking, derived deterministically from the index so the node is stakeable
# later (stake-relay-node.sh ${NODE_NUM}) without tracking keys. OPERATOR_KEY is a small fixed
# integer (test-only, throwaway; won't collide with the mnemonic-derived genesis accounts); ADDRESS
# is computed from it with `cast` and baked into the node's proof-of-possession at keygen
# (blsPubkey||validatorAddress), so it can't change afterward. Without `cast` it falls back to the
# zero address (pure observer, not stakeable). Override ADDRESS or OPERATOR_KEY to use your own.
OPERATOR_KEY="${OPERATOR_KEY:-0x$(printf '%064x' $((1000 + NODE_NUM)))}"
if [[ -z "${ADDRESS:-}" ]]; then
    if command -v cast >/dev/null 2>&1; then
        ADDRESS=$(cast wallet address --private-key "$OPERATOR_KEY")
    else
        echo "note: Foundry 'cast' not found -> keygen with the zero address (pure observer, not stakeable)."
        ADDRESS="0x0000000000000000000000000000000000000000"
    fi
fi

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
if [[ ! -x "$RELAY_BIN" ]]; then
    echo "Building rayls-relay..."
    cargo build --bin rayls-relay $([[ "$BUILD_CONFIG" == "release" ]] && echo --release)
fi

# Restart-safe: an existing datadir means this node was already added, so skip keygen + genesis and
# just (re)start its processes.
RESTART=0
[[ -d "$DATADIR" ]] && RESTART=1
RELAY_PID_FILE="${ROOTDIR}/relay-${NODE_NUM}.pid"
NODE_PID_FILE="${ROOTDIR}/${NODE_NAME}.pid"
alive() { [[ -f "$1" ]] && kill -0 "$(cat "$1" 2>/dev/null)" 2>/dev/null; }

# --- 1. (re)start this node's relay (fixed identity from SEED, so the peer id is stable) ---
if alive "$RELAY_PID_FILE"; then
    echo "relay-${NODE_NUM} already running (pid $(cat "$RELAY_PID_FILE")); reusing."
else
    echo "Starting relay-${NODE_NUM} on ${RELAY_HOST}:${RELAY_PORT}..."
    RELAY_SEED_HEX="$SEED" RELAY_PORT="$RELAY_PORT" "$RELAY_BIN" >> "$RELAY_LOG" 2>&1 &
    echo $! > "$RELAY_PID_FILE"
fi

if [[ "$RESTART" -eq 0 ]]; then
    # --- 2. first add: read the relay's peer id and generate node keys with a circuit on it ---
    RELAY_PEER=""
    for _ in $(seq 1 40); do
        RELAY_PEER=$(grep -ao '12D3KooW[A-Za-z0-9]*' "$RELAY_LOG" 2>/dev/null | head -1 || true)
        [[ -n "$RELAY_PEER" ]] && break
        sleep 0.25
    done
    [[ -n "$RELAY_PEER" ]] || { echo "Error: relay did not report a peer id (see $RELAY_LOG)."; exit 1; }
    RELAY_ADDR="/ip4/${RELAY_HOST}/udp/${RELAY_PORT}/quic-v1/p2p/${RELAY_PEER}"
    echo "relay-${NODE_NUM} up: ${RELAY_ADDR}"

    mkdir -p "${DATADIR}/genesis"
    "$BIN" keytool generate validator \
        --datadir "$DATADIR" \
        --address "$ADDRESS" \
        --relay "$RELAY_ADDR"

    # --- 3. give it the genesis + committee so it knows the bootstrap seeds ---
    cp "${ROOTDIR}/genesis/genesis.yaml" "${DATADIR}/genesis/"
    cp "${ROOTDIR}/genesis/committee.yaml" "${DATADIR}/genesis/"
    cp "${ROOTDIR}/parameters.yaml" "${DATADIR}/"
else
    echo "Restart: reusing existing datadir ${DATADIR} (skipping keygen + genesis copy)."
fi

# If the network was started with --relay-dns, committee members are advertised as /dnsaddr and can
# only be resolved against the local dnsmasq -- otherwise this node queries the system/public
# resolver, gets NXDomain for *.rayls.test, resolves no circuit addresses, and never connects. Point
# it at $DNSMASQ_HOST:$DNSMASQ_PORT like the base validators/observer do. This node's own address is a
# concrete circuit (--relay above), so no DNS records are needed for it.
NODE_ENV=()
if grep -q '/dnsaddr/' "${DATADIR}/genesis/committee.yaml"; then
    echo "committee uses /dnsaddr -> resolving via dnsmasq at ${DNSMASQ_HOST}:${DNSMASQ_PORT}"
    NODE_ENV=("RAYLS_DNS_SERVER=${DNSMASQ_HOST}:${DNSMASQ_PORT}")
fi

# Mirror the base validators' txpool / db-growth flags (see local-testnet.sh) so this node behaves
# identically -- same pool limits and zero min-fee, otherwise it would reject the local zero-fee txs
# the rest of the committee accepts. Honors the same env toggles: TX_POOL_LARGE_LIMITS=1, DB_GROW_STEP.
FLAG_TX_POOL_MAX_COUNT="50000"
FLAG_TX_POOL_MAX_SIZE="1048556000"
if [[ "${TX_POOL_LARGE_LIMITS:-}" == "1" ]]; then
    FLAG_TX_POOL_MAX_COUNT="1000000"
    FLAG_TX_POOL_MAX_SIZE="20971120000"
fi
FLAG_DB_GROW=""
FLAG_CONSENSUS_DB_GROW=""
if [[ -n "${DB_GROW_STEP:-}" ]]; then
    FLAG_DB_GROW="--db.growth-step $DB_GROW_STEP"
    FLAG_CONSENSUS_DB_GROW="--consensus-db.growth-step $DB_GROW_STEP"
fi

# --- 4. start the node; it follows the committee (via their relays), syncs, and promotes to a
#        validator once staked. No --observer: that flag pins it out of the committee permanently. ---
if alive "$NODE_PID_FILE"; then
    echo "Error: ${NODE_NAME} already running (pid $(cat "$NODE_PID_FILE")). Stop it first."; exit 1
fi
echo "Starting ${NODE_NAME} (instance ${INSTANCE}, rpc http://localhost:${HTTP_PORT} ws ws://localhost:${WS_PORT})..."
env "${NODE_ENV[@]}" "$BIN" node \
    --datadir "$DATADIR" \
    --instance "$INSTANCE" \
    --metrics "127.0.0.1:${METRICS_PORT}" \
    --log.stdout.format log-fmt \
    --full \
    --storage.v2 \
    ${FLAG_DB_GROW} \
    ${FLAG_CONSENSUS_DB_GROW} \
    --txpool.pending-max-count "$FLAG_TX_POOL_MAX_COUNT" \
    --txpool.pending-max-size "$FLAG_TX_POOL_MAX_SIZE" \
    --txpool.basefee-max-count "$FLAG_TX_POOL_MAX_COUNT" \
    --txpool.basefee-max-size "$FLAG_TX_POOL_MAX_SIZE" \
    --txpool.queued-max-count "$FLAG_TX_POOL_MAX_COUNT" \
    --txpool.queued-max-size "$FLAG_TX_POOL_MAX_SIZE" \
    --txpool.max-pending-txns "$FLAG_TX_POOL_MAX_COUNT" \
    --txpool.max-new-txns "$FLAG_TX_POOL_MAX_COUNT" \
    --txpool.minimal-protocol-fee 0 \
    --txpool.max-tx-input-bytes 999999999999 \
    --txpool.max-account-slots "$FLAG_TX_POOL_MAX_COUNT" \
    --gpo.default-suggested-fee 0 \
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
if [[ "$ADDRESS" != "0x0000000000000000000000000000000000000000" ]]; then
    echo "Operator ${ADDRESS} baked in. To promote it to a validator (one-time), run:"
    echo "  ./stake-relay-node.sh ${NODE_NUM}"
    echo "  (derives the same operator from the index and uses the default registry-owner admin key;"
    echo "   override ADMIN_PRIVATE_KEY if your network's owner differs from anvil #0.)"
fi
echo "Stop everything with: killall rayls-network rayls-relay"
