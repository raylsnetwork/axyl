#!/bin/bash
#
# One-time on-chain staking for a node previously added by add-relay-node.sh. Registers it on the
# ConsensusRegistry (fund -> allowlist -> stake -> activate) so it promotes from observer to a
# committee validator at the next epoch boundary. Run ONCE per node -- staking is not idempotent
# (re-running will revert once the node is already staked/active), and it is NOT needed on restart.
#
# Preconditions:
#   - The node exists (added via add-relay-node.sh INDEX) and is running so it can vote when promoted.
#   - It was keygen'd stakeable by add-relay-node.sh (which, with `cast` present, derives the SAME
#     operator address from the index and bakes it into the proof-of-possession). This script derives
#     the matching operator key from the index, so the addresses line up automatically.
#
# Permissioning: allowlisting is onlyOwner (governance controls onboarding), so ADMIN_PRIVATE_KEY
# must be the ConsensusRegistry owner. The operator only signs stake + activate.
#
# Requires Foundry 'cast'.
#
# Usage:
#   ./stake-relay-node.sh [INDEX]        # default INDEX=5
#
# Zero-config for the local relay testnet: the operator identity is derived from the index (same
# formula as add-relay-node.sh) and the registry-owner admin key defaults to anvil #0. Override any
# of PRIVATE_KEY (operator), ADMIN_PRIVATE_KEY (registry owner), ADDRESS, STAKE_AMOUNT, RPC_URL,
# REGISTRY_CONTRACT_ADDRESS for a non-default network.

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
BIN="$scriptDir/../../target/${BUILD_CONFIG}/rayls-network"
ROOTDIR="$scriptDir/local-validators"
NODE_NAME="relay-node-${NODE_NUM}"
DATADIR="${ROOTDIR}/${NODE_NAME}"

# Operator key derived from the index -- identical formula to add-relay-node.sh, so the resulting
# address matches the one baked into the node at keygen. Override PRIVATE_KEY to use your own.
PRIVATE_KEY="${PRIVATE_KEY:-0x$(printf '%064x' $((1000 + NODE_NUM)))}"
# Registry owner (allowlisting is onlyOwner). Defaults to the local relay testnet owner = anvil #0;
# override if your network was started with a different --dev-funds / consensus-registry-owner.
ADMIN_PRIVATE_KEY="${ADMIN_PRIVATE_KEY:-0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80}"
STAKE_AMOUNT="${STAKE_AMOUNT:-1000000000000000000000000}"
RPC_URL="${RPC_URL:-http://localhost:8545}"  # an existing network node's RPC
REGISTRY_CONTRACT_ADDRESS="${REGISTRY_CONTRACT_ADDRESS:-0x07E17e17E17e17E17e17E17E17E17e17e17E17e1}"

# --- guards ---
[[ -x "$BIN" ]] || { echo "Error: $BIN not built."; exit 1; }
[[ -d "$DATADIR" ]] || { echo "Error: $DATADIR not found -- add it first: ./add-relay-node.sh ${NODE_NUM}"; exit 1; }
command -v cast >/dev/null 2>&1 || { echo "Error: needs Foundry 'cast' on PATH."; exit 1; }

# operator address computed from the key -- matches the address baked into the node at keygen
ADDRESS="${ADDRESS:-$(cast wallet address --private-key "$PRIVATE_KEY")}"

echo "Staking ${NODE_NAME} on ConsensusRegistry ${REGISTRY_CONTRACT_ADDRESS} (operator ${ADDRESS}) via ${RPC_URL}"

# 1. fund the operator so it has enough to stake (admin pays)
echo "1/4 funding ${ADDRESS} with ${STAKE_AMOUNT} wei (admin)..."
cast send --private-key "$ADMIN_PRIVATE_KEY" --rpc-url "$RPC_URL" --value "$STAKE_AMOUNT" "$ADDRESS"

# 2. allowlist the operator -- onlyOwner; governance controls who may join
echo "2/4 allowlisting ${ADDRESS} (admin)..."
cast send "$REGISTRY_CONTRACT_ADDRESS" "allowlistValidator(address)" "$ADDRESS" \
    --private-key "$ADMIN_PRIVATE_KEY" --rpc-url "$RPC_URL"

# 3. stake -- signed by the operator; requires allowlist; PoP must be bound to $ADDRESS at keygen
echo "3/4 submitting stake (operator)..."
CALLDATA=$("$BIN" keytool stake-calldata --datadir "$DATADIR" | grep 'Calldata:' | awk '{print $2}')
[[ -n "$CALLDATA" ]] || { echo "Error: failed to produce stake calldata (is the datadir keygen'd?)"; exit 1; }
cast send "$REGISTRY_CONTRACT_ADDRESS" "$CALLDATA" --private-key "$PRIVATE_KEY" --rpc-url "$RPC_URL" -vvvv

# 4. activate -> PendingActivation -> Active at the next epoch boundary
echo "4/4 submitting activate (operator)..."
cast send "$REGISTRY_CONTRACT_ADDRESS" "activate()" --private-key "$PRIVATE_KEY" --rpc-url "$RPC_URL" -vvvv

echo
echo "Done: ${NODE_NAME} staked + activated. It promotes to a committee validator at the next epoch"
echo "boundary. Keep the node running (./add-relay-node.sh ${NODE_NUM}) so it's ready to vote."
