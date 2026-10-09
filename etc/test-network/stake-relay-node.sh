#!/bin/bash
#
# One-time on-chain staking for a node previously added by add-relay-node.sh. Registers it on the
# ConsensusRegistry so it promotes from observer to a committee validator at the next epoch boundary:
#   fund native gas -> mint RLS stake -> allowlist -> approve -> stake -> activate.
# The stake is paid in the RLS ERC-20 (not the native gas token), so the admin (MINTER_ROLE at
# genesis) mints it to the operator, which then approves the registry to pull it. Run ONCE per node
# -- staking is not idempotent (it reverts once staked/active) and is NOT needed on restart.
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
# The stake is paid in the RLS ERC-20 (0x..E17eA), NOT the native gas token. Required amount is the
# genesis default validator stake: 5,000,000 RLS = 5e24. The admin (MINTER_ROLE at genesis) mints it
# to the operator; the operator then approves the registry to pull it.
STAKE_AMOUNT="${STAKE_AMOUNT:-5000000000000000000000000}"
RLS_ADDRESS="${RLS_ADDRESS:-0x07E17e17E17e17E17e17E17E17E17e17e17E17eA}"   # RLS ERC-20 staking token
GAS_FUND="${GAS_FUND:-1000000000000000000}"  # native sent to the operator for gas (1 coin)
RPC_URL="${RPC_URL:-http://localhost:8545}"  # an existing network node's RPC
REGISTRY_CONTRACT_ADDRESS="${REGISTRY_CONTRACT_ADDRESS:-0x07E17e17E17e17E17e17E17E17E17e17e17E17e1}"

# --- guards ---
[[ -x "$BIN" ]] || { echo "Error: $BIN not built."; exit 1; }
[[ -d "$DATADIR" ]] || { echo "Error: $DATADIR not found -- add it first: ./add-relay-node.sh ${NODE_NUM}"; exit 1; }
command -v cast >/dev/null 2>&1 || { echo "Error: needs Foundry 'cast' on PATH."; exit 1; }

# operator address computed from the key -- matches the address baked into the node at keygen
ADDRESS="${ADDRESS:-$(cast wallet address --private-key "$PRIVATE_KEY")}"

# Readiness gate. Right after `local-testnet.sh --start` the genesis system contracts are not live
# yet: the RLS proxy has no implementation (ERC-1967 impl slot = 0), so mint/transfer silently no-op,
# and the ConsensusRegistry has no owner, so allowlist reverts. Staking during that window fails with
# cryptic mid-flow errors (ERC20InsufficientBalance / OwnableUnauthorized). Poll until RLS is wired
# AND the registry is owned before doing anything on-chain.
ERC1967_IMPL_SLOT="0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
echo "Waiting for genesis system contracts to come live (RLS wired + registry owned)..."
ready=0
for _ in $(seq 1 60); do
    rls_impl=$(cast storage "$RLS_ADDRESS" "$ERC1967_IMPL_SLOT" --rpc-url "$RPC_URL" 2>/dev/null || true)
    reg_owner=$(cast call "$REGISTRY_CONTRACT_ADDRESS" "owner()(address)" --rpc-url "$RPC_URL" 2>/dev/null || true)
    # rls_impl has a non-zero hex digit (proxy points at an implementation) AND owner != zero address
    if [[ "$rls_impl" =~ [1-9a-fA-F] \
        && -n "$reg_owner" && "$reg_owner" != "0x0000000000000000000000000000000000000000" ]]; then
        ready=1
        break
    fi
    sleep 2
done
[[ "$ready" -eq 1 ]] || {
    echo "Error: genesis system contracts not live after ~2min (RLS impl='${rls_impl:-}', registry owner='${reg_owner:-}')."
    echo "The network is still initializing -- wait a bit and re-run, or check the node is up on ${RPC_URL}."
    exit 1
}
echo "System contracts live (RLS impl ${rls_impl}, registry owner ${reg_owner})."

echo "Staking ${NODE_NAME} on ConsensusRegistry ${REGISTRY_CONTRACT_ADDRESS} (operator ${ADDRESS}) via ${RPC_URL}"

# 1. fund the operator with native so it can pay gas for its own txs (admin pays)
echo "1/6 funding ${ADDRESS} with ${GAS_FUND} wei native for gas (admin)..."
cast send --private-key "$ADMIN_PRIVATE_KEY" --rpc-url "$RPC_URL" --value "$GAS_FUND" "$ADDRESS"

# 2. mint the RLS stake to the operator -- staking pulls the RLS ERC-20, not native. Genesis grants
#    MINTER_ROLE to the network admin, so the admin can mint. (A real deployment funds RLS differently.)
echo "2/6 minting ${STAKE_AMOUNT} RLS to ${ADDRESS} (admin, MINTER_ROLE)..."
cast send "$RLS_ADDRESS" "mint(address,uint256)" "$ADDRESS" "$STAKE_AMOUNT" \
    --private-key "$ADMIN_PRIVATE_KEY" --rpc-url "$RPC_URL"

# 3. allowlist the operator -- onlyOwner; governance controls who may join. Tolerate re-runs.
echo "3/6 allowlisting ${ADDRESS} (admin)..."
cast send "$REGISTRY_CONTRACT_ADDRESS" "allowlistValidator(address)" "$ADDRESS" \
    --private-key "$ADMIN_PRIVATE_KEY" --rpc-url "$RPC_URL" || echo "  (allowlist may already be set; continuing)"

# 4. operator approves the registry to pull its RLS stake (transferFrom)
echo "4/6 approving registry to spend ${STAKE_AMOUNT} RLS (operator)..."
cast send "$RLS_ADDRESS" "approve(address,uint256)" "$REGISTRY_CONTRACT_ADDRESS" "$STAKE_AMOUNT" \
    --private-key "$PRIVATE_KEY" --rpc-url "$RPC_URL"

# 5. stake -- signed by the operator; requires allowlist + approval; PoP is bound to $ADDRESS at keygen
echo "5/6 submitting stake (operator)..."
CALLDATA=$("$BIN" keytool stake-calldata --datadir "$DATADIR" | grep 'Calldata:' | awk '{print $2}')
[[ -n "$CALLDATA" ]] || { echo "Error: failed to produce stake calldata (is the datadir keygen'd?)"; exit 1; }
cast send "$REGISTRY_CONTRACT_ADDRESS" "$CALLDATA" --private-key "$PRIVATE_KEY" --rpc-url "$RPC_URL" -vvvv

# 6. activate -> PendingActivation -> Active at the next epoch boundary
echo "6/6 submitting activate (operator)..."
cast send "$REGISTRY_CONTRACT_ADDRESS" "activate()" --private-key "$PRIVATE_KEY" --rpc-url "$RPC_URL" -vvvv

echo
echo "Done: ${NODE_NAME} staked + activated. It promotes to a committee validator at the next epoch"
echo "boundary. Keep the node running (./add-relay-node.sh ${NODE_NUM}) so it's ready to vote."
