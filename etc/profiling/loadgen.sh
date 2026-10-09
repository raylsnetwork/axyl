#!/bin/sh
# Steady stream of plain transfers into validator1 so execution, txpool gossip and batch
# propagation show up in the profiles. Uses the genesis dev-funded account (Anvil key #0).
#
# The gas price is set explicitly: the chain's base-fee floor is far above what eth_gasPrice
# suggests, and underpriced txns sit in the basefee subpool and are never mined.
set -eu

RPC_URL="${RPC_URL:-http://10.10.0.21:8545}"
PRIVATE_KEY="${PRIVATE_KEY:-0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80}"
GAS_PRICE="${GAS_PRICE:-100gwei}"
# pause between sends; each `cast send --async` is one RPC round trip on top of this
INTERVAL="${INTERVAL:-0.05}"

echo "waiting for $RPC_URL"
until cast block-number --rpc-url "$RPC_URL" >/dev/null 2>&1; do sleep 2; done

from=$(cast wallet address --private-key "$PRIVATE_KEY")
nonce=$(cast nonce --block pending --rpc-url "$RPC_URL" "$from")
echo "sending from $from starting at nonce $nonce"

while true; do
    # a fresh recipient per txn, so execution also creates accounts
    to=$(printf '0x%040x' $((nonce + 4096)))
    if cast send --async --rpc-url "$RPC_URL" --private-key "$PRIVATE_KEY" \
        --gas-price "$GAS_PRICE" --gas-limit 21000 --nonce "$nonce" \
        "$to" --value 1wei >/dev/null; then
        nonce=$((nonce + 1))
    else
        # resync after a rejected send (e.g. node restart)
        sleep 1
        nonce=$(cast nonce --block pending --rpc-url "$RPC_URL" "$from")
    fi
    sleep "$INTERVAL"
done
