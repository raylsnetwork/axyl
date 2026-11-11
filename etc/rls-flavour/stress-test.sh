#!/usr/bin/env bash
# Stress tests for the second-chain-id design, one per criticism (see README.md).
# Needs the local chain on :8545 and rpc-proxy.mjs on :8645. Keys are Anvil's public dev keys.
set -uo pipefail

NODE=${NODE:-http://127.0.0.1:8545}
RLS_RPC=http://127.0.0.1:8645
RLS=0x07e17e17e17e17e17e17e17e17e17e17e17e17ea
USDR=0x0000000000000000000000000000000000000400
DEV_KEY=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
DEV=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
USER_KEY=0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d
USER=0x70997970C51812dc3A010C7d01b50e0d17dc79C8
RECIPIENT=0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC
FRESH=0x976EA74026E726554dB657fA54763abd0C3a0aa9

rls() { cast call "$RLS" "balanceOf(address)(uint256)" "$1" --rpc-url "$NODE" | awk '{print $1}'; }
rpc() { curl -s -X POST "$1" -H 'content-type: application/json' -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$2\",\"params\":$3}"; }
send_raw() { rpc "$NODE" eth_sendRawTransaction "[\"$1\"]" | jq -c '.result // .error.message'; }
wait_blocks() { perl -e 'select(undef,undef,undef,4)'; }
status() { rpc "$NODE" eth_getTransactionReceipt "[\"$1\"]" | jq -r 'if .result then (if .result.status == "0x1" then "mined ok" else "mined, reverted" end) else "not mined" end'; }

echo "== A. overflow: value = 2^256-1 from the RLS chain id"
MAX=115792089237316195423570985008687907853269984665640564039457584007913129639935
before=$(rls $FRESH)
tx=$(cast mktx --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$FRESH" --value $MAX --gas-limit 21000 2>&1)
if [[ "$tx" == 0x* ]]; then echo "   pool: $(send_raw $tx)"; wait_blocks; echo "   status $(status $(cast keccak $tx)); recipient RLS $before -> $(rls $FRESH)"; else echo "   could not sign: $tx"; fi

echo "== B. one nonce, both chain ids (MetaMask reads the latest-block nonce per network)"
n=$(cast nonce $DEV --rpc-url $NODE)
cast send --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$DEV" --value 10ether >/dev/null
u=$(cast mktx --rpc-url "$NODE" --private-key "$DEV_KEY" "$RECIPIENT" --value 1 --nonce $n)
r=$(cast mktx --rpc-url "$RLS_RPC" --private-key "$DEV_KEY" "$RECIPIENT" --value 1 --nonce $n)
echo "   USDr tx at nonce $n first: $(send_raw $u); then RLS tx at the same nonce: $(send_raw $r)"
wait_blocks; echo "   USDr tx: $(status $(cast keccak $u)); RLS tx: $(status $(cast keccak $r))"
n=$(cast nonce $DEV --rpc-url $NODE)
r=$(cast mktx --rpc-url "$RLS_RPC" --private-key "$DEV_KEY" "$RECIPIENT" --value 1 --nonce $n)
u=$(cast mktx --rpc-url "$NODE" --private-key "$DEV_KEY" "$RECIPIENT" --value 1 --nonce $n)
echo "   RLS tx at nonce $n first: $(send_raw $r); then USDr tx at the same nonce: $(send_raw $u)"
wait_blocks; echo "   RLS tx: $(status $(cast keccak $r)); USDr tx: $(status $(cast keccak $u))"

echo "== C. two RLS sends that each fit the balance but together do not"
P_KEY=0x47e179ec197488593b187f80a00eb0da91f1b9d0b13f8733639f19c30a34926a
P=0x15d34AAf54267DB7D7c367839AAf71A00a2C6A65
cast send --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$P" --value 5ether >/dev/null
bal=$(rls $P); pn=$(cast nonce $P --rpc-url $NODE); v=$(python3 -I -c "print($bal - 1300000000000000000)")
t1=$(cast mktx --rpc-url "$RLS_RPC" --private-key "$P_KEY" "$RECIPIENT" --value $v --nonce $pn)
t2=$(cast mktx --rpc-url "$RLS_RPC" --private-key "$P_KEY" "$RECIPIENT" --value $v --nonce $((pn+1)))
echo "   tx1: $(send_raw $t1); tx2: $(send_raw $t2)"
wait_blocks; echo "   tx1: $(status $(cast keccak $t1)); tx2: $(status $(cast keccak $t2)); pool now: $(rpc $NODE txpool_status '[]' | jq -c .result)"

echo "== C2. a waiting RLS send the sender can no longer pay after an ERC-20 transfer drains RLS"
cast send --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$P" --value 10ether >/dev/null
bal=$(rls $P); pn=$(cast nonce $P --rpc-url $NODE); drain=$(python3 -I -c "print($bal - 2500000000000000000)")
t1=$(cast mktx --rpc-url "$RLS_RPC" --private-key "$P_KEY" "$RLS" "transfer(address,uint256)" "$RECIPIENT" $drain --nonce $pn --gas-limit 40000)
t2=$(cast mktx --rpc-url "$RLS_RPC" --private-key "$P_KEY" "$RECIPIENT" --value 1ether --nonce $((pn+1)))
echo "   transfer: $(send_raw $t1); send: $(send_raw $t2)"
wait_blocks; wait_blocks; echo "   transfer: $(status $(cast keccak $t1)); send: $(status $(cast keccak $t2)); pool now: $(rpc $NODE txpool_status '[]' | jq -c .result)"

echo "== D. USDr precompile signatures from the RLS chain id (domain fixed at node start)"
echo "   0x0400 DOMAIN_SEPARATOR via node:      $(cast call $USDR 'DOMAIN_SEPARATOR()(bytes32)' --rpc-url $NODE 2>&1)"
echo "   0x0400 DOMAIN_SEPARATOR via RLS proxy: $(cast call $USDR 'DOMAIN_SEPARATOR()(bytes32)' --rpc-url $RLS_RPC 2>&1)"
echo "   RLS DOMAIN_SEPARATOR via node:         $(cast call $RLS 'DOMAIN_SEPARATOR()(bytes32)' --rpc-url $NODE)"
echo "   RLS DOMAIN_SEPARATOR via RLS proxy:    $(cast call $RLS 'DOMAIN_SEPARATOR()(bytes32)' --rpc-url $RLS_RPC)"

echo "== E. gas suggestions on the USDr RPC after RLS txs with a tip"
for i in 1 2 3; do cast send --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$RECIPIENT" --value 1 --priority-gas-price 50gwei >/dev/null; done
echo "   USDr RPC eth_maxPriorityFeePerGas: $(cast to-dec $(rpc $NODE eth_maxPriorityFeePerGas '[]' | jq -r .result))"
echo "   USDr RPC eth_gasPrice:             $(cast to-dec $(rpc $NODE eth_gasPrice '[]' | jq -r .result))"
echo "   recent reward percentiles:         $(rpc $NODE eth_feeHistory '["0x5","latest",[50]]' | jq -c '[.result.reward[][0]]')"

echo "== F. what an explorer sees for an RLS send"
h=$(cast send --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$RECIPIENT" --value 1ether --json | jq -r .transactionHash)
rpc $NODE eth_getTransactionByHash "[\"$h\"]" | jq -c '.result | {chainId, value, maxFeePerGas, maxPriorityFeePerGas}'
rpc $NODE eth_getTransactionReceipt "[\"$h\"]" | jq -c '.result | {effectiveGasPrice, gasUsed, logs: (.logs|length)}'
