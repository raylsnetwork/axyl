#!/usr/bin/env bash
# Checks for the review fixes on PR #386. Same setup as the other scripts.
set -uo pipefail

NODE=${NODE:-http://127.0.0.1:8545}
RLS_RPC=${RLS_RPC:-http://127.0.0.1:8645}
RLS=0x07e17e17e17e17e17e17e17e17e17e17e17e17ea
FEE_RECIPIENT=0x07e17e17e17e17e17e17e17e17e17e17e17e17ec
USER_KEY=0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d
USER=0x70997970C51812dc3A010C7d01b50e0d17dc79C8
DEV_KEY=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
RECIPIENT=0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC

pass=0 fail=0
check() { local label=$1 actual=$2 expected=$3; if [[ "$actual" == "$expected" ]]; then echo "PASS  $label"; pass=$((pass + 1)); else echo "FAIL  $label: got $actual, expected $expected"; fail=$((fail + 1)); fi; }
send_raw() { local raw=$1; curl -s -X POST "$NODE" -H 'content-type: application/json' -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_sendRawTransaction\",\"params\":[\"$raw\"]}" | jq -r '.result // .error.message'; }
REFUSED=refused
refused() { local result=$1; [[ "$result" == 0x* ]] && echo "accepted" || echo "$REFUSED"; }
nonce=$(cast nonce $USER --rpc-url $NODE)

echo "== intrinsic gas: 21,000 gas with 2 KB of calldata (needs more)"
data=0x$(printf 'ab%.0s' $(seq 1 2048))
tx=$(cast mktx --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$RECIPIENT" "$data" --gas-limit 21000 --nonce $nonce 2>&1)
out=$(send_raw "$tx"); echo "   $out"
check "too little gas for the calldata is refused at the pool" "$(refused "$out")" "$REFUSED"

echo "== tip above the fee cap"
tx=$(cast mktx --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$RECIPIENT" --value 1 --gas-price 30000gwei --priority-gas-price 40000gwei --nonce $nonce 2>&1)
if [[ "$tx" == 0x* ]]; then out=$(send_raw "$tx"); echo "   $out"; check "tip > max fee is refused at the pool" "$(refused "$out")" "$REFUSED";
else echo "   cast would not sign it: ${tx:0:100}"; fi

echo "== RLS value to a precompile (0x0400, the USDr precompile)"
tx=$(cast mktx --rpc-url "$RLS_RPC" --private-key "$USER_KEY" 0x0000000000000000000000000000000000000400 --value 1 --gas-limit 30000 --nonce $nonce)
out=$(send_raw "$tx"); echo "   $out"
check "RLS value to a precompile is refused" "$(refused "$out")" "$REFUSED"

echo "== the gas paid shows as an RLS Transfer to the fee recipient"
r=$(cast send --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$RECIPIENT" --value 1ether --json)
gas=$(cast to-dec $(jq -r .gasUsed <<<"$r")); price=$(cast to-dec $(jq -r .effectiveGasPrice <<<"$r"))
fee_log=$(jq -r --arg fr "0x000000000000000000000000${FEE_RECIPIENT:2}" '[.logs[] | select(.topics[2] == $fr)][0].data' <<<"$r")
check "two Transfer logs (value + gas)" "$(jq '.logs | length' <<<"$r")" "2"
check "the gas Transfer equals gasUsed × price" "$(cast to-dec $fee_log)" "$((gas * price))"

echo "== nonce guard compares flavours, not raw chain ids: a type-0 tx replaced by a type-2 fee bump, both USDr (pre-EIP-155, no chain id, is covered by the is_rls unit test)"
dn=$(cast nonce 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 --rpc-url $NODE)
low=$(cast mktx --rpc-url "$NODE" --private-key "$DEV_KEY" "$RECIPIENT" --value 1 --legacy --gas-price 48gwei --nonce $dn)
high=$(cast mktx --rpc-url "$NODE" --private-key "$DEV_KEY" "$RECIPIENT" --value 1 --gas-price 96gwei --priority-gas-price 48gwei --nonce $dn)
o1=$(send_raw "$low"); o2=$(send_raw "$high"); echo "   first: ${o1:0:20}…; replacement: ${o2:0:90}"
check "a same-flavour fee bump is not refused as 'other Rayls network'" "$([[ "$o2" == *"other Rayls network"* ]] && echo "$REFUSED" || echo ok)" "ok"

echo "== a waiting RLS value tx is dropped once its recipient gains code (EIP-7702 delegation)"
fresh=$(cast wallet new --json); fresh_key=$(jq -r '(.data // .)[0].private_key' <<<"$fresh"); fresh_addr=$(jq -r '(.data // .)[0].address' <<<"$fresh")
un=$(cast nonce $USER --rpc-url $NODE)
# nonce gap: the tx waits in the queued sub-pool
gapped=$(cast mktx --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$fresh_addr" --value 1 --gas-limit 21000 --nonce $((un + 1)))
gh=$(send_raw "$gapped"); echo "   queued: ${gh:0:20}…"
check "RLS value to an account without code is admitted" "$(refused "$gh")" "accepted"
in_txpool() { local hash=$1; curl -s -X POST "$NODE" -H 'content-type: application/json' -d '{"jsonrpc":"2.0","id":1,"method":"txpool_content","params":[]}' | jq --arg h "$hash" '[.result[][][] | select(.hash == $h)] | length'; }
check "it waits in the pool" "$(in_txpool "$gh")" "1"
auth=$(cast wallet sign-auth "$RLS" --private-key "$fresh_key" --nonce 0 --chain 487 --rpc-url "$NODE")
# the fee-bump check above leaves a dev tx pending: let it land first
for _ in $(seq 1 20); do (( $(cast nonce 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 --rpc-url $NODE) > dn )) && break; perl -e 'select(undef,undef,undef,1)'; done
st=$(cast send --force --rpc-url "$NODE" --private-key "$DEV_KEY" "$fresh_addr" --auth "$auth" --json 2>&1 | tail -1)
echo "   delegation tx: $(jq -r '.status // empty' <<<"$st" 2>/dev/null || echo "${st:0:120}")"
check "the recipient is now delegated" "$(cast code "$fresh_addr" --rpc-url "$NODE" | cut -c1-8)" "0xef0100"
perl -e 'select(undef,undef,undef,3)'
check "the waiting tx is dropped after the block" "$(in_txpool "$gh")" "0"

echo "== RLS value in a type-4 tx whose own authorization delegates the recipient"
f2=$(cast wallet new --json); f2_key=$(jq -r '(.data // .)[0].private_key' <<<"$f2"); f2_addr=$(jq -r '(.data // .)[0].address' <<<"$f2")
auth2=$(cast wallet sign-auth "$RLS" --private-key "$f2_key" --nonce 0 --chain 4871 --rpc-url "$RLS_RPC" 2>&1 | tail -1)
nonce=$(cast nonce $USER --rpc-url $NODE)
tx=$(yes | cast mktx --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$f2_addr" --value 1 --auth "$auth2" --gas-limit 60000 --nonce $nonce 2>&1 | tail -1)
out=$(send_raw "$tx"); echo "   $out"
type4=$(refused "$out"); [[ "$out" == *"EIP-7702"* ]] || type4="wrong reason: $out"
check "RLS value in a type-4 tx is refused" "$type4" "$REFUSED"

echo "== overspend is settled in nonce order: a lower nonce filled after a higher one waits is admitted"
f3=$(cast wallet new --json); f3_key=$(jq -r '(.data // .)[0].private_key' <<<"$f3"); f3_addr=$(jq -r '(.data // .)[0].address' <<<"$f3")
cast send --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$f3_addr" --value 3ether >/dev/null
# nonce 1 first (queued behind the gap): max cost ~1 RLS value + ~1.1 RLS max gas
later=$(cast mktx --rpc-url "$RLS_RPC" --private-key "$f3_key" "$RECIPIENT" --value 1ether --gas-limit 21000 --nonce 1)
lh=$(send_raw "$later"); echo "   nonce 1: ${lh:0:20}…"
# then nonce 0: fits on its own, but not together with nonce 1
first=$(cast mktx --rpc-url "$RLS_RPC" --private-key "$f3_key" "$RECIPIENT" --value 0.5ether --gas-limit 21000 --nonce 0)
fh=$(send_raw "$first"); echo "   nonce 0: ${fh:0:20}…"
check "the earlier nonce is admitted though a later one waits" "$(refused "$fh")" "accepted"
perl -e 'select(undef,undef,undef,4)'
check "the earlier nonce is mined" "$(cast receipt "$fh" --rpc-url "$NODE" --json 2>/dev/null | jq -r .status)" "0x1"
check "the later one, which no longer fits, is dropped after the block" "$(in_txpool "$lh")" "0"

echo "== $pass passed, $fail failed"
[[ $fail == 0 ]]
