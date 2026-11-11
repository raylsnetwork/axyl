#!/usr/bin/env bash
# RLS-flavour spike checks against a local 4-validator chain (see README.md).
#
# Needs the chain on :8545 and rpc-proxy.mjs on :8645. Keys are Anvil's public dev keys.
set -uo pipefail

NODE=${NODE:-http://127.0.0.1:8545}
RLS_RPC=${RLS_RPC:-http://127.0.0.1:8645}
RLS=0x07e17e17e17e17e17e17e17e17e17e17e17e17ea
FEE_RECIPIENT=0x07e17e17e17e17e17e17e17e17e17e17e17e17ec
DEV_KEY=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80  # USDr, deployer
USER_KEY=0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d # RLS only
USER=0x70997970C51812dc3A010C7d01b50e0d17dc79C8
RECIPIENT=0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC
POOR_KEY=0x7c852118294e51e653712a81e05800f419141751be58f605c371e15141b007a6 # no RLS, no USDr
SPENDER=0x15d34AAf54267DB7D7c367839AAf71A00a2C6A65

pass=0 fail=0
check() { # check <label> <actual> <expected>
  local label=$1 actual=$2 expected=$3
  if [[ "$actual" == "$expected" ]]; then echo "PASS  $label"; pass=$((pass + 1));
  else echo "FAIL  $label: got $actual, expected $expected"; fail=$((fail + 1)); fi
}
rls() { local account=$1; cast call "$RLS" "balanceOf(address)(uint256)" "$account" --rpc-url "$NODE" | awk '{print $1}'; }
usdr() { local account=$1; cast balance "$account" --rpc-url "$NODE"; }
add() { python3 -I -c "import sys; print(sum(int(a) for a in sys.argv[1:]))" "$@"; }

NODE_CHAIN=$(cast chain-id --rpc-url "$NODE")
RLS_CHAIN=$(cast chain-id --rpc-url "$RLS_RPC")
echo "node chain $NODE_CHAIN, RLS chain $RLS_CHAIN"
check "RLS chain id is node chain id * 10 + 1" "$RLS_CHAIN" "$((NODE_CHAIN * 10 + 1))"
check "wallet sees RLS as the native balance" "$(cast balance $USER --rpc-url $RLS_RPC)" "$(rls $USER)"

echo "== 1. native send of 1 RLS from an account with no USDr"
u_rls=$(rls $USER); u_usdr=$(usdr $USER); r_rls=$(rls $RECIPIENT); f_rls=$(rls $FEE_RECIPIENT)
receipt=$(cast send --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$RECIPIENT" --value 1ether --json)
status=$(jq -r .status <<<"$receipt"); gas=$(cast to-dec "$(jq -r .gasUsed <<<"$receipt")")
price=$(cast to-dec "$(jq -r .effectiveGasPrice <<<"$receipt")"); fee=$((gas * price))
hash=$(jq -r .transactionHash <<<"$receipt")
check "send succeeded" "$status" "0x1"
check "signed for the RLS chain id" "$(cast to-dec "$(cast tx $hash chainId --rpc-url $NODE)")" "$RLS_CHAIN"
check "recipient got 1 RLS" "$(rls $RECIPIENT)" "$(add $r_rls 1000000000000000000)"
check "sender paid 1 RLS + gas in RLS" "$(rls $USER)" "$(add $u_rls -1000000000000000000 -$fee)"
check "gas went to the staker rewards contract" "$(rls $FEE_RECIPIENT)" "$(add $f_rls $fee)"
check "sender's USDr untouched" "$(usdr $USER)" "$u_usdr"
check "RLS Transfer event emitted" "$(jq -r '.logs[0].address' <<<"$receipt")" "$RLS"
echo "      gas $gas at $price RLS-wei = $fee RLS-wei"

echo "== 2. dapp call: approve() from the RLS chain id runs as the user"
receipt=$(cast send --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$RLS" "approve(address,uint256)" "$SPENDER" 123 --json)
check "approve succeeded" "$(jq -r .status <<<"$receipt")" "0x1"
check "allowance is the user's (msg.sender = user)" \
  "$(cast call $RLS 'allowance(address,address)(uint256)' $USER $SPENDER --rpc-url $NODE)" "123"

echo "== 3. CHAINID returns the flavour's chain id"
probe=$(cast send --rpc-url "$NODE" --private-key "$DEV_KEY" --create 0x6004600c60003960046000f3465f5500 --json | jq -r .contractAddress)
cast send --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$probe" >/dev/null
check "CHAINID in an RLS-flavour tx" "$(cast to-dec "$(cast storage $probe 0 --rpc-url $NODE)")" "$RLS_CHAIN"
cast send --rpc-url "$NODE" --private-key "$DEV_KEY" "$probe" >/dev/null
check "CHAINID in a USDr-flavour tx" "$(cast to-dec "$(cast storage $probe 0 --rpc-url $NODE)")" "$NODE_CHAIN"

echo "== 4. an RLS permit signed for the RLS chain id"
domain=$(cast call "$RLS" "eip712Domain()(bytes1,string,string,uint256,address,bytes32,uint256[])" --rpc-url "$NODE")
name=$(sed -n 2p <<<"$domain" | tr -d '"'); version=$(sed -n 3p <<<"$domain" | tr -d '"')
nonce=$(cast call "$RLS" "nonces(address)(uint256)" "$USER" --rpc-url "$NODE")
permit_json() { # permit_json <chainId>
  local chain_id=$1
  cat <<EOF
{"types":{"EIP712Domain":[{"name":"name","type":"string"},{"name":"version","type":"string"},{"name":"chainId","type":"uint256"},{"name":"verifyingContract","type":"address"}],
"Permit":[{"name":"owner","type":"address"},{"name":"spender","type":"address"},{"name":"value","type":"uint256"},{"name":"nonce","type":"uint256"},{"name":"deadline","type":"uint256"}]},
"primaryType":"Permit","domain":{"name":"$name","version":"$version","chainId":$chain_id,"verifyingContract":"$RLS"},
"message":{"owner":"$USER","spender":"$SPENDER","value":"777","nonce":"$nonce","deadline":"99999999999"}}
EOF
}
tmp=$(mktemp); permit_json "$RLS_CHAIN" >"$tmp"
sig=$(cast wallet sign --data --from-file "$tmp" --private-key "$USER_KEY"); rm -f "$tmp"
r=0x${sig:2:64}; s=0x${sig:66:64}; v=$((16#${sig:130:2}))
args=("$RLS" "permit(address,address,uint256,uint256,uint8,bytes32,bytes32)" "$USER" "$SPENDER" 777 99999999999 "$v" "$r" "$s")
if cast send --rpc-url "$NODE" --private-key "$DEV_KEY" "${args[@]}" >/dev/null 2>&1; then usdr_ok=yes; else usdr_ok=no; fi
check "that permit fails in a USDr-flavour tx" "$usdr_ok" "no"
receipt=$(cast send --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "${args[@]}" --json)
check "that permit works in an RLS-flavour tx" "$(jq -r .status <<<"$receipt")" "0x1"
check "allowance set by the permit" \
  "$(cast call $RLS 'allowance(address,address)(uint256)' $USER $SPENDER --rpc-url $NODE)" "777"

echo "== 5. refusals"
out=$(cast send --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$probe" --value 1 2>&1)
check "RLS value to a contract is refused" "$(grep -c 'without code' <<<"$out")" "1"
out=$(cast send --rpc-url "$RLS_RPC" --private-key "$POOR_KEY" "$RECIPIENT" --value 1 2>&1)
check "a sender without RLS is refused" "$(grep -ci 'insufficient funds' <<<"$out")" "1"
out=$(cast send --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$RECIPIENT" --value 1 --gas-price 1gwei --priority-gas-price 0 2>&1)
check "a fee below the RLS base fee is refused" "$(grep -ciE 'fee cap|underpriced|less than' <<<"$out")" "1"

echo "== 6. USDr flavour unchanged"
r_usdr=$(usdr $RECIPIENT)
status=$(cast send --rpc-url "$NODE" --private-key "$DEV_KEY" "$RECIPIENT" --value 1ether --json | jq -r .status)
check "USDr send succeeded" "$status" "0x1"
check "recipient got 1 USDr" "$(usdr $RECIPIENT)" "$(add $r_usdr 1000000000000000000)"

echo "== 7. all validators agree"
sleep 3
n=$(( $(cast block-number --rpc-url "$NODE") - 1 ))
hashes=$(for port in 8545 8544 8543 8542; do cast block $n --field hash --rpc-url "http://127.0.0.1:$port"; done | sort -u | wc -l | tr -d ' ')
check "block $n has one hash on all 4 validators" "$hashes" "1"

echo "== $pass passed, $fail failed"
[[ $fail == 0 ]]
