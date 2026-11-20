#!/usr/bin/env bash
# More stress tests for the second-chain-id design: mixed blocks, load, deploys, tx types, estimates.
# Needs the local chain on :8545 and rpc-proxy.mjs on :8645. Keys are Anvil's public dev keys.
set -uo pipefail

NODE=${NODE:-http://127.0.0.1:8545}
RLS_RPC=http://127.0.0.1:8645
RLS=0x07e17e17e17e17e17e17e17e17e17e17e17e17ea
FEE_RECIPIENT=0x07e17e17e17e17e17e17e17e17e17e17e17e17ec
DEV_KEY=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
USER_KEY=0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d
USER=0x70997970C51812dc3A010C7d01b50e0d17dc79C8
RECIPIENT=0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC
KEYS=(0x8b3a350cf5c34c9194ca85829a2df0ec3153be0318b5e2d3348e872092edffba
      0xdbda1821b80551c9d65939329250298aa3472ba22feea921c0cf5d620ea67b97
      0x2a871d0798f97d79848a013d4936a73bf4cc922c825d33c1cf7073dff6d409c6)
ADDRS=(0x9965507D1a55bcC2695C58ba16FB37d819B0A4dc
       0x23618e81E3f5cdF7f54C3d65f7FBc0aBf5B21E8f
       0xa0Ee7A142d267C1f36714E4a8F75612F20a79720)

rpc() { local url=$1 body=$2; curl -s -X POST "$url" -H 'content-type: application/json' -d "$body"; }
rls() { local account=$1; cast call "$RLS" "balanceOf(address)(uint256)" "$account" --rpc-url "$NODE" | awk '{print $1}'; }
receipt() { local hash=$1; rpc "$NODE" "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getTransactionReceipt\",\"params\":[\"$hash\"]}" | jq -c .result; }
pause() { local seconds=$1; perl -e "select(undef,undef,undef,$seconds)"; }

echo "== setup: three senders get RLS (from the RLS-only user) and USDr (from the dev account)"
for a in "${ADDRS[@]}"; do
  # 20 waiting RLS txs reserve 21,000 gas x max fee (~2x the RLS base fee) each: ~22 RLS per
  # sender, and the pool refuses whatever the sender's RLS can't cover
  cast send --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$a" --value 25ether >/dev/null
  cast send --rpc-url "$NODE" --private-key "$DEV_KEY" "$a" --value 50ether >/dev/null
done

echo "== G. load: 3 senders x 40 txs, alternating RLS and USDr flavour, all sent at once"
start=$(cast block-number --rpc-url $NODE)
batch="["; hashes=(); id=0
for i in 0 1 2; do
  n=$(cast nonce "${ADDRS[$i]}" --rpc-url $NODE)
  for j in $(seq 0 39); do
    if (( j % 2 == 0 )); then url=$RLS_RPC; else url=$NODE; fi
    raw=$(cast mktx --rpc-url "$url" --private-key "${KEYS[$i]}" "$RECIPIENT" --value 1 --nonce $((n + j)) --gas-limit 21000)
    hashes+=("$(cast keccak "$raw")"); id=$((id + 1))
    batch+="{\"jsonrpc\":\"2.0\",\"id\":$id,\"method\":\"eth_sendRawTransaction\",\"params\":[\"$raw\"]},"
  done
done
batch="${batch%,}]"
replies=$(rpc "$NODE" "$batch")
accepted=$(jq '[.[] | select(.result)] | length' <<<"$replies")
echo "   submitted ${#hashes[@]}, pool accepted $accepted"
jq -r '[.[] | .error.message // empty] | group_by(.) | .[] | "   refused \(length): \(.[0][0:140])"' <<<"$replies"
pause 12
mined=0; failed=0; blocks=()
for h in "${hashes[@]}"; do
  r=$(receipt "$h")
  if [[ "$r" != "null" ]]; then mined=$((mined + 1)); [[ $(jq -r .status <<<"$r") == "0x1" ]] || failed=$((failed + 1)); blocks+=("$(jq -r .blockNumber <<<"$r")"); fi
done
echo "   mined $mined, reverted $failed"
mixed=0
for b in $(printf '%s\n' "${blocks[@]}" | sort -u); do
  ids=$(rpc "$NODE" "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getBlockByNumber\",\"params\":[\"$b\",true]}" | jq -r '[.result.transactions[].chainId] | unique | join(",")')
  [[ "$ids" == *","* ]] && mixed=$((mixed + 1)) && echo "   block $(cast to-dec $b) holds both flavours: chain ids $ids"
done
echo "   blocks with both flavours: $mixed of $(printf '%s\n' "${blocks[@]}" | sort -u | wc -l | tr -d ' ')"
end=$(cast block-number --rpc-url $NODE)
roots=$(for p in 8545 8544 8543 8542 8541; do cast block $((end - 2)) --field stateRoot --rpc-url http://127.0.0.1:$p 2>/dev/null; done | sort -u | wc -l | tr -d ' ')
echo "   block $((end - 2)): $roots distinct state root(s) across 4 validators + observer"

echo "== H. deploy a contract from the RLS chain id, paying gas in RLS"
u0=$(rls $USER)
out_h=$(cast send --rpc-url "$RLS_RPC" --private-key "$USER_KEY" --create 0x6004600c60003960046000f3465f5500 --json)
echo "   status $(jq -r .status <<<"$out_h"), contract $(jq -r .contractAddress <<<"$out_h"), code $(cast code $(jq -r .contractAddress <<<"$out_h") --rpc-url $NODE), RLS paid $(( u0 - $(rls $USER) ))"

echo "== I. legacy (type 0, EIP-155) tx from the RLS chain id"
out=$(cast send --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$RECIPIENT" --value 1 --legacy --json 2>&1)
echo "   $(jq -c '{status, type, effectiveGasPrice}' <<<"$out" 2>/dev/null || echo "$out" | tail -1)"

echo "== J. EIP-7702 (type 4) from the RLS chain id: delegate the user's account to the probe contract"
probe=$(jq -r .contractAddress <<<"$out_h")
auth=$(cast wallet sign-auth "$probe" --private-key "$USER_KEY" --rpc-url "$RLS_RPC" --self-broadcast 2>&1 | tail -1)
u0=$(rls $USER)
out=$(yes | cast send --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$USER" --auth "$auth" --json 2>/dev/null | tail -1)
echo "   status $(jq -r .status <<<"$out" 2>/dev/null || echo "$out"), type $(jq -r .type <<<"$out" 2>/dev/null), code at user $(cast code $USER --rpc-url $NODE), RLS paid $(( u0 - $(rls $USER) ))"
auth0=$(cast wallet sign-auth 0x0000000000000000000000000000000000000000 --private-key "$USER_KEY" --chain 0 --force --nonce $(( $(cast nonce $USER --rpc-url $NODE) + 1 )) 2>&1 | tail -1)
out=$(yes | cast send --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$USER" --auth "$auth0" --json 2>/dev/null | tail -1)
echo "   undelegate: status $(jq -r .status <<<"$out" 2>/dev/null || echo "$out"), code at user now '$(cast code $USER --rpc-url $NODE)'"

echo "== K. estimate vs actual gas for a dapp call (approve) on the RLS chain id"
est=$(cast estimate --rpc-url "$RLS_RPC" --from "$USER" "$RLS" "approve(address,uint256)" "$RECIPIENT" 5)
act=$(cast send --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$RLS" "approve(address,uint256)" "$RECIPIENT" 5 --json | jq -r .gasUsed)
echo "   estimate $est, used $(cast to-dec $act)"
est=$(cast estimate --rpc-url "$RLS_RPC" --from "$USER" "$RECIPIENT" --value 1ether)
echo "   estimate for a 1 RLS send: $est"

echo "== L. nonce as each RPC reports it"
echo "   node latest $(cast nonce $USER --rpc-url $NODE), RLS RPC latest $(cast nonce $USER --rpc-url $RLS_RPC), pending $(cast to-dec $(rpc $RLS_RPC '{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionCount","params":["'$USER'","pending"]}' | jq -r .result))"

echo "== M. fees went where they should"
echo "   staker rewards contract RLS: $(rls $FEE_RECIPIENT)"

echo "== N. tracing replays an RLS-flavour tx with the same result (debug_traceTransaction)"
h=$(cast send --rpc-url "$RLS_RPC" --private-key "$USER_KEY" "$RLS" "approve(address,uint256)" "$RECIPIENT" 6 --json | jq -r .transactionHash)
used=$(cast to-dec $(receipt "$h" | jq -r .gasUsed))
traced=$(rpc "$NODE" "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"debug_traceTransaction\",\"params\":[\"$h\",{\"tracer\":\"callTracer\"}]}" | jq -r '.result.gasUsed // .error.message')
echo "   receipt gasUsed $used, trace gasUsed $(cast to-dec $traced 2>/dev/null || echo "$traced")"
