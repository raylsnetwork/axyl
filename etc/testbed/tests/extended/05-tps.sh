# Burst load with tps-checker: TPS_TXS transfers (default 3000) from 3 wallets through the rpc nodes all land.
tps_setup || skip "tps-checker not available; clone github.com/raylsnetwork/tps-checker next to this repo"
txs=${TPS_TXS:-3000}
keys=$(wallet_keys 20 3)
# tps-checker can report success when submissions were rejected, so count what reached the chain
landed() {
    local k sum=0
    for k in $(echo "$keys" | tr , ' '); do
        sum=$((sum + $(cast nonce "$(cast wallet address --private-key "$k")" --rpc-url "$(url validator-1)")))
    done
    echo "$sum"
}
before=$(landed)
out=$(with_timeout 300 tps_checker --private-keys "$keys" --rpc-urls "$(load_urls)" \
    --num-transactions "$txs" --funder-private-key "$ADMIN_KEY" 2>&1) || fail "tps-checker failed: $(echo "$out" | tail -3)"
echo "$out" | grep -E 'Transactions: .* sent|Avg Confirm TPS|TPS \(block\)' | sed 's/^ */  /'
echo "$out" | grep -q 'Transactions: [0-9]* sent / 0 failed' || fail "some transactions were rejected"
all_landed() { (($(landed) - before == txs)); }
wait_for 60 "all $txs transactions to be on-chain" all_landed
log "$txs transactions on-chain"
