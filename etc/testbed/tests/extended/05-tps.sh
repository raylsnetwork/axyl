# Load test with etc/tps: TPS_TXS transfers (default 2000) spread over validators 1-3 all land.
command -v node > /dev/null || skip "needs Node.js 22+"
tps_setup
keys=$ADMIN_KEY,0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d,0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a
out=$(with_timeout 300 tps "${TPS_TXS:-2000}" "$(url validator-1),$(url validator-2),$(url validator-3)" "$keys" 2>&1) || true
# the tool always exits 0 and prints the TPS line only once every wallet's last transaction has landed
echo "$out" | grep 'True TPS' | sed 's/^/  /' || fail "batch did not complete: $(echo "$out" | tail -3)"
