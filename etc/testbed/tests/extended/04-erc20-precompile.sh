# The native ERC-20 precompile follows the OpenZeppelin ERC-20 spec (etc/test-network/erc20_precompile_test.sh).
command -v jq > /dev/null || skip "needs jq"
bash "$ROOT/etc/test-network/erc20_precompile_test.sh" "$(url validator-1)"
