# A late rpc node joins the running chain, syncs from genesis, relays a transfer and survives a restart.
n=$(add_rpc)
wait_synced "$n"
has_role "$n" observer || fail "$n is $(role "$n"), want observer"
to=0x$(od -An -N20 -tx1 /dev/urandom | tr -d ' \n')
cast send --rpc-url "$(url "$n")" --private-key "$ADMIN_KEY" --value 1gwei "$to" > /dev/null || fail "transfer via $n failed"
landed() { [[ $(cast balance "$to" --rpc-url "$(url validator-1)") == 1000000000 ]]; }
wait_for 30 "the transfer to reach validator-1" landed
log "transfer via $n landed"
stop_node "$n"
wait_blocks validator-1 5
start_node "$n"
wait_synced "$n"
check_consistency
