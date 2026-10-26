# An rpc (observer) node syncs from genesis, relays transactions and survives a restart.
add_rpc
wait_synced rpc
has_role rpc observer || fail "rpc is $(role rpc), want observer"
to=0x$(od -An -N20 -tx1 /dev/urandom | tr -d ' \n')
cast send --rpc-url "$(url rpc)" --private-key "$ADMIN_KEY" --value 1gwei "$to" > /dev/null || fail "transfer via rpc failed"
landed() { [[ $(cast balance "$to" --rpc-url "$(url validator-1)") == 1000000000 ]]; }
wait_for 30 "the transfer to reach validator-1" landed
log "transfer via rpc landed"
stop_node rpc
wait_blocks validator-1 5
start_node rpc
wait_synced rpc
check_consistency
