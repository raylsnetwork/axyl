# Graceful restart of one validator: the rest keep producing blocks, it rejoins and catches up.
v=$(victim)
stop_node "$v"
wait_blocks validator-1 5
start_node "$v"
wait_synced "$v"
check_consistency
