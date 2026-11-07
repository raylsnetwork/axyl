# Hard crash (SIGKILL) of one validator: the chain continues and the node recovers on restart.
v=$(validators | tail -1)
kill_node "$v"
wait_blocks validator-1 5
start_node "$v"
wait_synced "$v"
check_consistency
