# Crash a validator while it is catching up after downtime; it still recovers.
v=$(validators | tail -1)
stop_node "$v"
wait_blocks validator-1 30
start_node "$v"
sleep 3
kill_node "$v"
start_node "$v"
wait_synced "$v"
check_consistency
