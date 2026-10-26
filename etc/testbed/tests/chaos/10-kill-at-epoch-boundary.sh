# Crash a validator right after an epoch boundary; it recovers into the new epoch.
v=$(validators | tail -1)
e=$(epoch validator-1)
wait_for $((EPOCH_SECS * 2)) "epoch $((e + 1))" at_epoch validator-1 $((e + 1))
kill_node "$v"
wait_blocks validator-1 5
start_node "$v"
wait_synced "$v"
check_consistency
