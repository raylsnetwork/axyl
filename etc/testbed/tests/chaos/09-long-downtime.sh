# A validator down across 2 epoch boundaries syncs the missed epochs and rejoins the committee.
v=$(validators | tail -1)
e=$(epoch validator-1)
stop_node "$v"
wait_for $((EPOCH_SECS * 4)) "epoch $((e + 2))" at_epoch validator-1 $((e + 2))
start_node "$v"
wait_synced "$v"
has_role "$v" active_cvv || fail "$v is $(role "$v"), want active_cvv"
check_consistency
