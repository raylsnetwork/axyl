# Kill and restart the same validator 3 times in quick succession; it still recovers.
v=$(validators | tail -1)
for i in 1 2 3; do
    kill_node "$v"
    start_node "$v"
    sleep 3
done
wait_synced "$v"
check_consistency
