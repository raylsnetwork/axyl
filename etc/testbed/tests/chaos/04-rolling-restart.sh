# Restart every validator one after another; the chain never stops.
for v in $(validators); do
    ref=$(validators | grep -vx "$v" | head -1)
    stop_node "$v"
    wait_blocks "$ref" 3
    start_node "$v"
    wait_synced "$v"
done
check_consistency
