# Soak: stop a validator once it is caught up, restart it, repeat (CYCLES, default 3).
v=$(validators | tail -1)
for ((i = 1; i <= ${CYCLES:-3}; i++)); do
    wait_synced "$v"
    stop_node "$v"
    start_node "$v"
    log "cycle $i done"
done
wait_synced "$v"
check_consistency
