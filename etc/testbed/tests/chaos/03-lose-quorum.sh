# Stopping enough validators to break quorum halts the chain; restarting them resumes it without a fork.
down=$(($(validators | wc -l) - $(quorum) + 1))
victims=$(validators | tail -n "$down")
for v in $victims; do stop_node "$v"; done
log "stopped" $victims
assert_stalled validator-1 10
for v in $victims; do start_node "$v"; done
wait_blocks validator-1 5 120
for v in $victims; do wait_synced "$v"; done
check_consistency
