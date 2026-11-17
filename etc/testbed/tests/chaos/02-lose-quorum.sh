# Stopping enough committee members to break quorum halts the chain; restarting them resumes it without a fork.
members=$(committee) && [[ -n $members ]] || fail "could not read the committee from the registry"
n=$(echo "$members" | wc -l)
# a quorum is 2n/3+1 of the n members, so stopping n - 2n/3 of them leaves one vote short
victims=$(echo "$members" | tail -n $((n - 2 * n / 3)))
for v in $victims; do stop_node "$v"; done
log "stopped" $victims
assert_stalled validator-1 10
for v in $victims; do start_node "$v"; done
wait_blocks validator-1 5 120
for v in $victims; do wait_synced "$v"; done
check_consistency
