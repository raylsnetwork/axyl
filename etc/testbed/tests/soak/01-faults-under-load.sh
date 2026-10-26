# Soak: steady load while a random node is stopped, killed or frozen each round, for SOAK_MINUTES (default 480).
[[ $LOAD == 1 ]] || { start_load; trap stop_load EXIT; }
end=$((SECONDS + ${SOAK_MINUTES:-480} * 60))
round=0
actions=(stop kill pause)
while ((SECONDS < end)); do
    round=$((round + 1))
    # validator-1 carries the load and is the reference, so every other node is fair game
    victims=($(nodes | grep -vx validator-1))
    v=${victims[RANDOM % ${#victims[@]}]}
    a=${actions[RANDOM % 3]}
    nonce=$(load_nonce) || fail "could not read the load account nonce"
    log "round $round $(date '+%F %T'): $a $v at block $(height validator-1), epoch $(epoch validator-1)"
    "${a}_node" "$v"
    wait_blocks validator-1 5 120
    sleep $((RANDOM % 30))
    if [[ $a == pause ]]; then resume_node "$v"; else start_node "$v"; fi
    wait_synced "$v"
    check_consistency
    now=$(load_nonce) || fail "could not read the load account nonce"
    ((now > nonce)) || fail "no load transactions landed during round $round"
    sleep "${SOAK_INTERVAL:-60}"
done
log "$round rounds, no fork or stall"
