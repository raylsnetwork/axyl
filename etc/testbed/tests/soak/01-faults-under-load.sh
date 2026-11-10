# Soak: steady load (with occasional nonce collisions) while a random node is stopped or frozen each round, for SOAK_MINUTES (default 480).
tps_setup || skip "tps-checker not available; clone github.com/raylsnetwork/tps-checker next to this repo"
start=$(load_nonce) || fail "could not read the load account nonce"
if [[ $LOAD != 1 ]]; then
    start_load
    trap stop_load EXIT
fi
nonce_above() { local n; n=$(load_nonce) && ((n > $1)); }
wait_for 300 "the load to land its first transactions" nonce_above "$start"
end=$((SECONDS + ${SOAK_MINUTES:-480} * 60))
# the first rpc node is the reference (what a user sees) and is never faulted; SOAK_VICTIMS narrows the
# targets, e.g. SOAK_VICTIMS=validator-5 keeps bouncing only the added validator
ref=$(ref_node)
victims=(${SOAK_VICTIMS:-$(nodes | grep -vx "$ref")})
round=0
actions=(stop pause)
while ((SECONDS < end)); do
    round=$((round + 1))
    v=${victims[RANDOM % ${#victims[@]}]}
    a=${actions[RANDOM % 2]}
    nonce=$(load_nonce) || fail "could not read the load account nonce"
    log "round $round $(date '+%F %T'): $a $v at block $(height "$ref"), epoch $(epoch "$ref")"
    "${a}_node" "$v"
    wait_blocks "$ref" 5 120
    sleep $((RANDOM % 30))
    if [[ $a == pause ]]; then resume_node "$v"; else start_node "$v"; fi
    wait_synced "$v"
    check_consistency
    now=$(load_nonce) || fail "could not read the load account nonce"
    ((now > nonce)) || fail "no load transactions landed during round $round"
    sleep "${SOAK_INTERVAL:-60}"
done
log "$round rounds, no fork or stall"
