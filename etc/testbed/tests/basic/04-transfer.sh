# A native transfer submitted through each node lands, and every node sees the final balance.
to=0x$(od -An -N20 -tx1 /dev/urandom | tr -d ' \n')
nonce=$(cast nonce "$ADMIN" --rpc-url "$(url validator-1)")
sent=0
for n in $(nodes); do
    cast send --rpc-url "$(url "$n")" --private-key "$ADMIN_KEY" --nonce $((nonce + sent)) \
        --value 1gwei "$to" > /dev/null || fail "transfer via $n failed"
    sent=$((sent + 1))
    log "transfer $sent mined via $n"
done
want=$((sent * 1000000000))
balance_is() { [[ $(cast balance "$to" --rpc-url "$(url "$1")") == "$want" ]]; }
for n in $(nodes); do wait_for 30 "$n to show balance $want" balance_is "$n"; done
log "all nodes see balance $want wei"
