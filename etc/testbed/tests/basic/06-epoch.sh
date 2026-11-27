# The chain crosses an epoch boundary and serves the certified record of the closed epoch.
e=$(epoch validator-1)
wait_for $((EPOCH_SECS * 3)) "epoch $((e + 1))" at_epoch validator-1 $((e + 1))
rpc validator-1 rayls_epochRecord "[$e]" | grep -q '"result"' || fail "no epoch record for epoch $e"
log "epoch $e closed, record served"
