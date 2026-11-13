# A 5th validator is staked and activated on-chain, joins the committee and keeps up with it.
# a validator-5 that never got staked is set aside, so the rest of the run keeps the 4-validator
# network; once staked it is on-chain and stays
trap '[[ $? == 0 || $(v5_status) != 0 ]] || set_aside validator-5' EXIT
add_validator
wait_for $((EPOCH_SECS * 6)) "validator-5 to become active_cvv" has_role validator-5 active_cvv
wait_synced validator-5
e=$(epoch validator-1)
wait_for $((EPOCH_SECS * 2)) "epoch $((e + 1))" at_epoch validator-1 $((e + 1))
size=$(committee_size "$e")
((size == 5)) || fail "committee of epoch $e has $size members, want 5"
log "committee of epoch $e has 5 members"
check_consistency
