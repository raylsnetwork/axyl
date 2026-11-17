# A 5th validator is staked and activated on-chain, joins the committee and keeps up with it.
# A validator-5 whose stake was never sent is set aside, so the rest of the run keeps the 4-validator
# network. Once sent, the stake can land at any time, so it stays. An interrupt sets nothing aside.
trap 'rc=$?; [[ $rc == 0 || $rc == 130 || $rc == 143 || -f $DATA/validator-5/stake-sent || $(v5_status) != 0 ]] ||
    set_aside validator-5' EXIT
add_validator
wait_for $((EPOCH_SECS * 6)) "validator-5 to become active_cvv" has_role validator-5 active_cvv
wait_synced validator-5
e=$(epoch validator-1)
wait_for $((EPOCH_SECS * 2)) "epoch $((e + 1))" at_epoch validator-1 $((e + 1))
size=$(committee_size "$e")
((size == 5)) || fail "committee of epoch $e has $size members, want 5"
log "committee of epoch $e has 5 members"
check_consistency
