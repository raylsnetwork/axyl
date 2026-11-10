# With validator-5 and the extra rpc node in place: a frozen validator is tolerated, quorum loss halts and recovers.
[[ -d $DATA/validator-5 ]] || skip "needs validator-5 (extended/01)"
source "$TESTBED/tests/chaos/01-pause.sh"
source "$TESTBED/tests/chaos/02-lose-quorum.sh"
