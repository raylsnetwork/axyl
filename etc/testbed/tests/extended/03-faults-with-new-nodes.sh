# With validator-5 and the rpc node in place: a frozen validator is tolerated, quorum loss halts and recovers.
[[ -d $DATA/validator-5 && -d $DATA/rpc ]] || skip "needs validator-5 and rpc (extended/01, extended/02)"
source "$TESTBED/tests/chaos/01-pause.sh"
source "$TESTBED/tests/chaos/02-lose-quorum.sh"
