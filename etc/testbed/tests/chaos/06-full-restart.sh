# Stop every node and start them again: the chain resumes from where it stopped, without a fork.
h=$(height validator-1)
stop_all
for n in $(nodes); do ! rpc "$n" eth_blockNumber > /dev/null || fail "$n still answers after stop"; done
start_all
wait_for 120 "the chain to pass block $h" at_height validator-1 $((h + 3))
for n in $(nodes); do wait_synced "$n"; done
check_consistency
