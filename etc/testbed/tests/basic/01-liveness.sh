# Every node is up, answers RPC and sees the chain grow.
for n in $(nodes); do
    running "$n" || fail "$n is down"
    wait_blocks "$n" 3
done
