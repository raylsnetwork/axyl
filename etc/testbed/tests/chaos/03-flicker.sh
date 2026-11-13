# Flickering validator: 5 short freeze/resume cycles; the chain keeps going and the node converges.
v=$(validators | tail -1)
for i in 1 2 3 4 5; do
    pause_node "$v"
    wait_blocks validator-1 2
    resume_node "$v"
    sleep 2
done
wait_synced "$v"
check_consistency
