# Frozen validator (SIGSTOP, a stand-in for a partition): the chain continues, the node catches up on resume.
v=$(validators | tail -1)
pause_node "$v"
wait_blocks validator-1 10
resume_node "$v"
wait_synced "$v"
check_consistency
