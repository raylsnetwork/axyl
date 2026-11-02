# Live reproduction: crash when syncing across a committee change

This directory reproduces, on a real local network, the issue where a node that catches up across
an epoch in which a validator joined the committee crashes, and crashes again on every restart.

A syncing node's subscriber keeps the committee of the epoch it started in. If it is handed a
commit of the next epoch, it verifies the commit with that epoch's keys, then looks up each
certificate's author in its own, older committee to find the block-reward address. The new
validator is not in it, the lookup fails with `UnexpectedAuthority`, and the subscriber panics
(`subscriber catch-up failed fatally: Consensus referenced unexpected authority`). Release builds
abort on panic, so the node exits. On restart it is still in the old epoch, reaches the same
commit, and exits again.

## Why it needs two faults

With honest proposers the subscriber is never handed such a commit. The header streamer releases
a commit only once the node has executed the block its leader's header is anchored on
(`latest_execution_block`). Honest headers of a new epoch are anchored on the previous epoch's
closing block or later, and the node executes that block only after it has stopped the previous
epoch's subscriber.

The harness opens that path with two test-only faults from the `fault-injection-test-hooks`
build. Neither exists in a production build, and both are armed by environment per process:

- **A faulty proposer** (`RL_FAULT_STALE_ANCHOR_ROUNDS`, set on every committee member except the
  victim): headers of the first rounds of each epoch are anchored on an older block. Voters check
  only that an anchor does not diverge from their recent executed blocks, not that it is recent,
  so these headers are certified. Whichever validator leads the new epoch's first commit is
  faulty, and the streamer releases that commit before the victim leaves the old epoch.
- **A slow victim** (`RL_FAULT_TEARDOWN_DELAY_MS`, set on the victim only): at each epoch boundary
  the victim waits before stopping the epoch's consensus tasks, as with a slow checkpoint write or
  a stalled task. Without it the old subscriber is usually stopped a few milliseconds before the
  commit arrives.

## Run it

From a fresh clone, with a Rust toolchain, `curl` and Foundry's `cast` available:

```
./etc/test/committee-change-sync-crash/run.sh               # a validator catching up to rejoin
./etc/test/committee-change-sync-crash/run.sh -m observer   # an observer following the chain
./etc/test/committee-change-sync-crash/run.sh -s 0          # honest proposers (stays healthy)
```

The script is self-contained. It builds the node binary with the hooks and `panic=abort` (as in
release) under `target/panic-abort/`, generates keys and genesis for a fresh local network under
`target/committee-change-sync-crash/<timestamp>/`, starts the nodes, drives the scenario, and
cleans up the processes on exit. It needs no `.env` and no prior setup. Pass `-b <binary>` to skip
the build; a binary built with the default `panic=unwind` keeps running after the panic, which the
script reports separately.

What it does:

1. Starts four genesis validators and node5, a post-genesis node that follows the chain. In
   `observer` mode it also starts an observer. Epochs last 60 s (`-e`).
2. Starts a stream of transfers (`-l` senders), so commits carry batches: the subscriber looks a
   certificate's author up only for a commit that has batches.
3. Stakes and activates node5 through the on-chain registry. When validator 1 schedules node5 for
   epoch N, waits for epoch N-1 to start and stops the victim (validator 4, or the observer), so
   the committee change is the first boundary the victim crosses when it catches up. In
   `observer` mode it also stops validator 4 for good: a quorum of the new committee (4 of 5) then
   needs node5, so every commit of epoch N carries node5's certificates, as it does in
   `validator` mode.
4. Waits until node5 is seated in epoch N and 20 more blocks are committed.
5. Restarts the victim three times (`-r`), as a supervisor would. Each time it waits up to 180 s
   for the victim either to exit or to reach the chain height.

## Reading the result

- Exit 1, `REPRODUCED`: the victim's subscriber panicked on the new validator's commit. The
  summary says whether it exited on every restart. Each `<victim>.restart-<n>.log` in the run
  directory shows `Authority missing from committee` followed by the panic.
- Exit 0, `HEALTHY`: the victim caught up past the committee change.
- Exit 2, `INCONCLUSIVE`: the network did not start, node5 was never seated, the victim exited for
  another reason, or it neither exited nor caught up.

Logs are `<node>.log` in the run directory, with a `summary.txt` of the run.

The same failure runs in-process, with no network, as two regression tests:

```
cargo test -p rayls-middleware-bridge --test it committee_change
```
