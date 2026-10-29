# Live reproduction: state-sync walk never republishes the covered anchor

This directory reproduces, on a real local network, the issue where a validator that catches up
across an epoch boundary stays inactive forever and halts a network that has exactly a quorum up.

The returning validator's first catch-up leg caches every missing consensus header and executes up
to the end of its old epoch. The epoch transition then resets its `last_consensus_header` watch to
the default header (number 0). In the new epoch the peer probe reports the network tip again, which
is the top of the headers already cached (the walk's covered anchor). The walk starts at the anchor,
stops at once and publishes nothing, so the watch stays at 0. The forward streamer and the rejoin
check learn the head only from that watch, so the node never executes the cached headers and stays
`inactive_cvv`. With exactly a quorum up the other validators cannot produce a new header without
it, and a new header is the only thing that would publish the head: the network halts.

No test hook is needed: the fault is an honest crash and restart.

## Run it

From a fresh clone, with a Rust toolchain, `curl` and `jq` available:

```
./etc/test/statesync-anchor-republish/run.sh
```

The script is self-contained. It builds the node binary, generates keys and genesis for a fresh
local network under `target/statesync-anchor-republish/<timestamp>/`, starts the nodes, drives the
scenario, and cleans up the processes on exit. It needs no `.env` and no prior setup.

What it does:

1. Starts five validators with 30 s epochs and lets the chain reach header 10.
2. Kills validator 3 (SIGKILL) and keeps it down for 40 s (past the 30 s idle connection timeout,
   so no stale connection is involved) and until the network has crossed an epoch boundary.
3. Restarts validator 3 and stops validator 5 as soon as validator 3 serves RPC, leaving exactly a
   quorum (4 of 5) that includes validator 3.
4. Requires the chain to commit 20 more headers within 120 s.
5. On a stall, checks validator 3's role and its log since the restart for the signature:
   "derived walk coverage from cached headers anchor=N", then a probe with
   "current=0 peer_latest=N", and no "notifying watchers" after it.
6. Otherwise restarts validator 5, waits for both returned nodes to catch up, and injects the next
   fault, up to 6.

A stall without the signature belongs to another defect: it is logged, the network is recovered,
and the next fault runs; it is not counted as a reproduction.

Options: `-f` faults, `-o` offline seconds, `-e` epoch seconds, `-w` progress window, `-k` headers
to commit, `-i` first instance number (RPC ports), `-b` a prebuilt binary, `-d` work directory.
See `run.sh -h`.

## Exit status

- `0`: no fault showed the signature. The bug is absent or fixed.
- `1`: a fault halted the network with the signature. The issue is reproduced. The summary prints
  every node's status (validator 3 `inactive_cvv`) and the victim log lines with the anchor.
- `2`: the run could not start or was inconclusive. The work directory holds the logs.

On current code the script exits 1, usually on the first fault.

## Unit reproduction

The same defect is covered by a unit test that runs in CI with plain `cargo test`, in
`crates/consensus/state-sync/src/consensus.rs`:

```
cargo test -p rayls-consensus-state-sync --lib walk_starting_at_the_covered_anchor_publishes_it
```

On current code it fails: the watch reads 0 instead of the covered anchor 102.
