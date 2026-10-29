# Live reproduction: vote refused by a peer behind our epoch is never asked again

This directory reproduces, on a real local network, the issue where a header vote refused by a
peer still in the previous epoch is never requested again.

A proposer asks every other validator for a vote on its header. A peer that has not yet closed the
previous epoch refuses (a recoverable `Invalid epoch` error one epoch behind, `EpochMismatch`
further behind), and the certifier stops asking that peer for the rest of the proposal. The
proposal stays open while its vote task to a validator that is down keeps retrying, about four
minutes, and a re-proposal of the same header is deduplicated. With exactly a quorum up, the late
peer's vote is needed, so no header of the new epoch certifies and the chain stops committing
until that task gives up.

## The test hook

The late peer is played by a validator that closes every epoch a few seconds after its peers. That
behaviour lives in `crates/middleware/orchestrator/src/epoch_manager/transition.rs` and is
compiled only with the `test-hooks` cargo feature, so a normal build never contains it. It is
armed per process by environment:

- `RL_TEST_LATE_EPOCH_CLOSE_MS=<ms>`: after committing the boundary, keep running the closing
  epoch this long before shutting it down. In that window the node refuses the next epoch's vote
  requests as a peer one epoch behind does.

With all validators up this costs nothing: the others certify without the late one.

## Run it

From a fresh clone, with a Rust toolchain and `curl` available:

```
./etc/test/vote-retry-behind-epoch/run.sh
```

The script is self-contained. It builds the node binary with `--features test-hooks`, generates
keys and genesis for a fresh local network with 30 s epochs under
`target/vote-retry-behind-epoch/<timestamp>/`, starts the nodes, drives the scenario, and stops
the processes it started on exit. It needs no `.env` and no prior setup.

What it does:

1. Starts five validators, validator 3 armed with `RL_TEST_LATE_EPOCH_CLOSE_MS=5000`, and lets the
   chain reach consensus header 10.
2. Ten seconds into an epoch, kills validator 5 (SIGKILL), so exactly a quorum is left and the
   late validator's vote is needed.
3. Requires the chain to commit 10 headers of the next epoch within 90 s of that epoch's start,
   and counts what the proposers logged since the kill: `stopping vote requests for stale header`
   or `ignoring epoch rejection from stale peer` (gave up on a refusal) and `retrying vote
   request` (retried it).
4. Restarts validator 5 past the 30 s connection idle timeout (so a stale connection cannot be
   the cause) and waits for it to catch up before the next trial.

Options: `-n` validators, `-t` trials (boundaries), `-l` late close in ms, `-e` epoch seconds,
`-p` deadline seconds, `-i` first instance number (RPC ports), `-b` a prebuilt binary, `-d` work
directory. See `run.sh -h`.

## Exit status

- `0`: the chain kept committing across every boundary. The bug is absent or fixed.
- `1`: the chain stalled at a boundary after the proposers gave up on the late validator's
  refusals. The issue is reproduced.
- `2`: the run could not start or was inconclusive (including a stall with no refusal given up,
  or a binary without the hook). The work directory holds the logs and `summary.txt`.

On current code the script exits 1. A run takes about 3 minutes plus the build.

## Unit reproduction

The same defect is covered by regression tests that run with plain `cargo test`, without a live
network or the hook, in `crates/consensus/primary/src/tests/certifier_tests.rs`:

```
cargo test -p rayls-consensus-primary --lib -- vote_request_to_a_peer
```

On current code both tests fail with `the lagging peer was not asked again once it caught up`.
