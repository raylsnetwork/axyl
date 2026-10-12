# Live reproduction: single-peer gap-fetch stall and fork

This directory reproduces, on a real local network, the issue where one connected peer forks or
stalls a following node through the by-number consensus gap fetch.

An observer or inactive validator follows the chain with a forward streamer. When a consensus
header is missing locally, the streamer fetches it by block number from a peer. The reply is
accepted after a check that only proves the reply's number and parent link, not that it is the real
header the committee produced. A single peer can answer with a crafted header. With a real
quorum-signed leader but a raised commit timestamp, the reply passes every later check, including the
signature re-verify before execution, yet executes to a different block, so the follower's state
forks from the rest of the network.

## The attacker hook

The attack needs a peer that answers a by-number consensus-header request with a crafted header.
That behaviour lives in `crates/consensus/primary/src/network/gap_fetch_byzantine.rs` and is
compiled only with the `byzantine-test-hooks` cargo feature, so a normal build never contains it.
It is armed per process by environment, so a single node in the network lies while the rest stay
honest:

- `RL_BYZ_GAP_TARGET=<N>` or `<A-B>`: the block number, or inclusive range, to lie about.
- `RL_BYZ_GAP_MODE=fork`: serve the real header with its commit timestamp raised (default).
- `RL_BYZ_GAP_MODE=stall`: serve a fabricated header with the real parent link.
- `RL_BYZ_GAP_BUMP=<secs>`: the commit-timestamp increase for fork mode (default 1000000).

## Run it

From a fresh clone, with a Rust toolchain and `curl` available:

```
./etc/test/gap-fetch-stall/run.sh
```

The script is self-contained. It builds the node binary with `--features byzantine-test-hooks`,
generates keys and genesis for a fresh local network under `target/gap-fetch-stall/<timestamp>/`,
starts the nodes, drives the scenario, and cleans up the processes on exit. It needs no `.env` and
no prior setup.

What it does:

1. Starts four validators and one observer and lets them reach block 20.
2. Stops the observer to open a gap, then lets the validators build 50 more blocks.
3. Arms validator 1 for the gap range and restarts it, so one committee peer now answers the
   observer's by-number gap fetches for those numbers with a real leader and a raised commit
   timestamp.
4. Restarts the observer. As it catches up it gap-fetches the missing numbers, and for the numbers
   it asks the armed validator it receives the forged header.
5. Checks whether the observer executes a block whose timestamp is exactly the bump above the
   committee's block of the same number, and whether its executed tip then stalls at that block
   while the committee keeps producing.

Options: `-n` validators, `-w` watch seconds, `-g` gap size, `-i` first instance number (RPC
ports), `-b` a prebuilt binary, `-d` work directory. See `run.sh -h`.

## Exit status

- `0`: the observer stayed on the canonical chain. The bug is absent or fixed.
- `1`: the observer executed a forged block and forked. The issue is reproduced. The summary prints
  the forked block number, the observer and canonical hashes and timestamps, the timestamp
  difference equal to the bump, and the attacker's log line.
- `2`: the run could not start or was inconclusive. The work directory holds the logs.

On current code the script exits 1. The forked observer does not recover on its own, and a plain
restart does not heal it, because its executed anchor is now the forged block.

## The attacker does not need a committee seat

The script arms a validator because that is the simplest node to add to a committee, but the attack
does not depend on it. Any node the victim connects to can answer the gap fetch, whether or not it
is a validator. To arm a plain observer instead, generate a second observer with a dialable address
(so the victim discovers and connects to it) rather than `--advertise-identity-only`, copy in the
genesis files, and start it with the same `RL_BYZ_GAP_*` environment.

## Unit reproduction

The same defect is covered by unit tests that run in CI with plain `cargo test`, without a live
network, in `crates/consensus/state-sync/src/tests.rs`:

```
cargo test -p rayls-consensus-state-sync --lib -- \
  test_forward_streamer_stalls_on_promoted_gap_fetch_fake \
  test_forward_streamer_stalls_via_closed_epoch_reply_after_fake \
  test_gap_fetch_forwards_bls_valid_tampered_subdag
```

On current code these three tests fail, each for the reason stated in its doc comment.
