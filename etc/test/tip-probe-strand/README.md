# Live reproduction: one validator strands a following node by returning a forged header to its tip probe

This directory reproduces, on a real local network, the issue where one committee validator stops a
following node from finding the chain tip.

A node that follows the chain without voting (an observer, or a validator not in the active
committee) learns the latest block in two ways. It listens to gossip, and when gossip is quiet it
asks every committee member for its latest consensus header and keeps the answer with the highest
block number. Nothing checks that answer before it is picked, so one validator that returns a forged
header numbered `u64::MAX` wins every probe, even when every other validator answers honestly. The
follower then fetches that header, waits out a committee for the claimed epoch it will never get,
and never reaches the real tip.

## The attacker hook

The attack needs a validator that answers the latest-header probe with a forged header. That
behaviour lives in `crates/consensus/primary/src/network/tip_probe_byzantine.rs` and is compiled
only with the `byzantine-test-hooks` cargo feature, so a normal build never contains it. It is
armed per process by environment, so a single node in the network returns a forged header while
the rest stay honest:

- `RL_BYZ_FAKE_TIP=1`: answer a latest-header request (no number, no hash) with a forged header
  numbered `u64::MAX` whose leader claims epoch `u32::MAX`, and answer a by-hash request for that
  forged header's digest with the same header.

## Run it

From a fresh clone, with a Rust toolchain and `curl` available:

```
./etc/test/tip-probe-strand/run.sh
```

The script is self-contained. It builds the node binary with `--features byzantine-test-hooks`,
generates keys and genesis for a fresh local network under `target/tip-probe-strand/<timestamp>/`,
starts the nodes, drives the scenario, and cleans up the processes on exit. It needs no `.env` and
no prior setup.

What it does:

1. Starts four validators and lets them reach block 20.
2. Stops two of the four validators. Only the adversary and one honest validator remain, which is
   below the quorum of three, so consensus halts and no new output is gossiped. The probe is now the only
   way for a fresh follower to learn the tip.
3. Phase 1 (adversarial): restarts the adversary with `RL_BYZ_FAKE_TIP=1`, starts a fresh
   observer, and watches whether it reaches the tip.
4. Phase 2 (control): restarts the adversary honest, starts a fresh observer, and watches again.

## Exit status

- `0`: the observer reached the tip in both phases. The bug is absent or fixed.
- `1`: the observer never reached the tip in the adversarial phase but did in the control. The
  issue is reproduced. The summary prints the block each phase reached, and two lines from the
  adversarial-phase observer log: the probe that followed the forged header
  (`peer_latest=18446744073709551615`) and the walk stalling on the claimed epoch
  (`timed out waiting for committee keys`).
- `2`: the run could not start or was inconclusive (for example the control also failed to reach
  the tip, or consensus did not halt). The work directory holds the logs.

On current code the script exits 1. A stranded observer does not recover on its own while the
adversary is present: every probe, ten seconds apart, picks the same forged answer.

## The attacker does not need a committee seat for the forged answer

The script arms a validator because the probe only asks committee members. Any committee member can
answer the probe with the forged header, with no transaction, gas, or signature from another party.

## Unit reproduction

The same defect is covered by unit tests that run in CI with plain `cargo test`, without a live
network, in `crates/consensus/state-sync/src/consensus.rs`:

```
cargo test -p rayls-consensus-state-sync --lib -- \
  tip_probe_is_not_captured_by_one_validator_inflating_the_number \
  forged_tip_probe_does_not_strand_the_follower_or_poison_the_epoch_watch
```

On current code these two tests fail, each for the reason stated in its doc comment.
