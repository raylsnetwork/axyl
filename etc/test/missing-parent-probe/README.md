# Live reproduction: a parent certificate lost from gossip waits for the 60 s idle probe

This directory reproduces, on a real local network, the issue where a node missing a parent
certificate does not ask its peers for it until the certificate fetcher's idle probe, which runs
once a minute.

With exactly a quorum of validators up, every round needs the certificate of every live
validator. If one validator's certificates for a round are lost by gossip in both directions, no
live node holds a quorum of that round, nobody proposes the next round, and the proposers only
re-propose their last header every max header delay. Nothing asks peers for the missing
certificates until the idle probe, so the whole network stops committing for up to a minute.

## The test hook

The loss is played by a validator that drops one round of certificate gossip, sent and received.
That behaviour lives in `crates/consensus/primary/src/test_hooks.rs` (called from
`network/mod.rs` `publish_certificate` and `network/handler.rs` `process_gossip`) and is compiled
only with the `cert-gossip-drop-hook` cargo feature, so a normal build never contains it. It is
armed per process by environment:

- `RAYLS_TEST_CERT_GOSSIP_DROP_FILE=<path>`: when the file appears, the node removes it and drops
  every certificate gossip, sent or received, of the second round after the one it is handling.

## Run it

From a fresh clone, with a Rust toolchain, `curl` and `bc` available:

```
./etc/test/missing-parent-probe/run.sh
```

The script is self-contained. It builds the node binary with `--features cert-gossip-drop-hook`,
generates keys and genesis for a fresh local network (1 s max header delay) under
`target/missing-parent-probe/<timestamp>/`, starts the nodes, drives the scenario, and stops the
processes it started on exit. It needs no `.env` and no prior setup.

What it does:

1. Starts five validators, validator 3 armed with the hook, and lets the chain reach consensus
   header 10.
2. Stops validator 5, so exactly a quorum is left, and waits for 5 more headers.
3. Three times: creates the trigger file, waits for the hook to arm, then follows the chain until
   it commits 10 more headers, recording the longest time without a new header and the number of
   certificates the hook dropped.

Options: `-n` validators, `-f` losses, `-g` longest allowed gap in seconds, `-i` first instance
number (RPC ports), `-b` a prebuilt binary, `-d` work directory. See `run.sh -h`.

## Exit status

- `0`: the chain resumed within the allowed gap (30 s) after every loss. The bug is absent or
  fixed.
- `1`: after at least one loss the chain went more than 30 s without a new header. The issue is
  reproduced.
- `2`: the run could not start or was inconclusive (including a hook that never armed or dropped
  nothing, or a chain stuck for 180 s). The work directory holds the logs and `summary.txt`.

On current code the script exits 1, with gaps of about 50 s. A run takes about 4 minutes
plus the build.

## Unit reproduction

The same defect is covered by a regression test that runs with plain `cargo test`, without a live
network or the hook, in `crates/consensus/primary/src/tests/proposer_tests.rs`:

```
cargo test -p rayls-consensus-primary --lib -- parent_lost_from_gossip_is_fetched_within_a_few_max_header_delays
```

On current code it fails: the lost certificate is first fetched by the idle probe, about 64 s
(virtual time) after the loss.
