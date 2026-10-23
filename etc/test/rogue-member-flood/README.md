# Live reproduction: unthrottled transaction-topic flood

This directory reproduces, on a real local network, the issue where any peer can flood the
transaction gossip topic (`rayls-batch`) with oversized messages that validators accept, forward to
the whole mesh, and process, with no penalty and no rate limit.

Every other gossip topic rejects a message over `max_gossip_message_size` (12 KB) and penalizes the
sender. The transaction topic skips that size check, so a message is capped only by the gossipsub
transport limit of 2 MiB, about 170 times the normal cap. The topic is also subscribed with no
authorized-publisher set, so a signed message from any connected peer is accepted. An accepted
message is forwarded across the mesh, and the validator that owns the batch decodes and
signature-checks every transaction in it. Nothing penalizes the sender, and nothing limits the rate.

## The flood hook

The attack needs a peer that publishes oversized messages on the transaction topic in a loop. That
behaviour lives in `crates/consensus/worker/src/network/txn_flood.rs` and is compiled only with the
`txn-flood-hook` cargo feature, so a normal build never contains it. It is armed per process by
environment, so a single node floods while the rest stay honest:

- `RL_FLOOD=1` arms the flood. Unset, the node behaves normally.
- `RL_FLOOD_PAYLOAD=<path>` is a file holding the encoded `WorkerGossip` bytes to publish.
- `RL_FLOOD_INTERVAL_MS=<n>` is the delay between publishes (default 100; 0 is as fast as possible).
- `RL_FLOOD_WARMUP_SECS=<n>` is the wait before the first publish (default 10).

The payload files are produced by an ignored test that reuses the production transaction factory and
`encode(WorkerGossip::Txn(..))`, so the bytes are exactly what a node decodes:

- `filler.bin`: about 2 MiB of undecodable filler. Every validator accepts and forwards it; the
  owner discards each entry after a failed decode. Shows the accept-forward amplification and the
  absence of any penalty.
- `txns.bin`: many valid signed transactions, all from one sender so one validator owns the whole
  batch and runs signature recovery on every entry. Shows the per-validator CPU cost.

## Run it

From this checkout, with a Rust toolchain and `curl` available:

```
./etc/test/rogue-member-flood/run.sh
```

The script is self-contained. It builds the node binary with `--features txn-flood-hook`, generates
the payload files, generates keys and genesis for a fresh local network under
`target/rogue-member-flood/<timestamp>/`, starts the nodes, drives the scenario, and cleans up the
processes on exit. It needs no `.env` and no prior setup.

What it does:

1. Starts a committee of validators (default 5) and lets them reach block 20.
2. Records a baseline window: block-production rate, per-node CPU, and loopback bandwidth.
3. Restarts the last validator armed to flood the transaction topic with the chosen payload, with no
   rate limit.
4. Records a flood window with the same measurements.
5. Checks each honest validator's log for any applied penalty, ban, or graylist against the sender.

Options: `-n` validators, `-p` payload (`txns` or `filler`), `-w` window seconds, `-r` publish
interval ms, `-i` first instance number (RPC ports), `-b` a prebuilt binary, `-d` work directory.
See `run.sh -h`.

## Exit status

- `0`: the flood had no measurable effect, or the sender was penalized. The bug is absent or fixed.
- `1`: the flood was accepted, forwarded, and processed with no penalty and a measurable cost (a
  CPU rise on the owning validator, a loopback-bandwidth rise, or a block-rate drop). The issue is
  reproduced.
- `2`: the run could not start or was inconclusive. The work directory holds the logs.

## The attacker does not need a committee seat

The script arms a committee validator because that is the simplest node to add, and a seated member
is always mapped and never penalized, which isolates the accept-forward-process behaviour. The
attack does not depend on a seat: any peer the validators connect to and have a BLS mapping for
reaches the same per-validator processing path, and the mesh-forwarding cost needs no mapping at
all. To flood from a non-committee node instead, generate an observer with a dialable address, copy
in the genesis files, and start it with the same `RL_FLOOD_*` environment.

## Unit reproduction

The same gaps are pinned by unit tests that run in CI with plain `cargo test`, without a live
network, in `crates/consensus/network/src/tests/`:

```
cargo test -p rayls-consensus-network --lib -- \
  test_txn_topic_accepts_oversized_gossip_from_any_publisher \
  test_gossipsub_peer_scoring_is_disabled \
  test_committee_member_connections_are_unbounded \
  test_ordinary_peer_extra_connections_are_not_counted
```
