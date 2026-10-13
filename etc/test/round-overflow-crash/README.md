# Live reproduction: one missing-certificates request crashes a validator

This directory reproduces, on a real local network, the issue where one crafted
missing-certificates request with the highest round number and an empty skip bitmap aborts a
release-build validator.

## The attacker hook

The attack needs a committee member that sends a crafted `MissingCertificatesRequest` to its peers.
That behaviour lives in `crates/consensus/primary/src/round_overflow_byzantine.rs` and is compiled
only with the `byzantine-test-hooks` cargo feature, so a normal build never contains it. It is
armed per process by environment:

- `RL_ROUND_OVERFLOW_ATTACK=1`: enable the hook (inert otherwise).
- `RL_ROUND_OVERFLOW_DELAY=<s>`: seconds to wait after startup before firing (default 20).

When armed, the node sends a single `MissingCertificatesRequest` with `exclusive_lower_bound =
u32::MAX` and one authority carrying an empty roaring skip bitmap to every committee peer in
parallel. On each victim the request reaches `certificate_store.rs:next_round_number` where
`round + 1` overflows `Round` (u32). Release builds check overflow and abort.

## Run it

From a fresh clone, with a Rust toolchain and `curl` available:

```
./etc/test/round-overflow-crash/run.sh
```

The script is self-contained. It builds the node binary with `--features byzantine-test-hooks` in
release mode, generates keys and genesis for a fresh local network, starts the nodes, drives the
scenario, and cleans up the processes on exit. It needs no `.env` and no prior setup.

What it does:

1. Starts four validators and one observer and lets them reach block 10.
2. Stops one validator and restarts it with `RL_ROUND_OVERFLOW_ATTACK=1`.
3. After 20 seconds the armed validator sends the crafted request to all three other validators.
4. Checks whether the three victims abort and block production halts.

Options: `-n` validators, `-b` a prebuilt binary, `-d` work directory, `-i` first instance number.
See `run.sh -h`.

## Exit status

- `0`: all victims survived. The bug is absent or fixed.
- `1`: at least one victim aborted. The issue is reproduced.
- `2`: the run could not start or was inconclusive. The work directory holds the logs.

On current code the script exits 1 — all three victims abort, the chain halts, and the attacker
can repeat the request after every restart.

## Unit reproduction

The same defect is covered by unit tests that run with plain `cargo test`, without a live network:

```
cargo test -p rayls-consensus-primary --lib -- \
  test_collector_refuses_max_lower_bound_with_empty_skip_bitmap \
  test_collector_refuses_max_lower_bound_no_skip_entries \
  test_collector_refuses_max_minus_one_lower_bound_with_one_skip
```

On current code these tests fail with "attempt to add with overflow" at `certificate_store.rs:415`.
