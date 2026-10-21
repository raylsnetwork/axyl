# Live reproduction: one validator fails the certificate catch-up of every restarting validator

This directory reproduces, on a real local network, the issue where one committee member makes the
certificate catch-up of a restarting validator fail.

A validator in catch-up mode asks every other committee member for its missing certificates at once
and merges the answers into one list. If any single certificate in that list is invalid, the whole
list is discarded, including the valid certificates from honest validators. One dishonest member can
therefore make every catch-up fetch fail for as long as it keeps answering, and each failure makes
the next request ask for everything again, so every honest validator re-sends a full batch and the
victim verifies all of it again with no pause between rounds.

## The attacker hook

The attack needs a committee member that answers a missing-certificates request with one unusable
certificate. That behaviour lives in `crates/consensus/primary/src/network/merged_fetch_poison.rs`
and is compiled only with the `merged-fetch-poison` cargo feature, so a normal build never contains
it. It is armed per process by environment, so a single node in the network lies while the rest stay
honest:

- `RL_POISON_CERT_FETCH=1`: answer every missing-certificates request with one certificate that
  carries the requester's current epoch and a round just above its garbage-collection round, so the
  victim runs signature verification on it instead of skipping it as too old, and an `Unverified`
  zero signature, so that verification fails and the victim discards the whole merged batch.

The hook only changes what the armed node returns to missing-certificates requests. The armed node
still proposes and votes, so the chain keeps running and the one armed validator is an honest
committee member in every other respect.

## Run it

From a fresh clone, with a Rust toolchain and `curl` available:

```
./etc/test/merged-fetch-poison/run.sh
```

The script is self-contained. It builds the node binary with `--features merged-fetch-poison`,
generates keys and genesis for a fresh local network under `target/merged-fetch-poison/<timestamp>/`,
starts the nodes, drives the scenario, and cleans up the processes on exit. It needs no `.env` and
no prior setup.

What it does:

1. Starts five validators, one of them (validator 1) armed to poison missing-certificates replies,
   and lets them reach block 20. Five validators means quorum is four, so the chain keeps producing
   with the victim down.
2. Stops a different validator (validator 2, the victim) to open a gap, then lets the remaining
   validators build 40 more blocks.
3. Restarts the victim. With local history it enters catch-up mode and fetches missing certificates
   from all peers at once, merging the answers. For the answer it gets from the armed validator it
   receives the one bad certificate, which fails the whole merged batch.
4. Watches the victim for the merged fetch failing on every round: the armed validator's poison
   replies, the victim's hard failures, its `skip_rounds` clears, and zero fetch successes.
5. Control: disarms the armed validator and restarts it honest. The armed validator served a poison
   reply for every request it answered while armed, and the victim recorded no fetch success in that
   window, which isolates it as the cause. The control is best-effort: the victim usually recovers
   through the committed-block stream before a fresh gap forces another fetch, so a post-disarm fetch
   success may not be observed, and the script then reports the control inconclusive without changing
   the reproduced verdict.

Options: `-n` validators, `-w` watch seconds, `-g` gap size, `-i` first instance number (RPC ports),
`-b` a prebuilt binary, `-d` work directory. See `run.sh -h`.

## Exit status

- `0`: the victim's catch-up fetch succeeded despite the armed validator. The bug is absent or
  fixed.
- `1`: the victim's catch-up fetch failed on every round while the armed validator answered, and
  recovered once it was disarmed. The issue is reproduced. The summary prints the poison-reply
  count, the victim's merged-fetch failure count, its `skip_rounds` clears, its zero successes, and
  the attacker's log line.
- `2`: the run could not start or was inconclusive. The work directory holds the logs.

On current code the script exits 1.

## What to read in the logs

In the victim's log (`validator-2.log`, log target `primary::cert_fetcher`):

- `peer <attacker> returned 1 certificates` each round, followed by
  `Error from fetch certificates task`: the merged batch is discarded.
- `clearing skip_rounds - stale session fallback after repeated failures`: after two failures the
  next request stops listing the rounds the victim already has, so every honest validator re-sends
  everything.
- `merged certificates from all peers`, then `processing <n> fetched certificates`: the honest
  validators' full batches arrive and are re-verified every round.
- no `Finished task to fetch certificates successfully` for as long as the attacker answers.

In the attacker's log (`validator-1.log`, log target `primary::merged_fetch_poison`):

- `POISON CERT-FETCH: replacing honest certificates with one unverifiable certificate` once per
  request it answers.

## Unit reproduction

The same defect is covered by unit tests that run in CI with plain `cargo test`, without a live
network, in `crates/consensus/primary/src/tests/certificate_fetcher_tests.rs`. See the issue
writeup for the exact test names and the reproduce command.
