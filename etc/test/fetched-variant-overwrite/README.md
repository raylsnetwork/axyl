# Live reproduction: fetched-variant certificate overwrite

This directory reproduces, on a real local network, the issue where the certificate fetcher's accept
path silently overwrites a stored certificate with a peer-supplied valid *variant*.

A certificate is identified by its digest, which commits to the header but not to *which* 2f+1
subset of the committee signed it. For one header there can be several valid certificates: same
digest, different signer subset, different aggregate signature. The gossip accept path
(`CertificateValidator::process_certificate`) guards against this by skipping a certificate whose
digest is already stored. The fetcher's accept path
(`CertificateManager::process_verified_certificates` in
`crates/consensus/primary/src/state_sync/cert_manager.rs`) has no such guard, so a valid variant
arriving on the fetch path is `write_all`-stored last-writer-wins, replacing the canonical copy.

## Severity

Low / Informational. The certificate digest is unchanged, so consensus is unaffected: the network
does not halt, crash, or fork, and a restart is clean. The only effect is that the overwritten
node's stored certificate bytes (signer set and aggregate signature) no longer match the copy its
honest peers hold for that digest. The stored signature bytes are inert while `EpochCloseSeedV2` is
active. A real quorum (2f+1) of valid signatures is required to build a variant, so an observer with
no committee key cannot trigger it; a single validator suffices.

## The attacker hook

The reproduction needs a node that, on a chosen round, builds a second valid certificate for its own
header from a different signer subset and submits it through the fetch accept path. That behaviour
lives in `crates/consensus/primary/src/certifier_byzantine.rs` and is compiled only with the
`byzantine-test-hooks` cargo feature, so a normal build never contains it. It is armed per process by
environment, so a single node in the network forges while the rest stay honest:

- `RL_FVO_ROUND=<round>`: the epoch-1-or-later round whose header the armed node forges a variant
  for. The node collects quorum + 1 votes for its own header that round, stores the canonical
  certificate through the guarded own/gossip path, builds a valid variant from a different subset,
  and pushes it through `ProcessVerifiedCertificates`.

## Run it

From a fresh clone, with a Rust toolchain and `curl` available:

```
./etc/test/fetched-variant-overwrite/run.sh
```

The script is self-contained. It builds the node binary with `--features byzantine-test-hooks`,
generates keys and genesis for a fresh local network under
`target/fetched-variant-overwrite/<timestamp>/`, starts the nodes, drives the scenario, and cleans
up the processes on exit. It needs no `.env` and no prior setup.

What it does:

1. Starts four validators on a short-epoch local network (so epoch 1 is reached quickly) and arms
   validator 1 with `RL_FVO_ROUND`.
2. Waits for the armed node to reach that round in epoch 1, where it stores its canonical certificate
   and then injects a valid variant for the same digest through the fetch accept path.
3. Reads the armed node's own log line, which reports the stored signer set before and after the
   injection and the accept result, and compares them.
4. Samples block height before and after to confirm the network stays live through the overwrite.

Options: `-n` validators, `-r` armed round, `-e` epoch seconds, `-w` watch seconds, `-i` first
instance number (RPC ports), `-b` a prebuilt binary, `-d` work directory. See `run.sh -h`.

## Exit status

- `0`: the fetch accept path did not overwrite the stored certificate (`stored_before == stored_after`).
  The bug is absent or fixed.
- `1`: the stored certificate was overwritten through the fetch path (`stored_before != stored_after`
  for the same digest, with `accept=Ok(Ok(()))`). The issue is reproduced. The summary prints the
  signer set before and after, and confirms blocks kept advancing (no halt).
- `2`: the run could not start or was inconclusive (for example the armed round was not reached in
  the watch window). The work directory holds the logs.

On current code the script exits 1.

## Optional cross-check with rayls-db-inspect

The overwrite is in the consensus database, not visible over RPC, so the script detects it from the
armed node's own log. To confirm the on-disk divergence with an independent reader, arm a round the
node *leads* so the overwritten certificate is a committed leader certificate, stop the nodes, and
compare the stored certificate across the per-node `consensus-db` directories:

```
rayls-db-inspect cert <consensus-header-number> \
  -d n1=<run>/validator-1 -d n2=<run>/validator-2 -d n3=<run>/validator-3 -d n4=<run>/validator-4
```

The armed node reports a different signer set and aggregate signature for the same certificate
digest (`verdict: DIVERGENT ... what=signers`), while `header-check` reports `OK` (the header chain
re-verifies, no fork).

## Unit reproduction

The same defect is covered by a unit test that runs in CI with plain `cargo test`, without a live
network, in `crates/consensus/primary/src/tests/cert_manager_tests.rs`:

```
cargo test -p rayls-consensus-primary --lib -- fetched_variant_silently_overwrites_stored_certificate
```

It stores a canonical certificate, feeds a valid variant (same digest, different signer subset)
through `process_verified_certificates`, and asserts the stored signer set and aggregate signature
became the variant's. On current code it passes, demonstrating the unguarded overwrite.
