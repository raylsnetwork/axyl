# Node lifecycle

This document walks through the entire lifetime of an Axyl node — from
install through running operation to shutdown and crash recovery. It is the
operator's counterpart to the per-crate technical docs in
[`crates/`](crates/index.md): use this page to answer "what stage is the
node in right now and which subsystem owns what runs next?".

There are three modes a node can be in at any moment, declared by the
orchestrator on the `ConsensusBus`:

| Mode | Description |
|---|---|
| `CvvActive` | Committee-voting validator currently participating in consensus (proposing, voting). |
| `CvvInactive` | Allowlisted validator that is *catching up* or otherwise temporarily out of consensus; runs the state-sync subscriber. |
| `Observer` | Non-validating node that streams committed consensus output from a peer and serves RPC. |

Every transition described below is one of those three modes, or the
`NodeShutdown` outcome of the orchestrator's `select!` loop.

---

## 1. Install and build

Two artifacts are needed before anything else:

- The `rayls-network` binary. Build from source with `cargo build -p
  rayls-network --release` (the canonical command used by every script in
  `etc/`). The release binary lives at `target/release/rayls-network`.
- The network's bootstrap files: `genesis.yaml`, `committee.yaml`, and
  `parameters.yaml`. These come from the network operator (or, on
  developer machines, from `etc/test-network/local-testnet.sh`).

System prerequisites: a recent Rust toolchain matching `rust-toolchain`,
clang/libclang for the rocksdb dependency, and (for genesis ceremonies)
Foundry's `cast`.

## 2. Key generation

`rayls-network keytool generate {validator,observer}` writes encrypted BLS
keys plus deterministic Ed25519 network keys under `<datadir>/node-keys/`
along with `node-info.yaml`. See
[`crates/infrastructure/overview.md#keyconfig`](crates/infrastructure/overview.md#keyconfig)
for the cryptographic shape.

Two important properties:

- **BLS private keys never leave `KeyConfig`** in memory — the `BlsSigner`
  trait returns signatures without exposing the secret. Storage is AES-GCM-SIV
  encrypted on disk with the operator-chosen passphrase.
- **Network keys are deterministically derived** from a BLS signature of a
  seed string, so they do not need separate persistence.

For a validator the same `keytool` step is wrapped by
[`etc/validator/create-validator.sh`](../etc/validator/README.md); for an
observer it is wrapped by
[`etc/observer/create-observer.sh`](../etc/observer/README.md).

## 3. Joining the network — on-chain registration

Only validators have an on-chain join sequence; observers can simply
provision their data directory and start running.

| Step | Contract / function | Sent by |
|---|---|---|
| Fund | `cast send ... --value` | Admin key |
| Allowlist | `ConsensusRegistry.allowlistValidator(address)` | Admin key (`MAINTAINER` role) |
| Stake | `ConsensusRegistry.stake(...)` (calldata built by `keytool stake-calldata`) | Validator's operator key |
| Activate | `ConsensusRegistry.activate()` | Validator's operator key |

Activation puts the validator in `PendingActivation`; the next
`concludeEpoch()` system call promotes it to `Active`. From that point the
EVM-side state machine considers the validator part of the committee
schedule.

## 4. Local config and startup

The orchestrator's entry point is `launch_node`
([`doc/crates/middleware/overview.md#launch_node`](crates/middleware/overview.md))
which:

1. Opens the reth (execution) DB.
2. Opens the consensus DB (MDBX by default).
3. Creates the `ConsensusBus` and `EpochManager`.
4. Calls `EpochManager::run()` and blocks on it for the lifetime of the
   process.

`KeyConfig`, the `RaylsBuilder`, and the long-lived `Primary`/`Worker`
network handles are loaded once and stay alive across epochs.

## 5. Sync and catch-up — `CvvInactive`

A node that has just joined (or has fallen behind) starts in `CvvInactive`.
While in this mode the orchestrator runs the **state-sync subscriber**
([`doc/crates/consensus/state-sync.md`](crates/consensus/state-sync.md)),
which fetches missing consensus blocks and batches from peers, validates
them, and feeds them into the bridge → processor pipeline. The
`reconstruct_batch_digests` pass at processor startup (see
[middleware overview](crates/middleware/overview.md)) walks the last
~1000 canonical blocks to rehydrate the dedup set, so blocks already
executed in a previous run are skipped.

Once the local tip matches the network tip, the orchestrator detects the
catch-up via a `ModeTransition` outcome and switches the node to
`CvvActive`. At that moment the in-process `Primary` and `Worker` tasks
take over from the state-sync subscriber.

## 6. Steady state — `CvvActive`

In `CvvActive` mode the node:

- Runs a `Primary` instance that proposes/votes headers and aggregates
  certificates ([`crates/consensus/primary/`](crates/consensus/overview.md)).
- Runs one or more `Worker` instances that build, validate, and broadcast
  batches ([`crates/consensus/worker/`](crates/consensus/overview.md)).
- Forwards each committed `ConsensusOutput` to the middleware processor
  via the bridge subscriber. The processor turns each batch into an EVM
  block via `RethEnv::build_block_from_batch_payload`
  ([`crates/execution/overview.md`](crates/execution/overview.md)).
- Serves the standard `eth_*` JSON-RPC, the `rayls_*` namespace, and the
  optional faucet from the worker RPC server.

The execution layer also runs the `applyIncentives`/`concludeEpoch`/
`distributeRewards` system calls on every epoch-closing block (see
[`docs/reward-distribution/README.md`](../docs/reward-distribution/README.md)).

## 7. Epoch transitions

Epochs are the natural unit of state rotation. At each boundary the
orchestrator's `run_epoch()` loop emits an `EpochBoundary` outcome, which
triggers:

1. Drain the current `Primary` and `Worker` tasks (waited up to the
   configured `TaskKind::Drainable` timeout).
2. Read the new committee from `ConsensusRegistry`.
3. Spawn fresh `Primary` / `Worker` instances bound to the new committee
   in a fresh `Epoch Task Manager`.
4. Update the `GasAccumulator` / `RewardsCounter` for the new epoch.
5. Write the epoch record + certificate to the consensus DB (see
   [`SYNC.md`](../SYNC.md)).

The committee selection on the new epoch is deterministic and seeded by
`keccak(aggregate_bls_signature)` of the closing leader certificate.

## 8. Graceful shutdown

A SIGTERM (or the orchestrator-internal `node_shutdown: Notifier` being
fired) produces the `NodeShutdown` outcome. The shutdown sequence:

1. The subscriber drains in-flight `ConsensusOutput` values (bounded by the
   `Drainable` timeout).
2. Per-task `Noticer` clones cause every `select!` loop to exit cleanly.
3. The execution side calls `flush_persistence` so any in-memory blocks in
   `CanonicalInMemoryState` are written to MDBX before exit.
4. Open databases (Reth + consensus) are closed.

Active validators should typically `beginExit()` on `ConsensusRegistry`
first so the on-chain state is consistent with the operator's intent.

## 9. Crash recovery

A crash that bypasses the graceful shutdown above is recoverable from
on-disk state:

- **Execution chain** — `CanonicalInMemoryState` is rebuilt from MDBX on
  restart. Any block that was persisted is replayed; blocks that were only
  in CIM at the time of the crash are produced again from the cached
  `ConsensusOutput` (which the consensus DB still holds).
- **Consensus DAG** — the certificate store is the source of truth.
  `ConsensusState` is reconstructed by `consensus/state.rs` on startup.
- **Dedup** — `reconstruct_batch_digests` walks the last
  `DIGEST_RECONSTRUCTION_DEPTH = 1000` canonical blocks and recovers each
  block's `batch_digest` from the header (see the hardfork-gated encoding
  in [`doc/index.md`](index.md)).
- **Epoch transition checkpoints** — `epoch_transition_checkpoints` (one
  row per phase) persists in-progress epoch transitions so a crash in the
  middle of an epoch boundary resumes from the checkpointed phase rather
  than re-running prior phases.

If the node was `Active` when it crashed, it will typically come back as
`CvvInactive` and catch up from peers (Section 5) before re-promoting
itself.

## 10. Moving a node to another host

A node can be moved to a host with a different default page size, for example
from x86 to ARM. Stop the node, copy the whole data directory, and start it on
the new host. Its databases open as they are, so no conversion is needed.

## 11. Retiring

A validator retires by submitting `beginExit()` (see Section 3); the
on-chain finalisation takes two epochs of committee exclusion. After
`Exited`, one further epoch must elapse before `unstake()` becomes
callable. Off-chain, the operator can `kill` the node any time after
`beginExit()` confirms — the node does not need to be running for the
on-chain unstake.

---

## Where each subsystem lives

| Concern | Crate / page |
|---|---|
| Process bootstrap | [`crates/infrastructure/network-cli`](crates/infrastructure/overview.md#rayls-network-cli) |
| Epoch state machine | [`crates/middleware/orchestrator`](crates/middleware/overview.md#rayls-middleware-orchestrator) |
| Consensus subscribe | [`crates/middleware/bridge`](crates/middleware/overview.md#rayls-middleware-bridge) |
| Execution | [`crates/middleware/processor`](crates/middleware/overview.md#rayls-middleware-processor) + [`crates/execution/evm`](crates/execution/evm.md) |
| Catch-up | [`crates/consensus/state-sync`](crates/consensus/state-sync.md) |
| Primary / Worker | [`crates/consensus/primary`](crates/consensus/overview.md), [`crates/consensus/worker`](crates/consensus/worker.md) |
| Validator scripts | [`etc/validator/`](../etc/validator/README.md) |
| Observer scripts | [`etc/observer/`](../etc/observer/README.md) |
| Sync data model | [`SYNC.md`](../SYNC.md) |
