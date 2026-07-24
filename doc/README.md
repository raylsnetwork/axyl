# Axyl documentation

This directory is the maintained source of truth for Axyl's per-crate and
system-level documentation. Per-crate Rust READMEs that used to live next
to source files are now mostly stubs that link here (see #380 for the
consolidation rationale).

Start with one of the pages below depending on what you are trying to do.

## Map by audience

### "I want to run a node"

- [`dev-mode.md`](dev-mode.md) — **one-command local dev chain**
  (`rayls-network dev`, built with `--features dev-single-node-setup`): auto-bootstrap, RPC on,
  pre-funded accounts. The fastest way to get a working chain for local
  development.
- [`../README.md`](../README.md) — top-level project README with quick-start
  build/run instructions.
- [`../etc/validator/README.md`](../etc/validator/README.md) — provision a
  validator from scratch (keys, fund, allowlist, stake, activate, exit).
- [`../etc/observer/README.md`](../etc/observer/README.md) — provision an
  observer that follows consensus without voting.
- [`../etc/test-network/README.md`](../etc/test-network/README.md) — bring
  up a local 4-validator chain on one machine.
- [`../etc/docker-network/README.md`](../etc/docker-network/README.md) —
  same network as above, packaged as Docker Compose.
- [`gasless-mode.md`](gasless-mode.md) — how to run / join a fee-free
  network.
- [`node-lifecycle.md`](node-lifecycle.md) — what a running node is doing
  at every stage (install → keygen → join → sync → steady state → epoch
  transitions → shutdown → crash recovery).

### "I want to understand how the system works"

- [`index.md`](index.md) — system overview: end-to-end transaction flow,
  architecture diagram, custom database tables, header-field encoding,
  RPC interface.
- [`node-types.md`](node-types.md) — Validator, Observer, and Archive: what
  each one is, why it matters, and how consensus participation, state
  pruning, and RPC exposure differ between them.
- [`glossary.md`](glossary.md) — terms that mean different things in
  different parts of Axyl (e.g. "network", "whitelist"). Check here before
  using an overloaded term in a design doc or PR description.
- [`node-lifecycle.md`](node-lifecycle.md) — the operational counterpart:
  which subsystem owns which stage of the node's life.
- [`../SYNC.md`](../SYNC.md) — the trust-bootstrapping epoch chain and
  consensus chain that let a fresh node sync from genesis without trusting
  an indexer.
- [`gasless-mode.md`](gasless-mode.md) — the EIP-1559 base-fee model and
  how zero-fee chains are configured.

### "I want to read about a specific crate"

- [`crates/index.md`](crates/index.md) — index of every workspace crate
  with a maintained doc page.

Per-crate pages (the canonical source — in-crate Rust READMEs now point
back here):

- Consensus: [overview](crates/consensus/overview.md) · [primary](crates/consensus/primary.md) · [worker](crates/consensus/worker.md) · [network](crates/consensus/network.md) · [state-sync](crates/consensus/state-sync.md) · [batch-builder](crates/consensus/batch-builder.md) · [batch-validator](crates/consensus/batch-validator.md) · [primary-metrics](crates/consensus/primary-metrics.md) · [consensus-metrics](crates/consensus/consensus-metrics.md)
- Middleware: [overview](crates/middleware/overview.md) (covers bridge,
  orchestrator, processor)
- Execution: [overview](crates/execution/overview.md) · [evm](crates/execution/evm.md) · [rpc](crates/execution/rpc.md) · [faucet](crates/execution/faucet.md) · [execution-metrics](crates/execution/execution-metrics.md)
- Infrastructure: [overview](crates/infrastructure/overview.md) (covers
  types, storage, config, network-cli, network-types, utils)
- Testing: [overview](crates/testing/overview.md) (covers test-utils-committee,
  test-utils, e2e-tests)
- Binary: [`rayls-network`](crates/rayls-network.md)

### "I want to understand the on-chain side"

- [`../docs/reward-distribution/README.md`](../docs/reward-distribution/README.md) —
  fee aggregation, USDr→RLS swap, validator/ecosystem/burn split, and the
  epoch-end `distributeRewards` system call.
- [`../rayls-contracts/README.md`](../rayls-contracts/README.md) —
  validator lifecycle and `ConsensusRegistry` overview.
- [`../rayls-contracts/src/consensus/design.md`](../rayls-contracts/src/consensus/design.md) —
  detailed design of the on-chain registry.
- [`../rayls-contracts/src/consensus/invariants.md`](../rayls-contracts/src/consensus/invariants.md) —
  the invariants the registry contract is intended to maintain.
- [`../rayls-contracts/script/interactions/README.md`](../rayls-contracts/script/interactions/README.md) —
  Foundry scripts for operating the deployed contracts.

### "I want to contribute"

- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — review process, formatting,
  tests, commit conventions.
- [`../SECURITY.md`](../SECURITY.md) — how to disclose a vulnerability.
- [`../.github/ACTIONS.md`](../.github/ACTIONS.md) — what each CI workflow
  does and the local commands that mirror them.
- [`../CHANGELOG.md`](../CHANGELOG.md) — historical changelog (GitHub
  Releases is authoritative for the 1.x line).

## Conventions

- Per-crate pages live under `crates/<group>/<crate>.md` and own all
  technical documentation for that crate. In-crate `crates/**/README.md`
  files are stubs that link here.
- Operational guides (`etc/<purpose>/README.md`) own the end-to-end
  runbooks; this directory cross-references them rather than duplicating
  their contents.
- Solidity-side documentation lives under
  [`../rayls-contracts/`](../rayls-contracts/)
- When a doc page references code, prefer linking the file (`evm/src/x.rs`)
  with a line number than copying snippets — the audit trail in #380
  showed how quickly inlined excerpts drift from the source.
