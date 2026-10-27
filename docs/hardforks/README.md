# Hardfork Schedule Verification

This document describes the hardfork schedule model for the Rayls network,
the three sets of contract-related hardforks, and the verification layers
that protect against schedule drift.

## 1. Behavioral vs Migration Hardforks

The 18 hardforks fall into two categories:

**Migration hardforks** perform a one-shot atomic state rewrite the client
executes at the first block where the fork activates
(`apply_activated_migrations` in `evm/hardforks/mod.rs`). They rewrite
contract state (bytecode or storage slots), and a failure in the migration
fails the block and halts the chain. Their activation block is therefore a
historical fact (or the canonical bootstrap value on a new chain).

**Behavioral hardforks** change client-side consensus, EVM, or system-call
behavior without any state write. Their activation block is a rollout
decision coordinated by the network (SRE), not by contract-state consistency.
They can be moved to the future via the schedule-record gate on the
built-in mainnet/testnet.

`HybridRewards` is both: it performs a state rewrite (the `ConsensusRegistry`
bytecode swap) and flips the epoch-close reward ABI from 1-arg to 2-arg.

### 18-fork inventory

| # | Fork | Kind | Set |
|---|------|------|-----|
| 1 | Eip1559 | behavioral | — |
| 2 | BatchDigestV2 | behavioral | — |
| 3 | AdminTransfer | migration | A |
| 4 | PrecompileGasFix | behavioral | — |
| 5 | RlsStorage | migration | A |
| 6 | Tokenomics | migration | A |
| 7 | Uups | migration | A |
| 8 | Erc20PrecompileBytecode | migration | B |
| 9 | TransactionLoadBalancing | behavioral | — |
| 10 | UsdrSupplyCorrection | migration | B |
| 11 | EmptyOutputBlock | behavioral | — |
| 12 | DynamicCommitteeSizing | behavioral | — |
| 13 | HybridRewards | both | C |
| 14 | OutputSeqNormalization | behavioral | — |
| 15 | SenderAffinityLoadBalancing | behavioral | — |
| 16 | EpochCloseSeedV2 | behavioral | — |
| 17 | CertificateFetchEpochV2 | behavioral | — |
| 18 | Erc20PrecompileDirectCall | behavioral | — |

### What each behavioral fork does

The eleven behavioral forks switch client behavior without a state write; the
activation block is a rollout decision, not a state-consistency requirement.
What each one does:

| Fork | What it does |
|------|--------------|
| `Eip1559` | EIP-1559 base fee: each block's base fee is derived from its parent header (standard Ethereum gas pricing). |
| `BatchDigestV2` | Moves the DAG batch digest from the `ommers_hash` header field to `requests_hash` (go-ethereum compatibility) and retries an output's batches that were all rejected nonce-too-high. |
| `PrecompileGasFix` | Runs the native ERC-20 (USDr) precompile through the standard precompile dispatch with correct gas charging instead of the legacy inspector path. |
| `TransactionLoadBalancing` | Partitions the transaction pool across validators by a hash of the raw transaction bytes, replacing the legacy slot digest. |
| `SenderAffinityLoadBalancing` | Partitions by the sender address instead, so one validator owns a sender's whole nonce chain. Takes precedence over `TransactionLoadBalancing`. |
| `EmptyOutputBlock` | Guarantees every DAG output produces at least one EVM block, building a fallback empty block when the output's batches were parked or drained. |
| `DynamicCommitteeSizing` | Sizes the next committee from the current Active + PendingActivation validators instead of reusing the fixed-size epoch committee, so it grows and shrinks with the validator set. |
| `OutputSeqNormalization` | Discards batches parked at the epoch boundary (their transactions stay pooled and re-seal in the new epoch) instead of force-executing them. |
| `EpochCloseSeedV2` | Seeds the epoch-closing block's `extra_data` with the consensus header hash, identical on every node, instead of the leader-certificate signature hash. |
| `CertificateFetchEpochV2` | Sends certificate fetch requests stamped with the epoch their rounds refer to, so a peer that has already closed that epoch serves them from the kept tail instead of the new epoch. Gates only the request format a node sends — every node accepts both variants, but a peer that has not been upgraded cannot decode the stamped one. |
| `Erc20PrecompileDirectCall` | Rejects `DELEGATECALL`/`CALLCODE` frames into the native ERC-20 precompile (`0x0400`); the precompile is dispatched by bytecode address, so a delegate frame would otherwise run its privileged logic with `target_address != bytecode_address`. Only direct `CALL`/`STATICCALL` are allowed once active. |

The seven migration forks' behavior is described by their set in section 2.

## 2. The Three Sets

The 7 migration forks touch exactly three disjoint
state domains. There are zero cross-set dependencies.

### Set A — UUPS / RLS admin pipeline

Forks: `AdminTransfer` (#3), `RlsStorage` (#5), `Tokenomics` (#6), `Uups` (#7).

These deploy and upgrade UUPS proxies and implementations for
`DelegationPool`, `RewardDistributor`, `RLS`, `RLSAccumulator`,
`NativeTokenController`, and `FeeAggregator`. Internal ordering:
`#3 → #6 ← #5`, `#6 → #7`.

### Set B — USDr precompile

Forks: `Erc20PrecompileBytecode` (#8), `UsdrSupplyCorrection` (#10).

These touch the USDr precompile at `0x…0400`: the STOP bytecode install
(`Erc20PrecompileBytecode`) and the one-time supply rebase
(`UsdrSupplyCorrection`). Internally order-independent for final state.

### Set C — Hybrid rewards registry

Fork: `HybridRewards` (#13).

This rewrites the `ConsensusRegistry` at `0x…17e1` (bytecode swap) and
flips the epoch-close reward ABI from 1-arg to 2-arg `applyIncentives`.

#### Post-activation (all optional)

The fork itself is self-contained: from the activation block the chain runs
under the hybrid registry with no further action. Three governance calls can
then change what the hybrid weights do; none is required for the chain to
function, so a network may leave them all unset.

| call | who can call | if left unset |
|------|--------------|---------------|
| `ConsensusRegistry.setDelegationPool(pool)` (`onlyOwner`) | registry owner | delegated stake counts as 0 in the stake tier (every validator sits at the baseline tier) and the slash pool-split path is skipped |
| `ConsensusRegistry.setParticipationFloorBps(bps)` (`onlyOwner`) | registry owner | the participation floor is disabled (`0`) |
| `RewardDistributor.setPerformanceWeightBps(bps)` (admin role) | distributor admin | `distributeRewards()` ignores the hybrid weights entirely (payouts stay 100% stake/target-APY-derived). **Pending** — exists only on the unmerged `wire-hybrid-performance-weights` branch (Fixes #178), not in any shipped release. Enabling it additionally requires a Part 2 proxy upgrade of the distributor first. |

How to make these calls: the Foundry Ops scripts under
`rayls-contracts/script/interactions/` (note `ConsensusRegistryOps.s.sol` is
read-only today, so registry owner calls go via `cast send`), and
[`../contract-upgrades/README.md`](../contract-upgrades/README.md) for the
upgrade runbook.

### Why these are the canonical new-chain values

A greenfield genesis is initialized by
`apply_greenfield_fixes` (`reth_env/genesis.rs`), which bakes in:
- the UUPS impls at their `uups` version + RLS proxy/storage (Set A state)
- the **pre-hybrid** `ConsensusRegistry` (Set C, pre-hybrid ABI)
- **no** USDr STOP byte (Set B)

The canonical new-chain schedule therefore uses:

| fork | canonical value | rationale |
|------|----------------|-----------|
| Set A (`AdminTransfer`, `RlsStorage`, `Tokenomics`, `Uups`) | `Block(0)` | The genesis already carries their state. `Block(0)` means "already active at genesis" — the migration bodies never run (`newly_activated_forks` only returns forks that transition between the parent and the block; the first executed block's parent is block 0, where these are already active). |
| `Erc20PrecompileBytecode` | **exactly `Block(1)`** | The genesis has no STOP byte, so the first post-genesis block must write it. Block 0 is never executed (genesis initializes state directly, no EVM execution). A later block (e.g. 5) leaves a live EIP-161 cleanup exposure window between blocks 1 and 5. |
| `UsdrSupplyCorrection` | **exactly `Never`** | A one-time historical mainnet rebase. The corrected value is already baked into the genesis. |
| `HybridRewards` | **exactly `Block(1)`** | The genesis bakes the pre-hybrid `ConsensusRegistry`. The 2-arg ABI must not be live at genesis (calling it against the pre-hybrid registry reverts — fatal system call). A new chain starts the complete current protocol from the first block, with no legacy-regime window. |
| 11 behavioral forks | any value | Business-logic based (see below). |

On a new chain these values are required for correctness, but the node does not enforce them: the shape gate applies only the relaxed rules of section 4 to every non-mainnet/testnet chain-id, so a deviating new-chain schedule will boot. In particular, the built-in **devnet** schedule keeps `HybridRewards: Never`, which those rules allow.

## 3. Behavioral Changes Are Business-Logic Based

The 11 behavioral forks (`Eip1559`, `BatchDigestV2`, `PrecompileGasFix`,
`TransactionLoadBalancing`, `EmptyOutputBlock`, `DynamicCommitteeSizing`,
`OutputSeqNormalization`, `SenderAffinityLoadBalancing`, `EpochCloseSeedV2`,
`CertificateFetchEpochV2`, `Erc20PrecompileDirectCall`) change protocol behavior
without state writes.
Their activation blocks are network-coordinated rollout or risk decisions
(SRE), not contract-state consistency requirements.

This is why behavioral forks are:
- **Unchecked by the boot shape gate** — any value is valid.
- **Not frozen on the built-in mainnet/testnet** — SRE can move future
  boundaries via the schedule-record gate (config file + re-record).
- **Frozen in CI only for the binary** — the freeze tests pin the values
  that ship in the current release; future operational moves via config
  file are outside the binary and do not need test updates.

## 4. What Verifies What

| layer | what it protects | where |
|-------|-----------------|-------|
| **CI freeze tests** | mainnet and testnet baked schedules (full 18-fork literal tables) | `chainspec.rs` |
| **Boot shape gate** (`verify_schedule_shape`) | all non-mainnet/testnet chain-ids on the relaxed rules; mainnet/testnet chain-ids always excluded | `network_profile/shape.rs` |
| **Schedule-record gate** (`schedule-record.yaml`, trust-on-first-use) | per-datadir drift on all networks | `network_profile/verify.rs` |
| **Genesis ceremony** | out of scope by decision | — |

### Design decisions

- Mainnet and testnet are excluded from the shape check. Their schedules
  are frozen in CI (the freeze tests) and guarded per datadir by the
  schedule-record gate. A modified `--config-file` targeting a built-in
  mainnet/testnet chain-id is not shape-checked; it is still protected by
  the record gate for forks that have fired, and by the freeze tests for
  the binary schedule.
- The shape check has one relaxed rule set, applied regardless of the
  datadir's executed head: Set A forks at `Block(0)` or `Never`,
  `Erc20PrecompileBytecode` exactly `Block(1)`, `UsdrSupplyCorrection`
  at any value (the node warns when it is not `Never`), and `HybridRewards`
  at `Never` or `Block(>= 1)`. The canonical new-chain values (section 2)
  are recommended but not enforced. The schedule-record gate then protects
  the executed history per datadir.
- A new chain via `--config-file` must enumerate all forks; a partial
  file is refused at load by the completeness check. Setting
  `Erc20PrecompileBytecode: never` explicitly is refused by the shape gate
  (catches the EIP-161 exposure); `HybridRewards: never` is allowed.
- The shape check runs **before** the schedule-record write, so a refused
  boot never pollutes the datadir via trust-on-first-use.
