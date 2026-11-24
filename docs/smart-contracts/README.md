# Axyl's Solidity Contract Sources, and What a Fresh Chain Actually Runs

The repository contains several copies of the same contracts. This doc inventories them, specifies
which one determines the code a new chain executes, and records where the copies disagree.

**TL;DR:** the code a new chain runs comes from `rayls-contracts/artifacts/*.json` — contracts 1–8
and 27 — except the implementations of contracts 2–7, which come from
`crates/execution/evm/src/evm/hardforks/bytecodes/*_impl.bin`, contracts 28–30, which come from S3, and
contract 1, whose code `HybridRewards` replaces at block 1. None of it is generated from
`rayls-contracts/src/**/*.sol` automatically. Three of those six — `DelegationPool`, `RewardDistributor`,
`FeeAggregator` — are older builds missing functions the source now has.

See also `docs/contract-upgrades/README.md` (proxy upgrade vs hardfork decision) and
`docs/hardforks/` (scheduling mechanics).

---

## Part 1 — Contracts and sources

### Contract enumeration

Numbering used throughout this doc: **1–9** are the deployable classes declared in
`rayls-contracts/src`, **10–26** are the remaining `.sol` files there, **27–31** are sources outside
`rayls-contracts/src` that a chain still executes. Per-tier coverage is in the next section.

| # | Class | UUPS |
|---|---|---|
| 1 | `ConsensusRegistry` | no — replaced in place by `HybridRewards` |
| 2 | `DelegationPool` | yes |
| 3 | `FeeAggregator` | yes |
| 4 | `RewardDistributor` | yes |
| 5 | `NativeTokenController` | yes |
| 6 | `RLS` | yes |
| 7 | `RLSAccumulator` | yes |
| 8 | `BlsG1` (library) | no |
| 9 | `RewardCurve` | no |

The remaining 17 files in `rayls-contracts/src` produce no bytecode that reaches a chain:

| # | File | Declared type | Notes |
|---|---|---|---|
| 10 | `consensus/StakeManager.sol` | abstract contract | legacy base; deployed by nothing |
| 11 | `consensus/SystemCallable.sol` | abstract contract | mixin that gates system calls |
| 12 | `interfaces/IAlgebraPool.sol` | interface | no bytecode |
| 13 | `interfaces/IConsensusRegistry.sol` | interface | no bytecode |
| 14 | `interfaces/IDelegationPool.sol` | interface | no bytecode |
| 15 | `interfaces/IFeeAggregator.sol` | interface | no bytecode |
| 16 | `interfaces/IMintableBurnable.sol` | interface | no bytecode |
| 17 | `interfaces/INativeTokenController.sol` | interface | no bytecode |
| 18 | `interfaces/IOFT.sol` | interface | no bytecode |
| 19 | `interfaces/IRLS.sol` | interface | no bytecode |
| 20 | `interfaces/IRewardCurve.sol` | interface | no bytecode |
| 21 | `interfaces/IRewardDistributor.sol` | interface | no bytecode |
| 22 | `interfaces/IStakeManager.sol` | interface | no bytecode |
| 23 | `interfaces/ISwapRouter.sol` | interface | no bytecode |
| 24 | `interfaces/callback/IAlgebraSwapCallback.sol` | interface | no bytecode |
| 25 | `mocks/MockAlgebraDex.sol` | mock | test helper |
| 26 | `mocks/MockOFTBridge.sol` | mock | test helper |

Contract 9 is deployed on demand by `rayls-contracts/script/interactions/reward-curve/` and reached
through `IRewardCurve`.

**External sources** — code a chain executes that is not declared in `rayls-contracts/src`:

| # | Class | Declared in | Notes |
|---|---|---|---|
| 27 | `ERC1967Proxy` | `rayls-contracts/node_modules` (OpenZeppelin) | code behind all 6 system proxies |
| 28 | `SafeImpl` | `rayls-contracts/node_modules/safe-contracts` | `Safe.sol` singleton at `0x41675c099f32341bf84bfc5382af534df5c7461a`; installed by S3 |
| 29 | `SafeProxyFactory` | `rayls-contracts/node_modules/safe-contracts` | at `0x4e1dcf7ad4e460cfd30791ccc4f9c8a4f820ec67`; installed by S3 |
| 30 | Safe proxy | `rayls-contracts/node_modules/safe-contracts` | `SafeProxy` instance at `0x…07a0`; code and storage from S3 |
| 31 | USDr precompile | `crates/execution/evm/src/native_erc20/mod.rs` | `0x…0400`. Not Solidity — the node answers the calls directly. `Erc20PrecompileBytecode` installs a STOP byte so EIP-161 cleanup cannot reap its storage |

### The six source tiers

| Tier | What | Path | Count | Consumer |
|---|---|---|---|---|
| S1 | Solidity source | `rayls-contracts/src/**/*.sol` | 26 files: contracts 1–9, abstract contracts 10–11, interfaces 12–24, mocks 25–26 | `forge build` → `rayls-contracts/out/` (gitignored) |
| S2 | Compiled artifacts | `rayls-contracts/artifacts/*.json` | 9 — contracts 1–8 and 27 | `include_str!` in `crates/infrastructure/config/src/genesis.rs` → pre-genesis EVM simulation |
| S3 | Genesis alloc YAML | `rayls-contracts/deployments/genesis/precompile-config.yaml` | 13 accounts: contracts 2–5 and 7 (proxy and impl each), plus contracts 28–30 | `include_str!`, same file → genesis alloc. Only contracts 28–30 survive genesis |
| S4 | Genesis override binaries | `crates/execution/evm/src/evm/hardforks/bytecodes/*.bin` | 6 — implementations of contracts 2–7 | `include_bytes!` in `crates/execution/evm/src/reth_env/genesis.rs` → `apply_greenfield_fixes()` |
| S5 | Hardfork migrations | `crates/execution/evm/src/evm/hardforks/bytecodes/<fork>/*.bin`, plus byte literals in `crates/execution/evm/src/evm/hardforks/*.rs` | 18 fixture files covering contracts 1–7 and 27, plus the STOP byte of 31 | state built in `crates/execution/evm/src/evm/hardforks/*.rs`, applied at an activation block |
| S6 | UUPS proxy upgrade | none — a transaction sent from `rayls-contracts/script/interactions/upgrade/UpgradeOps.s.sol` | no files in the repository; the deployed code and the event exist only on-chain | operator-initiated; applies only to contracts 2–7, the six upgradable ones |

S1–S5 are where the code a chain runs comes from: S2 and S4 determine block 0, S5 acts at an
activation height, and S3 supplies contracts 28–30. S2–S5 are compiled into the node binary and require
a rebuild to change. S6 is the only tier with no footprint in the repository: the code it installs is
compiled locally from S1, so its provenance cannot be pinned from the repo — only by hashing the live
code (see Part 3). Contracts 27–30 come from `rayls-contracts/node_modules` and 31 is node-internal, so
none of them appears in S1.

### Propagation paths

A `.sol` change reaches a **new chain** only through the checked-in artifacts and binaries.
`forge build` writes to the gitignored `rayls-contracts/out/`. The checked-in
`rayls-contracts/artifacts/*.json` set is a separate hand-exported copy: no script, Makefile target
or CI job regenerates it, and `git log main -- rayls-contracts/artifacts` contains only the root commit
of this history ("Porting codebase from old repository", 2026-07-03).
Re-exporting the selected files from `out/` into `rayls-contracts/artifacts/` and rebuilding the node is
therefore the only route from S1 to S2 — except `ConsensusRegistry.json`, which is not a build of `main`
(that compiles to 23,042 B) but the 21,929 B pre-hybrid generation `hybrid_rewards.rs` requires.

An **existing chain** can be changed two ways:

- **Hardfork (S5).** The new code ships inside the node binary and is applied at an activation height.
  Its fixtures are compiled by hand from S1, so it carries the same regeneration problem as S2 and S4.
- **Proxy upgrade (S6).** A transaction from
  `rayls-contracts/script/interactions/upgrade/UpgradeOps.s.sol` points a proxy at new code, with no
  client change. Only contracts 2–7 can receive it: contract 1 has no proxy, and contract 27's code
  is fixed once the proxy is deployed.

### The genesis alloc YAML (S3)

`rayls-contracts/deployments/genesis/precompile-config.yaml` is generated by
`rayls-contracts/script/GeneratePrecompileGenesisConfig.s.sol`, which broadcasts real deploys in a
forge VM — contracts 28–30, the implementations and proxies of contracts 2–5 and 7, each proxy run
through its `initialize` — and writes the resulting code, storage and balance under the addresses
recorded in `rayls-contracts/deployments/deployments.json`. 13 accounts, 69,855 B of code, 55 slots.

| S3 account | Runtime code | Slots |
|---|---|---|
| contracts 28–30 | 12,180 / 1,812 / 81 B | 7 — singleton pointer, the single owner `0x0e301bee…f3161`, threshold, module and initialization flags; the governance Safe also carries the file's only balance, 10 native units |
| implementation of 2–5 and 7 | 17,102 / 16,454 / 11,707 / 4,431 / 5,491 B | 0 — `__self` in this code points at the generator (`0xa8452ec9…c00496`) |
| proxy of 2–5 and 7 | 122 B each — contract 27 | 4 shared per contract: ERC-1967 implementation slot, `_initialized = 1`, `DEFAULT_ADMIN_ROLE` and `UPGRADER_ROLE` granted to `0x0e301bee…f3161`. Plus initializer arguments: 2 → 7 (delegation bounds, 14 unbonding epochs), 3 → 14 (fee split 5000/0/5000 bps, swap bounds, USDr `0x…0400`), 4 → 5, 5 → 0, 7 → 2 (wiring) |

### Tier freshness against S1

Provenance is by build: `forge build` of `rayls-contracts/src` at the tag named, compared over the solc link
and immutable slots, which hold the spliced addresses at runtime. `S2 of main` means that file as checked in
at `main`; `S1 of <tag>` means a build of `src` at that tag, and a tag range means the source, the pinned
dependencies and `foundry.toml` are unchanged across it.

#### S2 — `rayls-contracts/artifacts/*.json`

| Contract | In sync with | Action |
|---|---|---|
| 1 `ConsensusRegistry` | S1 of `mainnet-v1.0.0` | no — no S4 entry; `HybridRewards` overwrites this code at its fork |
| 2 `DelegationPool` | S1 of `mainnet-v1.0.0` | no — S4 rewrites the impl at genesis with different bytes |
| 3 `FeeAggregator` | S1 of `main` | no — S4 rewrites the impl at genesis with different bytes |
| 4 `RewardDistributor` | S1 of `v0.1.5`–`mainnet-v1.2.0` | no — S4 rewrites the impl at genesis with the same bytes |
| 5 `NativeTokenController` | S1 of `main` | no — S4 rewrites the impl at genesis with the same bytes |
| 6 `RLS` | S1 of `main` | no — S4 rewrites the impl at genesis with the same bytes |
| 7 `RLSAccumulator` | S1 of `main` | no — S4 rewrites the impl at genesis with the same bytes |
| 8 `BlsG1` | S1 of `main` | no — already in sync, and S2 is the only tier that installs it |
| 9 `RewardCurve` | not in `artifacts/` | n/a |

#### S3 — `rayls-contracts/deployments/genesis/precompile-config.yaml`

| Contract | In sync with |
|---|---|
| 28–30 Safe | outside the nine — vendored `safe-contracts`, not built from `src` |

#### S4 — `crates/execution/evm/src/evm/hardforks/bytecodes/*_impl.bin`

| Contract | In sync with | Action |
|---|---|---|
| 1 `ConsensusRegistry` | not in `bytecodes/` | n/a |
| 2 `DelegationPool` | S1 of `v0.1.5` | yes — rebuild from S1 of `main` |
| 3 `FeeAggregator` | not a build of any `.sol` state in this repo | yes — rebuild from S1 of `main` |
| 4 `RewardDistributor` | S1 of `v0.1.5`–`mainnet-v1.2.0` | yes — rebuild from S1 of `main` |
| 5 `NativeTokenController` | S1 of `main` | no |
| 6 `RLS` | S1 of `main` | no |
| 7 `RLSAccumulator` | S1 of `main` | no |
| 8 `BlsG1` | not in `bytecodes/` | n/a |
| 9 `RewardCurve` | not in `bytecodes/` | n/a |

Rebuilding these binaries decides what a new chain runs. Contracts 2–7 sit behind UUPS proxies, so an
existing chain keeps the implementation it has and takes the change only through an upgrade transaction —
S6. The alternative is a hardfork migration (S5).

#### S5 — `crates/execution/evm/src/evm/hardforks/bytecodes/<fork>/*.bin`

| Contract | At fork | In sync with |
|---|---|---|
| 1 `ConsensusRegistry` | `HybridRewards` | S1 of `main` |
| 2 `DelegationPool` | `AdminTransfer` | S4 of `v0.1.2`–`v0.1.3` |
| 4 `RewardDistributor` | `AdminTransfer` | S4 of `v0.1.2`–`v0.1.3` |
| 2 `DelegationPool` | `Tokenomics` | S4 of `v0.1.4` |
| 3 `FeeAggregator` | `Tokenomics` | S4 of `v0.1.4` |
| 4 `RewardDistributor` | `Tokenomics` | S4 of `v0.1.4` |
| 5 `NativeTokenController` | `Tokenomics` | S4 of `v0.1.4` |
| 6 `RLS` | `Tokenomics` | S4 of `v0.1.4` |
| 7 `RLSAccumulator` | `Tokenomics` | S4 of `v0.1.4` |
| 2 `DelegationPool` | `Uups` | S1 of `v0.1.5` |
| 3 `FeeAggregator` | `Uups` | not a build of any `.sol` state in this repo |
| 4 `RewardDistributor` | `Uups` | S1 of `v0.1.5`–`mainnet-v1.2.0` |
| 5 `NativeTokenController` | `Uups` | S1 of `main` |
| 6 `RLS` | `Uups` | S1 of `main` |
| 7 `RLSAccumulator` | `Uups` | S1 of `main` |
| 27 `ERC1967Proxy` | `AdminTransfer` | S2 of `main` |
| 27 `ERC1967Proxy` | `RlsStorage` | S2 of `main` |
| 27 `ERC1967Proxy` | `Tokenomics` | S2 of `main` |
| 31 USDr precompile | `Erc20PrecompileBytecode` | not Solidity — one STOP byte |
| 31 USDr precompile | `UsdrSupplyCorrection` | the same STOP byte, reasserted with the `TOTAL_SUPPLY` storage fix |

## Part 2 — New chain

A new chain is `local()` (`crates/execution/evm/src/chainspec/schedule.rs`): every fork at
`Block(0)` except `Erc20PrecompileBytecode` and `HybridRewards` at `Block(1)`, and
`UsdrSupplyCorrection` at `Never`. A migration runs on the block where its fork crosses from inactive at
the parent to active at the block (`newly_activated_forks` in
`crates/execution/evm/src/chainspec/hardforks.rs`), so block 0 is never a crossing and a `Block(0)`
fork's body never runs.

`GenesisArgs::execute` (`crates/infrastructure/network-cli/src/genesis/mod.rs`):

- **S1** — not used.
- **S2** — installs contracts 1–8 and 27, built by the pre-genesis simulation.
- **S3** — updates code and storage at the proxies of 2–5 and 7, and code at their implementations;
  installs 28–30.
- **S4** — replaces code and storage at the implementations of 2–7, and adds one storage slot on the
  proxy of 6.
- **S2 again** — replaces code and storage at the proxies of 2–5 and 7 with the simulation's accounts,
  discarding S3's.
- **S5** — at block 1, `HybridRewards` replaces contract 1's code and `Erc20PrecompileBytecode` installs
  a STOP byte on contract 31. S5's other 17 fixtures, covering contracts 2–7 and 27, belong to forks
  `local()` schedules `Block(0)`.
- **S6** — none.

S4 exists because the simulation cannot deploy at the well-known protocol addresses: `0x…17e1`–`0x…17eD`
are constants no `CREATE` nonce sequence produces, so it deploys at temporary addresses — wrong `__self`,
which makes the proxies permanently un-upgradeable, `_initialized` unlocked, and the allowance on the
proxy of 6 keyed to the temporary accumulator.

| # | Contract | Final code | Final storage | Matches S1? |
|---|---|---|---|---|
| 1 | `ConsensusRegistry` | S5 — `crates/execution/evm/src/evm/hardforks/bytecodes/hybrid_rewards/consensus_registry_deployed.bin` (23,042 B), spliced over S2's 21,929 B at block 1 | S2 | yes, after block 1 |
| 2 | `DelegationPool` impl | S4 — `crates/execution/evm/src/evm/hardforks/bytecodes/delegation_pool_impl.bin` (15,790 B) | S4 — `_initialized = max` | **no** — 8 of 53 functions absent |
| 3 | `FeeAggregator` impl | S4 — `crates/execution/evm/src/evm/hardforks/bytecodes/fee_aggregator_impl.bin` (16,402 B) | S4 — `_initialized = max` | **no** — 2 of 54 absent (`initialize`, `setPoolConfig`) |
| 4 | `RewardDistributor` impl | S4 — `crates/execution/evm/src/evm/hardforks/bytecodes/reward_distributor_impl.bin` (11,707 B) | S4 — `_initialized = max` | **no** — 6 of 39 absent (RewardCurve and target-APY API) |
| 5 | `NativeTokenController` impl | S4 — `crates/execution/evm/src/evm/hardforks/bytecodes/native_token_controller_impl.bin` (4,431 B) | S4 — `_initialized = max` | yes |
| 6 | `RLS` impl | S4 — `crates/execution/evm/src/evm/hardforks/bytecodes/rls_impl.bin` (9,977 B) | S4 — `_initialized = max` | yes |
| 6 | `RLS` proxy | contract 27 (122 B) | S2 (mint, prefunds, ERC-1967 slot) plus the S4 allowance slot | n/a — not declared in `src` |
| 7 | `RLSAccumulator` impl | S4 — `crates/execution/evm/src/evm/hardforks/bytecodes/rls_accumulator_impl.bin` (5,491 B) | S4 — `_initialized = max` | yes |
| 2–5, 7 | proxies | contract 27 (122 B) | S2 (simulation), installed after S3 and S4 | n/a — not declared in `src` |
| 8 | `BlsG1` — derived address, not a constant | S2 — `rayls-contracts/artifacts/BlsG1.json` (7,371 B) | none, library | yes |
| 27 | `ERC1967Proxy` | S2 — `rayls-contracts/artifacts/ERC1967Proxy.json` (122 B) | n/a | n/a — not declared in `src` |
| 28–30 | `SafeImpl`, `SafeProxyFactory`, Safe proxy | S3 (12,180 B, 1,812 B, 81 B) | S3 (7 slots) | n/a — not declared in `src` |
| 31 | USDr precompile | S5 — one STOP byte at block 1; no code at block 0 | node state, not EVM storage | n/a |

Three builds of one contract coexist on a new chain: for contract 2, 15,790 B executes at `…17e9`
(S4), 17,102 B ran in the simulation that computed its genesis storage (S2), and `main`'s source
compiles to 18,278 B (S1). Storage is therefore laid out by code that never executes: where the
generations differ, one decides the layout and the other executes it. Size and selector differences
between them are real compilation differences.

`crates/execution/evm/src/evm/hardforks/hybrid_rewards.rs` refuses to splice unless the live
ConsensusRegistry runtime code is exactly 21,929 B — a new chain satisfies this, the current testnet
does not (22,253 B). `HybridRewards` is `Never` in this repo's `devnet()`, `testnet()` and `mainnet()`
tables, at `main` and at `testnet-v1.5.2`, but that table is only the default: the schedule
is read from `networks.<name>.hardforks` in the node's config file
(`crates/execution/evm/src/network_profile/profile.rs`, validated at boot), so activation on a running
network is an operator setting this repository cannot observe. The fixture is `main`'s
`rayls-contracts/src/consensus/ConsensusRegistry.sol`: zeroing its 8 BlsG1 link sites and 12 immutable
sites in a fresh build reproduces it byte-for-byte.

## Part 3 — Deployments on testnet

Measured from `https://testnet-rpc.rayls.com` (chain id 7295799) at block 25,331,108 in November 2026. The
RPC reports node `1.5.2`, tag `testnet-v1.5.2`, which is not an ancestor of `main`.

| # | Contract | Testnet code equals |
|---|---|---|
| 1 | `ConsensusRegistry` | S2 at `v0.1.1`–`v0.1.5` |
| 2 | `DelegationPool` | S1 of `main` |
| 3 | `FeeAggregator` | S2 = S1 of `main` |
| 4 | `RewardDistributor` | S1 of `testnet-v1.3.0`, identical source at `mainnet-v1.3.0` |
| 5 | `NativeTokenController` | S2 = S1 = S4 of `main` |
| 6 | `RLS` | S2 = S1 = S4 of `main` |
| 7 | `RLSAccumulator` | S2 = S1 = S4 of `main` |
| 8 | `BlsG1` | S2 at `v0.1.1`–`v0.1.5`, byte-for-byte |
| 9 | `RewardCurve` | not deployed — no S2 entry, no protocol address, installed by no tier |

Contracts 1 and 8 match an S2 generation replaced at `v0.1.6`. The `v0.x` tags hang off a second, older root
commit (2025-11-26) that `main` does not contain.

DelegationPool, FeeAggregator and RewardDistributor have been proxy-upgraded (S6); the superseded
implementations at `…17e9`, `…17e8` and `…17e4` remain deployed and byte-identical to S4.

## Part 4 — Deployments on mainnet

Measured from `https://mainnet-rpc.rayls.com` (chain id 72957) at block 15,126,703 in November 2026. The
RPC reports node `1.4.3`, tag `mainnet-v1.4.3`, which is not an ancestor of `main`.

| # | Contract | Mainnet code equals |
|---|---|---|
| 1 | `ConsensusRegistry` | S2 of `main` |
| 2 | `DelegationPool` | S1 of `main` |
| 3 | `FeeAggregator` | S4 of `main` |
| 4 | `RewardDistributor` | S1 of `mainnet-v1.3.0`, identical source at `testnet-v1.3.0` |
| 5 | `NativeTokenController` | S2 = S1 = S4 of `main` |
| 6 | `RLS` | S2 = S1 = S4 of `main` |
| 7 | `RLSAccumulator` | S2 = S1 = S4 of `main` |
| 8 | `BlsG1` | S2 = S1 of `main` |
| 9 | `RewardCurve` | not deployed |

Contracts 3, 5, 6 and 7 still run the implementations genesis installed; 2 and 4 have been proxy-upgraded
(S6), with the retired implementations still deployed and byte-identical to S4.

Contracts 3, 5, 6 and 7 run byte-identical code on both chains. Contracts 1 and 8 disagree: mainnet runs
the generation `main` ships, testnet the one `v0.1.6` replaced. Contracts 2 and 4 run the same code on both,
at different implementation addresses.

## Part 5 — Local chain

Measured from `http://localhost:8545` (chain id 487, the `local` network) at block 55 in November 2026. The
RPC reports node `1.5.2`, built from `main`.

| # | Contract | Local code equals |
|---|---|---|
| 1 | `ConsensusRegistry` | S5 of `main` |
| 2 | `DelegationPool` | S4 of `main` |
| 3 | `FeeAggregator` | S4 of `main` |
| 4 | `RewardDistributor` | S4 = S2 of `main` |
| 5 | `NativeTokenController` | S2 = S1 = S4 of `main` |
| 6 | `RLS` | S2 = S1 = S4 of `main` |
| 7 | `RLSAccumulator` | S2 = S1 = S4 of `main` |
| 8 | `BlsG1` | S2 = S1 of `main` |
| 9 | `RewardCurve` | not deployed |

Contract 1 was read at block 0 too: there it is S2 of `main`, replaced by the `HybridRewards` fixture at
block 1. Every proxy points at the implementation address genesis installed, so S6 has not applied.
