# Axyl's Solidity Contract Sources, and What a Fresh Chain Actually Runs

The repository contains several copies of the same contracts. This doc inventories them, specifies which one determines the code a new chain executes, and records where the copies disagree.

**TL;DR:** the code a new chain runs comes from `rayls-contracts/artifacts/*.json` — contracts 1–8 and 27 — except the implementations of contracts 2–7, which come from `crates/execution/evm/src/evm/hardforks/bytecodes/*_impl.bin`, contracts 28–30, which come from `rayls-contracts/deployments/genesis/precompile-config.yaml`, and contract 1, whose code `HybridRewards` replaces at block 1. None of it is generated from `rayls-contracts/src/**/*.sol` automatically. Three of the six `*_impl.bin` files are behind the source: `DelegationPool` and `RewardDistributor` are older builds missing functions the source now has, and `FeeAggregator`'s is not a build of any source state in this repo.

Everything read as "S1 of `main`" means `main` (`508f91e3`), whose `rayls-contracts/` and `crates/execution/evm/src/evm/hardforks/` match `v1.5.3` (`76896d7`) apart from contract 2's two read-only getters — the other eight contracts build identically to that release. Parts 3–5 are live measurements from November 2026 at the blocks each part names. Code equality means equality after zeroing the solc link and immutable windows, which hold the implementation's own address, baked in at construction.

See also `docs/contract-upgrades/README.md` (proxy upgrade vs hardfork decision) and `docs/hardforks/` (scheduling mechanics).

---

## Part 1 — Contracts and sources

### Contract enumeration

Numbering used throughout this doc: **1–9** are the deployable classes declared in `rayls-contracts/src`, **10–26** are the remaining `.sol` files there, **27–31** are sources outside `rayls-contracts/src` that a chain still executes. Per-tier coverage is in the next section.

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

Contract 9 is deployed on demand by `rayls-contracts/script/interactions/reward-curve/` and reached through `IRewardCurve`.

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
| S2 | Compiled artifacts | `rayls-contracts/artifacts/*.json` | 9 files — contracts 1–8 and 27 | `include_str!` in `crates/infrastructure/config/src/genesis.rs` → pre-genesis EVM simulation → `extend_accounts` in `crates/execution/evm/src/reth_env/genesis.rs` |
| S3 | Genesis alloc YAML | `rayls-contracts/deployments/genesis/precompile-config.yaml` | 13 accounts: contracts 2–5 and 7 (proxy and impl each), plus contracts 28–30 | `include_str!`, same file → genesis alloc. All 13 accounts go in; S4 replaces the five implementations and S2 again replaces the five proxies, so only 28–30 remain |
| S4 | Genesis override binaries | `crates/execution/evm/src/evm/hardforks/bytecodes/*.bin` | 6 — implementations of contracts 2–7 | `include_bytes!` in `crates/execution/evm/src/reth_env/genesis.rs` → `apply_greenfield_fixes()` |
| S5 | Hardfork migrations | `crates/execution/evm/src/evm/hardforks/bytecodes/<fork>/*.bin`, plus byte literals in `crates/execution/evm/src/evm/hardforks/*.rs` | 18 fixture files covering contracts 1–7 and 27, plus the STOP byte of 31 | state built in `crates/execution/evm/src/evm/hardforks/*.rs`, applied at an activation block |
| S6 | UUPS proxy upgrade | none — a transaction sent from `rayls-contracts/script/interactions/upgrade/UpgradeOps.s.sol` | no files in the repository; the deployed code and the event exist only on-chain | operator-initiated; applies only to contracts 2–7, the six upgradable ones |

S1–S5 are where the code a chain runs comes from: S2 and S4 determine block 0, S5 acts at an activation height, and S3 supplies contracts 28–30. S2–S5 are compiled into the node binary and require a rebuild to change. S6 is the only tier with no footprint in the repository: the code it installs is compiled locally from S1, so its provenance cannot be pinned from the repo — only by hashing the live code (see Part 3). Contracts 27–30 come from `rayls-contracts/node_modules` and 31 is node-internal, so none of them appears in S1.

### Propagation paths

A `.sol` change reaches a **new chain** only through the checked-in artifacts and binaries. `forge build` writes to the gitignored `rayls-contracts/out/`. The checked-in `rayls-contracts/artifacts/*.json` set is a separate hand-exported copy: no script, Makefile target or CI job regenerates it, and `git log main -- rayls-contracts/artifacts` contains only the root commit of this history ("Porting codebase from old repository", 2026-07-03). Re-exporting the selected files from `out/` into `rayls-contracts/artifacts/` and rebuilding the node is therefore the only route from S1 to S2.

An **existing chain** can be changed two ways:

- **Hardfork (S5).** The new code ships inside the node binary and is applied at an activation height. Its fixtures are compiled by hand from S1, so it carries the same regeneration problem as S2 and S4.
- **Proxy upgrade (S6).** A transaction from `rayls-contracts/script/interactions/upgrade/UpgradeOps.s.sol` points a proxy at new code, with no client change. Only contracts 2–7 can receive it: contract 1 has no proxy, and contract 27's code is fixed once the proxy is deployed.

### The genesis alloc YAML (S3)

`rayls-contracts/deployments/genesis/precompile-config.yaml` is generated by `rayls-contracts/script/GeneratePrecompileGenesisConfig.s.sol`, which broadcasts real deploys in a forge VM — contracts 28–30, the implementations and proxies of contracts 2–5 and 7, each proxy run through its `initialize` — and writes the resulting code, storage and balance under the addresses recorded in `rayls-contracts/deployments/deployments.json`: 13 accounts, 55 storage slots.

| S3 account | Runtime code | Slots |
|---|---|---|
| contracts 28–30 | Safe singleton, proxy factory, Safe proxy | 7 — singleton pointer, the single owner `0x0e301bee…f3161`, threshold, module and initialization flags; the governance Safe also carries the file's only balance, 10 native units |
| implementation of 2–5 and 7 | the S2 artifact with the forge script's own implementation address written in at the three `__self` sites: `62c20a…6dd7d1` 2, `a8452e…c00496` 3, `d718d5…9869a0` 4, `db8cff…ee3c76` 5, `416c42…d87ae6` 7 | 0 |
| proxy of 2–5 and 7 | contract 27 | 4 shared per contract: ERC-1967 implementation slot, `_initialized = 1`, `DEFAULT_ADMIN_ROLE` and `UPGRADER_ROLE` granted to `0x0e301bee…f3161`. Plus initializer arguments: 2 → 7 (delegation bounds, 14 unbonding epochs), 3 → 14 (fee split 5000/0/5000 bps, swap bounds, USDr `0x…0400`), 4 → 5, 5 → 0, 7 → 2 (wiring) |

### Tier freshness against S1

Provenance is by build: `forge build` of `rayls-contracts/src` at the tag named, compared over the solc link and immutable slots, whose values are set when the contract is constructed. `S2 of main` means that file as checked in at `main`; `S1 of <tag>` means a build of `src` at that tag. A tag range names the set of tags carrying the blob shown — tags are labels, not a commit walk, and the `v0.x` tags hang off a second root that `main` does not contain, so the endpoints carry no ancestry. `git ls-tree <tag> <path>` resolves any of them.

#### S2 — `rayls-contracts/artifacts/*.json`

This is what the simulation deploys. For contracts 2–5 and 7 it decides only the proxies' storage layout: the implementation code that executes comes from S3 and then S4.

| Contract | In sync with | Action |
|---|---|---|
| 1 `ConsensusRegistry` | S1 of `mainnet-v1.0.0` | no — replaced at the `HybridRewards` fork |
| 2 `DelegationPool` | S1 of `mainnet-v1.0.0` | yes — together with S4: this artifact's `initialize` lays out the proxy storage the S4 impl reads |
| 3 `FeeAggregator` | S1 of `main` | no |
| 4 `RewardDistributor` | S1 of `v0.1.5`–`mainnet-v1.2.0`, blob `3134b71c` | yes — together with S4: this artifact's `initialize` lays out the proxy storage the S4 impl reads |
| 5 `NativeTokenController` | S1 of `main` | no |
| 6 `RLS` | S1 of `main` | no |
| 7 `RLSAccumulator` | S1 of `main` | no |
| 8 `BlsG1` | S1 of `main` | no |
| 9 `RewardCurve` | not in `artifacts/` | n/a |

#### S3 — `rayls-contracts/deployments/genesis/precompile-config.yaml`

| Contract | In sync with |
|---|---|
| 28–30 Safe | outside the nine — vendored `safe-contracts`, not built from `src` |

#### S4 — `crates/execution/evm/src/evm/hardforks/bytecodes/*_impl.bin`

| Contract | In sync with | Action |
|---|---|---|
| 1 `ConsensusRegistry` | not in `bytecodes/` | n/a |
| 2 `DelegationPool` | S1 of `v0.1.5` | yes |
| 3 `FeeAggregator` | not a build of any `.sol` state in this repo | yes |
| 4 `RewardDistributor` | S1 of `v0.1.5`–`mainnet-v1.2.0`, blob `3134b71c` | yes |
| 5 `NativeTokenController` | S1 of `main` | no |
| 6 `RLS` | S1 of `main` | no |
| 7 `RLSAccumulator` | S1 of `main` | no |
| 8 `BlsG1` | not in `bytecodes/` | n/a |
| 9 `RewardCurve` | not in `bytecodes/` | n/a |

Rebuilding these binaries decides what a new chain runs: the stale ones are missing functions the source already has — contract 2 lacks 10 of 55, contract 3 lacks 2 of 54 (`initialize`, `setPoolConfig`), contract 4 lacks 6 of 39 (the RewardCurve and target-APY API). Contracts 2–7 sit behind UUPS proxies, so an existing chain keeps the implementation it has and takes the change only through an upgrade transaction — S6. The alternative is a hardfork migration (S5).

#### S5 — `crates/execution/evm/src/evm/hardforks/bytecodes/<fork>/*.bin`

| Contract | At fork | In sync with |
|---|---|---|
| 1 `ConsensusRegistry` | `HybridRewards` | S1 of `main` |
| 2 `DelegationPool` | `AdminTransfer` | S4 of `v0.1.2`–`v0.1.3`, blob `0d5f70a4` |
| 4 `RewardDistributor` | `AdminTransfer` | S4 of `v0.1.2`–`v0.1.3`, blob `8d6dd62b` |
| 2 `DelegationPool` | `Tokenomics` | S4 of `v0.1.4` |
| 3 `FeeAggregator` | `Tokenomics` | S4 of `v0.1.4` |
| 4 `RewardDistributor` | `Tokenomics` | S4 of `v0.1.4` |
| 5 `NativeTokenController` | `Tokenomics` | S4 of `v0.1.4` |
| 6 `RLS` | `Tokenomics` | S4 of `v0.1.4` |
| 7 `RLSAccumulator` | `Tokenomics` | S4 of `v0.1.4` |
| 2 `DelegationPool` | `Uups` | S1 of `v0.1.5` |
| 3 `FeeAggregator` | `Uups` | not a build of any `.sol` state in this repo |
| 4 `RewardDistributor` | `Uups` | S1 of `v0.1.5`–`mainnet-v1.2.0`, blob `3134b71c` |
| 5 `NativeTokenController` | `Uups` | S1 of `main` |
| 6 `RLS` | `Uups` | S1 of `main` |
| 7 `RLSAccumulator` | `Uups` | S1 of `main` |
| 27 `ERC1967Proxy` | `AdminTransfer` | S2 of `main` |
| 27 `ERC1967Proxy` | `RlsStorage` | S2 of `main` |
| 27 `ERC1967Proxy` | `Tokenomics` | S2 of `main` |
| 31 USDr precompile | `Erc20PrecompileBytecode` | not Solidity — one STOP byte |
| 31 USDr precompile | `UsdrSupplyCorrection` | the same STOP byte, reasserted with the `TOTAL_SUPPLY` storage fix |

## Part 2 — New chain

Under `local()`'s schedule (`crates/execution/evm/src/chainspec/schedule.rs`), the only S5 migrations a new chain runs are `HybridRewards` and `Erc20PrecompileBytecode`, both at block 1; S5's other fixtures are scheduled `Block(0)` and never run.

Genesis is built by `GenesisArgs::execute` (`crates/infrastructure/network-cli/src/genesis/mod.rs`) in four passes over one account map: the pre-genesis simulation (S2), the YAML (S3), the implementation binaries (S4), and S2 again — its five proxy accounts re-applied on top. The table is that map read as a timeline: left to right is cause, `Block ≥ 0` is the effect, and the last column that writes is what a new chain gets. S4 exists because an implementation's `__self` is baked into its runtime code when the contract is constructed and cannot be rewritten afterwards: the YAML's copies carry the forge script's throwaway addresses there, so S4 installs runtime code whose `__self` is the protocol address.

| Contract | S1 | S2 | S3 | S4 | S2 again | S5 | S6 | Block ≥ 0 |
|---|---|---|---|---|---|---|---|---|
| 1 `ConsensusRegistry` | `—` | `cs` | `—` | `—` | `—` | `c` at 1 | `—` | S2 code → S5's fixture at block 1; S2 storage |
| 2 `DelegationPool` impl | `—` | `i` | `c` | `cs` | `—` | `—` | `—` | S4 code; S4 storage: initializer lock |
| 2 `DelegationPool` proxy | `—` | `cs` | `cs` | `—` | `cs` | `—` | `—` | contract 27 code from S2; S2 storage |
| 3 `FeeAggregator` impl | `—` | `i` | `c` | `cs` | `—` | `—` | `—` | S4 code; S4 storage: initializer lock |
| 3 `FeeAggregator` proxy | `—` | `cs` | `cs` | `—` | `cs` | `—` | `—` | contract 27 code from S2; S2 storage |
| 4 `RewardDistributor` impl | `—` | `i` | `c` | `cs` | `—` | `—` | `—` | S4 code; S4 storage: initializer lock |
| 4 `RewardDistributor` proxy | `—` | `cs` | `cs` | `—` | `cs` | `—` | `—` | contract 27 code from S2; S2 storage |
| 5 `NativeTokenController` impl | `—` | `i` | `c` | `cs` | `—` | `—` | `—` | S4 code; S4 storage: initializer lock |
| 5 `NativeTokenController` proxy | `—` | `cs` | `cs` | `—` | `cs` | `—` | `—` | contract 27 code from S2; S2 storage |
| 6 `RLS` impl | `—` | `c` | `—` | `cs` | `—` | `—` | `—` | S4 code; S4 storage: initializer lock |
| 6 `RLS` proxy | `—` | `cs` | `—` | `s` | `—` | `—` | `—` | contract 27 code from S2; S2 storage, plus the S4 allowance slot |
| 7 `RLSAccumulator` impl | `—` | `i` | `c` | `cs` | `—` | `—` | `—` | S4 code; S4 storage: initializer lock |
| 7 `RLSAccumulator` proxy | `—` | `cs` | `cs` | `—` | `cs` | `—` | `—` | contract 27 code from S2; S2 storage |
| 8 `BlsG1`, derived address | `—` | `c` | `—` | `—` | `—` | `—` | `—` | S2 code; no storage, library |
| 9 `RewardCurve` | `—` | `—` | `—` | `—` | `—` | `—` | `—` | not deployed |
| 27 `ERC1967Proxy`, the code of all six proxies | `—` | `c` | `c` | `—` | `c` | `—` | `—` | the code in every proxy row |
| 28 `SafeImpl` singleton | `—` | `—` | `cs` | `—` | `—` | `—` | `—` | S3 code; S3 storage, 1 slot |
| 29 `SafeProxyFactory` | `—` | `—` | `c` | `—` | `—` | `—` | `—` | S3 code; no storage |
| 30 Safe proxy | `—` | `—` | `cs` | `—` | `—` | `—` | `—` | S3 code; S3 storage, 6 slots and a balance |
| 31 USDr precompile | `—` | `—` | `—` | `—` | `—` | `c` at 1 | `—` | no code at block 0 → one STOP byte; storage is node state, not EVM |

**Legend**

| Token | Meaning |
|---|---|
| `c`, `s`, `cs` | the pass writes code, storage, or both |
| `i` | implementation code the simulation seeds so the proxies' `initialize` has something to run against, then discards. That run is what set the proxies' layout; the code never reaches genesis |
| `—` | the pass writes nothing here |
| `c` at 1 | code written at block 1 by a hardfork rather than at genesis. On an existing chain the fork may not run: `HybridRewards` first checks the live code's length (`OLD_DEPLOYED_LEN`), and schedules are an operator setting — `networks.<name>.hardforks` in the node's config file (`crates/execution/evm/src/network_profile/profile.rs`) |
| `→` | in `Block ≥ 0`: what changes at block 1 |
| contract 6's `s` | the allowance letting contract 4 spend contract 6's token on contract 7's behalf: the simulation files it under a temporary address, so the real slot would be empty without this write |

## Part 3 — Deployments on testnet

Measured from `https://testnet-rpc.rayls.com` (chain id 7295799) at block 25,331,108 in November 2026. The RPC reports node `1.5.2`, tag `testnet-v1.5.2`, which is not an ancestor of `main`.

| # | Contract | Testnet code equals |
|---|---|---|
| 1 | `ConsensusRegistry` | S2 at `v0.1.1`–`v0.1.5` |
| 2 | `DelegationPool` | S1 of `v1.5.3` |
| 3 | `FeeAggregator` | S2 = S1 of `main` |
| 4 | `RewardDistributor` | S1 of `testnet-v1.3.0`, identical source at `mainnet-v1.3.0` |
| 5 | `NativeTokenController` | S2 = S1 = S4 of `main` |
| 6 | `RLS` | S2 = S1 = S4 of `main` |
| 7 | `RLSAccumulator` | S2 = S1 = S4 of `main` |
| 8 | `BlsG1` | S2 at `v0.1.1`–`v0.1.5`, byte-for-byte |
| 9 | `RewardCurve` | not deployed — no S2 entry, no protocol address, installed by no tier |

Contracts 1 and 8 match an S2 generation replaced at `v0.1.6`. The `v0.x` tags hang off a second, older root commit (2025-11-26) that `main` does not contain.

DelegationPool, FeeAggregator and RewardDistributor — contracts 2, 3 and 4 — have been proxy-upgraded (S6); the implementations they replaced are still deployed and byte-identical to S4.

## Part 4 — Deployments on mainnet

Measured from `https://mainnet-rpc.rayls.com` (chain id 72957) at block 15,126,703 in November 2026. The RPC reports node `1.4.3`, tag `mainnet-v1.4.3`, which is not an ancestor of `main`.

| # | Contract | Mainnet code equals |
|---|---|---|
| 1 | `ConsensusRegistry` | S2 of `main` |
| 2 | `DelegationPool` | S1 of `v1.5.3` |
| 3 | `FeeAggregator` | S4 of `main` |
| 4 | `RewardDistributor` | S1 of `mainnet-v1.3.0`, identical source at `testnet-v1.3.0` |
| 5 | `NativeTokenController` | S2 = S1 = S4 of `main` |
| 6 | `RLS` | S2 = S1 = S4 of `main` |
| 7 | `RLSAccumulator` | S2 = S1 = S4 of `main` |
| 8 | `BlsG1` | S2 = S1 of `main` |
| 9 | `RewardCurve` | not deployed |

Contracts 3, 5, 6 and 7 still run the implementations genesis installed; 2 and 4 have been proxy-upgraded (S6), with the retired implementations still deployed and byte-identical to S4.

## Part 5 — Local chain

Measured from `http://localhost:8545` (chain id 487, the `local` network) at block 55 in November 2026. The RPC reports node `1.5.2`, built from `main` at `e8c4ea1b`.

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

Contract 1 was read at block 0 too: there it is S2 of `main`, replaced by the `HybridRewards` fixture at block 1. Every proxy points at the implementation address genesis installed, so S6 has not applied.
