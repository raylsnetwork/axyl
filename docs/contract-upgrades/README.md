# Shipping a Change to Axyl's On-Chain Contracts: Proxy Upgrade vs. Hardfork

Two different mechanisms exist for changing what a deployed Solidity contract does on a running
Axyl network, and picking the wrong one is either a wasted multi-week coordination effort or a
network-split risk. This doc gives the decision procedure, then the full step-by-step runbook for
each path.

**TL;DR:** most contract changes are a normal, single-transaction proxy upgrade
(`script/interactions/upgrade/UpgradeOps.s.sol`). A hardfork
(`crates/execution/evm/src/evm/hardforks/`) is only needed when the change touches something the
node's own Rust client encodes assumptions about, or something that gates consensus safety itself.

---

## Part 1 — Which one do I need?

Ask two questions about the change:

1. **Does the node's own client code depend on this contract's exact interface or behavior?**
   Not "does something call this contract" — every contract gets called. The question is whether
   `crates/execution/evm/src/evm/handler.rs` / `system_calls.rs` (the Rust side) hardcodes an ABI
   encoding, argument count, or return-value shape for this contract that would break if the
   contract's *external* interface changed.
2. **Does this contract gate a consensus-safety property** — who is allowed to validate, how
   stake/slashing/committee membership works, what counts as a valid block — such that one
   `UPGRADER_ROLE` key changing it unilaterally and instantly would be a materially bigger trust
   concession than "this app's internal logic changed"?

**If either answer is yes → hardfork.** The change needs every validator's client software to
agree on new rules, which a single transaction can't force — see Part 3.

**If both answers are no → normal proxy upgrade.** See Part 2.

### Why this distinction exists (short version)

A proxy upgrade only ever changes *what state a contract holds* (which implementation address a
storage slot points to). Every client — old or new — already knows how to execute a
`delegatecall` against whatever's there; nothing about *how to interpret it* changed, so no
coordination is needed and one transaction is sufficient.

A hardfork changes *the function that computes state from a block* — which is exactly what's
needed when the Rust client itself has to change (e.g. because it builds different calldata, or
because the genesis-time deployment mechanism has to run differently). A node running old
software would compute a different, wrong result after the change point unless it's specifically
upgraded and told exactly when the new rules start applying. That's what a hardfork's
chain-height-gated activation buys you: every node changes in lockstep, by construction, instead
of by hoping everyone upgrades in time.

### Worked examples from this repo

| Change | Needs | Why |
|---|---|---|
| #103 — wire `RewardCurve` into `RewardDistributor`, add `setRewardCurve`/`setOpenTierRewardCurve` | **Proxy upgrade** | `RewardDistributor.distributeRewards()` — the only function the client calls (`crates/execution/evm/src/evm/block.rs:317`, `RewardDistributor::distributeRewardsCall {}.abi_encode()`) — is a **zero-argument call**, unchanged by #103. The client only checks success/failure; it never decodes a return value or knows the new fields exist. New storage fields were appended at the end of `RewardDistributorStorage` (safe, no layout collision). No consensus-safety gating involved. |
| #85 — two-track (Track A/B) staking split | **Proxy upgrade** (precedent, already shipped) | Same reasoning: internal `RewardDistributor` logic changed substantially, external `distributeRewards()` call from the client didn't. |
| #79 / hybrid rewards — `ConsensusRegistry.applyIncentives` moves from a 1-arg to a 2-arg ABI | **Hardfork** (`HybridRewards`, shipped as PR #88) | The client's own calldata-building code (`system_calls.rs`) had to branch on which ABI to encode, depending on fork status. That's client-side coupling by definition — a plain proxy upgrade can't force every node's *client binary* to switch encodings at the same block. |
| Making `RewardDistributor`/`FeeAggregator` upgradeable in the first place | **Hardfork** (`AdminTransfer`, `Tokenomics`, `Uups` — already done, in the past) | Before these forks, the contracts weren't proxies at all (or had a broken `__self` immutable blocking `upgradeToAndCall`). Turning a non-upgradeable/broken contract into a normal proxy is itself a one-time, consensus-critical bytecode change — every node's execution of historical + future blocks must agree on the switch. Once done, all *later* logic changes to these same contracts are ordinary proxy upgrades — the fork was a one-time bootstrap, not a recurring requirement. |
| Any future change to `ConsensusRegistry`'s logic | **Hardfork** | It isn't a proxy at all — see below. |

### Why `ConsensusRegistry` specifically can never use Part 2's path

`ConsensusRegistry` has no `UUPSUpgradeable` inheritance and no admin-controlled implementation
slot — by design. At genesis, its address, its linked `BlsG1` library address, and its `_rls`
immutable are computed and baked directly into its **deployed bytecode** by Rust code
(`crates/execution/evm/src/reth_env/genesis.rs`), not stored as a portable artifact a proxy could
point to. There is no "redeploy and repoint" option; changing its logic means the client itself
must read the live account's linked values and splice them into new bytecode
(`crates/execution/evm/src/evm/hardforks/hybrid_rewards.rs` is the template for this). That
splice is exactly the kind of client-side computation that must ship as a versioned client
release, gated to one chain height — i.e., always a hardfork.

---

## Part 2 — Runbook: normal contract upgrade (proxy)

Applies to `RewardDistributor`, `FeeAggregator`, `DelegationPool`, `NativeTokenController`, `RLS`,
`RLSAccumulator` — the six proxies `script/interactions/upgrade/UpgradeOps.s.sol` already covers
— and to deploying a brand-new standalone contract like `RewardCurve` that these six call into.

### Pre-checks

1. Confirm the change passes both of Part 1's questions with "no."
2. Confirm new storage fields (if any) were **appended** to the end of the contract's
   ERC-7201-style storage struct, never inserted or reordered — reordering corrupts every
   existing deployment's storage on upgrade regardless of anything else being correct.
3. `forge build && forge test` clean, no regressions, from `rayls-contracts/`.

### Step 1 — Dry run locally

Deploy fresh via a throwaway local `anvil` instance first (not a fork — just proving the deploy
mechanics work) before touching a network with real state:

```bash
anvil --host 0.0.0.0 --port 8555
```

Exercise every new script action against it (deploy, status reads, admin writes) with a
throwaway/anvil test key, not any real key. This is cheap insurance against a typo in an env var
name or a wrong function selector before it matters.

### Step 2 — Dry run against a fork of the target network

```bash
anvil --fork-url $TESTNET_RPC_URL --port 8555
```

Run the **exact** sequence you intend to broadcast for real, against this fork. This catches
storage-layout surprises or wiring mistakes against the network's *actual* current state, not a
clean-room approximation of it.

### Step 3 — Pre-flight on the real network (read-only)

```bash
forge script script/interactions/upgrade/UpgradeOps.s.sol:UpgradeOps --sig "verify()" \
  --rpc-url $RPC_URL -vvvv
```

Confirms the current implementation address for every proxy and who holds `UPGRADER_ROLE` —
compare against who's about to sign the broadcast in Step 4.

### Step 4 — Broadcast the upgrade

```bash
forge script script/interactions/upgrade/UpgradeOps.s.sol:UpgradeOps \
  --sig "upgrade<ContractName>()" \
  --rpc-url $RPC_URL --broadcast --skip-simulation --private-key $UPGRADER_PK -vvvv
```

`--skip-simulation` is intentional — this deploys a real new implementation contract in the same
script, and forge's local revm doesn't always reproduce live chain state perfectly for that.

### Step 5 — Re-verify

```bash
forge script script/interactions/upgrade/UpgradeOps.s.sol:UpgradeOps --sig "verify()" \
  --rpc-url $RPC_URL -vvvv
```

Confirm the ERC-1967 implementation slot actually moved to the new address.

### Step 6 — Deploy any brand-new contracts, then wire them in

If the change introduces a genuinely new contract (no existing proxy to upgrade — e.g.
`RewardCurve`), deploy it now via its own Ops script's `deploy()` action, then wire the resulting
address into whatever upgraded contract needs it via that contract's `setConfig()`-style action
(e.g. `RewardDistributorOps.s.sol --sig "setConfig()"` with `REWARD_CURVE=<addr>`).

### Step 7 — Confirm

```bash
forge script script/interactions/<contract>/<Contract>Ops.s.sol:<Contract>Ops --sig "status()" \
  --rpc-url $RPC_URL -vvvv
```

Confirm the new wiring/values read back correctly before considering the change live.

### Making the same upgrade live from block 0 (fresh/local/private chains)

Steps 1–7 above upgrade an **already-running** network — they work on any network, because
they're just a transaction against an already-deployed proxy. A chain that hasn't been born yet
(a fresh local dev chain, CI's own dev-chain boot, a new private deployment) has no running proxy
to send that transaction to: its block 0 is not executed, it's **precomputed offline and baked
into the node binary at compile time**. If you only ship the live-upgrade path, a brand-new chain
boots with the *old* wiring and needs the same upgrade run against it all over again after the
fact — it does not get the change "for free" just because the source code changed.

**How block 0 is actually built** (`rayls-contracts/deployments/genesis/`):

1. `RlGenesis.sol` is a Foundry abstract contract that **simulates** the exact deploy → initialize
   → wire sequence a live upgrade would perform, entirely offline, against the network's fixed,
   well-known genesis addresses (the ones in `deployments.json`). It uses Foundry cheatcodes
   (`vm.startStateDiffRecording`, `vm.store`, `vm.etch`) to capture the resulting bytecode and
   storage without ever running on a real chain — see `instantiateRewardDistributor` for the
   existing pattern: it deploys the proxy, calls `initialize(...)`, and then — still inside the
   same recording — makes an *additional* post-init admin call,
   [`simulatedDeployment.setAccumulator(accumulator_)`](https://github.com/raylsnetwork/axyl/blob/f72fadcd8324b24bd3e7370cac1a2272b4be7ec2/rayls-contracts/deployments/genesis/RlGenesis.sol#L329),
   exactly the shape a new wiring call needs.
2. `script/GeneratePrecompileGenesisConfig.s.sol` calls every `instantiateX()` function in
   dependency order and writes the resulting accounts to
   `rayls-contracts/deployments/genesis/precompile-config.yaml`, keyed by the real genesis address
   (not the incidental address Foundry assigned the simulation).
3. That YAML is `include_str!`'d directly into the `rayls-network` binary at **compile time**
   (`crates/infrastructure/config/src/genesis.rs:46`), parsed by `fetch_precompile_genesis_accounts()`
   into block 0's account allocations. Regenerating the YAML on disk without rebuilding the binary
   changes nothing — the file has to be current *before* `cargo build` runs.

**To wire a new contract + admin call into this path** (worked example: what #103's `RewardCurve`
would need, since it currently is *not* genesis-embedded — a fresh chain today boots with it
unwired):

1. Add fixed genesis addresses for any brand-new contract to `deployments.json` (a new field on
   the `Deployments` struct in `deployments/Deployments.sol` — remember upper-case field names
   must sort before lower-case ones, per that file's own doc comment).
2. In `RlGenesis.sol`: add state variables and `instantiateRewardCurveImpl()` /
   `instantiateRewardCurve(impl, admin_)` functions, mirroring
   `instantiateRewardDistributorImpl` / `instantiateRewardDistributor` — deploy the impl once,
   deploy each proxy instance via `new ERC1967Proxy(impl, abi.encodeCall(RewardCurve.initialize, (admin_)))`.
3. Extend `instantiateRewardDistributor`'s own simulation to add
   `simulatedDeployment.setRewardCurve(...)` / `.setOpenTierRewardCurve(...)` calls in the same
   `vm.prank(admin_)` block that already wires `setAccumulator` (line 329 above) — same pattern,
   two more lines.
4. In `GeneratePrecompileGenesisConfig.s.sol`: call the new `instantiateRewardCurveImpl()` /
   `instantiateRewardCurve()` **before** `instantiateRewardDistributor(...)` (which now needs
   their addresses as additional wiring parameters), and append each result via
   `yamlAppendGenesisAccount`, same as every other contract in that script.
5. Run the script to regenerate `precompile-config.yaml`, then **rebuild `rayls-network`** — per
   the compile-time-embedding point above, this step is easy to forget and produces no error, just
   a binary that silently doesn't have the change.

This is genuinely separate work from Steps 1–7 — shipping only the live-upgrade scripts (which is
all #103 has today) is correct and sufficient for upgrading `devnet`/`testnet`/`mainnet`, but
means every *freshly started* chain (including `local`, and CI's own dev-chain boots) will need
the same live-upgrade procedure run against it once it's up, until someone also does this genesis
wiring.

### Rollback

Trivial and one-directional: broadcast `upgradeToAndCall` again, pointing at the **previous**
implementation address (recorded in Step 3's `verify()` output before you upgraded). No client
release, no validator coordination, no chain-height gating — it's just another ordinary
transaction, the same as the upgrade itself.

### Quick reference

| Item | Value |
|---|---|
| Script | `rayls-contracts/script/interactions/upgrade/UpgradeOps.s.sol` |
| Pre-flight / post-verify | `--sig "verify()"` |
| Upgrade a proxy | `--sig "upgrade<ContractName>()"` |
| Required role | `UPGRADER_ROLE` on the target proxy |
| New-contract deploy pattern | `new Impl(); new ERC1967Proxy(address(impl), abi.encodeCall(Impl.initialize, (...)))` |
| Storage-safety rule | Append new fields to the end of the storage struct only |
| Reversible how | Another `upgradeToAndCall`, pointing at the prior impl address |
| Coordination needed | None — one signer, one transaction |
| Making it live from block 0 too | Separate work — extend `rayls-contracts/deployments/genesis/RlGenesis.sol` + `script/GeneratePrecompileGenesisConfig.s.sol`, regenerate `precompile-config.yaml`, **rebuild the node** (`include_str!`'d at compile time) |

---

## Part 3 — Runbook: hardfork

Applies whenever Part 1 says "hardfork" — most commonly a `ConsensusRegistry` logic change, or any
change requiring the Rust client's own calldata-building/decoding code to change in lockstep.

### Step 0 — Design the migration as a pure function of existing chain state

The state mutation that runs at the activation block must be **fully deterministic given only
prior on-chain state** — never a fresh, non-reproducible input. Study
`crates/execution/evm/src/evm/hardforks/hybrid_rewards.rs` as the template: it reads the *live*
account's linked `BlsG1` address and `_rls` immutable straight out of its currently-deployed
bytecode and splices them into new logic, rather than hardcoding a value — every node computes
the identical splice because every node reads the identical prior state.

### Step 1 — Implement the migration module

Add a new file under `crates/execution/evm/src/evm/hardforks/` exposing a function like
`fn <name>_state<DB: alloy_evm::Database>(db: &mut State<DB>) -> HashMap<Address, RevmAccount>`,
using `account_with_code` (`hardforks/mod.rs`) to build replacement bytecode where needed.

### Step 2 — Register the new fork

- Add a new variant (with a doc comment) to the `hardfork!(RaylsHardFork { ... })` block in
  `crates/execution/evm/src/chainspec.rs`.
- Give it a `mainnet_id`-style discriminant byte (see the `match self { ... => 0x0d, ... }` block).
- Add it to **every** per-network schedule array (`devnet()`, `testnet()`, `mainnet()`, `local()`)
  — the array length in each function's return type (`[(Self, ForkCondition); N]`) must bump by
  one everywhere, which the compiler enforces. Set it `ForkCondition::Never` on networks not yet
  ready, and a real `ForkCondition::Block(N)` only where you actually want it active. There's no
  blanket rule that `local()` activates every fork immediately — some historical forks are
  correctly `Never` there because Local's genesis already reflects their post-fork state — but a
  **new** migration you're actively developing should get an early `Block` height on `local()` so
  it's exercised by every test run and dev-chain boot, not just a real deployment.
- Wire the new variant into the exhaustive `match fork { ... }` in
  `apply_activated_migrations` (`hardforks/mod.rs`), calling your Step 1 function.
- `schedule_contains_both_hardforks_for_all_networks` (`chainspec.rs` tests) and the general
  compiler exhaustiveness checks will catch a forgotten network or a missed match arm.

### Step 3 — Test in isolation

Unit-test the migration's byte-level correctness independent of hardcoded constants — e.g.
re-derive expected offsets from the checked-in build artifact so a Solidity recompile that shifts
them is caught immediately, rather than silently producing a corrupted splice (see
`hybrid_rewards.rs`'s own unit tests for the pattern).

### Step 4 — Test at the fork boundary

Write an integration test that drives execution through blocks immediately before, at, and after
the activation height (`crates/execution/evm/src/reth_env/tests.rs`,
`test_hybrid_rewards_fork_boundary` is the template): assert pre-fork calls still succeed under
old logic, assert the migration actually ran (e.g. account code length changed to the expected
new size), assert post-fork calls succeed under new logic.

### Step 5 — Prove it on Local

Because `local()`'s schedule activates a new fork early per Step 2, this is already happening on
every `cargo test` run and every local dev-chain boot by the time Step 4 passes — nothing
additional to do here, just don't skip Step 2's guidance to set an early local activation height.

### Step 6 — Schedule Devnet, then Testnet

Set real, near-term activation block heights in `devnet()`'s and then `testnet()`'s schedules —
`testnet()` is the first time this runs against independently-operated validators, not a single
test process.

### Step 7 — Ship a client release and coordinate every validator

This is the step with no equivalent in Part 2. Cut a versioned `rayls-network` release containing
the new fork logic and the chosen activation height. Write release notes stating the exact block
height and what changes. Communicate a firm deadline to **every** operator who must independently
upgrade before that height — validators, and any full-node/RPC/indexer operator who needs to stay
in sync. A proxy upgrade asks nothing of anyone outside the signer; a hardfork only works if
enough of the network has actually installed the new binary before the height arrives.

Pre-height sanity check: confirm via logs/version reporting that every known validator is running
a build containing the fork logic, the same way Part 2's `verify()` confirms upgrade readiness.

### Step 8 — Activation block arrives — monitor for a split

Every upgraded node independently applies the identical migration the instant the fork condition
newly holds for the block it's processing — nobody coordinates live, they just all compute the
same deterministic function against the same prior state. What to watch for: are all known
validators still certifying blocks at the same height on the same state root, or has the network
partitioned because some validator's node wasn't upgraded in time and is now computing a
different result on old software. This is the one moment where "did the hardfork work" and "is
the chain still one chain" are the same question.

### Step 9 — Post-activation verification

Confirm the migration produced **identical** state on every node, not just "didn't crash
anywhere" — cross-check state roots / account code across multiple independently-upgraded nodes
the way `bin/rayls-replay/src/integrity.rs` already cross-checks a node's own consensus-DB
commitments against its execution DB. Confirm the *new* behavior actually works end-to-end in
production (new-ABI calls succeed, no reverts), not only that old behavior kept working
pre-height.

### Rollback

There isn't a one-transaction undo. Reverting a hardfork **is itself a hardfork** — a new
release, a new activation height, the full Step 6–8 cycle again, now against a network that has
already executed some post-fork blocks you need to account for. This asymmetry with Part 2's
trivial rollback is exactly why Part 1's decision test matters: get it wrong in the
"needs a hardfork" direction and you've paid a large, mostly avoidable coordination cost; get it
wrong in the other direction (ship a hardfork-worthy change as a plain proxy upgrade) and you risk
an actual chain split, discovered in production.

### Quick reference

| Item | Value |
|---|---|
| Migration modules | `crates/execution/evm/src/evm/hardforks/*.rs` |
| Fork enum + per-network schedule | `crates/execution/evm/src/chainspec.rs` (`hardfork!(RaylsHardFork { ... })`, `devnet()`/`testnet()`/`mainnet()`/`local()`) |
| Dispatcher | `apply_activated_migrations` (`hardforks/mod.rs`), driven by `spec.newly_activated_forks(parent_number, block_number)` |
| Bytecode-splice helper | `account_with_code` (`hardforks/mod.rs`) |
| Template migration (contract-logic change) | `hardforks/hybrid_rewards.rs` (#79/#88) |
| Template fork-boundary test | `test_hybrid_rewards_fork_boundary` (`reth_env/tests.rs`) |
| Exhaustiveness safety net | `schedule_contains_both_hardforks_for_all_networks` test + compiler-enforced array length |
| What ships, concretely | A new **client binary release** with a chosen activation height — not a contract deployment |
| Coordination needed | Every validator must upgrade before the activation height |
| Reversible how | Another hardfork — no one-transaction undo |

---

## See also

- [`../reward-distribution/README.md`](../reward-distribution/README.md) — the fee/reward system
  Part 2's worked example (#103) operates on.
- [`../../rayls-contracts/script/interactions/README.md`](../../rayls-contracts/script/interactions/README.md) —
  full index of the Foundry Ops scripts referenced in Part 2.
- [`../../rayls-contracts/src/consensus/design.md`](../../rayls-contracts/src/consensus/design.md) —
  why `ConsensusRegistry` specifically has no upgrade path other than a hardfork.
