# Interaction scripts

Foundry scripts for operating the deployed Rayls contracts. Each contract has a single consolidated `*Ops.s.sol` file with multiple actions — pick one with `--sig "actionName()"`. The DelegationPool keeps role separation (inspect/admin/validator/delegator), with one `Ops` file per role.

## Layout

```
interactions/
├── _base/InteractionScript.sol         ← shared base (helpers + deployments loader)
├── _mocks/                             ← TESTNET ONLY mock deployments
│   └── DeployFeeAggregatorMocks.s.sol
├── fee-aggregator/
│   └── FeeAggregatorOps.s.sol          ← status / setConfig / swap / distribute / pipeline
├── reward-distributor/
│   └── RewardDistributorOps.s.sol      ← status / setConfig / claim / setRecipient
├── rls-accumulator/
│   └── RlsAccumulatorOps.s.sol         ← status / deposit / setConfig / refreshApproval / revokeApproval
├── testnet-minters/                  ← TESTNET ONLY
│   └── TestnetMinterOps.s.sol          ← status / grantRls / grantUsdr / revokeRls / revokeUsdr
└── delegation-pool/
    ├── inspect/InspectOps.s.sol        ← status / rewardDebt
    ├── admin/AdminOps.s.sol            ← setConfig
    ├── validator/ValidatorOps.s.sol    ← register / registerBatch / claimCommission
    └── delegator/DelegatorOps.s.sol    ← delegate / requestUndelegation / completeUndelegation / claimRewards / setRecipient
```

## Tokenomics flow

```
                                    USDr fees (basefee)
                                            │
                                            ▼
   ┌───────────────────────────────────────────────────────────────┐
   │                       FeeAggregator                            │
   │  ┌─────────────┐    ┌──────────────────────────────────────┐  │
   │  │ swap()      │ →  │ pendingRlsForDistribution            │  │
   │  └─────────────┘    └──────────────────────────────────────┘  │
   │                                  │                             │
   │                          distribute()                          │
   │                                  │                             │
   │      ┌───────────────────────────┼────────────────────────┐    │
   │      │ validatorPoolBps          │            burnBps     │    │
   │      ▼                           ▼                ▼       │    │
   │  RewardDistributor      ecosystemTreasury    OFT bridge   │    │
   └──────│──────────────────────────────────────────────────────┘
          │
          ▼ (each epoch, system call)
   ┌──────────────────────────────────────────────────────┐
   │                RewardDistributor                      │
   │  • Splits totalPending by perf weight                 │
   │  • Top-up shortfall from RLSAccumulator if APY < tgt  │
   │  • Per-validator: validatorShare → pending            │
   │                   poolShare      → DelegationPool     │
   └──────────────────────────────────────────────────────┘
          │                              │
          ▼ claim()                      ▼ distributePoolRewards
   ┌──────────────────┐          ┌──────────────────────────┐
   │ Validator wallet │          │     DelegationPool       │
   │ (or recipient)   │          │  rewardPerShareAccum++   │
   └──────────────────┘          └──────────────────────────┘
                                              │
                                              ▼ claimRewards()
                                     ┌──────────────────┐
                                     │ Delegator wallet │
                                     │ (or recipient)   │
                                     └──────────────────┘
```

## Quick reference

| Goal | Script + sig |
|---|---|
| **Inspect FA** | `fee-aggregator/FeeAggregatorOps.s.sol --sig "status()"` |
| **Update FA config** | `fee-aggregator/FeeAggregatorOps.s.sol --sig "setConfig()"` |
| **Fee → reward pipeline** | `fee-aggregator/FeeAggregatorOps.s.sol --sig "pipeline()"` |
| **Set 50% APY target** | `reward-distributor/RewardDistributorOps.s.sol --sig "setConfig()"` (`TARGET_APY_BPS=5000`) |
| **Top-up reserve** | `rls-accumulator/RlsAccumulatorOps.s.sol --sig "deposit()"` |
| **Validator: open pool** | `delegation-pool/validator/ValidatorOps.s.sol --sig "register()"` |
| **Validator: claim commission** | `delegation-pool/validator/ValidatorOps.s.sol --sig "claimCommission()"` |
| **Delegator: stake** | `delegation-pool/delegator/DelegatorOps.s.sol --sig "delegate()"` |
| **Delegator: claim** | `delegation-pool/delegator/DelegatorOps.s.sol --sig "claimRewards()"` |
| **Validator: claim RD reward** | `reward-distributor/RewardDistributorOps.s.sol --sig "claim()"` |

## Conventions

### File naming

- One `*Ops.s.sol` per contract (or per role for DelegationPool)
- `--sig "funcName()"` selects the action
- `run()` is the default — typically aliases to `status()`

### Function naming inside an Ops file

- `status()` — read-only state dump
- `setConfig()` — admin updater (env vars are optional; pass any subset)
- Action verbs (`delegate`, `claim*`, `swap`, `distribute`, `deposit`, `refreshApproval`) — single state-changing operation
- Auxiliary diagnostics (`rewardDebt`) when relevant

### Env vars

- ALL_CAPS_SNAKE
- For multi-input admin ops, use `vm.envOr(..., sentinel)` and treat any subset as optional
- For required user inputs, use `vm.envOr(..., 0)` then `require(... > 0, "Set X env var")` for a clear error

### Logging

- Section headers via `logSection("Title")` from the base contract
- Log pre-state, mutation result, post-state — every state-changing function
- Use `balanceOf(token, account)` from the base contract — works with the USDr precompile

### Broadcast safety

- `vm.startBroadcast()` / `vm.stopBroadcast()` per function — one block
- Verify-after-write with `require` statements
- Pass `--skip-simulation` when touching the USDr precompile or LayerZero OFT calls

### Inheritance

All Ops contracts extend [`_base/InteractionScript`](_base/InteractionScript.sol), which provides:
- `setUp()` that loads `deployments/deployments.json` into a typed `Deployments` struct
- `rls` — the RLS token address
- `balanceOf(token, account)` and `allowance(token, owner, spender)` — precompile-safe staticcalls
- `logSection(title)` for consistent output

Override `_setUp()` (note the underscore) to bind your contract's instance.

## Adding a new action

1. Find the appropriate `*Ops.s.sol` (by contract / role)
2. Add an `external` or `public` function — that's the new action
3. Document required/optional env vars in a `///` doc comment above it
4. Add a row to the folder's `README.md`

## Adding a new contract

1. Create `<contract-name>/<ContractName>Ops.s.sol`
2. Inherit from `InteractionScript`, override `_setUp()` to bind the instance
3. Add `status()`, `setConfig()`, action functions
4. Create a `README.md` in the folder

## Mocks (testnet only)

`_mocks/` contains scripts that deploy mock contracts for local testing. They refuse to run on the production chain id (487).

- `_mocks/DeployFeeAggregatorMocks.s.sol` — deploys MockAlgebraRouter + MockAlgebraPool + MockOFTBridge and wires them into the FeeAggregator
