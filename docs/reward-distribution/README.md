# Reward Distribution

This document describes the reward distribution flow in the Rayls network, covering how fees are collected, swapped, and distributed to validators, the ecosystem treasury, and the burn bridge.

## Overview

Reward distribution happens in two stages that run on a different schedule:

- **`RewardDistributor.distributeRewards()` is fully automatic** — it is `onlySystemCall` and
  is invoked by the EVM each epoch as the third post-execution system call
  (`applyIncentives` → `concludeEpoch` → `distributeRewards`, see
  `crates/execution/evm/src/evm/block.rs:277`). No human action is required.
- **`FeeAggregator.distributeEpochFees()` is keeper-driven** — a `KEEPER_ROLE` holder calls
  it to convert accumulated USDr fees into RLS and forward them to the validator pool,
  ecosystem treasury, and the burn bridge.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        EPOCH END (Automatic)                        │
│                                                                     │
│  Rust Client (block.rs::finish)                                     │
│    ├─ applyIncentives(RewardInfo[], totalRounds) → ConsensusRegistry│
│    │    Records a hybrid blend (participation 80% + Anchor Commit   │
│    │    Share 10% + stake tier 10%) per validator as performance     │
│    │    weights for the completed epoch                             │
│    │                                                                │
│    └─ concludeEpoch(newCommittee[])  → ConsensusRegistry            │
│         Rotates the validator committee for the next epoch          │
└─────────────────────────────────────────────────────────────────────┘

                              ⬇ (decoupled)

┌─────────────────────────────────────────────────────────────────────┐
│                   DISTRIBUTION (Manual - Keeper)                    │
│                                                                     │
│  Step 1: FeeAggregator.distributeEpochFees()                        │
│    ├─ Collects accumulated USDr fees                                │
│    ├─ Swaps USDr → RLS via Algebra DEX                              │
│    └─ Distributes RLS to 3 categories:                              │
│         ├─ Validators (50%) → RewardDistributor.receiveRewards()    │
│         ├─ Ecosystem (30%) → Ecosystem Treasury address             │
│         └─ Burn (20%)      → LayerZero OFT bridge to Ethereum       │
│                                                                     │
│  Step 2: RewardDistributor.distributeRewards()                      │
│    ├─ Reads performance weights from ConsensusRegistry              │
│    ├─ Distributes RLS proportionally to the recorded weights        │
│    └─ Splits between validator own stake and delegation pool        │
│                                                                     │
│  Step 3: Validators call claimRewards() (permissionless)            │
│    └─ Withdraws accumulated rewards to validator address            │
└─────────────────────────────────────────────────────────────────────┘
```

## Contracts

| Contract | Address | Role |
|----------|---------|------|
| **ConsensusRegistry** | `0x07E17...E17e1` | Stores validator info, stake balances, performance weights |
| **FeeAggregator** | `0x07E17...E17e3` | Collects fees, swaps USDr→RLS, distributes to 3 categories |
| **RewardDistributor** | `0x07E17...E17e5` | Distributes RLS to validators based on performance |
| **DelegationPool** | `0x07E17...E17e2` | Manages delegated stake and pool rewards |
| **RLS Token** | `0x07E17...E17eA` | ERC-20 governance and staking token |

## Distribution Categories

The `DistributionConfig` defines how RLS is split after the USDr→RLS swap:

| Category | Field | Recipient | Description |
|----------|-------|-----------|-------------|
| Validators | `validatorPoolBps` | RewardDistributor | Distributed to validators proportional to block production |
| Ecosystem | `ecosystemBps` | Ecosystem Treasury | Foundation operations, grants, governance |
| Burn | `burnBps` | LayerZero bridge → Ethereum | Bridged to Ethereum and burned via OFT adapter |

All values are in basis points (1 bps = 0.01%). Total must equal 10,000 (100%).

## Detailed Flow

### 1. Epoch End (Automatic)

At each epoch boundary, the Rust execution layer makes two system calls:

**`applyIncentives(RewardInfo[], totalRounds)`** — Records each validator's hybrid performance weight:
- Input: array of `(validatorAddress, participationRounds, anchorRounds)` triples, plus the
  epoch-wide `totalRounds` committed
- Computes a blended weight per validator: 80% normalized participation share (rounds whose
  committed sub-dag included the validator's certificate), 10% Anchor Commit Share (rounds the
  validator led — the sole signal in the pre-hybrid model), 10% stake tier (own required stake +
  delegated stake, tiered above a configurable minimum)
- A validator that contributed no certificate this epoch (`participationRounds == 0`) earns
  nothing from any component; validators above zero but below an optional participation floor
  (`participationFloorBps`, disabled by default) are also excluded
- Stores in `_performanceWeights` for RewardDistributor to consume
- Called with `SYSTEM_ADDRESS` as msg.sender

> **Activation is out of scope for this change.** The blend above is implemented and tested, but
> when/how it activates on a network (fork schedule, and how the contract carrying it is
> deployed or upgraded) is a separate follow-up — see `src/consensus/design.md` ("Rollout is
> intentionally out of scope").
- Stores in `_performanceWeights` for RewardDistributor to consume
- Called with `SYSTEM_ADDRESS` as msg.sender

**`concludeEpoch(address[] newCommittee)`** — Transitions to the next epoch:
- Shuffles eligible validators deterministically using BLS signature randomness
- Updates epoch info ring buffer
- Processes validator activation/exit queue
- Emits `NewEpoch` event

### 2. Fee Distribution (Manual)

**`FeeAggregator.distributeEpochFees()`** — Keeper-triggered:
- Reads accumulated USDr balance
- Swaps to RLS via Algebra DEX (with slippage protection)
- Splits RLS according to `DistributionConfig`:
  - **Validator pool**: Transfers to RewardDistributor + calls `receiveRewards(amount)`
  - **Ecosystem**: Direct ERC-20 transfer to treasury address
  - **Burn**: Bridges to Ethereum via LayerZero OFT `send()` — fails silently if bridge is unconfigured

Each recipient transfer is isolated via try/catch — a failing recipient does not block others.

### 3. Reward Distribution (Automatic)

**`RewardDistributor.distributeRewards()`** — Automatic, system-call only:
- `onlySystemCall` (no `KEEPER_ROLE` involvement); the EVM invokes it once per epoch from
  `RaylsBlockExecutor` (`evm/block.rs:277`) after `concludeEpoch`.
- Reads `getEpochPerformanceWeights()` from `ConsensusRegistry`.
- If performance data exists: distributes proportionally by `stake × headerCount`.
- If no performance data: falls back to pure stake-based distribution.
- For each validator, splits between own stake and the delegation pool.

There is no `startDistribution()` / `continueDistribution(batchSize)` batched alternative
in the current contract — distribution always completes in a single call.

### 4. Claiming (Permissionless)

**`RewardDistributor.claimRewards(validatorAddress)`** — Validator-initiated:
- Only the validator themselves can claim
- Transfers accumulated `pendingValidatorRewards` to the validator (or custom recipient)
- Validators can set a custom reward recipient via `setRewardRecipient(address)`

## LayerZero Burn Bridge

The burn category bridges RLS to Ethereum instead of sending to a burn address:

```
FeeAggregator._lzBridgeBurn(amount)
  ├─ IOFT(adapter).quoteSend(params)     → Get native fee estimate
  ├─ rls.approve(adapter, amount)        → Approve OFT adapter
  └─ IOFT(adapter).send{value: fee}()   → Bridge tokens to Ethereum
       ├─ OFT adapter burns/locks RLS on Rayls
       └─ LZ message sent to Ethereum peer
            └─ Tokens minted/released at burn recipient address
```

Configuration is set by `DEFAULT_ADMIN_ROLE` post-deployment via individual setters on
`FeeAggregator`:

- `setOftBridge(address)` — LayerZero OFT adapter address on Rayls (the `lzOftAdapter` slot).
- `setDstEid(uint32)` — Destination endpoint ID (e.g. 30101 for Ethereum mainnet).
- `setBurnAddress(address)` — Burn recipient on Ethereum.

There is no single `setLzBurnConfig` aggregate setter; the three values are configured
individually.

The contract must hold native tokens to pay LayerZero messaging fees.

## Access Control

| Function | Required Role | Contract |
|----------|--------------|----------|
| `distributeEpochFees()` | `KEEPER_ROLE` | FeeAggregator |
| `distributeRewards()` | System call only | RewardDistributor |
| `claimRewards()` | Validator only | RewardDistributor |
| `setConfig()` | `DEFAULT_ADMIN_ROLE` | FeeAggregator |
| `setOftBridge()` / `setDstEid()` / `setBurnAddress()` | `DEFAULT_ADMIN_ROLE` | FeeAggregator |
| `applyIncentives()` | System call only | ConsensusRegistry |
| `concludeEpoch()` | System call only | ConsensusRegistry |

## Timing Considerations

- The keeper should call `distributeRewards()` **before** the next epoch's `applyIncentives()`, which clears `_performanceWeights`. If missed, RewardDistributor falls back to stake-based distribution — no funds are lost.
- `distributeEpochFees()` can be called at any time — fees accumulate if skipped.
- If the USDr balance is below the minimum swap amount, distribution is silently skipped and fees accumulate to the next call.

## Staking Flow

Staked RLS tokens are held by the ConsensusRegistry contract:

```
Stake:   Validator → transferFrom → ConsensusRegistry (holds RLS)
                                     balances[validator] = stakeAmount

Unstake: ConsensusRegistry → transfer → Validator (receives RLS + rewards)
                              balances[validator] = 0
```

Rewards accumulate in `balances[validator]` above the initial stake amount. The difference `balance - stakeAmount` is the claimable reward via `claimStakeRewards()`.
