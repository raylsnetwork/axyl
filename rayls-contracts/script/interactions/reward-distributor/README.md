# RewardDistributor interactions

All operations live in [`RewardDistributorOps.s.sol`](RewardDistributorOps.s.sol). Pick the action with `--sig`.

The RewardDistributor receives RLS from the FeeAggregator and allocates per-validator rewards each epoch using consensus performance weights (or pure stake fallback), split across two tracks — Track A (own stake + whitelisted/locked delegators, `targetApyBps`) and Track B (open-tier delegators, `openTierTargetApyBps`). Each track's target APY can instead be resolved from a wired `RewardCurve` (issue #103, see [`../reward-curve/`](../reward-curve/)) — `address(0)` on `rewardCurve`/`openTierRewardCurve` disables it and falls back to the static bps, so wiring a curve in is fully reversible. When a track's target is above what fee throughput covers, it pulls top-ups from the `RLSAccumulator`. Validators (or their custom recipients) claim accrued RLS via `claim`.

## Functions

| `--sig` | Purpose | Caller role | Required env vars |
|---|---|---|---|
| `status()` | Wiring, balances, both tracks' APY/RewardCurve, per-validator pending | none | — (`VALIDATOR`, `ADMIN` optional) |
| `setConfig()` | Update FA / DP / CR / Accumulator wiring, both tracks' `targetApyBps`, and both `RewardCurve` addresses | DEFAULT_ADMIN_ROLE | any of `FEE_AGGREGATOR`, `DELEGATION_POOL`, `CONSENSUS_REGISTRY`, `ACCUMULATOR`, `TARGET_APY_BPS`, `OPEN_TIER_TARGET_APY_BPS`, `REWARD_CURVE`, `OPEN_TIER_REWARD_CURVE` |
| `claim()` | Withdraw a validator's pending RLS | anyone | `VALIDATOR` |
| `setRecipient()` | Validator sets custom reward recipient | the validator | `RECIPIENT` (0x0 to clear) |
| `run()` | Default — alias for `status()` | none | — |

## Examples

### Set 50% target APY (subsidy on)

```bash
TARGET_APY_BPS=5000 \
  forge script script/interactions/reward-distributor/RewardDistributorOps.s.sol:RewardDistributorOps \
  --sig "setConfig()" \
  --rpc-url $RPC_URL --broadcast --private-key $ADMIN_PK -vvvv
```

### Wire a RewardCurve into Track A + Track B

```bash
REWARD_CURVE=0xTrackACurveAddress OPEN_TIER_REWARD_CURVE=0xTrackBCurveAddress \
  forge script script/interactions/reward-distributor/RewardDistributorOps.s.sol:RewardDistributorOps \
  --sig "setConfig()" \
  --rpc-url $RPC_URL --broadcast --private-key $ADMIN_PK -vvvv
```

### Validator claims rewards

```bash
VALIDATOR=0x... \
  forge script script/interactions/reward-distributor/RewardDistributorOps.s.sol:RewardDistributorOps \
  --sig "claim()" \
  --rpc-url $RPC_URL --broadcast --private-key $VALIDATOR_PK -vvvv
```

### Validator routes rewards to a cold wallet

```bash
RECIPIENT=0xColdWalletAddress \
  forge script script/interactions/reward-distributor/RewardDistributorOps.s.sol:RewardDistributorOps \
  --sig "setRecipient()" \
  --rpc-url $RPC_URL --broadcast --private-key $VALIDATOR_PK -vvvv
```

## Notes

- Per-epoch allocation (`distributeRewards`) is a **system call** — runs automatically each epoch, not via these scripts.
- `ACCUMULATOR`, `REWARD_CURVE`, and `OPEN_TIER_REWARD_CURVE` env vars all use `0x000000000000000000000000000000000000dEaD` as a "not provided" sentinel, since `address(0)` is a valid input for each (disables top-ups / falls back to the static bps target, respectively). `TARGET_APY_BPS` and `OPEN_TIER_TARGET_APY_BPS` use `type(uint256).max` as their "not provided" sentinel the same way.
- `setRecipient` here is for validator's portion. Delegators set their own recipients on the DelegationPool.
