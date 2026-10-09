# RewardCurve interactions

All operations live in [`RewardCurveOps.s.sol`](RewardCurveOps.s.sol). Pick the action with `--sig`.

`RewardCurve` (issue #103) is a self-regulating, revenue-based reward curve: `APY = (baseMonthlyEmission + variableMonthlyEmission) * 12 / rlsStaked`, always derived, never stored. It doesn't custody or transfer RLS — real payout stays in `RewardDistributor`/`RLSAccumulator`. Unlike every other contract under `interactions/`, it has **no fixed genesis address and no entry in `deployments.json`** — it hasn't been deployed anywhere yet, and the design uses **two independent instances** (Track A priority + Track B open-tier), not a shared singleton.

## Functions

| `--sig` | Purpose | Caller role | Required env vars |
|---|---|---|---|
| `deploy()` | Deploy a fresh implementation + UUPS proxy | none (broadcaster becomes deployer) | — (`ADMIN`, `LABEL` optional) |
| `status()` | Emission breakdown, phase, roles, APY at a stake level | none | `REWARD_CURVE` (`RLS_STAKED`, `ADMIN` optional) |
| `setBaseMonthlyEmission()` | Set the flat Foundation-committed monthly emission | DEFAULT_ADMIN_ROLE | `REWARD_CURVE`, `BASE_MONTHLY_EMISSION` |
| `setPhase()` | Set the observable emission-phase marker | DEFAULT_ADMIN_ROLE | `REWARD_CURVE`, `PHASE` (0/1/2) |
| `recordRevenue()` | Add to the rolling variable monthly emission | REVENUE_REPORTER_ROLE | `REWARD_CURVE`, `REVENUE_AMOUNT` |
| `resetMonthlyRevenue()` | Zero the variable monthly emission | REVENUE_REPORTER_ROLE | `REWARD_CURVE` |
| `run()` | Default — alias for `deploy()` | none | — |

## Examples

### Deploy both tracks

```bash
LABEL="Track A" \
  forge script script/interactions/reward-curve/RewardCurveOps.s.sol:RewardCurveOps \
  --sig "deploy()" \
  --rpc-url $RPC_URL --broadcast --private-key $ADMIN_PK -vvvv
# -> record the logged proxy address as TRACK_A_CURVE

LABEL="Track B" \
  forge script script/interactions/reward-curve/RewardCurveOps.s.sol:RewardCurveOps \
  --sig "deploy()" \
  --rpc-url $RPC_URL --broadcast --private-key $ADMIN_PK -vvvv
# -> record the logged proxy address as TRACK_B_CURVE
```

### Seed initial emission on a deployed curve

```bash
REWARD_CURVE=$TRACK_A_CURVE BASE_MONTHLY_EMISSION=100000000000000000000000 \
  forge script script/interactions/reward-curve/RewardCurveOps.s.sol:RewardCurveOps \
  --sig "setBaseMonthlyEmission()" \
  --rpc-url $RPC_URL --broadcast --private-key $ADMIN_PK -vvvv
```

### Wire a deployed curve into RewardDistributor

Once deployed and seeded, wire both addresses in via [`RewardDistributorOps.s.sol`](../reward-distributor/RewardDistributorOps.s.sol)'s `setConfig()` — see that folder's README for the full activation sequence.

### Monthly revenue report

```bash
REWARD_CURVE=$TRACK_A_CURVE REVENUE_AMOUNT=5000000000000000000000 \
  forge script script/interactions/reward-curve/RewardCurveOps.s.sol:RewardCurveOps \
  --sig "recordRevenue()" \
  --rpc-url $RPC_URL --broadcast --private-key $REPORTER_PK -vvvv
```

## Notes

- `deploy()` refuses to run on real Rayls mainnet (chain-id `72957`) but is otherwise permissive — it logs (does not block) when the chain id isn't testnet (`7295799`) either, so a plain local anvil smoke-test still works.
- `deploy()`'s ADMIN receives `DEFAULT_ADMIN_ROLE`, `UPGRADER_ROLE`, and `REVENUE_REPORTER_ROLE` all at once (matching `RewardCurve.initialize`). Grant/revoke `REVENUE_REPORTER_ROLE` to a separate reporter address later via the contract's inherited `grantRole`/`revokeRole` if the monthly report shouldn't come from the admin key.
- `recordRevenue`/`setBaseMonthlyEmission`/`resetMonthlyRevenue` are all still just standalone state on the curve — none of this has any effect on `RewardDistributor` until the curve's address is passed to `setRewardCurve`/`setOpenTierRewardCurve` there.
- No automatic monthly trigger exists for `recordRevenue` — it's a manual admin/reporter action today. If that needs to become a keeper/automation, that's separate follow-up work, not part of this script.
