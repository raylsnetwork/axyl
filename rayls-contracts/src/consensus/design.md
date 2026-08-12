Consensus Registry

# ConsensusRegistry Design

The `ConsensusRegistry` contract is a core component of the Rayls Network, designed to manage the validator lifecycle, staking mechanisms, and historical epoch data.

## Consensus Mechanisms

### System Calls

The Rayls Network leverages Bullshark and Narwhal protocols, enabling nodes to build blocks in parallel. Epochs are delineated by timestamps rather than block numbers.

At the epoch boundary, the protocol performs gasless system calls to the ConsensusRegistry to update its state with epoch, validator, and rewards information. System call logic is abstracted into the `SystemCallable` module.

- **Epoch Conclusion (`concludeEpoch()`)**: Finalizes the previous epoch, updates the voting committee and validator set, and stores new epoch information. Validator committees are protocol-managed and stored historically and for future epochs using ring buffers.
- **Performance Tracking (`applyIncentives()`)**: Records block production performance weights for each validator as a hybrid blend of participation share (80%, distinct committed rounds a validator's certificate was included in), Anchor Commit Share (10%, committed rounds led - the sole signal before the HybridRewards upgrade), and a stake tier (10%, own required stake plus delegated stake, tiered above a configurable minimum). Validators below an optional participation floor are excluded entirely. These weights are stored on-chain and consumed by the RewardDistributor to proportionally distribute fee-based rewards. Must be called before slashing and epoch conclusion.
- **Slashing (`applySlashes()`)**: Proportionally slashes validator stake and delegated stake. When a validator's balance is fully depleted, triggers a consensus burn that ejects the validator from all committees and retires them. Slashed funds from DelegationPool are accumulated in the registry and withdrawable by governance.This is not live yet but has a preliminary implementation.

## Staking and Delegation

- **Configurable Stake Amounts**: Stake amounts are configurable to support iterative adjustments in early phases based on node operator feedback and protocol updates.
- **Stake Versions**: Records are kept of validators joining under different versions for accurate stake tracking and weighted reward calculation
- **Delegation**: DPOS is supported via the DelegationPool contract. Validators accept delegated stake from multiple delegators.
- **Delegation Rewards**: Rewards are split proportionally between the validator's own stake and delegated stake by the RewardDistributor. The DelegationPool deducts a configurable commission (basis points) for the validator, then distributes the remainder to delegators via per-share reward accumulators.

## Fee-Based Rewards

- **No Token Issuance**: No new tokens are minted at block production. Rewards are sourced entirely from transaction fees.
- **Fee Flow**: Transaction gas fees are paid in the native token (USDr). The FeeAggregator collects accumulated USDr, swaps it to RLS via the Algebra DEX, and distributes the resulting RLS to configured recipients (validator pool via RewardDistributor, ecosystem treasury, and burn).
- **RewardDistributor**: Receives the validator pool share of RLS from FeeAggregator. Distributes to validators weighted by the hybrid performance data (participation + Anchor Commit Share + stake tier) recorded by `applyIncentives()`. Falls back to pure stake-proportional distribution if no performance data exists.
- **Rewards Claiming**: Pull-only claim flow. Validators claim pending rewards from the RewardDistributor and pool commission from DelegationPool. Delegators claim their proportional share from DelegationPool. Both may set custom reward recipients.
- **Balance Tracking**: Validator stake balances use a uint256 ledger in the StakeManager. Reward balances are tracked separately in the RewardDistributor.

## Hybrid Reward Model — Design Decisions & Rollout

The reward tally moved from `stake × committed-leader-count` (a hard zero-count cliff that
starved latency-distant validators and pushed operators to cluster nodes) to a hybrid blend:
**80% participation + 10% Anchor Commit Share + 10% stake tier**, computed on-chain in
`applyIncentives`.

- **Participation is measured by certificate inclusion, not the reputation score.** The 80%
  component is `participation_rounds / Σ participation_rounds`, where a validator's
  `participation_rounds` is the number of committed rounds whose sub-DAG included its
  certificate. This is read from data **already persisted** in `ConsensusBlocks`
  (`CommittedSubDag.certificates`, the flattened `order_dag` output) — the reward tally simply
  stops skipping the certificate authors it previously ignored. **No consensus- or
  networking-layer change, and no post-quorum vote tracking, is required** — the central
  objection raised in review. The `reputation_score` (floated as a possible "Leader Hit Ratio"
  proxy) is deliberately **not** used: it is itself latency-sensitive (a validator only earns
  it when its round+1 certificate references the leader *in time* — the exact geographic bias
  being removed), and it is advisory data excluded from the consensus digest. Raw
  cert-inclusion is a direct, deterministic liveness signal instead.
- **Determinism.** The blend is integer/basis-point math only (no floating point anywhere in
  the tally → calldata → contract path). `applyIncentives` runs as an EVM system call whose
  result feeds both `state_root` and `withdrawals_root`, so it must be bit-for-bit identical on
  every node.
- **Weight precision.** Each of the three normalized shares is scaled by `WEIGHT_PRECISION`
  (`1e18`) before its truncating integer division. Without it, a per-validator numerator such as
  `PARTICIPATION_BPS * participationRounds` can round down to zero once a large committee splits
  the ~10 000-bps pool (one round out of a >8 000-round submitted sum), silently dropping a
  validator that did contribute. Scaling preserves sub-bps resolution; because `RewardDistributor`
  consumes weights purely as ratios (`weight / totalWeight`), scaling every weight by the same
  constant leaves the distribution unchanged — and the scaled weights stay well below the
  pre-hybrid `stake × headerCount` magnitudes this contract already stored.
- **Anchor Commit Share (10%, capped)** preserves the incentive to run competitive
  infrastructure — a validator that wins the latency-sensitive fast path still earns more —
  without letting geography dominate rewards.
- **Stake tier (10%, capped).** Weight is `own required stake + delegated stake`, tiered at 5M
  RLS minimum, +2% per additional 5M, capped at 30M (5 tiers). Because the own required stake
  is uniform per stake-version, the differentiation is driven by delegation; the 10% cap keeps
  a large validator from buying a dominant reward share.
- **Absent → 0 (unconditional).** A validator that contributed no certificate to any committed
  sub-DAG in the epoch (`participationRounds == 0`) earns nothing from *any* component,
  regardless of the floor. Because `order_dag` always includes a leader's own certificate in
  the sub-DAG it commits, `participationRounds ≥ anchorRounds`, so zero participation also means
  zero leadership — the validator neither participated nor led. This keeps the stake component
  from leaking to fully-down validators and preserves the liveness incentive.
- **Participation floor.** `participationFloorBps` additionally excludes validators *above* zero
  but below a liveness bar (share of committed rounds participated) — the "up but chronically
  under-participating" band. It is **governance-settable and disabled (0) by default**: the
  exact threshold is a policy decision best made against real post-activation participation
  data, and disabling it initially excludes no one beyond the absent-→-0 rule while remaining
  tunable without a new hardfork.
- **Storage is append-only for a future in-place upgrade.** The only added state variable,
  `participationFloorBps`, is appended after all pre-existing storage (guarded by the
  `test_storageLayout_participationFloorBps_appendOnly_migrationSafe` test), so a later bytecode
  swap onto an existing `ConsensusRegistry`'s storage cannot shift any live validator state.

### Rollout is intentionally out of scope for this change

This change lands the **reward computation only** — the on-chain hybrid `applyIncentives` blend
and the off-chain participation tally (`ConsensusHeaderParticipation` + `tally_hybrid`), with
their tests. It does **not** wire activation: no fork schedule, no change to which contract
genesis deploys, and no in-place upgrade path. That activation/rollout wiring is a separate
follow-up, for two reasons surfaced during review:
- **Deployment/upgrade of `ConsensusRegistry` is non-trivial.** It is a directly-deployed
  (non-proxy) contract that links the external `BlsG1` library at `owner.create(0)` — a
  *per-network* address filled in at genesis. A naive bytecode swap would carry a wrong,
  code-less library address and revert; the upgrade must re-link `BlsG1` for the target network
  (or the network must re-genesis). This is an e2e-verified constraint, not a theoretical one.
- **The activation ABI must match the deployed contract at every block.** The pre-hybrid
  contract serves a 1-arg `applyIncentives`; the hybrid contract a 2-arg one. The follow-up must
  keep the deployed bytecode and the node's reward-call path consistent across the activation
  boundary (fresh genesis vs. in-place upgrade).

Pre-activation, the hybrid tally can be computed against a real consensus DB and compared to the
current leader-only split (the PoC measured a latency-injected validator going from 5.7% to
19.8%).
