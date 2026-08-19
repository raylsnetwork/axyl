//! Integration tests for BatchOrdering restart semantics.
//!
//! Verifies that BatchOrdering state survives a drop-and-reopen of the
//! deferred-persistence layer so the engine keeps parking out-of-order batches.

#![allow(dead_code, unreachable_pub)]

use std::{collections::VecDeque, sync::Arc, time::Duration};

use assert_matches::assert_matches;
use rayls_execution_evm::{
    chainspec::RaylsChainSpec, reth_env::RethEnv, test_utils::create_committee_from_state,
    BaseFeeParams, RethChainSpec,
};
use rayls_infrastructure_storage::{open_db, DatabaseType};
use rayls_infrastructure_types::{
    batch_tracker::{BatchStage, BatchTracker},
    gas_accumulator::GasAccumulator,
    now, test_genesis, Address, AuthorityIdentifier, Batch, BlockHash, Certificate, CertifiedBatch,
    CommittedSubDag, Committee, ConsensusOutput, Database, ExecHeader, Notifier, ReputationScores,
    SealedHeader, TaskManager, B256, ETHEREUM_BLOCK_GAS_LIMIT_56BITS, MIN_PROTOCOL_BASE_FEE,
};
use rayls_middleware_processor::{batch::BatchOrdering, ExecutorEngine, RLEngineError};
use rayls_testing_test_utils::{execution_builder_no_args, TestExecutionNode};
use tempfile::TempDir;
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};

const ENGINE_TIMEOUT: Duration = Duration::from_secs(10);

/// Test harness for BatchOrdering restart scenarios.
///
/// Each call to [`Self::drive_session`] simulates one process boot. The
/// MDBX tempdir, gas accumulator, and committee outlive sessions; on
/// [`Self::restart`] the `RethEnv` handle is dropped and reopened against
/// the same path so the test exercises a real drop-and-reopen of the
/// deferred persistence layer.
pub struct BatchOrderingHarness {
    base_chain: Arc<RethChainSpec>,
    rayls_spec: Arc<RaylsChainSpec>,
    tmp_dir: TempDir,
    ordering_dir: TempDir,
    execution_node: Option<TestExecutionNode>,
    gas_accumulator: GasAccumulator,
    ordering_store: Option<DatabaseType>,
    committee: Committee,
    leader_id: AuthorityIdentifier,
    committee_addresses: Vec<Address>,
    last_consensus_hash: B256,
    next_output_number: u64,
    next_round: u32,
    /// Batch lifecycle tracker handed to every subsequent session's engine, so a test observes
    /// park and execution events instead of inferring them from the chain tip.
    tracker: Option<Arc<BatchTracker>>,
}

impl BatchOrderingHarness {
    /// Boot a fresh harness with a 4-authority committee and BatchDigestV2 active from block 0.
    pub async fn new() -> eyre::Result<Self> {
        Self::boot(false).await
    }

    /// Boot with the OutputSeqNormalization fork also active from genesis.
    pub async fn new_with_seq_normalization() -> eyre::Result<Self> {
        Self::boot(true).await
    }

    async fn boot(seq_normalization: bool) -> eyre::Result<Self> {
        let base_chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let mut builder = RaylsChainSpec::builder(base_chain.clone())
            .batch_digest_v2(0)
            .empty_output_block(0)
            .base_fee_params(BaseFeeParams::ethereum());
        if seq_normalization {
            builder = builder.output_seq_normalization(0);
        }
        let rayls_spec = Arc::new(builder.build());

        let tmp_dir = TempDir::new()?;
        let ordering_dir = TempDir::new()?;
        let gas_accumulator = GasAccumulator::new(1);
        let ordering_store = open_db(ordering_dir.path());

        let execution_node =
            open_execution_node(&base_chain, &rayls_spec, tmp_dir.path(), &gas_accumulator).await?;

        let committee =
            create_committee_from_state(execution_node.epoch_state_from_canonical_tip().await?)
                .await?;
        let leader_id = committee.authorities().first().expect("committee has authority 0").id();
        let committee_addresses: Vec<Address> =
            committee.authorities().iter().map(|a| a.execution_address()).collect();
        gas_accumulator.rewards_counter().set_committee(committee.clone());

        Ok(Self {
            base_chain,
            rayls_spec,
            tmp_dir,
            ordering_dir,
            execution_node: Some(execution_node),
            gas_accumulator,
            ordering_store: Some(ordering_store),
            committee,
            leader_id,
            committee_addresses,
            last_consensus_hash: B256::ZERO,
            next_output_number: 0,
            next_round: 0,
            tracker: None,
        })
    }

    /// Install a batch lifecycle tracker observed by every subsequent session.
    pub fn set_tracker(&mut self, tracker: Arc<BatchTracker>) {
        self.tracker = Some(tracker);
    }

    fn node(&self) -> &TestExecutionNode {
        self.execution_node.as_ref().expect("execution_node initialized")
    }

    /// Number of authorities in the test committee.
    pub fn authority_count(&self) -> usize {
        self.committee_addresses.len()
    }

    /// Drop the RethEnv and the ordering DB, then reopen both on the same paths.
    ///
    /// Load-bearing primitive that exercises restart fidelity. Production
    /// loses `BatchOrdering` here because it lives in-memory inside the
    /// `ExecutorEngine` that the next session constructs from scratch. The
    /// ordering DB is reopened against the same MDBX path so only what was
    /// flushed survives.
    pub async fn restart(&mut self) -> eyre::Result<()> {
        let reth_env = self.node().get_reth_env().await;
        reth_env.flush_persistence().await?;
        drop(reth_env);

        self.execution_node = None;
        if let Some(store) = self.ordering_store.take() {
            store.sync_persist().expect("persist");
            drop(store);
        }
        tokio::task::yield_now().await;

        self.ordering_store = Some(open_db(self.ordering_dir.path()));
        let node = open_execution_node(
            &self.base_chain,
            &self.rayls_spec,
            self.tmp_dir.path(),
            &self.gas_accumulator,
        )
        .await?;
        self.execution_node = Some(node);

        Ok(())
    }

    /// Build a ConsensusOutput carrying one batch from `authority_idx` at the given seq.
    pub fn build_batch_output(&mut self, authority_idx: usize, seq: u64) -> ConsensusOutput {
        let beneficiary = self.committee_addresses[authority_idx];
        self.build_output_with_batches(vec![(beneficiary, seq)])
    }

    /// Build a ConsensusOutput with no batches (empty round / leader-reward block).
    pub fn build_empty_output(&mut self) -> ConsensusOutput {
        self.build_output_with_batches(vec![])
    }

    fn build_output_with_batches(&mut self, batches: Vec<(Address, u64)>) -> ConsensusOutput {
        let output_number = self.next_output_number;
        self.next_output_number += 1;
        self.next_round += 1;
        let round = self.next_round;
        let timestamp = now() + output_number;

        let mut leader = Certificate::default();
        leader.update_created_at_for_test(timestamp);
        leader.header.round = round;
        leader.header_mut_for_test().author = self.leader_id.clone();
        self.assemble_output(leader, output_number, batches)
    }

    /// Build a ConsensusOutput with an explicit node-local `number` and leader `round`,
    /// decoupled from the auto-increment counters. Lets tests reproduce the number/round
    /// drift that occurs across a catch-up handoff (validator-2's failure mode).
    pub fn build_output_explicit(
        &mut self,
        number: u64,
        round: u32,
        batches: Vec<(Address, u64)>,
    ) -> ConsensusOutput {
        // Timestamp ordered by round (execution order) with `number` as a tiebreaker: increasing
        // rounds give increasing block timestamps (the EVM requires monotonic time) while two
        // SAME-round outputs with distinct numbers still get distinct subdag digests.
        let timestamp = now() + (round as u64) * 1000 + number;

        let mut leader = Certificate::default();
        leader.header.round = round;
        // Set the HEADER's created_at: it feeds the header digest and the subdag commit
        // timestamp. The Certificate-level `update_created_at_for_test` helper does not.
        leader.header.created_at = timestamp;
        leader.header_mut_for_test().author = self.leader_id.clone();
        self.assemble_output(leader, number, batches)
    }

    /// Wrap `leader` in a single-certificate subdag and `batches` into a [`ConsensusOutput`]
    /// numbered `number`, advancing the parent-hash chain.
    fn assemble_output(
        &mut self,
        leader: Certificate,
        number: u64,
        batches: Vec<(Address, u64)>,
    ) -> ConsensusOutput {
        let parent_hash = self.last_consensus_hash;
        let sub_dag = Arc::new(CommittedSubDag::new(
            vec![leader.clone()],
            leader,
            number,
            ReputationScores::default(),
            None,
        ));

        let mut certified: Vec<CertifiedBatch> = Vec::with_capacity(batches.len());
        let mut digests: VecDeque<BlockHash> = VecDeque::with_capacity(batches.len());
        for (beneficiary, seq) in batches {
            let mut batch = Batch::new_for_test(vec![], ExecHeader::default(), 0, 0, seq);
            batch.beneficiary = beneficiary;
            batch.base_fee_per_gas = MIN_PROTOCOL_BASE_FEE;
            digests.push_back(batch.digest());
            certified.push(CertifiedBatch { address: beneficiary, batches: vec![batch] });
        }

        let output = ConsensusOutput {
            sub_dag,
            batches: certified,
            batch_digests: digests,
            parent_hash,
            number,
            ..Default::default()
        };
        self.last_consensus_hash = output.consensus_header_hash();
        output
    }

    /// Drive outputs through a fresh `ExecutorEngine` session, returning the new chain tip.
    ///
    /// Each call constructs a brand-new engine; sessions never share
    /// in-memory state. Pair with [`Self::restart`] to also drop and reopen
    /// the MDBX-backed `RethEnv`.
    pub async fn drive_session(&self, outputs: Vec<ConsensusOutput>) -> eyre::Result<u64> {
        let (result, tip) = self.drive_session_raw(outputs).await?;
        assert_matches!(result, Err(RLEngineError::ConsensusOutputStreamClosed));
        Ok(tip)
    }

    /// Like [`Self::drive_session`] but returns the engine's terminal result alongside the
    /// chain tip without asserting a specific outcome, so tests can inspect fork/halt behavior.
    pub async fn drive_session_raw(
        &self,
        outputs: Vec<ConsensusOutput>,
    ) -> eyre::Result<(Result<(), RLEngineError>, u64)> {
        let reth_env = self.node().get_reth_env().await;
        let parent_header = self.parent_for_next_session(&reth_env);

        let store = self.ordering_store.as_ref().expect("ordering_store open").clone();
        let batch_ordering = BatchOrdering::from_history(store, 0);

        let capacity = outputs.len().max(1);
        let (to_engine, from_consensus) = mpsc::channel(capacity);
        let shutdown = Notifier::default();
        let task_manager = TaskManager::default();
        let engine = ExecutorEngine::new_for_test(
            reth_env.clone(),
            None,
            from_consensus,
            parent_header,
            shutdown.subscribe(),
            task_manager.get_spawner(),
            self.gas_accumulator.clone(),
            self.tracker.clone(),
            ETHEREUM_BLOCK_GAS_LIMIT_56BITS,
            batch_ordering,
            rayls_execution_evm::in_flight::InFlightTracker::new(),
        );

        for output in outputs {
            to_engine.send((rayls_infrastructure_types::CameFrom::Test, output)).await?;
        }
        drop(to_engine);

        let (tx, rx) = oneshot::channel();
        task_manager.spawn_task("batch_ordering_session_raw", async move {
            let res = engine.await;
            let _ = tx.send(res);
        });
        let engine_result = timeout(ENGINE_TIMEOUT, rx).await??;

        reth_env.flush_persistence().await?;
        Ok((engine_result, reth_env.canonical_tip().number))
    }

    /// Number of transactions in the canonical block at `number`.
    pub async fn block_tx_count(&self, number: u64) -> eyre::Result<usize> {
        let reth_env = self.node().get_reth_env().await;
        let block = reth_env
            .sealed_block_by_number(number)?
            .ok_or_else(|| eyre::eyre!("no canonical block at number {number}"))?;
        Ok(block.body().transactions.len())
    }

    /// Current canonical chain tip number.
    pub async fn tip_number(&self) -> u64 {
        self.node().get_reth_env().await.canonical_tip().number
    }

    fn parent_for_next_session(&self, reth_env: &RethEnv) -> SealedHeader {
        if reth_env.canonical_tip().number == 0 {
            self.base_chain.sealed_genesis_header()
        } else {
            reth_env.canonical_tip()
        }
    }

    /// Address of the authority at `idx`.
    pub fn authority_address(&self, idx: usize) -> Address {
        self.committee_addresses[idx]
    }

    /// Borrow the committee fixture (used by future tests that need authority metadata).
    pub fn committee(&self) -> &Committee {
        &self.committee
    }
}

async fn open_execution_node(
    base_chain: &Arc<RethChainSpec>,
    rayls_spec: &Arc<RaylsChainSpec>,
    path: &std::path::Path,
    gas_accumulator: &GasAccumulator,
) -> eyre::Result<TestExecutionNode> {
    let reth_env = RethEnv::new_for_temp_chain_with_rayls_spec(
        base_chain.clone(),
        rayls_spec.clone(),
        path,
        &TaskManager::default(),
        Some(gas_accumulator.rewards_counter()),
    )
    .await?;

    let (builder, _) = execution_builder_no_args(Some(base_chain.clone()), None, path)?;
    TestExecutionNode::new(&builder, reth_env).map_err(Into::into)
}

/// Drive three in-order batches from one authority through one engine session.
#[tokio::test]
async fn in_order_baseline() -> eyre::Result<()> {
    let mut h = BatchOrderingHarness::new().await?;
    let out_1 = h.build_batch_output(0, 1);
    let out_2 = h.build_batch_output(0, 2);
    let out_3 = h.build_batch_output(0, 3);

    let tip = h.drive_session(vec![out_1, out_2, out_3]).await?;
    assert_eq!(tip, 3, "three in-order batches should produce three blocks");
    Ok(())
}

/// Drive seqs 1,2,3 then a gap (5) then 4 within one engine session.
///
/// Every output builds a block: seqs 1,2,3 -> three blocks; out=5 is parked but still emits a
/// fallback empty block at its own position; out=4 fills the gap then drains the parked 5 (two
/// more blocks). Six blocks total.
#[tokio::test]
async fn parking_and_drain_single_session() -> eyre::Result<()> {
    let mut h = BatchOrderingHarness::new().await?;
    let outputs = vec![
        h.build_batch_output(0, 1),
        h.build_batch_output(0, 2),
        h.build_batch_output(0, 3),
        h.build_batch_output(0, 5),
        h.build_batch_output(0, 4),
    ];

    let tip = h.drive_session(outputs).await?;
    assert_eq!(
        tip, 6,
        "three in-order + one parked-fallback-empty + one in-order-filler + one drained = six blocks"
    );
    Ok(())
}

/// One output carrying an authority's batches in requeue-shaped order: seqs 2 and 3 ride ahead
/// of seq 1 inside the SAME consensus output (a requeued digest lands in a later-round header,
/// and certificates collate round-ascending). The certificate collation must preserve the
/// output's internal order - so the engine parks 2 and 3 on encounter - and the moment 1 lands
/// the drain collects both within the same output pass, executing blocks in seq order.
///
/// Pins two contracts at once: the collation never silently reorders (the park marks prove the
/// inverted encounter order reached the ordering state), and an intra-output inversion costs
/// only same-pass park/collect churn - never a stall, never a changed replicated block order.
#[tokio::test]
async fn intra_output_inversion_parks_and_drains_within_one_output() -> eyre::Result<()> {
    let mut h = BatchOrderingHarness::new().await?;
    let authority = h.authority_address(0);
    // seq 1 seeds `last_executed_seq`: the very first batch from an authority is accepted
    // unconditionally, so a park needs an established watermark to trip over
    let seed = h.build_batch_output(0, 1);
    let inverted =
        h.build_output_with_batches(vec![(authority, 3), (authority, 4), (authority, 2)]);
    // digests in the inverted output's own order: [seq 3, seq 4, seq 2]
    let digests: Vec<BlockHash> = inverted.batch_digests.iter().copied().collect();

    let tracker = Arc::new(BatchTracker::new());
    h.set_tracker(tracker.clone());
    let tip = h.drive_session(vec![seed, inverted]).await?;
    assert_eq!(tip, 4, "the seed and all three inverted batches execute; nothing stays parked");

    // the collation preserved the inverted encounter order: the two leading higher seqs parked
    assert!(tracker.batch_reached(digests[0], BatchStage::Parked), "seq 3 must park");
    assert!(tracker.batch_reached(digests[1], BatchStage::Parked), "seq 4 must park");
    assert!(!tracker.batch_reached(digests[2], BatchStage::Parked), "seq 2 is in order");

    // the same-pass drain normalized execution to seq order
    assert_eq!(tracker.batch_executed_block(digests[2]), Some(2), "seq 2 executes first");
    assert_eq!(tracker.batch_executed_block(digests[0]), Some(3), "parked seq 3 drains second");
    assert_eq!(tracker.batch_executed_block(digests[1]), Some(4), "parked seq 4 drains third");
    Ok(())
}

/// With OutputSeqNormalization active, an intra-output inversion is repaired in place before
/// the ordering walk: each authority's batches execute in seq order at the positions that
/// authority already held, other authorities' batches keep their slots, and nothing parks.
/// The pre-fork behavior (park, then gap-fill placement) is pinned by
/// `intra_output_inversion_parks_and_drains_within_one_output`; together the pair is the
/// rolling-safety contract at the fork boundary.
#[tokio::test]
async fn seq_normalization_repairs_intra_output_inversion_in_place() -> eyre::Result<()> {
    let mut h = BatchOrderingHarness::new_with_seq_normalization().await?;
    let a = h.authority_address(0);
    let b = h.authority_address(1);
    // seed both authorities' seq watermarks
    let seed = h.build_output_with_batches(vec![(a, 1), (b, 1)]);
    // one output where A's seqs invert around B's in-order batch: positions run A, B, A
    let inverted = h.build_output_with_batches(vec![(a, 3), (b, 2), (a, 2)]);
    // digests in the output's own order: [A seq 3, B seq 2, A seq 2]
    let digests: Vec<BlockHash> = inverted.batch_digests.iter().copied().collect();

    let tracker = Arc::new(BatchTracker::new());
    h.set_tracker(tracker.clone());
    let tip = h.drive_session(vec![seed, inverted]).await?;
    assert_eq!(tip, 5, "two seed batches plus three normalized batches");

    // the inversion was repaired before the ordering walk: nothing parks
    for digest in &digests {
        assert!(!tracker.batch_reached(*digest, BatchStage::Parked), "no batch parks");
    }
    // in place: A's batches take A's original slots in seq order; B's batch keeps its slot
    assert_eq!(tracker.batch_executed_block(digests[2]), Some(3), "A seq 2 takes A's first slot");
    assert_eq!(tracker.batch_executed_block(digests[1]), Some(4), "B seq 2 keeps its slot");
    assert_eq!(tracker.batch_executed_block(digests[0]), Some(5), "A seq 3 takes A's second slot");
    Ok(())
}

/// Verify persistence restores `last_executed_seq` across restart.
///
/// Persistence must restore `last_executed_seq` across restart so that a post-restart engine still
/// parks an out-of-order batch (instead of jumping the counter), even though every output now
/// builds a block. Fails if `BatchOrdering` persistence is removed or broken.
#[tokio::test]
async fn restart_preserves_ordering_state() -> eyre::Result<()> {
    let mut h = BatchOrderingHarness::new().await?;

    let pre_restart =
        vec![h.build_batch_output(0, 1), h.build_batch_output(0, 2), h.build_batch_output(0, 3)];
    let tip_before = h.drive_session(pre_restart).await?;
    assert_eq!(tip_before, 3);

    h.restart().await?;

    let gap_output = h.build_batch_output(0, 5);
    let tip_after = h.drive_session(vec![gap_output]).await?;
    assert_eq!(
        tip_after, 4,
        "post-restart engine still parks the gap batch (last_executed_seq=3 recovered), but the \
         parked output now emits a fallback empty block, advancing the tip to 4"
    );

    let gap_filler = h.build_batch_output(0, 4);
    let tip_drained = h.drive_session(vec![gap_filler]).await?;
    assert_eq!(
        tip_drained, 6,
        "gap-filler executes (block 5) then drains the parked seq-5 batch (block 6)"
    );

    Ok(())
}

/// The per-block nonce-round sequence over the canonical chain (epoch is 0, so nonce == round).
async fn block_rounds(h: &BatchOrderingHarness) -> eyre::Result<Vec<u32>> {
    let reth_env = h.node().get_reth_env().await;
    let tip = reth_env.canonical_tip();
    let rounds = reth_env
        .blocks_for_range(1..=tip.number)?
        .iter()
        .map(|b| RethEnv::deconstruct_nonce(u64::from(b.nonce)).1)
        .collect();
    Ok(rounds)
}

/// Determinism across liveness modes: the SAME output sequence (one of which is parked then
/// drained) must yield an identical chain and watermark whether executed continuously or with a
/// restart injected mid-sequence. Proves the executed-anchor watermark is restart-invariant: the
/// canonical tip anchors to the drained batch's ORIGIN output (round 3) and the highest executed
/// output (max nonce, round 4) is the same in both modes.
#[tokio::test]
async fn watermark_identical_continuous_vs_restart() -> eyre::Result<()> {
    // Continuous: drive seqs 1,2,4,3 (rounds 1,2,3,4) in a single session.
    let mut cont = BatchOrderingHarness::new().await?;
    let outputs = vec![
        cont.build_batch_output(0, 1),
        cont.build_batch_output(0, 2),
        cont.build_batch_output(0, 4),
        cont.build_batch_output(0, 3),
    ];
    cont.drive_session(outputs).await?;
    let continuous_rounds = block_rounds(&cont).await?;

    // Restart: same outputs, but drop+reopen RethEnv after the first two, so the parked/drained
    // pair (seqs 4 then 3) is processed by a fresh post-restart engine.
    let mut restarted = BatchOrderingHarness::new().await?;
    let pre = vec![restarted.build_batch_output(0, 1), restarted.build_batch_output(0, 2)];
    restarted.drive_session(pre).await?;
    restarted.restart().await?;
    let post = vec![restarted.build_batch_output(0, 4), restarted.build_batch_output(0, 3)];
    restarted.drive_session(post).await?;
    let restart_rounds = block_rounds(&restarted).await?;

    assert_eq!(
        continuous_rounds, restart_rounds,
        "block nonce-round sequence must be identical across continuous and restart execution"
    );
    assert_eq!(
        continuous_rounds,
        vec![1, 2, 3, 4, 3],
        "in-order(1), in-order(2), parked-fallback-empty(3), filler(4), drained-origin(3)"
    );
    assert_eq!(
        *continuous_rounds.iter().max().expect("non-empty chain"),
        4,
        "watermark (max nonce) is the highest executed output in both modes"
    );
    assert_eq!(
        *continuous_rounds.last().expect("non-empty chain"),
        3,
        "canonical tip anchors to the drained parked batch's origin output in both modes"
    );
    Ok(())
}

/// Regression for the validator-2 divergence: deterministic `(epoch, leader_round)` ordering
/// must admit a genuinely newer commit even when its node-local `number` has regressed.
///
/// Reproduces the numbering drift that froze validator-2: a catch-up handoff left the local
/// output `number` out of step with the deterministic leader round. The OLD number-based guard
/// dropped `out_b` (`number=5 <= last_seen=10`), silently losing its batch and forking the
/// chain. With identity keyed on `(epoch, round)`, `round 2 > round 1` is admitted regardless
/// of the stale number, so both batches execute.
#[tokio::test]
async fn numbering_drift_does_not_drop_higher_round_output() -> eyre::Result<()> {
    let mut h = BatchOrderingHarness::new().await?;
    let auth = h.authority_address(0);

    // out_a: leader round 1, local number 10, batch seq 1
    let out_a = h.build_output_explicit(10, 1, vec![(auth, 1)]);
    // out_b: a NEWER commit (leader round 2) whose local number (5) drifted below out_a's
    let out_b = h.build_output_explicit(5, 2, vec![(auth, 2)]);

    let tip = h.drive_session(vec![out_a, out_b]).await?;
    assert_eq!(
        tip, 2,
        "deterministic ordering must admit the higher-round output despite its lower local number"
    );
    Ok(())
}

/// After draining a parked batch the canonical tip anchors to the parked batch's ORIGIN (the
/// previous output), so the true high-watermark must be the MAX nonce over the recent block
/// window, not the tip's nonce. Characterization test: passes on current code, documents the
/// regression the tip-derived recovery suffers and the max-nonce derivation that fixes it.
#[tokio::test]
async fn restart_watermark_regresses_to_parked_origin_output() -> eyre::Result<()> {
    let mut h = BatchOrderingHarness::new().await?;

    // Epoch is 0, so block nonce round == leader round == build_batch_output call index.
    let outputs = vec![
        h.build_batch_output(0, 1), // round 1, seq 1 -> block nonce round=1
        h.build_batch_output(0, 2), // round 2, seq 2 -> block nonce round=2
        h.build_batch_output(0, 4), // round 3, seq 4 -> GAP (expected 3): PARKED, fallback empty
        // block at its own position (nonce round=3)
        h.build_batch_output(0, 3), /* round 4, seq 3 -> block nonce round=4; THEN drains parked
                                     * seq-4 batch as a block carrying its ORIGIN nonce round=3 */
    ];

    // Canonical blocks (by number) end up with nonces [1, 2, 3, 4, 3]: the parked round-3 output
    // emits a fallback empty block (nonce round=3), then the round-4 filler executes, then the
    // drained parked block is pushed AFTER it carrying its ORIGIN (round 3) nonce/anchor and
    // becomes the tip, while the max nonce (round 4) sits at an earlier block.
    let tip = h.drive_session(outputs).await?;
    assert_eq!(
        tip, 5,
        "four outputs: three in-order/fallback blocks, one filler, one drained parked = five blocks"
    );

    // Drop and reopen RethEnv against the same MDBX path: mirrors a real process restart.
    h.restart().await?;

    let reth_env = h.node().get_reth_env().await;
    let tip = reth_env.canonical_tip();

    // 1. Tip-derived watermark (the BUGGY derivation): the tip is the drained parked block, so its
    //    nonce resolves to the parked batch's ORIGIN output = the SECOND-TO-LAST output built.
    let (_epoch, tip_round) = RethEnv::deconstruct_nonce(u64::from(tip.nonce));
    assert_eq!(
        tip_round, 3,
        "canonical tip anchors to the parked batch's ORIGIN output (second-to-last); \
         seeding the high-watermark from the tip regresses to round 3"
    );

    // 2. Max-nonce-over-window watermark (the CORRECT derivation the fix uses): the true highest
    //    executed output is the LAST output built (round 4), found at an earlier block.
    let start = tip.number.saturating_sub(200);
    let window = reth_env.blocks_for_range(start..=tip.number)?;
    let max_header =
        window.iter().max_by_key(|h| u64::from(h.nonce)).expect("non-empty block window");
    let (_epoch, max_round) = RethEnv::deconstruct_nonce(u64::from(max_header.nonce));
    assert_eq!(
        max_round, 4,
        "max nonce over the recent window resolves to the true highest executed output (last)"
    );

    // The tip and the max-nonce block anchor to DIFFERENT consensus outputs: distinct nonces carry
    // distinct origin digests, proving the tip is anchored to an older output than the watermark.
    assert_ne!(
        max_header.parent_beacon_block_root, tip.parent_beacon_block_root,
        "max-nonce block and tip must anchor to different consensus outputs"
    );

    Ok(())
}

/// Divergent content at an already-executed `(epoch, leader_round)` must surface as a fork and
/// halt the engine for resync, instead of being silently dropped (number-based guard would have
/// dropped it) or double-executed.
#[tokio::test]
async fn divergent_content_at_same_position_is_detected_as_fork() -> eyre::Result<()> {
    let mut h = BatchOrderingHarness::new().await?;
    let auth = h.authority_address(0);

    // out_a: leader round 1, number 1
    let out_a = h.build_output_explicit(1, 1, vec![(auth, 1)]);
    // out_fork: SAME leader round 1 but a different number => different timestamp => different
    // subdag digest => divergent content at the same consensus position.
    let out_fork = h.build_output_explicit(2, 1, vec![(auth, 2)]);

    let (result, _tip) = h.drive_session_raw(vec![out_a, out_fork]).await?;
    assert_matches!(
        result,
        Err(RLEngineError::ConsensusFork { epoch: 0, round: 1 }),
        "divergent content at the same (epoch, round) must surface as a fork, got {result:?}"
    );
    Ok(())
}
