//! Integration tests for the seed stamped into the epoch-closing block's `extra_data`.
//!
//! With `EpochCloseSeedV2` active the seed is the closing output's consensus header hash, which
//! every node derives identically. Before the fork it is the keccak of the leader certificate's
//! aggregate BLS signature, which varies with the 2f+1 signer subset and let two honest nodes
//! stamp different `extra_data` for identical state (#233). The genesis-active case and both
//! sides of a mid-chain activation boundary are covered here; the happy-path tests in `main.rs`
//! cover the pre-fork value on a temp chain that runs without Rayls hardforks.

use std::{collections::VecDeque, sync::Arc, time::Duration};

use assert_matches::assert_matches;
use rayls_execution_evm::{
    chainspec::RaylsChainSpec, reth_env::RethEnv, test_utils::create_committee_from_state,
    BaseFeeParams, RethChainSpec,
};
use rayls_infrastructure_storage::{mem_db::MemDatabase, open_db};
use rayls_infrastructure_types::{
    executed_batch_registry::ExecutedBatchRegistry, gas_accumulator::GasAccumulator, now,
    test_genesis, Address, AuthorityIdentifier, Batch, BlockHash, Certificate, CertifiedBatch,
    CommittedSubDag, ConsensusHeader, ConsensusOutput, ExecHeader, Notifier, ReputationScores,
    TaskManager, B256, ETHEREUM_BLOCK_GAS_LIMIT_56BITS, MIN_PROTOCOL_BASE_FEE,
};
use rayls_middleware_processor::{batch::BatchOrdering, ExecutorEngine, RLEngineError};
use rayls_testing_test_utils::{execution_builder_no_args, TestExecutionNode};
use tempfile::TempDir;
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};

use crate::write_canonical_header;

/// Build a single-certificate [`ConsensusOutput`] led by `leader_id` carrying one empty batch at
/// `seq` from `beneficiary`, advancing the parent-hash chain. `close_epoch` marks it as the
/// epoch's last output.
fn build_output(
    leader_id: AuthorityIdentifier,
    beneficiary: Address,
    seq: u64,
    number: u64,
    round: u32,
    parent_hash: B256,
    close_epoch: bool,
) -> ConsensusOutput {
    let timestamp = now() + (round as u64) * 1000 + number;
    let mut leader = Certificate::default();
    leader.header.round = round;
    leader.header.created_at = timestamp;
    leader.header_mut_for_test().author = leader_id;
    // One commit per output in these tests, so the sub-dag index equals the output number.
    let sub_dag = Arc::new(CommittedSubDag::new(
        vec![leader.clone()],
        leader,
        number,
        ReputationScores::default(),
        None,
    ));

    let mut batch = Batch::new_for_test(vec![], ExecHeader::default(), 0, 0, seq);
    batch.beneficiary = beneficiary;
    batch.base_fee_per_gas = MIN_PROTOCOL_BASE_FEE;
    let digest = batch.digest();
    let digests: VecDeque<BlockHash> = std::iter::once(digest).collect();

    ConsensusOutput {
        sub_dag,
        batches: vec![CertifiedBatch { address: beneficiary, batches: vec![batch] }],
        batch_digests: digests,
        parent_hash,
        number,
        close_epoch,
        ..Default::default()
    }
}

/// What the engine stamped into the epoch-closing block (block 2), next to the two candidate
/// seeds for that output.
struct ClosingBlock {
    extra_data: Vec<u8>,
    new_seed: B256,
    legacy_seed: B256,
}

/// Drive a plain output (block 1) and then an epoch-closing output (block 2) through the engine
/// with `EpochCloseSeedV2` activating at `fork_block`, and return the closing block's
/// `extra_data` together with both candidate seeds.
async fn run_epoch_close(fork_block: u64) -> eyre::Result<ClosingBlock> {
    let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
    let rayls_spec = Arc::new(
        RaylsChainSpec::builder(chain.clone())
            .epoch_close_seed_v2(fork_block)
            .base_fee_params(BaseFeeParams::ethereum())
            .build(),
    );
    let tmp_dir = TempDir::new().expect("temp dir");

    // The closing block tallies the epoch from the consensus store, so back the rewards counter
    // with one and write both canonical headers below, as the happy-path tests do.
    let consensus_store = MemDatabase::default();
    let rewards_counter = rayls_middleware_rewards::from_db(consensus_store.clone());
    let gas_accumulator = GasAccumulator::with_rewards(1, rewards_counter);
    let reth_env = RethEnv::new_for_temp_chain_with_rayls_spec(
        chain.clone(),
        rayls_spec,
        tmp_dir.path(),
        &TaskManager::default(),
        Some(gas_accumulator.rewards_counter()),
    )
    .await?;
    let (builder, _) = execution_builder_no_args(Some(chain.clone()), None, tmp_dir.path())?;
    let execution_node = TestExecutionNode::new(&builder, reth_env)?;

    let committee =
        create_committee_from_state(execution_node.epoch_state_from_canonical_tip().await?).await?;
    let leader = committee.authorities().first().expect("first authority").clone();
    let leader_id = leader.id();
    let beneficiary = leader.execution_address();
    gas_accumulator.rewards_counter().set_committee(committee);

    let output_1 = build_output(leader_id.clone(), beneficiary, 1, 0, 1, B256::ZERO, false);
    let output_2 =
        build_output(leader_id, beneficiary, 2, 1, 2, output_1.consensus_header_hash(), true);
    write_canonical_header(&consensus_store, &output_1.consensus_header());
    write_canonical_header(&consensus_store, &output_2.consensus_header());

    // The two derivations have structurally different inputs: the legacy seed is the keccak of
    // the leader certificate's aggregate signature (the default, vote-less signature here, since
    // the test leader carries no votes), the new one is the consensus header hash. The inequality
    // guards the callers' assertions against passing trivially, it is not a coincidence check.
    let new_seed = output_2.epoch_close_seed();
    let legacy_seed = output_2.keccak_leader_sigs();
    assert_ne!(new_seed, legacy_seed, "the two derivations must be distinguishable");

    let reth_env = execution_node.get_reth_env().await;
    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let ordering_dir = TempDir::new().unwrap();
    let batch_ordering = BatchOrdering::new_with_empty_state(open_db(ordering_dir.path()));

    let (to_engine, from_consensus) = mpsc::channel(2);
    let engine = ExecutorEngine::new(
        reth_env.clone(),
        None,
        from_consensus,
        chain.sealed_genesis_header(),
        shutdown.subscribe(),
        task_manager.get_spawner(),
        gas_accumulator,
        None,
        ETHEREUM_BLOCK_GAS_LIMIT_56BITS,
        batch_ordering,
        None,
        None,
        ConsensusHeader::default(),
        ExecutedBatchRegistry::default(),
        rayls_execution_evm::in_flight::InFlightTracker::new(),
    );

    to_engine.send((rayls_infrastructure_types::CameFrom::Test, output_1)).await?;
    to_engine.send((rayls_infrastructure_types::CameFrom::Test, output_2)).await?;
    drop(to_engine);

    let (tx, rx) = oneshot::channel();
    task_manager.spawn_task("epoch_close_seed_engine", async move {
        let _ = tx.send(engine.await);
    });
    let result = timeout(Duration::from_secs(10), rx).await??;
    assert_matches!(result, Err(RLEngineError::ConsensusOutputStreamClosed));

    reth_env.flush_persistence().await?;

    let blocks = reth_env.block_with_senders_range(1..=2)?;
    assert_eq!(blocks.len(), 2, "one block per output");
    assert!(blocks[0].extra_data.is_empty(), "a non-closing block carries no seed");

    Ok(ClosingBlock { extra_data: blocks[1].extra_data.to_vec(), new_seed, legacy_seed })
}

/// With the fork active from genesis, the closing block's `extra_data` is the closing output's
/// consensus header hash rather than the legacy leader-signature keccak.
#[tokio::test]
async fn epoch_close_block_extra_data_is_consensus_header_hash_post_fork() -> eyre::Result<()> {
    let closing = run_epoch_close(0).await?;
    assert_eq!(
        closing.extra_data,
        closing.new_seed.as_slice(),
        "post-fork the closing block's extra_data is the consensus header hash"
    );
    assert_ne!(
        closing.extra_data,
        closing.legacy_seed.as_slice(),
        "post-fork the closing block must not carry the leader-signature keccak"
    );
    Ok(())
}

/// The gate is evaluated at the block the closing output produces. Closing exactly at the
/// activation block uses the new seed; closing one block before it keeps the legacy seed byte
/// for byte, so a mid-chain activation changes block hashes only from the scheduled block on.
#[tokio::test]
async fn epoch_close_seed_switches_at_the_activation_block() -> eyre::Result<()> {
    let at_activation = run_epoch_close(2).await?;
    assert_eq!(
        at_activation.extra_data,
        at_activation.new_seed.as_slice(),
        "closing at the activation block uses the new seed"
    );

    let before_activation = run_epoch_close(3).await?;
    assert_eq!(
        before_activation.extra_data,
        before_activation.legacy_seed.as_slice(),
        "closing before the activation block keeps the legacy seed"
    );
    assert_ne!(
        before_activation.extra_data,
        before_activation.new_seed.as_slice(),
        "pre-fork the closing block must not carry the consensus header hash"
    );
    Ok(())
}
