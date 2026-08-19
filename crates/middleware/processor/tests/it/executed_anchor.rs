//! Integration test for the EVM-execution anchor watch channel.
//!
//! Verifies the [`ExecutorEngine`] advances `executed_anchor_tx` to the consensus header every
//! executed output commits to. Since every output now builds at least one block (a real block for
//! a block-producing output, a fallback empty block for a previously-blockless all-parked output),
//! the anchor advances on EVERY output.

#![allow(dead_code, unreachable_pub)]

use std::{collections::VecDeque, sync::Arc, time::Duration};

use assert_matches::assert_matches;
use rayls_execution_evm::{
    chainspec::RaylsChainSpec, reth_env::RethEnv, test_utils::create_committee_from_state,
    BaseFeeParams, RethChainSpec,
};
use rayls_infrastructure_storage::open_db;
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
    sync::{mpsc, oneshot, watch},
    time::timeout,
};

/// Build a single-certificate [`ConsensusOutput`] led by `leader_id` carrying one batch at `seq`
/// from `beneficiary` (empty payload, still block-producing when in-order), advancing the
/// parent-hash chain. The leader id must be a committee authority so a fallback empty block can
/// resolve the leader's execution address.
fn build_output(
    leader_id: AuthorityIdentifier,
    beneficiary: Address,
    seq: u64,
    number: u64,
    round: u32,
    parent_hash: B256,
) -> ConsensusOutput {
    let timestamp = now() + (round as u64) * 1000 + number;
    let mut leader = Certificate::default();
    leader.header.round = round;
    leader.header.created_at = timestamp;
    leader.header_mut_for_test().author = leader_id;
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
        ..Default::default()
    }
}

/// Drive a block-producing output then a previously-blockless (all-parked) output through the
/// engine and assert the execution anchor advances on BOTH: every output builds a block.
#[tokio::test]
async fn executed_anchor_advances_on_every_output() -> eyre::Result<()> {
    // This test asserts the post-fork guarantee that EVERY output advances the anchor (a block
    // per output, including the parked case below), so build the engine's reth_env with a rayls
    // spec that activates EmptyOutputBlock from genesis.
    let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
    let rayls_spec = Arc::new(
        RaylsChainSpec::builder(chain.clone())
            .empty_output_block(0)
            .base_fee_params(BaseFeeParams::ethereum())
            .build(),
    );
    let tmp_dir = TempDir::new().expect("temp dir");
    let gas_accumulator = GasAccumulator::new(1);
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

    // block-producing output: in-order seq 1, leader round 1.
    let block_output = build_output(leader_id.clone(), beneficiary, 1, 0, 1, B256::ZERO);
    let block_output_number = block_output.number;
    let block_anchor_digest: BlockHash = block_output.consensus_header().digest();

    // previously-blockless output: a NEWER leader round (admitted) whose batch seq jumps to 3
    // (expected 2), so the batch is parked. It now builds a fallback empty block at its own
    // position and advances the anchor.
    let parked_output =
        build_output(leader_id, beneficiary, 3, 1, 2, block_output.consensus_header_hash());
    let parked_output_number = parked_output.number;
    let parked_anchor_digest: BlockHash = parked_output.consensus_header().digest();

    // wire the executed-anchor watch channel into the engine.
    let (anchor_tx, anchor_rx) = watch::channel(ConsensusHeader::default());

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
        Some(anchor_tx),
        None,
        ConsensusHeader::default(),
        ExecutedBatchRegistry::default(),
        rayls_execution_evm::in_flight::InFlightTracker::new(),
    );

    // anchor starts at the default genesis header (number 0).
    assert_eq!(anchor_rx.borrow().number, 0, "anchor must start at genesis");

    to_engine.send((rayls_infrastructure_types::CameFrom::Test, block_output)).await?;
    to_engine.send((rayls_infrastructure_types::CameFrom::Test, parked_output)).await?;
    drop(to_engine);

    let (tx, rx) = oneshot::channel();
    task_manager.spawn_task("executed_anchor_engine", async move {
        let _ = tx.send(engine.await);
    });
    let result = timeout(Duration::from_secs(10), rx).await??;
    assert_matches!(result, Err(RLEngineError::ConsensusOutputStreamClosed));

    reth_env.flush_persistence().await?;

    // both outputs produced a block: the block-producing one a real block, the parked one a
    // fallback empty block at its own position.
    assert_eq!(
        reth_env.canonical_tip().number,
        2,
        "every output builds a block: block-producing one plus the previously-blockless one"
    );

    // anchor advanced past the block-producing output to the parked output's consensus header,
    // since the parked output also produced (an empty) block.
    let anchor = anchor_rx.borrow();
    assert_eq!(
        anchor.number, parked_output_number,
        "anchor must advance to the last executed output's consensus header number"
    );
    assert_eq!(
        anchor.digest(),
        parked_anchor_digest,
        "anchor digest must match the last executed (previously-blockless) output's consensus header"
    );
    assert_ne!(
        anchor.digest(),
        block_anchor_digest,
        "anchor must have advanced past the first block-producing output"
    );
    assert!(
        block_output_number < parked_output_number,
        "outputs were driven in order, anchor reflects the latest"
    );

    Ok(())
}

/// The engine publishes `engine_idle = true` once it has executed everything it admitted (queue
/// empty, no task in flight) and parks on `Poll::Pending`. A mode transition's
/// `drain_engine_backlog` waits on exactly this signal. Drive one output, keep the input channel
/// open so the engine parks (rather than exiting on channel-close), and assert the signal flips
/// `false -> true`.
#[tokio::test]
async fn engine_idle_flips_true_after_draining_queue() -> eyre::Result<()> {
    let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
    let rayls_spec = Arc::new(
        RaylsChainSpec::builder(chain.clone())
            .empty_output_block(0)
            .base_fee_params(BaseFeeParams::ethereum())
            .build(),
    );
    let tmp_dir = TempDir::new().expect("temp dir");
    let gas_accumulator = GasAccumulator::new(1);
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

    // one in-order, block-producing output.
    let output = build_output(leader_id, beneficiary, 1, 0, 1, B256::ZERO);

    let (idle_tx, mut idle_rx) = watch::channel(false);

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
        None,          // executed_anchor_tx
        Some(idle_tx), // engine_idle_tx
        ConsensusHeader::default(),
        ExecutedBatchRegistry::default(),
        rayls_execution_evm::in_flight::InFlightTracker::new(),
    );

    assert!(!*idle_rx.borrow(), "engine_idle starts false (engine not yet idle)");

    // Keep `to_engine` alive so the engine, after executing the output and emptying its queue,
    // parks on Poll::Pending (channel open, no items) and publishes idle=true - rather than
    // exiting on channel-close before reaching the idle-publish path.
    to_engine.send((rayls_infrastructure_types::CameFrom::Test, output)).await?;

    task_manager.spawn_task("engine_idle_engine", async move {
        let _ = engine.await;
    });

    // engine executes the output, drains its queue, and flips idle false -> true.
    timeout(Duration::from_secs(10), idle_rx.wait_for(|&idle| idle)).await??;
    assert!(*idle_rx.borrow(), "engine_idle must be true once the admitted queue is drained");

    // keep the input channel alive until here so the engine never exits via channel-close.
    drop(to_engine);
    Ok(())
}
