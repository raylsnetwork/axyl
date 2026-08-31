//! Test execution engine for full batches.
//!
//! Grant takes full responsibility for maintaining this madness.

#![allow(unused_crate_dependencies)]

mod batch_ordering_restart;
mod executed_anchor;
mod history_recovery;

use assert_matches::assert_matches;
use rayls_batch_builder::test_utils::execute_test_batch;
use rayls_execution_evm::{
    in_flight::InFlightTracker,
    reth_env::RethEnv,
    test_utils::{
        calculate_withdrawals_root, create_committee_from_state,
        seeded_genesis_from_random_batches, TransactionFactory, BEACON_ROOTS_ADDRESS,
        EMPTY_REQUESTS_HASH, HISTORY_STORAGE_ADDRESS,
    },
    FixedBytes, RethChainSpec,
};
use rayls_infrastructure_config::FEE_AGGREGATOR_ADDRESS;
use rayls_infrastructure_storage::{
    mem_db::MemDatabase,
    open_db,
    tables::{ConsensusBlockNumbersByDigest, ConsensusBlocks},
};
use rayls_infrastructure_types::{
    gas_accumulator::GasAccumulator, max_batch_gas, now, test_chain_spec_arc, test_genesis,
    Address, BlockHash, Bloom, Bytes, Certificate, CertifiedBatch, CommittedSubDag,
    ConsensusHeader, ConsensusOutput, Database, DbTxMut, Encodable2718, Hash as _, Notifier,
    ReputationScores, SealedBlock, TaskManager, B256, EMPTY_WITHDRAWALS,
    ETHEREUM_BLOCK_GAS_LIMIT_56BITS, MIN_PROTOCOL_BASE_FEE, U256,
};
use rayls_middleware_processor::{batch::BatchOrdering, ExecutorEngine, RLEngineError};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use rayls_testing_test_utils::default_test_execution_node;
use tempfile::TempDir;
use tokio::{sync::oneshot, time::timeout};
use tracing::debug;

/// The const used for EIP-4788 and EIP-2935
const HISTORY_BUFFER_LENGTH: u64 = 8191;
/// The amount of gas to transfer native tokens between EOAs. This is the expected cost for all test
/// transactions.
const TOTAL_GAS_PER_TX: u64 = 21_000;
/// Arbitrary value used for priority fee calcs in tests.
const MAX_PRIORITY_FEE_PER_GAS: u128 = 100;
/// Arbitrary value used for priority fee calcs in tests.
const MAX_FEE_PER_GAS: u128 = 100;

/// Persist `header` to `ConsensusBlocks` and `ConsensusBlockNumbersByDigest`
/// so `RewardsBackend::tally()` resolves it as canonical.
fn write_canonical_header<DB: Database>(db: &DB, header: &ConsensusHeader) {
    db.with_write_txn(|txn| {
        txn.insert::<ConsensusBlocks>(&header.number, header)?;
        txn.insert::<ConsensusBlockNumbersByDigest>(&header.digest(), &header.number)?;
        Ok(())
    })
    .expect("write canonical consensus header");
}

/// Helper function to calculate expected priority fees for batch producer.
fn calc_priority_fees(basefee: u128) -> u128 {
    let effective_gas_price = MAX_FEE_PER_GAS.min(basefee + MAX_PRIORITY_FEE_PER_GAS);
    let coinbase_gas_price = effective_gas_price - basefee;
    coinbase_gas_price * TOTAL_GAS_PER_TX as u128
}

/// Send consensus outputs through the engine, run to completion, and return the RethEnv.
async fn run_engine(
    execution_node: &rayls_testing_test_utils::TestExecutionNode,
    chain: &Arc<RethChainSpec>,
    gas_accumulator: GasAccumulator,
    outputs: Vec<ConsensusOutput>,
) -> eyre::Result<RethEnv> {
    run_engine_with_in_flight(execution_node, chain, gas_accumulator, outputs, None).await
}

/// [`run_engine`] with a pool in-flight tracker wired into the engine, as production wires it.
async fn run_engine_with_in_flight(
    execution_node: &rayls_testing_test_utils::TestExecutionNode,
    chain: &Arc<RethChainSpec>,
    gas_accumulator: GasAccumulator,
    outputs: Vec<ConsensusOutput>,
    in_flight: Option<InFlightTracker>,
) -> eyre::Result<RethEnv> {
    let (to_engine, from_consensus) = tokio::sync::mpsc::channel(outputs.len());
    let reth_env = execution_node.get_reth_env().await;
    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let temp_db_dir = TempDir::new().unwrap();
    let ordering_store = open_db(temp_db_dir.path());
    let batch_ordering = BatchOrdering::new_with_empty_state(ordering_store);
    let engine = ExecutorEngine::new_for_test(
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
        in_flight.unwrap_or_else(InFlightTracker::new),
    );

    for output in outputs {
        to_engine.send((rayls_infrastructure_types::CameFrom::Test, output)).await?;
    }
    drop(to_engine);

    let (tx, rx) = oneshot::channel();
    task_manager.spawn_task("engine_run", async move {
        let res = engine.await;
        let _ = tx.send(res);
    });

    let engine_task = timeout(Duration::from_secs(10), rx).await??;
    assert_matches!(engine_task, Err(RLEngineError::ConsensusOutputStreamClosed));

    reth_env.flush_persistence().await?;
    Ok(reth_env)
}

/// At queue capacity the engine must stop draining its inbound channel, so execution lag
/// backs up to consensus instead of being absorbed as unbounded memory. An engine that admits on
/// every wake always empties the channel, and the lag never reaches the producer.
#[tokio::test]
async fn test_engine_stops_admitting_at_queue_capacity() -> eyre::Result<()> {
    use futures::poll;
    use rayls_middleware_processor::MAX_QUEUED_OUTPUTS;

    let tmp_dir = TempDir::new().unwrap();
    let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        &tmp_dir.path().join("exc-node"),
        None,
    )
    .await?;
    let reth_env = execution_node.get_reth_env().await;
    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let temp_db_dir = TempDir::new().unwrap();
    let ordering_store = open_db(temp_db_dir.path());
    let batch_ordering = BatchOrdering::new_with_empty_state(ordering_store);

    let (to_engine, from_consensus) = tokio::sync::mpsc::channel(1);
    let mut engine = ExecutorEngine::new_for_test(
        reth_env.clone(),
        None,
        from_consensus,
        chain.sealed_genesis_header(),
        shutdown.subscribe(),
        task_manager.get_spawner(),
        GasAccumulator::default(),
        None,
        ETHEREUM_BLOCK_GAS_LIMIT_56BITS,
        batch_ordering,
        InFlightTracker::new(),
    );

    // fill the execution queue to its admission ceiling
    for _ in 0..MAX_QUEUED_OUTPUTS {
        engine.push_back_queued_for_test(ConsensusOutput::default());
    }

    // one output waiting in the channel (capacity 1, so the channel is now full)
    to_engine
        .send((rayls_infrastructure_types::CameFrom::Test, ConsensusOutput::default()))
        .await?;
    assert_eq!(to_engine.capacity(), 0, "channel holds the pending output");

    // poll the engine: it must decline to admit rather than drain the channel
    let mut engine = Box::pin(engine);
    let _ = poll!(&mut engine);

    assert_eq!(
        to_engine.capacity(),
        0,
        "engine drained the channel while at capacity, so lag cannot reach consensus"
    );

    Ok(())
}

/// Helper function to assert EIP-4788 correctly executed. (cancun)
fn assert_eip4788(
    reth_env: &RethEnv,
    block: &SealedBlock,
    consensus_hash: B256,
) -> eyre::Result<()> {
    // for EIP-4788, the storage slot is derived from the timestamp:
    //  - timestamp_slot = to_uint256_be(evm.timestamp) % HISTORY_BUFFER_LENGTH
    //  - root_slot = timestamp_slot + HISTORY_BUFFER_LENGTH
    let state_provider = reth_env.state_by_block_hash(block.hash())?;
    // assert the timestamp was correctly written to the contract
    let timestamp_storage_slot = U256::from(block.timestamp % HISTORY_BUFFER_LENGTH);
    let stored_value = state_provider
        .storage(BEACON_ROOTS_ADDRESS, timestamp_storage_slot.into())?
        .unwrap_or_default();
    assert_eq!(
        stored_value,
        U256::from(block.timestamp),
        "Timestamp should be written to beacon roots contract at slot {timestamp_storage_slot}"
    );

    // assert the block hash was correctly written to the contract
    let root_storage_slot = timestamp_storage_slot + U256::from(HISTORY_BUFFER_LENGTH);
    let expected_blockhash = U256::from_be_bytes(consensus_hash.0);
    let stored_value =
        state_provider.storage(BEACON_ROOTS_ADDRESS, root_storage_slot.into())?.unwrap_or_default();
    assert_eq!(
        stored_value, expected_blockhash,
        "Consensus header hash should be written to beacon roots contract at slot {root_storage_slot}"
    );

    Ok(())
}

/// Helper function to assert EIP-2935 correctly executed. (pectra)
fn assert_eip2935(reth_env: &RethEnv, block: &SealedBlock) -> eyre::Result<()> {
    // block.number-1 % HISTORY_BUFFER_LENGTH
    let state_provider = reth_env.state_by_block_hash(block.hash())?;
    let parent_storage_slot = U256::from((block.number - 1) % HISTORY_BUFFER_LENGTH);
    let stored_value = state_provider
        .storage(HISTORY_STORAGE_ADDRESS, parent_storage_slot.into())?
        .unwrap_or_default();
    assert_eq!(
        stored_value,
        U256::from_be_bytes(*block.parent_hash),
        "Genesis header hash should be written to history roots contract at slot {parent_storage_slot}"
    );
    Ok(())
}

/// This tests that a single block is executed if the output from consensus contains no
/// transactions.
#[tokio::test]
async fn test_empty_output_executes() -> eyre::Result<()> {
    let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
    let tmp_dir = TempDir::new().expect("temp dir");
    // execution node components
    let gas_accumulator = GasAccumulator::new(1); // 1 worker
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        tmp_dir.path(),
        Some(gas_accumulator.rewards_counter()),
    )
    .await?;
    // update rewards counter so execution address is visible
    let committee =
        create_committee_from_state(execution_node.epoch_state_from_canonical_tip().await?).await?;
    let leader_id = committee.authorities().first().expect("first authority").id();
    let expected_beneficiary =
        committee.authority(&leader_id).expect("leader in committee").execution_address();
    gas_accumulator.rewards_counter().set_committee(committee);

    // consensus side
    //
    // create consensus output bc transactions in batches
    // are randomly generated
    //
    // for each tx, seed address with funds in genesis
    let timestamp = now();
    let mut leader = Certificate::default();
    let sub_dag_index = 0;
    leader.header.round = sub_dag_index as u32;
    // update timestamp so it's not default 0
    leader.header.created_at = timestamp;
    let reputation_scores = ReputationScores::default();
    let previous_sub_dag = None;
    leader.header_mut_for_test().author = leader_id;

    let consensus_output = ConsensusOutput {
        sub_dag: CommittedSubDag::new(
            vec![leader.clone()],
            leader,
            sub_dag_index,
            reputation_scores,
            previous_sub_dag,
        )
        .into(),
        ..Default::default()
    };
    let consensus_output_hash = consensus_output.consensus_header_hash();

    let (to_engine, from_consensus) = tokio::sync::mpsc::channel(1);
    let reth_env = execution_node.get_reth_env().await;
    let max_round = None;
    let genesis_header = chain.sealed_genesis_header();

    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let temp_db_dir = TempDir::new().unwrap();
    let ordering_store = open_db(temp_db_dir.path());
    let batch_ordering = BatchOrdering::new_with_empty_state(ordering_store);
    let engine = ExecutorEngine::new_for_test(
        reth_env.clone(),
        max_round,
        from_consensus,
        genesis_header.clone(),
        shutdown.subscribe(),
        task_manager.get_spawner(),
        gas_accumulator,
        None,
        ETHEREUM_BLOCK_GAS_LIMIT_56BITS,
        batch_ordering,
        rayls_execution_evm::in_flight::InFlightTracker::new(),
    );

    // send output
    let broadcast_result = to_engine
        .send((rayls_infrastructure_types::CameFrom::Test, consensus_output.clone()))
        .await;
    assert!(broadcast_result.is_ok());

    // drop sending channel to shut engine down
    drop(to_engine);

    let (tx, rx) = oneshot::channel();

    let canonical_in_memory_state = reth_env.canonical_in_memory_state();
    assert_eq!(canonical_in_memory_state.canonical_chain().count(), 0);

    // spawn engine task
    task_manager.spawn_task("Test task eng", async move {
        let res = engine.await;
        let _ = tx.send(res);
    });

    let engine_task = timeout(Duration::from_secs(10), rx).await??;
    // consensus output stream closed
    assert_matches!(engine_task, Err(RLEngineError::ConsensusOutputStreamClosed));

    // flush deferred persistence so DB queries return up-to-date state
    reth_env.flush_persistence().await?;

    let canonical_tip = reth_env.canonical_tip();
    let final_block = reth_env.finalized_block_num_hash()?.expect("finalized block");

    assert_eq!(canonical_tip.number, final_block.number);

    let expected_block_height = 1;
    // assert 1 empty block was executed for consensus
    assert_eq!(canonical_tip.number, expected_block_height);
    // assert canonical tip and finalized block are equal
    assert_eq!(canonical_tip.hash(), final_block.hash);
    // assert last executed output is correct and finalized
    let last_output = execution_node.last_executed_output().await?;
    assert_eq!(last_output, consensus_output_hash);

    // pull newly executed block from database (skip genesis)
    let expected_block =
        reth_env.sealed_block_by_number(1)?.expect("block 1 successfully executed");
    assert_eq!(expected_block_height, expected_block.number);

    // min basefee in genesis
    let expected_base_fee = MIN_PROTOCOL_BASE_FEE;
    // assert expected basefee
    assert_eq!(genesis_header.base_fee_per_gas, Some(expected_base_fee));
    // basefee comes from workers - if no batches, then use parent's basefee
    assert_eq!(expected_block.base_fee_per_gas, Some(expected_base_fee));

    // assert blocks are executed as expected
    assert!(expected_block.senders()?.is_empty());
    assert!(expected_block.body().transactions.is_empty());

    // assert basefee is same as worker's block
    assert_eq!(expected_block.base_fee_per_gas, Some(expected_base_fee));
    // leader address used for empty blocks
    assert_eq!(expected_block.beneficiary, expected_beneficiary);
    // nonce matches subdag index and method all match
    assert_eq!(<FixedBytes<8> as Into<u64>>::into(expected_block.nonce), sub_dag_index);
    assert_eq!(<FixedBytes<8> as Into<u64>>::into(expected_block.nonce), consensus_output.nonce());

    // pre-fork: batch digest stored in ommers_hash (no BatchDigestV2 hardfork in test chain spec)
    assert_eq!(expected_block.header().ommers_hash, B256::ZERO);
    // pre-fork: requests_hash is empty
    assert_eq!(expected_block.header().requests_hash, Some(EMPTY_REQUESTS_HASH));
    // timestamp
    assert_eq!(expected_block.timestamp, consensus_output.committed_at());
    // parent beacon block root is output digest
    assert_eq!(
        expected_block.parent_beacon_block_root,
        Some(consensus_output.consensus_header_hash())
    );
    // first block's parent is expected to be genesis
    assert_eq!(expected_block.parent_hash, chain.genesis_hash());
    // expect state roots are different after writing parent hash to BEACON_ROOT_CONTRACT
    assert_ne!(expected_block.state_root, genesis_header.state_root);
    // expect header number genesis + 1
    assert_eq!(expected_block.number, expected_block_height);

    // mix hash is xor bitwise with worker sealed block's hash and consensus output
    // just use consensus output hash if no batches in the round
    let consensus_output_hash = B256::from(consensus_output.digest());
    assert_eq!(expected_block.mix_hash, consensus_output_hash);
    // bloom expected to be the same bc all proposed transactions should be good
    // ie) no duplicates, etc.
    assert_eq!(expected_block.logs_bloom, genesis_header.logs_bloom);
    // gas limit should come from parent for empty execution
    assert_eq!(expected_block.gas_limit, genesis_header.gas_limit);
    // no gas should be used - no txs
    assert_eq!(expected_block.gas_used, 0);
    // difficulty should be 0 to indicate first (and only) block from round
    assert_eq!(expected_block.difficulty, U256::ZERO);
    // assert extra data is default bytes
    assert_eq!(expected_block.extra_data, Bytes::default());
    // pre-fork: ommers_hash carries batch digest (B256::ZERO for empty output)
    assert_eq!(expected_block.ommers_hash, B256::ZERO);
    // pre-fork: requests_hash is empty
    assert_eq!(expected_block.requests_hash, Some(EMPTY_REQUESTS_HASH));
    // assert withdrawals are empty
    //
    // NOTE: this is currently always empty
    assert_eq!(expected_block.withdrawals_root, genesis_header.withdrawals_root);

    // assert consensus output written to BEACON_ROOTS contract (cancun - eip4788)
    assert_eip4788(&reth_env, &expected_block, consensus_output.consensus_header_hash())?;

    // assert parent root is written to HISTORY_STORAGE_ADDRESS (pectra - eip2935)
    assert_eip2935(&reth_env, &expected_block)?;

    Ok(())
}

/// The engine drains the queued output, then the output still in the channel, and shuts down
/// gracefully once the sender is dropped, in that order.
///
/// One output is queued (already received) and another is sent on the channel before the sender
/// is dropped and the engine starts. All batches are built with genesis as the parent, which is
/// valid. Priority-fee transactions are included to assert governance, batch producer, and block
/// rewards reach the correct addresses at the end of the epoch.
#[tokio::test]
async fn test_happy_path_full_execution_even_after_sending_channel_closed() -> eyre::Result<()> {
    let tmp_dir = TempDir::new().expect("temp dir");
    // create batches for consensus output
    let chain = test_chain_spec_arc();
    let mut batches_1 = rayls_execution_evm::test_utils::batches(chain.clone(), 4); // create 4 batches
    let mut batches_2 = rayls_execution_evm::test_utils::batches(chain, 4); // create 4 batches

    // add eip1559 transactions to set max priority fee per gas so batch producer earns fees
    let genesis = test_genesis();
    let mut tx_factory = TransactionFactory::new_random();
    let encoded_tx_priority_fee_1 = tx_factory
        .create_explicit_eip1559(
            Some(genesis.config.chain_id),
            None,
            Some(MAX_PRIORITY_FEE_PER_GAS),
            Some(MAX_FEE_PER_GAS),
            None,
            Some(Address::random()),
            None,
            None,
            None,
        )
        .encoded_2718();
    let encoded_tx_priority_fee_2 = tx_factory
        .create_explicit_eip1559(
            Some(genesis.config.chain_id),
            None,
            Some(MAX_PRIORITY_FEE_PER_GAS),
            Some(MAX_FEE_PER_GAS),
            None,
            Some(Address::random()),
            None,
            None,
            None,
        )
        .encoded_2718();
    if let Some(batch) = batches_1.first_mut() {
        batch.transactions_mut().push(encoded_tx_priority_fee_1.into())
    }
    if let Some(batch) = batches_2.first_mut() {
        batch.transactions_mut().push(encoded_tx_priority_fee_2.into())
    }

    // okay to clone these because they are only used to seed genesis, decode transactions, and
    // recover signers
    let all_batches = [batches_1.clone(), batches_2.clone()].concat();

    // use default genesis and seed accounts to execute batches
    let (genesis, txs_by_block, signers_by_block) =
        seeded_genesis_from_random_batches(genesis, all_batches.iter());
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    // consensus-DB-backed rewards calculator; headers are written canonically below
    // so close-epoch `tally()` resolves the same leaders the engine sees.
    let consensus_store = MemDatabase::default();
    let rewards_counter = rayls_middleware_rewards::from_db(consensus_store.clone());
    let gas_accumulator = GasAccumulator::with_rewards(1, rewards_counter);
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        &tmp_dir.path().join("exc-node"),
        Some(gas_accumulator.rewards_counter()),
    )
    .await?;

    // create committee from genesis state
    let committee =
        create_committee_from_state(execution_node.epoch_state_from_canonical_tip().await?).await?;
    let authority_1 =
        committee.authorities().first().expect("first in 4 auth committee for tests").id();
    let authority_2 =
        committee.authorities().last().expect("last in 4 auth committee for tests").id();
    let _ = committee.authority(&authority_1).expect("authority in committee").execution_address();
    let _ = committee.authority(&authority_2).expect("authority in committee").execution_address();

    // execute batches to update headers with valid data
    let mut inc_base_fee = MIN_PROTOCOL_BASE_FEE;
    let mut expected_base_fees = U256::ZERO;
    let mut expected_priority_fees = 0;
    let batch_producer =
        committee.authorities().get(2).expect("authority in committee").execution_address();

    // updated batches separately because they are mutated in-place
    // and need to be passed to different outputs
    //
    // update first round
    for (idx, batch) in batches_1.iter_mut().enumerate() {
        // increase basefee
        inc_base_fee += idx as u64;

        // update basefee and set beneficiary for priority fees to third validator
        batch.beneficiary = batch_producer;
        batch.base_fee_per_gas = inc_base_fee;

        // actually execute the batch now
        execute_test_batch(batch);

        // all txs in test batches are EOA->EOA native token transfers
        // which costs 21_000 gas
        let batch_basefees = U256::from(
            batch.transactions().len() as u64 * TOTAL_GAS_PER_TX * batch.base_fee_per_gas,
        );
        expected_base_fees = expected_base_fees
            .checked_add(batch_basefees)
            .expect("u256 did not overflow during add");

        // calculate expected priority fees
        // encoded_tx_priority_fee_1 is last tx in the first batch
        if idx == 0 {
            let priority_fees = calc_priority_fees(batch.base_fee_per_gas as u128);
            expected_priority_fees += priority_fees;
        }
    }

    // update second round
    for (idx, batch) in batches_2.iter_mut().enumerate() {
        // continue increasing basefee
        // add 4 to continue where previous round left off
        // this makes assertions easier at the end
        inc_base_fee += 4 + idx as u64;

        // update basefee and set beneficiary for priority fees to third validator
        batch.beneficiary = batch_producer;
        batch.base_fee_per_gas = inc_base_fee;

        // actually execute the block now
        execute_test_batch(batch);
        // all txs in test batches are EOA->EOA native token transfers
        // 21_000 gas
        let batch_basefees = U256::from(
            batch.transactions().len() as u64 * TOTAL_GAS_PER_TX * batch.base_fee_per_gas,
        );
        expected_base_fees = expected_base_fees
            .checked_add(batch_basefees)
            .expect("u256 did not overflow during add");

        // calculate expected priority fees
        // encoded_tx_priority_fee_2 is last tx in the first batch
        if idx == 0 {
            let priority_fees = calc_priority_fees(batch.base_fee_per_gas as u128);
            expected_priority_fees += priority_fees;
        }
    }

    // Reload all_batches so we can calculate mix_hash properly later.
    let all_batches = [batches_1.clone(), batches_2.clone()].concat();

    // consensus side

    // create consensus output bc transactions in batches
    // are randomly generated
    //
    // for each tx, seed address with funds in genesis
    let timestamp = now();
    let mut leader_1 = Certificate::default();
    // update cert
    leader_1.update_created_at_for_test(timestamp);
    leader_1.header_mut_for_test().author = authority_1;
    let sub_dag_index_1 = 1;
    leader_1.header.round = sub_dag_index_1 as u32;
    let reputation_scores = ReputationScores::default();
    let previous_sub_dag = None;
    let mut batch_digests_1: VecDeque<BlockHash> = batches_1.iter().map(|b| b.digest()).collect();
    let subdag_1 = Arc::new(CommittedSubDag::new(
        vec![leader_1.clone(), Certificate::default()],
        leader_1,
        sub_dag_index_1,
        reputation_scores,
        previous_sub_dag,
    ));

    let consensus_output_1 = ConsensusOutput {
        sub_dag: subdag_1.clone(),
        batches: vec![CertifiedBatch { address: batch_producer, batches: batches_1 }],
        batch_digests: batch_digests_1.clone(),
        ..Default::default()
    };

    // create second output
    let mut leader_2 = Certificate::default();
    let leader_2_epoch = leader_2.epoch();
    // update cert
    leader_2.update_created_at_for_test(timestamp + 2);
    leader_2.header_mut_for_test().author = authority_2;
    let sub_dag_index_2 = 2;
    leader_2.header.round = sub_dag_index_2 as u32;
    let reputation_scores = ReputationScores::default();
    let previous_sub_dag = Some(subdag_1.as_ref());
    let batch_digests_2: VecDeque<BlockHash> = batches_2.iter().map(|b| b.digest()).collect();
    let subdag_2 = CommittedSubDag::new(
        vec![leader_2.clone(), Certificate::default()],
        leader_2,
        sub_dag_index_2,
        reputation_scores,
        previous_sub_dag,
    )
    .into();

    let consensus_output_2 = ConsensusOutput {
        sub_dag: subdag_2,
        batches: vec![CertifiedBatch { address: batch_producer, batches: batches_2 }],
        batch_digests: batch_digests_2.clone(),
        parent_hash: consensus_output_1.consensus_header_hash(),
        number: 1,
        close_epoch: true, // close epoch after 2nd output
        ..Default::default()
    };
    let consensus_output_2_hash = consensus_output_2.consensus_header_hash();

    // combine VecDeque and convert to Vec for assertions later
    batch_digests_1.extend(batch_digests_2);
    let all_batch_digests: Vec<BlockHash> = batch_digests_1.into();

    // execution side
    // setup rewards for first two rounds of consensus
    let rewards_counter = gas_accumulator.rewards_counter();
    rewards_counter.set_committee(committee.clone());

    // prime the consensus DB so RewardsBackend::tally() sees the same leaders the
    // engine increments via process_committed_sub_dag.
    write_canonical_header(&consensus_store, &consensus_output_1.consensus_header());
    write_canonical_header(&consensus_store, &consensus_output_2.consensus_header());

    // retrieve rewards info for current epoch
    let _ = execution_node.epoch_state_from_canonical_tip().await?;
    // create engine
    let (to_engine, from_consensus) = tokio::sync::mpsc::channel(1);
    let max_round = None;
    let parent = chain.sealed_genesis_header();

    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let reth_env = execution_node.get_reth_env().await;
    let temp_db_dir = TempDir::new().unwrap();
    let ordering_store = open_db(temp_db_dir.path());
    let batch_ordering = BatchOrdering::new_with_empty_state(ordering_store);
    let mut engine = ExecutorEngine::new_for_test(
        reth_env.clone(),
        max_round,
        from_consensus,
        parent,
        shutdown.subscribe(),
        task_manager.get_spawner(),
        gas_accumulator.clone(),
        None,
        ETHEREUM_BLOCK_GAS_LIMIT_56BITS,
        batch_ordering,
        rayls_execution_evm::in_flight::InFlightTracker::new(),
    );

    // assert the canonical chain in-memory is empty
    let canonical_in_memory_state = reth_env.canonical_in_memory_state();
    assert_eq!(canonical_in_memory_state.canonical_chain().count(), 0);
    let (blocks, gas) = gas_accumulator.get_values(0);
    assert_eq!((blocks + gas), 0, "gas accumulator didn't start at 0");
    // queue the first output - simulate already received from channel
    engine.push_back_queued_for_test(consensus_output_1.clone());

    // send second output
    let broadcast_result = to_engine
        .send((rayls_infrastructure_types::CameFrom::Test, consensus_output_2.clone()))
        .await;
    assert!(broadcast_result.is_ok());

    // drop sending channel before receiver has a chance to process message
    drop(to_engine);

    // channels for engine shutting down
    let (tx, rx) = oneshot::channel();

    // spawn engine task
    //
    // one output already queued up, one output waiting in broadcast stream
    task_manager.spawn_task("test task eng", async move {
        let res = engine.await;
        let _ = tx.send(res);
    });

    let engine_task = timeout(Duration::from_secs(10), rx).await??;
    // consensus stream is closed
    assert_matches!(engine_task, Err(RLEngineError::ConsensusOutputStreamClosed));

    // flush deferred persistence so DB queries return up-to-date state
    reth_env.flush_persistence().await?;

    let canonical_tip = reth_env.canonical_tip();
    let final_block = reth_env.finalized_block_num_hash()?.expect("finalized block");

    let expected_block_height = 8;
    // assert all 8 batches were executed
    assert_eq!(canonical_tip.number, expected_block_height);
    // assert canonical tip and finalized block are equal
    assert_eq!(canonical_tip.hash(), final_block.hash);
    // assert last executed output is correct and finalized
    let last_output = execution_node.last_executed_output().await?;
    assert_eq!(last_output, consensus_output_2_hash);
    // assert priority fees went to batch producer
    let third_validator_account = reth_env
        .retrieve_account(
            &committee.authorities().get(2).expect("4 validators in committee").execution_address(),
        )?
        .expect("third validator account has priority fees");
    assert_eq!(third_validator_account.balance, U256::from(expected_priority_fees));
    // assert no issuance-based block rewards (rewards come from fee distribution now)
    // let rewards_1 = reth_env.get_validator_rewards(final_block.hash, leader_address_1)?;
    // let rewards_2 = reth_env.get_validator_rewards(final_block.hash, leader_address_2)?;
    // assert_eq!(rewards_1, U256::ZERO, "no issuance rewards for leader 1");
    // assert_eq!(rewards_2, U256::ZERO, "no issuance rewards for leader 2");
    // assert all basefees sent to fee aggregator
    let fee_aggregator_genesis_balance = chain
        .genesis()
        .alloc
        .get(&FEE_AGGREGATOR_ADDRESS)
        .map(|acct| acct.balance)
        .unwrap_or(U256::ZERO);
    let fee_aggregator = reth_env
        .retrieve_account(&FEE_AGGREGATOR_ADDRESS)?
        .map(|acct| acct.balance)
        .expect("fee aggregator has an account");
    assert_eq!(
        expected_base_fees,
        fee_aggregator
            .checked_sub(fee_aggregator_genesis_balance)
            .expect("fee aggregator balance doesn't underflow"),
        "Fee aggregator missing basefees"
    );

    // pull newly executed blocks from database (skip genesis)
    //
    // Uses the provided `headers_range` to get the headers for the range, and `assemble_block`
    // to construct blocks from the following inputs:
    //     – Header
    //     - Transactions
    //     – Ommers
    //     – Withdrawals
    //     – Requests
    //     – Senders
    let executed_blocks = reth_env.block_with_senders_range(1..=expected_block_height)?;
    assert_eq!(expected_block_height, executed_blocks.len() as u64);

    // basefee intentionally increased with loop
    let mut expected_base_fee = MIN_PROTOCOL_BASE_FEE;
    let output_digest_1: B256 = consensus_output_1.digest().into();
    let output_digest_2: B256 = consensus_output_2.digest().into();

    // assert blocks are executed as expected
    for (idx, txs) in txs_by_block.iter().enumerate() {
        let block = &executed_blocks[idx];
        let signers = &signers_by_block[idx];
        assert_eq!(&block.senders(), signers);
        assert_eq!(&block.body().transactions, txs);

        // basefee was increased for each batch
        expected_base_fee += idx as u64;
        // assert basefee is same as worker's block
        assert_eq!(block.base_fee_per_gas, Some(expected_base_fee));

        // define re-usable variable here for asserting all values against expected output
        let mut expected_output = &consensus_output_1;
        let mut expected_subdag_index = &sub_dag_index_1;
        let mut output_digest = output_digest_1;
        let mut expected_parent_beacon_block_root = consensus_output_1.consensus_header_hash();
        let mut expected_batch_index = idx;

        // update values based on index for all assertions below
        if idx >= 4 {
            // use different output for last 4 blocks
            expected_output = &consensus_output_2;
            expected_subdag_index = &sub_dag_index_2;
            output_digest = output_digest_2;
            expected_parent_beacon_block_root = consensus_output_2.consensus_header_hash();
            // takeaway 4 to compensate for independent loops for executing batches
            expected_batch_index = idx - 4;
        }

        // assert consensus output written to BEACON_ROOTS contract (cancun - eip4788)
        assert_eip4788(&reth_env, block.sealed_block(), expected_parent_beacon_block_root)?;

        // assert parent root is written to HISTORY_STORAGE_ADDRESS (pectra - eip2935)
        assert_eip2935(&reth_env, block.sealed_block())?;

        // beneficiary overwritten
        assert_eq!(block.beneficiary, batch_producer);
        // nonce matches subdag index and method all match
        assert_eq!(<FixedBytes<8> as Into<u64>>::into(block.nonce), *expected_subdag_index);
        assert_eq!(<FixedBytes<8> as Into<u64>>::into(block.nonce), expected_output.nonce());

        // timestamp
        assert_eq!(block.timestamp, expected_output.committed_at());
        // parent beacon block root is output digest
        assert_eq!(block.parent_beacon_block_root, Some(expected_parent_beacon_block_root));

        if idx == 0 {
            // first block's parent is expected to be genesis
            assert_eq!(block.parent_hash, chain.genesis_hash());
            // expect header number 1 for batch bc of genesis
            assert_eq!(block.number, 1);
        } else {
            // assert parents executed in order (sanity check)
            let expected_parent = executed_blocks[idx - 1].header().hash_slow();
            assert_eq!(block.parent_hash, expected_parent);
            // expect block numbers NOT the same as batch's headers
            assert_ne!(block.number, 1);
        }

        // mix hash is xor batch's hash and consensus output digest
        let expected_mix_hash = output_digest ^ all_batches[idx].digest();
        assert_eq!(block.mix_hash, expected_mix_hash);
        // blocks with EOA transfers emit Transfer events from native ERC-20 precompile
        assert_ne!(block.logs_bloom, Bloom::default());
        // gas limit should come from batch
        assert_eq!(block.gas_limit, max_batch_gas(leader_2_epoch));
        // difficulty should match the batch's index within consensus output
        // and default worker id 0
        assert_eq!(block.difficulty, U256::from(expected_batch_index << 16));
        // assert closing epoch randomness matches extra data field in last block
        let expected_extra = if idx == 7 {
            Bytes::from(expected_output.keccak_leader_sigs().0)
        } else {
            Bytes::default()
        };
        assert_eq!(block.extra_data, expected_extra);
        // pre-fork: batch digest stored in ommers_hash
        assert_eq!(block.ommers_hash, all_batch_digests[idx]);
        // pre-fork: requests_hash is empty
        assert_eq!(block.requests_hash, Some(EMPTY_REQUESTS_HASH));
        // close-epoch withdrawals_root is driven by RewardsBackend::tally(),
        // which walks the consensus DB primed below from the two outputs.
        let expected_withdrawals = if idx == 7 {
            let withdrawals = gas_accumulator.rewards_counter().generate_withdrawals();
            calculate_withdrawals_root(withdrawals.as_ref())
        } else {
            EMPTY_WITHDRAWALS
        };
        assert_eq!(block.withdrawals_root, Some(expected_withdrawals));
    }

    Ok(())
}

/// A batch whose transactions were already executed produces an empty block, and the engine
/// drains its queue and shuts down gracefully.
///
/// All batches are built with genesis as the parent, which is valid. Priority-fee transactions
/// are included to assert governance, batch producer, and block rewards reach the correct
/// addresses at the end of the epoch.
#[tokio::test]
async fn test_execution_succeeds_with_duplicate_transactions() -> eyre::Result<()> {
    let tmp_dir = TempDir::new().unwrap();
    // create batches for consensus output
    let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
    let mut batches_1 = rayls_execution_evm::test_utils::batches(chain.clone(), 4); // create 4 batches
    let mut batches_2 = rayls_execution_evm::test_utils::batches(chain, 4); // create 4 batches

    // add eip1559 transactions to set max priority fee per gas so batch producer earns fees
    let genesis = test_genesis();
    let mut tx_factory = TransactionFactory::new_random();
    let encoded_tx_priority_fee_1 = tx_factory
        .create_explicit_eip1559(
            Some(genesis.config.chain_id),
            None,
            Some(MAX_PRIORITY_FEE_PER_GAS),
            Some(MAX_FEE_PER_GAS),
            None,
            Some(Address::random()),
            None,
            None,
            None,
        )
        .encoded_2718();
    let encoded_tx_priority_fee_2 = tx_factory
        .create_explicit_eip1559(
            Some(genesis.config.chain_id),
            None,
            Some(MAX_PRIORITY_FEE_PER_GAS),
            Some(MAX_FEE_PER_GAS),
            None,
            Some(Address::random()),
            None,
            None,
            None,
        )
        .encoded_2718();
    if let Some(batch) = batches_1.first_mut() {
        batch.transactions_mut().push(encoded_tx_priority_fee_1.into())
    }
    if let Some(batch) = batches_2.first_mut() {
        batch.transactions_mut().push(encoded_tx_priority_fee_2.into())
    }
    // duplicate transactions in last batch for each round
    //
    // simulate duplicate batches from same round
    // and
    // duplicate transactions from a previous round
    const DUPLICATED_BATCH_FOR_ROUND_1_INDEX: usize = 0;
    const DUPLICATED_BATCH_FOR_ROUND_2_INDEX: usize = 1;
    const DUPLICATE_BATCH_INDEX: usize = 3;
    batches_1[DUPLICATE_BATCH_INDEX] = batches_1[DUPLICATED_BATCH_FOR_ROUND_1_INDEX].clone();
    batches_2[DUPLICATE_BATCH_INDEX] = batches_1[DUPLICATED_BATCH_FOR_ROUND_2_INDEX].clone();

    // okay to clone these because they are only used to seed genesis, decode transactions, and
    // recover signers
    let all_batches = [batches_1.clone(), batches_2.clone()].concat();

    // seed accounts to execute batches

    let (genesis, txs_by_block, signers_by_block) =
        seeded_genesis_from_random_batches(genesis, all_batches.iter());
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    // consensus-DB-backed rewards calculator; canonical headers are written below.
    let consensus_store = MemDatabase::default();
    let rewards_counter = rayls_middleware_rewards::from_db(consensus_store.clone());
    let gas_accumulator = GasAccumulator::with_rewards(1, rewards_counter);
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        &tmp_dir.path().join("exc-node"),
        Some(gas_accumulator.rewards_counter()),
    )
    .await?;

    // create committee from genesis state
    let committee =
        create_committee_from_state(execution_node.epoch_state_from_canonical_tip().await?).await?;
    let authority_1 =
        committee.authorities().first().expect("first in 4 auth committee for tests").id();
    let authority_2 =
        committee.authorities().last().expect("last in 4 auth committee for tests").id();
    let _leader_address_1 =
        committee.authority(&authority_1).expect("authority in committee").execution_address();
    let _leader_address_2 =
        committee.authority(&authority_2).expect("authority in committee").execution_address();
    // execute batches to update headers with valid data
    let mut inc_base_fee = MIN_PROTOCOL_BASE_FEE;
    let mut expected_base_fees = U256::ZERO;
    let mut expected_priority_fees = HashMap::new();
    let batch_producer_1 = Address::random();
    let batch_producer_2 = Address::random();
    // updated batches separately because they are mutated in-place
    // and need to be passed to different outputs
    //
    // update first round
    for (idx, batch) in batches_1.iter_mut().enumerate() {
        // increase basefee
        inc_base_fee += idx as u64;

        // update basefee and set beneficiary
        batch.beneficiary = batch_producer_1;
        batch.base_fee_per_gas = inc_base_fee;

        // actually execute the batch now
        execute_test_batch(batch);

        // skip duplicate batch, otherwise calculate expected basefees
        if idx != DUPLICATE_BATCH_INDEX {
            // all txs in test batches are EOA->EOA native token transfers
            // which costs 21_000 gas
            let batch_basefees = U256::from(
                batch.transactions().len() as u64 * TOTAL_GAS_PER_TX * batch.base_fee_per_gas,
            );
            expected_base_fees = expected_base_fees
                .checked_add(batch_basefees)
                .expect("u256 did not overflow during add");
        }

        // calculate expected priority fees
        // encoded_tx_priority_fee_1 is last tx in the first batch
        if idx == 0 {
            let priority_fees = calc_priority_fees(batch.base_fee_per_gas as u128);
            expected_priority_fees.insert(batch_producer_1, priority_fees);
        }
    }

    // update second round
    for (idx, batch) in batches_2.iter_mut().enumerate() {
        // continue increasing basefee
        // add 4 to continue where previous round left off
        // this makes assertions easier at the end
        inc_base_fee += 4 + idx as u64;

        // update basefee and set beneficiary
        batch.beneficiary = batch_producer_2;
        batch.base_fee_per_gas = inc_base_fee;

        // actually execute the block now
        execute_test_batch(batch);

        // skip duplicate batch, otherwise calculate expected basefees
        if idx != DUPLICATE_BATCH_INDEX {
            // all txs in test batches are EOA->EOA native token transfers
            // 21_000 gas
            let batch_basefees = U256::from(
                batch.transactions().len() as u64 * TOTAL_GAS_PER_TX * batch.base_fee_per_gas,
            );
            expected_base_fees = expected_base_fees
                .checked_add(batch_basefees)
                .expect("u256 did not overflow during add");
        }

        // calculate expected priority fees
        // encoded_tx_priority_fee_2 is last tx in the first batch
        if idx == 0 {
            let priority_fees = calc_priority_fees(batch.base_fee_per_gas as u128);
            expected_priority_fees.insert(batch_producer_2, priority_fees);
        }
    }

    // Reload all_batches so we can calculate mix_hash properly later.
    let all_batches = [batches_1.clone(), batches_2.clone()].concat();

    // store ref as variable for clarity
    let duplicated_batch_for_round_1 = &batches_1[DUPLICATED_BATCH_FOR_ROUND_1_INDEX];
    let duplicated_batch_for_round_2 = &batches_1[DUPLICATED_BATCH_FOR_ROUND_2_INDEX];
    let duplicate_batch_round_1 = &batches_1[DUPLICATE_BATCH_INDEX];
    let duplicate_batch_round_2 = &batches_2[DUPLICATE_BATCH_INDEX];

    // assert duplicate txs are same, but batches are different
    //
    // round 1
    assert_eq!(duplicate_batch_round_1.transactions(), duplicated_batch_for_round_1.transactions());
    assert_ne!(duplicate_batch_round_1, duplicated_batch_for_round_1);
    // round 2
    assert_eq!(duplicate_batch_round_2.transactions(), duplicated_batch_for_round_2.transactions());
    assert_ne!(duplicate_batch_round_2, duplicated_batch_for_round_2);

    // consensus side
    //
    // create consensus output bc transactions in batches
    // are randomly generated
    //
    // for each tx, seed address with funds in genesis
    let timestamp = now();
    let mut leader_1 = Certificate::default();
    // update timestamp
    leader_1.update_created_at_for_test(timestamp);
    leader_1.header_mut_for_test().author = authority_1;
    let sub_dag_index_1: u64 = 1;
    leader_1.header.round = sub_dag_index_1 as u32;
    let reputation_scores = ReputationScores::default();
    let previous_sub_dag = None;
    let mut batch_digests_1: VecDeque<BlockHash> = batches_1.iter().map(|b| b.digest()).collect();
    let mut cert_1 = Certificate::default();
    cert_1.header.round = 1;
    let subdag_1 = Arc::new(CommittedSubDag::new(
        vec![cert_1],
        leader_1,
        sub_dag_index_1,
        reputation_scores,
        previous_sub_dag,
    ));

    let consensus_output_1 = ConsensusOutput {
        sub_dag: subdag_1.clone(),
        batches: vec![CertifiedBatch { address: batch_producer_1, batches: batches_1 }],
        batch_digests: batch_digests_1.clone(),
        ..Default::default()
    };

    // create second output
    let mut leader_2 = Certificate::default();
    let leader_2_epoch = leader_2.epoch();
    // update timestamp
    leader_2.update_created_at_for_test(timestamp + 2);
    leader_2.header_mut_for_test().author = authority_2;
    let sub_dag_index_2 = 2;
    leader_2.header.round = sub_dag_index_2 as u32;
    let reputation_scores = ReputationScores::default();
    let previous_sub_dag = Some(subdag_1.as_ref());
    let batch_digests_2: VecDeque<BlockHash> = batches_2.iter().map(|b| b.digest()).collect();
    let mut cert_2 = Certificate::default();
    cert_2.header.round = 2;
    let subdag_2 = CommittedSubDag::new(
        vec![cert_2],
        leader_2,
        sub_dag_index_2,
        reputation_scores,
        previous_sub_dag,
    )
    .into();

    let consensus_output_2 = ConsensusOutput {
        sub_dag: subdag_2,
        batches: vec![CertifiedBatch { address: batch_producer_2, batches: batches_2 }],
        batch_digests: batch_digests_2.clone(),
        parent_hash: consensus_output_1.consensus_header_hash(),
        number: 1,
        close_epoch: true,
        ..Default::default()
    };
    let consensus_output_2_hash = consensus_output_2.consensus_header_hash();

    // combine VecDeque and convert to Vec for assertions later
    batch_digests_1.extend(batch_digests_2);
    let all_batch_digests: Vec<BlockHash> = batch_digests_1.into();

    // execution side
    // setup rewards for first two rounds of consensus
    let rewards_counter = gas_accumulator.rewards_counter();
    rewards_counter.set_committee(committee.clone());

    // inc leader counter - normally performed by `EpochManager`
    rewards_counter.inc_leader_count(consensus_output_1.leader().origin());
    rewards_counter.inc_leader_count(consensus_output_2.leader().origin());

    // prime the consensus DB so RewardsBackend::tally() resolves the same leaders.
    write_canonical_header(&consensus_store, &consensus_output_1.consensus_header());
    write_canonical_header(&consensus_store, &consensus_output_2.consensus_header());

    // retrieve rewards info for current epoch
    let _ = execution_node.epoch_state_from_canonical_tip().await?;

    // create engine
    let (to_engine, from_consensus) = tokio::sync::mpsc::channel(1);
    let max_round = None;
    let parent = chain.sealed_genesis_header();

    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let reth_env = execution_node.get_reth_env().await;
    let temp_db_dir = TempDir::new().unwrap();
    let ordering_store = open_db(temp_db_dir.path());
    let batch_ordering = BatchOrdering::new_with_empty_state(ordering_store);
    let mut engine = ExecutorEngine::new_for_test(
        reth_env.clone(),
        max_round,
        from_consensus,
        parent,
        shutdown.subscribe(),
        task_manager.get_spawner(),
        GasAccumulator::default(),
        None,
        ETHEREUM_BLOCK_GAS_LIMIT_56BITS,
        batch_ordering,
        rayls_execution_evm::in_flight::InFlightTracker::new(),
    );

    // queue the first output - simulate already received from channel
    engine.push_back_queued_for_test(consensus_output_1.clone());

    // send second output
    let broadcast_result = to_engine
        .send((rayls_infrastructure_types::CameFrom::Test, consensus_output_2.clone()))
        .await;
    assert!(broadcast_result.is_ok());

    // drop sending channel before receiver has a chance to process message
    drop(to_engine);

    // channels for engine shutting down
    let (tx, rx) = oneshot::channel();

    // spawn engine task
    //
    // one output already queued up, one output waiting in broadcast stream
    task_manager.spawn_task("test task eng", async move {
        let res = engine.await;
        let _ = tx.send(res);
    });

    let engine_task = timeout(Duration::from_secs(10), rx).await??;
    // consensus output stream closed
    assert_matches!(engine_task, Err(RLEngineError::ConsensusOutputStreamClosed));

    // flush deferred persistence so DB queries return up-to-date state
    reth_env.flush_persistence().await?;

    let canonical_tip = reth_env.canonical_tip();
    let final_block = reth_env.finalized_block_num_hash()?.expect("finalized block");

    // expect 1 block per batch still, but 2 blocks will be empty because they contained
    // duplicate transactions
    let expected_block_height = 8;
    let expected_duplicate_block_num_round_1 = 4;
    let expected_duplicate_block_num_round_2 = 8;
    // assert all 8 batches were executed
    assert_eq!(canonical_tip.number, expected_block_height);
    // assert canonical tip and finalized block are equal
    assert_eq!(canonical_tip.hash(), final_block.hash);
    // assert last executed output is correct and finalized
    let last_output = execution_node.last_executed_output().await?;
    assert_eq!(last_output, consensus_output_2_hash);
    // assert priority fees went to batch producer
    let batch_producer_1_account = reth_env
        .retrieve_account(&batch_producer_1)?
        .expect("batch_producer_1 account has priority fees");
    assert_eq!(
        batch_producer_1_account.balance,
        U256::from(
            *expected_priority_fees
                .get(&batch_producer_1)
                .expect("batch_producer_1 has expected base fees")
        )
    );
    let batch_producer_2_account = reth_env
        .retrieve_account(&batch_producer_2)?
        .expect("batch_producer_2 account has priority fees");
    assert_eq!(
        batch_producer_2_account.balance,
        U256::from(
            *expected_priority_fees
                .get(&batch_producer_2)
                .expect("batch_producer_2 has expected base fees")
        )
    );
    // assert no issuance-based block rewards (rewards come from fee distribution now)
    // let rewards_1 = reth_env.get_validator_rewards(final_block.hash, leader_address_1)?;
    // let rewards_2 = reth_env.get_validator_rewards(final_block.hash, leader_address_2)?;
    // assert_eq!(rewards_1, U256::ZERO, "no issuance rewards for leader 1");
    // assert_eq!(rewards_2, U256::ZERO, "no issuance rewards for leader 2");
    // assert all basefees sent to fee aggregator
    let fee_aggregator_genesis_balance = chain
        .genesis()
        .alloc
        .get(&FEE_AGGREGATOR_ADDRESS)
        .map(|acct| acct.balance)
        .unwrap_or(U256::ZERO);
    let fee_aggregator = reth_env
        .retrieve_account(&FEE_AGGREGATOR_ADDRESS)?
        .map(|acct| acct.balance)
        .expect("fee aggregator has an account");
    assert_eq!(
        expected_base_fees,
        fee_aggregator
            .checked_sub(fee_aggregator_genesis_balance)
            .expect("fee aggregator balance doesn't underflow"),
        "Fee aggregator missing basefees"
    );

    // pull newly executed blocks from database (skip genesis)
    //
    // Uses the provided `headers_range` to get the headers for the range, and `assemble_block`
    // to construct blocks from the following inputs:
    //     – Header
    //     - Transactions
    //     – Ommers
    //     – Withdrawals
    //     – Requests
    //     – Senders
    let executed_blocks = reth_env.block_with_senders_range(1..=expected_block_height)?;
    assert_eq!(expected_block_height, executed_blocks.len() as u64);

    // basefee intentionally increased with loop
    let mut expected_base_fee = MIN_PROTOCOL_BASE_FEE;
    let output_digest_1: B256 = consensus_output_1.digest().into();
    let output_digest_2: B256 = consensus_output_2.digest().into();

    // assert blocks are execute as expected
    for (idx, txs) in txs_by_block.iter().enumerate() {
        let block = &executed_blocks[idx];
        let signers = &signers_by_block[idx];

        // expect blocks 4 and 8 to be empty (no txs bc they are duplicates)
        // sub 1 to account for loop idx starting at 0
        if idx == expected_duplicate_block_num_round_1 - 1
            || idx == expected_duplicate_block_num_round_2 - 1
        {
            assert!(block.senders().is_empty());
            assert!(block.body().transactions.is_empty());
            // gas used should NOT be the same as bc duplicate transaction are ignored
            assert_ne!(block.gas_used, max_batch_gas(leader_2_epoch));
            // gas used should be zero bc all transactions were duplicates
            assert_eq!(block.gas_used, 0);
        } else {
            assert_eq!(&block.senders(), signers);
            assert_eq!(&block.body().transactions, txs);
        }

        // basefee was increased for each batch
        expected_base_fee += idx as u64;
        // assert basefee is same as worker's block
        assert_eq!(block.base_fee_per_gas, Some(expected_base_fee));

        // define re-usable variable here for asserting all values against expected output
        let mut expected_output = &consensus_output_1;
        let mut expected_subdag_index = &sub_dag_index_1;
        let mut output_digest = output_digest_1;
        // We just set this to default in the test...
        let mut expected_parent_beacon_block_root = consensus_output_1.consensus_header_hash();
        let mut expected_batch_index = idx;
        let mut batch_producer = &batch_producer_1;

        // update values based on index for all assertions below
        if idx >= 4 {
            // use different output for last 4 blocks
            expected_output = &consensus_output_2;
            expected_subdag_index = &sub_dag_index_2;
            output_digest = output_digest_2;
            expected_parent_beacon_block_root = consensus_output_2.consensus_header_hash();
            // takeaway 4 to compensate for independent loops for executing batches
            expected_batch_index = idx - 4;
            batch_producer = &batch_producer_2;
        }

        // assert consensus output written to BEACON_ROOTS contract (cancun - eip4788)
        assert_eip4788(&reth_env, block.sealed_block(), expected_parent_beacon_block_root)?;

        // assert parent root is written to HISTORY_STORAGE_ADDRESS (pectra - eip2935)
        assert_eip2935(&reth_env, block.sealed_block())?;

        // beneficiary
        assert_eq!(&block.beneficiary, batch_producer);

        // nonce matches subdag index and method all match
        assert_eq!(<FixedBytes<8> as Into<u64>>::into(block.nonce), *expected_subdag_index);
        assert_eq!(<FixedBytes<8> as Into<u64>>::into(block.nonce), expected_output.nonce());

        // timestamp
        assert_eq!(block.timestamp, expected_output.committed_at());
        // parent beacon block root is output digest
        assert_eq!(block.parent_beacon_block_root, Some(expected_parent_beacon_block_root));

        if idx == 0 {
            // first block's parent is expected to be genesis
            assert_eq!(block.parent_hash, chain.genesis_hash());
            // expect header number 1 for batch bc of genesis
            assert_eq!(block.number, 1);
        } else {
            // assert parents executed in order (sanity check)
            let expected_parent = executed_blocks[idx - 1].header().hash_slow();
            assert_eq!(block.parent_hash, expected_parent);
            // expect block numbers NOT the same as batch's headers
            assert_ne!(block.number, 1);
        }

        // mix hash is xor batch's hash and consensus output digest
        let expected_mix_hash = all_batches[idx].digest() ^ output_digest;
        assert_eq!(block.mix_hash, expected_mix_hash);
        // duplicate batches produce empty blocks; others emit Transfer events
        let is_duplicate = (idx == 3) || (idx == 7);
        if is_duplicate {
            assert_eq!(block.logs_bloom, Bloom::default());
        } else {
            assert_ne!(block.logs_bloom, Bloom::default());
        }
        // gas limit should come from batch
        assert_eq!(block.gas_limit, max_batch_gas(leader_2_epoch));
        // difficulty should match the batch's index within consensus output
        // and default worker id 0
        assert_eq!(block.difficulty, U256::from(expected_batch_index << 16));
        // assert closing epoch randomness matches extra data field in last block
        let expected_extra = if idx == 7 {
            Bytes::from(expected_output.keccak_leader_sigs().0)
        } else {
            Bytes::default()
        };
        assert_eq!(block.extra_data, expected_extra);
        // pre-fork: batch digest stored in ommers_hash
        assert_eq!(block.ommers_hash, all_batch_digests[idx]);
        // pre-fork: requests_hash is empty
        assert_eq!(block.requests_hash, Some(EMPTY_REQUESTS_HASH));
        // close-epoch withdrawals_root is driven by RewardsBackend::tally(),
        // which walks the consensus DB primed below from the two outputs.
        let expected_withdrawals = if idx == 7 {
            let withdrawals = gas_accumulator.rewards_counter().generate_withdrawals();
            calculate_withdrawals_root(withdrawals.as_ref())
        } else {
            EMPTY_WITHDRAWALS
        };
        assert_eq!(block.withdrawals_root, Some(expected_withdrawals));
    }

    Ok(())
}

/// The engine drops a duplicate `ConsensusOutput` via the deterministic `(epoch, leader_round)`
/// + subdag-digest guard.
///
/// Three empty outputs are sent: output_1 (epoch 0, round 0), output_dup (a byte-for-byte
/// re-delivery of output_1, the benign dual-feed case) and output_2 (epoch 0, round 2). Two
/// blocks are produced, one per distinct output; the re-delivery is silently dropped.
#[tokio::test]
async fn test_duplicate_consensus_output_is_dropped() -> eyre::Result<()> {
    let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
    let tmp_dir = TempDir::new().expect("temp dir");

    // execution node components
    let gas_accumulator = GasAccumulator::new(1); // 1 worker
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        tmp_dir.path(),
        Some(gas_accumulator.rewards_counter()),
    )
    .await?;

    // update rewards counter so execution address is visible
    let committee =
        create_committee_from_state(execution_node.epoch_state_from_canonical_tip().await?).await?;
    let leader_id = committee.authorities().first().expect("first authority").id();
    gas_accumulator.rewards_counter().set_committee(committee);

    // consensus side
    //
    // Build three empty ConsensusOutput objects:
    //   output_1: number=0 (valid)
    //   output_dup: number=0 (duplicate - should be dropped)
    //   output_2: number=1 (valid)

    let timestamp = now();

    // output 1 (number=0)
    let mut leader_1 = Certificate::default();
    leader_1.header.round = 0;
    leader_1.header.created_at = timestamp;
    leader_1.header_mut_for_test().author = leader_id.clone();
    let sub_dag_index_1 = 0;

    let consensus_output_1 = ConsensusOutput {
        sub_dag: CommittedSubDag::new(
            vec![leader_1.clone()],
            leader_1,
            sub_dag_index_1,
            ReputationScores::default(),
            None,
        )
        .into(),
        number: 0,
        ..Default::default()
    };

    // output_dup: a true re-delivery of output_1 (identical epoch, round, and subdag content).
    // The deterministic guard drops it as a benign dual-feed duplicate.
    let mut leader_dup = Certificate::default();
    leader_dup.header.round = 0;
    leader_dup.header.created_at = timestamp;
    leader_dup.header_mut_for_test().author = leader_id.clone();
    let sub_dag_index_dup = 0;

    let consensus_output_dup = ConsensusOutput {
        sub_dag: CommittedSubDag::new(
            vec![leader_dup.clone()],
            leader_dup,
            sub_dag_index_dup,
            ReputationScores::default(),
            None,
        )
        .into(),
        number: 0, // identical content + position as output_1 - should be dropped
        ..Default::default()
    };

    // output 2 (number=1)
    let mut leader_2 = Certificate::default();
    leader_2.header.round = 2;
    leader_2.header.created_at = timestamp + 2;
    leader_2.header_mut_for_test().author = leader_id;
    let sub_dag_index_2 = 2;

    let consensus_output_2 = ConsensusOutput {
        sub_dag: CommittedSubDag::new(
            vec![leader_2.clone()],
            leader_2,
            sub_dag_index_2,
            ReputationScores::default(),
            None,
        )
        .into(),
        parent_hash: consensus_output_1.consensus_header_hash(),
        number: 1,
        ..Default::default()
    };
    let consensus_output_2_hash = consensus_output_2.consensus_header_hash();

    // execution side

    // channel capacity 3 so all outputs can be sent before engine starts
    let (to_engine, from_consensus) = tokio::sync::mpsc::channel(3);
    let reth_env = execution_node.get_reth_env().await;
    let max_round = None;
    let genesis_header = chain.sealed_genesis_header();

    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let temp_db_dir = TempDir::new().unwrap();
    let ordering_store = open_db(temp_db_dir.path());
    let batch_ordering = BatchOrdering::new_with_empty_state(ordering_store);
    let engine = ExecutorEngine::new_for_test(
        reth_env.clone(),
        max_round,
        from_consensus,
        genesis_header.clone(),
        shutdown.subscribe(),
        task_manager.get_spawner(),
        gas_accumulator,
        None,
        ETHEREUM_BLOCK_GAS_LIMIT_56BITS,
        batch_ordering,
        rayls_execution_evm::in_flight::InFlightTracker::new(),
    );

    // send all three outputs through the channel (dedup guard only applies to stream)
    to_engine
        .send((rayls_infrastructure_types::CameFrom::Test, consensus_output_1))
        .await
        .expect("send output 1");
    to_engine
        .send((rayls_infrastructure_types::CameFrom::Test, consensus_output_dup))
        .await
        .expect("send duplicate output");
    to_engine
        .send((rayls_infrastructure_types::CameFrom::Test, consensus_output_2))
        .await
        .expect("send output 2");

    // drop sending channel to shut engine down after processing
    drop(to_engine);

    let (tx, rx) = oneshot::channel();

    // spawn engine task
    task_manager.spawn_task("test task eng", async move {
        let res = engine.await;
        let _ = tx.send(res);
    });

    let engine_task = timeout(Duration::from_secs(10), rx).await??;
    // consensus output stream closed after processing valid outputs
    assert_matches!(engine_task, Err(RLEngineError::ConsensusOutputStreamClosed));

    // flush deferred persistence so DB queries return up-to-date state
    reth_env.flush_persistence().await?;

    // assert only 2 blocks were produced (duplicate output was dropped)
    let expected_block_height = 2;
    let canonical_tip = reth_env.canonical_tip();
    assert_eq!(
        canonical_tip.number, expected_block_height,
        "expected 2 blocks (duplicate output should be dropped), got {}",
        canonical_tip.number
    );

    // assert canonical tip and finalized block are equal
    let canonical_tip = reth_env.canonical_tip();
    let final_block = reth_env.finalized_block_num_hash()?.expect("finalized block");
    assert_eq!(canonical_tip.hash(), final_block.hash);

    // assert last executed output matches output_2 (the last valid one)
    let last_output = execution_node.last_executed_output().await?;
    assert_eq!(last_output, consensus_output_2_hash);

    Ok(())
}

#[tokio::test]
async fn test_max_round_terminates_early() -> eyre::Result<()> {
    let tmp_dir = TempDir::new().unwrap();
    // create batches for consensus output
    let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
    let mut batches_1 = rayls_execution_evm::test_utils::batches(chain.clone(), 4); // create 4 batches
    let mut batches_2 = rayls_execution_evm::test_utils::batches(chain, 4); // create 4 batches

    // okay to clone these because they are only used to seed genesis, decode transactions, and
    // recover signers
    let all_batches = [batches_1.clone(), batches_2.clone()].concat();

    // use default genesis and seed accounts to execute batches
    let genesis = test_genesis();
    let (genesis, _txs_by_block, _signers_by_block) =
        seeded_genesis_from_random_batches(genesis, all_batches.iter());
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    // create execution node components
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        &tmp_dir.path().join("exc-node"),
        None,
    )
    .await?;

    // execute batches to update headers with valid data
    let mut inc_base_fee = MIN_PROTOCOL_BASE_FEE;

    // updated batches separately because they are mutated in-place
    // and need to be passed to different outputs
    //
    // update first round
    for (idx, batch) in batches_1.iter_mut().enumerate() {
        // increase basefee
        inc_base_fee += idx as u64;

        // update basefee and set beneficiary
        batch.beneficiary = Address::random();
        batch.base_fee_per_gas = inc_base_fee;

        // actually execute the block now
        execute_test_batch(batch);
        debug!("{idx}\n{:?}\n", batch);
    }

    // update second round
    for (idx, batch) in batches_2.iter_mut().enumerate() {
        // continue increasing basefee
        // add 4 to continue where previous round left off
        // this makes assertions easier at the end
        inc_base_fee += 4 + idx as u64;

        // update basefee and set beneficiary
        batch.beneficiary = Address::random();
        batch.base_fee_per_gas = inc_base_fee;

        // actually execute the block now
        execute_test_batch(batch);
        debug!("{idx}\n{:?}\n", batch);
    }

    // consensus side
    //
    // create consensus output bc transactions in batches
    // are randomly generated
    //
    // for each tx, seed address with funds in genesis
    let timestamp = now();
    let mut leader_1 = Certificate::default();
    // update timestamp
    leader_1.update_created_at_for_test(timestamp);
    let sub_dag_index_1 = 1;
    leader_1.header.round = sub_dag_index_1 as u32;
    let reputation_scores = ReputationScores::default();
    let previous_sub_dag = None;
    let batch_digests_1: VecDeque<BlockHash> = batches_1.iter().map(|b| b.digest()).collect();
    let subdag_1 = Arc::new(CommittedSubDag::new(
        vec![Certificate::default()],
        leader_1,
        sub_dag_index_1,
        reputation_scores,
        previous_sub_dag,
    ));

    let consensus_output_1 = ConsensusOutput {
        sub_dag: subdag_1.clone(),
        batches: vec![CertifiedBatch { address: Address::random(), batches: batches_1 }],
        batch_digests: batch_digests_1,
        ..Default::default()
    };
    let consensus_output_1_hash = consensus_output_1.consensus_header_hash();

    // create second output
    let mut leader_2 = Certificate::default();
    // update timestamp
    leader_2.update_created_at_for_test(timestamp + 2);
    let sub_dag_index_2 = 2;
    leader_2.header.round = sub_dag_index_2 as u32;
    let reputation_scores = ReputationScores::default();
    let previous_sub_dag = Some(subdag_1.as_ref());
    let batch_digests_2: VecDeque<BlockHash> = batches_2.iter().map(|b| b.digest()).collect();
    let subdag_2 = CommittedSubDag::new(
        vec![Certificate::default()],
        leader_2,
        sub_dag_index_2,
        reputation_scores,
        previous_sub_dag,
    )
    .into();

    let consensus_output_2 = ConsensusOutput {
        sub_dag: subdag_2,
        batches: vec![CertifiedBatch { address: Address::random(), batches: batches_2 }],
        batch_digests: batch_digests_2,
        parent_hash: consensus_output_1.consensus_header_hash(),
        number: 1,
        ..Default::default()
    };

    // execution side

    let (_to_engine, from_consensus) = tokio::sync::mpsc::channel(1);
    // set max round to "1" - this should receive both digests, but stop after the first round
    let max_round = Some(1);
    let parent = chain.sealed_genesis_header();

    let shutdown = Notifier::default();
    let task_manager = TaskManager::default();
    let reth_env = execution_node.get_reth_env().await;
    let temp_db_dir = TempDir::new().unwrap();
    let ordering_store = open_db(temp_db_dir.path());
    let batch_ordering = BatchOrdering::new_with_empty_state(ordering_store);
    let mut engine = ExecutorEngine::new_for_test(
        reth_env.clone(),
        max_round,
        from_consensus,
        parent,
        shutdown.subscribe(),
        task_manager.get_spawner(),
        GasAccumulator::default(),
        None,
        ETHEREUM_BLOCK_GAS_LIMIT_56BITS,
        batch_ordering,
        rayls_execution_evm::in_flight::InFlightTracker::new(),
    );

    // queue both output - simulate already received from channel
    engine.push_back_queued_for_test(consensus_output_1);
    engine.push_back_queued_for_test(consensus_output_2);

    // NOTE: sending channel is NOT dropped in this test, so engine should continue listening
    // until max block reached

    // channels for engine shutting down
    let (tx, rx) = oneshot::channel();

    // spawn engine task
    //
    // one output already queued up, one output waiting in broadcast stream
    task_manager.spawn_task("test task eng", async move {
        let res = engine.await;
        let _ = tx.send(res);
    });

    let engine_task = timeout(Duration::from_secs(10), rx).await??;
    assert!(engine_task.is_ok(), "{:?}", engine_task);

    // flush deferred persistence so DB queries return up-to-date state
    reth_env.flush_persistence().await?;

    let canonical_tip = reth_env.canonical_tip();
    let final_block = reth_env.finalized_block_num_hash()?.expect("finalized block");

    debug!("canonical tip: {canonical_tip:?}");
    debug!("final block num {final_block:?}");

    let expected_block_height = 4;
    // assert all 4 batches were executed from round 1
    assert_eq!(canonical_tip.number, expected_block_height);
    // assert canonical tip and finalized block are equal
    assert_eq!(canonical_tip.hash(), final_block.hash);
    // assert last executed output is correct and finalized
    let last_output = execution_node.last_executed_output().await?;
    assert_eq!(last_output, consensus_output_1_hash);

    Ok(())
}

/// Test that BatchDigestV2 hardfork correctly transitions batch digest placement
/// from ommers_hash (pre-fork) to requests_hash (post-fork).
///
/// Single consensus output with 4 batches, fork activates at block 3:
/// - Blocks 1-2: batch digest in ommers_hash, requests_hash = EMPTY_REQUESTS_HASH
/// - Blocks 3-4: batch digest in requests_hash, ommers_hash = EMPTY_OMMER_ROOT_HASH
#[tokio::test]
async fn test_batch_digest_v2_hardfork_transition() -> eyre::Result<()> {
    use rayls_execution_evm::{BaseFeeParams, RaylsChainSpec};
    use rayls_infrastructure_types::EMPTY_OMMER_ROOT_HASH;

    let tmp_dir = TempDir::new().expect("temp dir");

    // create 4 batches in a single consensus round -> 4 blocks
    let base_chain = test_chain_spec_arc();
    let mut batches = rayls_execution_evm::test_utils::batches(base_chain, 4);

    // seed genesis with funded accounts for batch transactions
    let genesis = test_genesis();
    let (genesis, _, _) = seeded_genesis_from_random_batches(genesis, batches.iter());
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    // build RaylsChainSpec with BatchDigestV2 activating at block 3
    let rayls_spec = Arc::new(
        RaylsChainSpec::builder(chain.clone())
            .batch_digest_v2(3)
            .empty_output_block(0)
            .base_fee_params(BaseFeeParams::ethereum())
            .build(),
    );

    // create execution node with custom rayls chain spec
    let gas_accumulator = GasAccumulator::new(1);
    let reth_env = RethEnv::new_for_temp_chain_with_rayls_spec(
        chain.clone(),
        rayls_spec,
        tmp_dir.path(),
        &TaskManager::default(),
        Some(gas_accumulator.rewards_counter()),
    )
    .await?;

    let (builder, _) = rayls_testing_test_utils::execution_builder_no_args(
        Some(chain.clone()),
        None,
        tmp_dir.path(),
    )?;
    let execution_node = rayls_testing_test_utils::TestExecutionNode::new(&builder, reth_env)?;

    // set up committee and rewards
    let committee =
        create_committee_from_state(execution_node.epoch_state_from_canonical_tip().await?).await?;
    let authority_1 = committee.authorities().first().expect("first authority").id();
    gas_accumulator.rewards_counter().set_committee(committee.clone());

    let batch_producer =
        committee.authorities().get(2).expect("third authority").execution_address();

    // prepare batches with valid execution data
    for batch in batches.iter_mut() {
        batch.beneficiary = batch_producer;
        batch.base_fee_per_gas = MIN_PROTOCOL_BASE_FEE;
        execute_test_batch(batch);
    }

    let batch_digests: VecDeque<BlockHash> = batches.iter().map(|b| b.digest()).collect();

    // single consensus output with 4 batches -> blocks 1-4
    let mut leader = Certificate::default();
    leader.update_created_at_for_test(now());
    leader.header_mut_for_test().author = authority_1;
    leader.header.round = 1;
    let subdag = Arc::new(CommittedSubDag::new(
        vec![leader.clone()],
        leader,
        1,
        ReputationScores::default(),
        None,
    ));
    let consensus_output = ConsensusOutput {
        sub_dag: subdag,
        batches: vec![CertifiedBatch { address: batch_producer, batches }],
        batch_digests: batch_digests.clone(),
        ..Default::default()
    };
    let all_batch_digests: Vec<BlockHash> = batch_digests.into();

    // run engine to completion
    let reth_env =
        run_engine(&execution_node, &chain, gas_accumulator, vec![consensus_output]).await?;

    // verify 4 blocks were produced
    let canonical_tip = reth_env.canonical_tip();
    assert_eq!(canonical_tip.number, 4);

    // blocks 1-2: pre-fork - batch digest in ommers_hash
    for block_num in 1..=2u64 {
        let block = reth_env
            .sealed_block_by_number(block_num)?
            .unwrap_or_else(|| panic!("block {block_num} exists"));
        let idx = (block_num - 1) as usize;
        assert_eq!(
            block.ommers_hash, all_batch_digests[idx],
            "pre-fork block {block_num}: batch digest should be in ommers_hash"
        );
        assert_eq!(
            block.requests_hash,
            Some(EMPTY_REQUESTS_HASH),
            "pre-fork block {block_num}: requests_hash should be empty"
        );
    }

    // blocks 3-4: post-fork - batch digest in requests_hash
    for block_num in 3..=4u64 {
        let block = reth_env
            .sealed_block_by_number(block_num)?
            .unwrap_or_else(|| panic!("block {block_num} exists"));
        let idx = (block_num - 1) as usize;
        assert_eq!(
            block.ommers_hash, EMPTY_OMMER_ROOT_HASH,
            "post-fork block {block_num}: ommers_hash should be empty root"
        );
        assert_eq!(
            block.requests_hash,
            Some(all_batch_digests[idx]),
            "post-fork block {block_num}: batch digest should be in requests_hash"
        );
    }

    Ok(())
}

/// Marks of txs an executed batch dropped as nonce-too-high are released for re-sealing.
///
/// A commit-order inversion executes a successor batch before its predecessor: the successor's
/// txs all drop nonce-too-high while staying pooled and marked in-flight from their seal. The
/// spent marks must release on the drop, or the txs sit unselectable until the TTL sweep while
/// every following batch of the sender drops in full.
#[tokio::test]
async fn test_dropped_txs_release_in_flight_marks() -> eyre::Result<()> {
    let tmp_dir = TempDir::new().expect("temp dir");
    let chain = test_chain_spec_arc();
    let mut batches = rayls_execution_evm::test_utils::batches(chain, 1);

    // two txs nonce-gapped ahead of the sender's account nonce (0): both drop nonce-too-high
    let genesis = test_genesis();
    let mut tx_factory = TransactionFactory::new_random();
    let gapped: Vec<_> = [5u64, 6]
        .iter()
        .map(|nonce| {
            tx_factory.create_explicit_eip1559(
                Some(genesis.config.chain_id),
                Some(*nonce),
                Some(MAX_PRIORITY_FEE_PER_GAS),
                Some(MAX_FEE_PER_GAS),
                None,
                Some(Address::random()),
                None,
                None,
                None,
            )
        })
        .collect();
    let gapped_hashes: Vec<B256> = gapped.iter().map(|tx| *tx.hash()).collect();
    let batch = batches.first_mut().expect("one batch");
    for tx in &gapped {
        batch.transactions_mut().push(tx.encoded_2718().into());
    }

    let (genesis, _txs, _signers) = seeded_genesis_from_random_batches(genesis, batches.iter());
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    let gas_accumulator = GasAccumulator::new(1);
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        tmp_dir.path(),
        Some(gas_accumulator.rewards_counter()),
    )
    .await?;
    let committee =
        create_committee_from_state(execution_node.epoch_state_from_canonical_tip().await?).await?;
    let authority_1 = committee.authorities().first().expect("first authority").id();
    let batch_producer =
        committee.authorities().get(2).expect("third authority").execution_address();
    for batch in batches.iter_mut() {
        batch.beneficiary = batch_producer;
        batch.base_fee_per_gas = MIN_PROTOCOL_BASE_FEE;
    }
    let batch_digests: VecDeque<BlockHash> = batches.iter().map(|b| b.digest()).collect();

    // the marks a seal would have left: the two gapped txs, plus one unrelated mark that must
    // survive the release untouched
    let in_flight = InFlightTracker::new();
    let decoy = B256::random();
    in_flight.mark_in_flight(gapped_hashes.iter().copied().chain([decoy]));
    assert_eq!(in_flight.len(), 3);

    let mut leader = Certificate::default();
    leader.update_created_at_for_test(now());
    leader.header_mut_for_test().author = authority_1;
    leader.header.round = 1;
    let subdag = Arc::new(CommittedSubDag::new(
        vec![leader.clone()],
        leader,
        1,
        ReputationScores::default(),
        None,
    ));
    let consensus_output = ConsensusOutput {
        sub_dag: subdag,
        batches: vec![CertifiedBatch { address: batch_producer, batches }],
        batch_digests,
        ..Default::default()
    };

    run_engine_with_in_flight(
        &execution_node,
        &chain,
        gas_accumulator,
        vec![consensus_output],
        Some(in_flight.clone()),
    )
    .await?;

    assert_eq!(
        in_flight.len(),
        1,
        "the dropped txs' marks must release on execution; only the unrelated mark remains"
    );
    Ok(())
}

/// Under OutputSeqNormalization, batches still parked at an epoch change are discarded, never
/// force executed: their predecessor seq never executed in their epoch, so executing them out of
/// order only drops their txs nonce-too-high or lands duplicates. The txs stay pooled and
/// are re-sealed in the new epoch.
#[tokio::test]
async fn test_epoch_boundary_discards_parked_batches_under_seq_normalization() -> eyre::Result<()> {
    use rayls_execution_evm::{BaseFeeParams, RaylsChainSpec};

    let tmp_dir = TempDir::new().expect("temp dir");
    let base_chain = test_chain_spec_arc();
    let mut batches = rayls_execution_evm::test_utils::batches(base_chain, 3);
    let genesis = test_genesis();
    let (genesis, _, _) = seeded_genesis_from_random_batches(genesis, batches.iter());
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let rayls_spec = Arc::new(
        RaylsChainSpec::builder(chain.clone())
            .batch_digest_v2(0)
            .empty_output_block(0)
            .output_seq_normalization(0)
            .base_fee_params(BaseFeeParams::ethereum())
            .build(),
    );
    let gas_accumulator = GasAccumulator::new(1);
    let reth_env = RethEnv::new_for_temp_chain_with_rayls_spec(
        chain.clone(),
        rayls_spec,
        tmp_dir.path(),
        &TaskManager::default(),
        Some(gas_accumulator.rewards_counter()),
    )
    .await?;
    let (builder, _) = rayls_testing_test_utils::execution_builder_no_args(
        Some(chain.clone()),
        None,
        tmp_dir.path(),
    )?;
    let execution_node = rayls_testing_test_utils::TestExecutionNode::new(&builder, reth_env)?;
    let committee =
        create_committee_from_state(execution_node.epoch_state_from_canonical_tip().await?).await?;
    let authority_1 = committee.authorities().first().expect("first authority").id();
    let producer = committee.authorities().get(2).expect("third authority").execution_address();
    for batch in batches.iter_mut() {
        batch.beneficiary = producer;
        batch.base_fee_per_gas = MIN_PROTOCOL_BASE_FEE;
        execute_test_batch(batch);
    }
    // epoch-0 output: seq 1 executes (fresh baseline), seq 3 parks behind the missing seq 2
    batches[0].seq = 1;
    batches[1].seq = 3;
    // epoch-1 output: a fresh baseline for the new epoch
    batches[2].seq = 1;

    let mut leader_0 = Certificate::default();
    leader_0.update_created_at_for_test(now());
    leader_0.header_mut_for_test().author = authority_1.clone();
    leader_0.header.round = 1;
    let output_0 = ConsensusOutput {
        sub_dag: Arc::new(CommittedSubDag::new(
            vec![leader_0.clone()],
            leader_0,
            1,
            ReputationScores::default(),
            None,
        )),
        batches: vec![CertifiedBatch {
            address: producer,
            batches: vec![batches[0].clone(), batches[1].clone()],
        }],
        batch_digests: vec![batches[0].digest(), batches[1].digest()].into(),
        ..Default::default()
    };

    let mut leader_1 = Certificate::default();
    leader_1.update_created_at_for_test(now());
    leader_1.header_mut_for_test().author = authority_1.clone();
    leader_1.header.round = 3;
    leader_1.header.epoch = 1;
    let output_1 = ConsensusOutput {
        sub_dag: Arc::new(CommittedSubDag::new(
            vec![leader_1.clone()],
            leader_1,
            2,
            ReputationScores::default(),
            None,
        )),
        batches: vec![CertifiedBatch { address: producer, batches: vec![batches[2].clone()] }],
        batch_digests: vec![batches[2].digest()].into(),
        ..Default::default()
    };

    let reth_env =
        run_engine(&execution_node, &chain, gas_accumulator, vec![output_0, output_1]).await?;

    // seq 1 (epoch 0) + seq 1 (epoch 1) execute; the parked seq 3 is discarded at the boundary
    assert_eq!(
        reth_env.canonical_tip().number,
        2,
        "the parked batch must be discarded at the boundary, not force executed"
    );
    Ok(())
}
