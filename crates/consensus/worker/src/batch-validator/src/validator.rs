//! Validation of peer batches and admission of gossiped and forwarded transactions.

use rayls_execution_evm::{
    bytes_to_txn, chainspec::RaylsHardforks, recover_pooled_transaction,
    recover_signed_transaction, reth_env::RethEnv, EthPooledTransaction, FixedBytes, PoolErrorKind,
    PoolTransaction as _, WorkerTxPool,
};
use rayls_infrastructure_types::{
    fxhash_slot_digest, gas_accumulator::BaseFeeContainer, legacy_slot_digest, max_batch_size,
    BatchValidation, BatchValidationError, BlockHash, Bytes, CommitteeSlots, Epoch, SealedBatch,
    SubmitBatchError, TransactionSigned, TransactionTrait as _, WorkerId,
};
use rayon::iter::{IntoParallelRefIterator as _, ParallelIterator as _};

use dashmap::DashMap;
use tracing::{debug, trace, warn};

/// Result alias for batch validation.
type BatchValidationResult<T> = Result<T, BatchValidationError>;

/// Recover signers in parallel above this many transactions; below it the rayon fan-out costs more
/// than it saves.
const PARALLEL_PARSE_THRESHOLD: usize = 100;

/// Recover signers for forwarded transactions, in parallel once the batch is large enough.
///
/// Invalid encodings are dropped; the pool validates the rest and reports any that are stale.
fn recover_forwarded_txns<T: AsRef<[u8]> + Sync>(txs_bytes: &[T]) -> Vec<EthPooledTransaction> {
    if txs_bytes.len() < PARALLEL_PARSE_THRESHOLD {
        txs_bytes.iter().filter_map(|bytes| bytes_to_txn(bytes.as_ref()).ok()).collect()
    } else {
        txs_bytes.par_iter().filter_map(|bytes| bytes_to_txn(bytes.as_ref()).ok()).collect()
    }
}

/// Validator for peer batches and the dispatch gate for inbound transactions.
///
/// Batches carry no signature of their own: libp2p authenticates the sending peer as a committee
/// member, so validation checks only the batch contents.
#[derive(Clone, Debug)]
pub struct BatchValidator {
    /// Execution environment providing the canonical tip and the chain spec.
    reth_env: RethEnv,
    /// The transaction pool inbound transactions are admitted to; `None` on a node that does not
    /// pool for the committee.
    tx_pool: Option<WorkerTxPool>,
    /// Worker id for this validator.
    worker_id: WorkerId,
    /// Current base fee for this validator's worker.
    base_fee: BaseFeeContainer,
    /// Epoch we are validating for.
    epoch: Epoch,
    /// Digests validated within the last minute, so a re-gossiped batch is not re-validated.
    validated_batches: DashMap<FixedBytes<32>, u64>,
    /// Block gas limit.
    gas_limit: u64,
}

#[async_trait::async_trait]
impl BatchValidation for BatchValidator {
    /// Validate a peer's batch.
    ///
    /// Workers do not execute full batches. This method validates the required information.
    async fn validate_batch(&self, sealed_batch: &SealedBatch) -> Result<(), BatchValidationError> {
        // ensure digest matches batch
        let batch = &sealed_batch.batch;
        let digest = sealed_batch.digest();

        let verified_hash = batch.digest();
        if digest != verified_hash {
            return Err(BatchValidationError::InvalidDigest);
        }
        if self.validated_batches.contains_key(&digest) {
            // already validated recently
            return Ok(());
        }

        // A validator belongs to a worker and that worker only handles batches with its id.
        if batch.worker_id != self.worker_id {
            return Err(BatchValidationError::InvalidWorkerId {
                expected_worker_id: self.worker_id,
                worker_id: batch.worker_id,
            });
        }

        if batch.epoch != self.epoch {
            return Err(BatchValidationError::InvalidEpoch {
                expected: self.epoch,
                found: batch.epoch,
            });
        }

        // obtain info for validation
        let transactions = batch.transactions();

        self.validate_batch_size_bytes(transactions, batch.epoch)?;

        let decoded_txs = self.decode_transactions(transactions, digest)?;

        self.validate_no_blob_txs(&decoded_txs)?;

        self.validate_batch_gas(&decoded_txs)?;

        // all batches for a worker and epoch share one base fee
        self.validate_basefee(batch.base_fee_per_gas)?;

        // `now()` is in seconds, so the dedup window is 60; sweep before insert so the map
        // stays bounded by the recent-batch rate.
        let now = rayls_infrastructure_types::now();
        self.validated_batches.retain(|_, v| *v > now.saturating_sub(60));
        self.validated_batches.insert(digest, now);

        Ok(())
    }

    /// Admit a gossiped transaction message to the pool when this node owns its committee slot.
    ///
    /// The message's first transaction decides the owner slot, so a forwarder must keep one
    /// sender's run in a single message. A node without a pool ignores every message.
    fn submit_batch_if_mine(
        &self,
        txs_bytes: &[Bytes],
        slots: &CommitteeSlots,
    ) -> Result<(), SubmitBatchError> {
        if let Some(tx_pool) = &self.tx_pool {
            if let Some(tx) = txs_bytes.iter().next() {
                if tx.len() < 8 {
                    return Err(SubmitBatchError::InvalidTransactionBytes);
                }
                // An empty slot table (a committee gap mid-transition) must not reach the
                // modulo: division by zero aborts the node, and a gossip message must never
                // be able to do that.
                if slots.size() == 0 {
                    return Ok(());
                }
                let owner = self.slot_digest(tx) % slots.size();
                // Under sender-affinity a down owner's senders fail over to the next live slot on
                // the ring; pre-fork ownership is exact, with no failover.
                let mine = if self.sender_affinity_active() {
                    slots.covers(owner)
                } else {
                    owner == slots.own_slot
                };
                if !mine {
                    return Ok(());
                }
                trace!(target: "worker::validator", ?owner, "tx accepted as committee owner");
            }

            let parsed_txns = if txs_bytes.len() < 100 {
                txs_bytes
                    .iter()
                    .map(|tx_bytes| bytes_to_txn(tx_bytes))
                    .collect::<Vec<Result<EthPooledTransaction, _>>>()
            } else {
                txs_bytes
                    .par_iter()
                    .map(|tx_bytes| bytes_to_txn(tx_bytes))
                    .collect::<Vec<Result<EthPooledTransaction, _>>>()
            };

            let tx_pool = tx_pool.clone();
            self.reth_env.get_task_spawner().spawn_task("submit-tx-batch", async move {
                let txs: Vec<_> = parsed_txns.into_iter().flatten().collect();
                for res in tx_pool.add_raw_transactions_external(txs).await {
                    match res {
                        Ok(_) => {}
                        // A hash this pool already holds is ordinary gossip dedup: multiple
                        // observers forward overlapping pools, and a forwarder re-sends what
                        // it cannot confirm landed. Under load this is the common outcome, so
                        // logging it at warn buries the submissions that did fail.
                        Err(e) if matches!(e.kind, PoolErrorKind::AlreadyImported) => {
                            debug!(target: "worker::validator", "gossipped txn already in pool: {e}");
                        }
                        Err(e) => {
                            warn!(target: "worker::validator", "failed to submit gossipped txn: {e}");
                        }
                    }
                }
            });
        }

        Ok(())
    }

    async fn submit_forwarded_txns(&self, tx_bytes: Vec<Bytes>) -> Vec<BlockHash> {
        let Some(tx_pool) = &self.tx_pool else {
            return Vec::new();
        };
        // Recover signers off the runtime; the owned Vec moves straight into the blocking task.
        let parsed =
            match tokio::task::spawn_blocking(move || recover_forwarded_txns(&tx_bytes)).await {
                Ok(parsed) => parsed,
                // A cancelled blocking task means runtime teardown; ack nothing so the sender keeps
                // the transactions and retries.
                Err(e) => {
                    warn!(target: "worker::validator", ?e, "signer recovery task did not complete");
                    return Vec::new();
                }
            };
        tx_pool.add_forwarded_txns(parsed).await
    }
}

impl BatchValidator {
    /// Create a validator for one worker and epoch.
    pub fn new(
        reth_env: RethEnv,
        tx_pool: Option<WorkerTxPool>,
        worker_id: WorkerId,
        base_fee: BaseFeeContainer,
        epoch: Epoch,
        gas_limit: u64,
    ) -> Self {
        Self {
            reth_env,
            tx_pool,
            worker_id,
            base_fee,
            epoch,
            validated_batches: Default::default(),
            gas_limit,
        }
    }

    /// Validate the size of transactions (in bytes).
    fn validate_batch_size_bytes(
        &self,
        transactions: &[Bytes],
        epoch: Epoch,
    ) -> BatchValidationResult<()> {
        // calculate size (in bytes) of included transactions
        let total_bytes = transactions
            .iter()
            .map(|tx| tx.len())
            .reduce(|total, size| total + size)
            .ok_or(BatchValidationError::EmptyBatch)?;
        let max_tx_bytes = max_batch_size(epoch);

        // allow txs that equal max tx bytes
        if total_bytes > max_tx_bytes {
            return Err(BatchValidationError::HeaderTransactionBytesExceedsMax(total_bytes));
        }

        Ok(())
    }

    /// Decode transactions to ensure encode/decode is valid.
    ///
    /// The decoded transactions are then used to validate max batch gas.
    #[inline]
    fn decode_transactions(
        &self,
        transactions: &Vec<Bytes>,
        digest: BlockHash,
    ) -> BatchValidationResult<Vec<TransactionSigned>> {
        transactions
            .par_iter()
            .map(|tx| Self::recover_and_validate(tx, digest))
            .collect::<BatchValidationResult<Vec<_>>>()
    }

    /// Possible gas used needs to be less than block's gas limit.
    ///
    /// Actual amount of gas used cannot be determined until execution.
    #[inline]
    fn validate_batch_gas(&self, transactions: &[TransactionSigned]) -> BatchValidationResult<()> {
        // `Self::validate_batch_size_bytes` checks for empty batch
        //
        // calculate total using tx gas limit and return error for u64 overflow
        let total_possible_gas =
            transactions.iter().map(|tx| tx.gas_limit()).try_fold(0_u64, |total, gas| {
                total.checked_add(gas).ok_or(BatchValidationError::GasOverflow)
            })?;

        // ensure total tx gas limit fits into block's gas limit
        let max_tx_gas = self.gas_limit;
        if total_possible_gas > max_tx_gas {
            return Err(BatchValidationError::HeaderMaxGasExceedsGasLimit {
                total_possible_gas,
                gas_limit: max_tx_gas,
            });
        }

        Ok(())
    }

    /// Compute the committee-slot dispatch digest for one transaction, which must be at least 8
    /// bytes.
    ///
    /// The algorithm follows the hardforks active at the next block: [`legacy_slot_digest`] of
    /// the bytes, then [`fxhash_slot_digest`] of the bytes under `TransactionLoadBalancing`, then
    /// [`fxhash_slot_digest`] of the recovered sender under `SenderAffinityLoadBalancing`. The
    /// gate reads the local canonical tip, so validators can briefly disagree across a fork block;
    /// the worst case is a duplicate inclusion that fails nonce-too-low at execution, never a
    /// consensus fork.
    fn slot_digest(&self, tx: &[u8]) -> u64 {
        let chain_spec = self.reth_env.rayls_chain_spec();
        let next_block = self.reth_env.canonical_tip().number + 1;
        if chain_spec.is_sender_affinity_load_balancing_active_at_block(next_block) {
            // Key the slot on the sender so one validator owns a sender's whole nonce chain instead
            // of consecutive ranges scattering across pools and parking nonce-gapped.
            if let Ok(pooled) = recover_pooled_transaction(tx) {
                return fxhash_slot_digest(pooled.sender().as_slice());
            }
            fxhash_slot_digest(tx)
        } else if chain_spec.is_transaction_load_balancing_active_at_block(next_block) {
            fxhash_slot_digest(tx)
        } else {
            legacy_slot_digest(tx)
        }
    }

    /// Whether sender-affinity dispatch (and its live-successor failover) is active for the next
    /// block. Reads the local canonical tip, so validators can briefly disagree across the fork.
    fn sender_affinity_active(&self) -> bool {
        let next_block = self.reth_env.canonical_tip().number + 1;
        self.reth_env
            .rayls_chain_spec()
            .is_sender_affinity_load_balancing_active_at_block(next_block)
    }

    /// Validate the block's basefee.
    ///
    /// After the EIP-1559 per-block fork, the payload builder computes the correct base fee
    /// from the parent header, so the batch-level value is best-effort and we skip the exact
    /// match check. The EVM itself rejects under-priced transactions at execution time.
    fn validate_basefee(&self, base_fee: u64) -> BatchValidationResult<()> {
        let chain_spec = self.reth_env.rayls_chain_spec();
        let tip = self.reth_env.canonical_tip();
        let next_block = tip.number + 1;
        if chain_spec.is_eip1559_active_at_block(next_block) {
            // the payload builder derives the base fee from the parent header; see above
            return Ok(());
        }
        let expected_base_fee = self.base_fee.base_fee();
        if base_fee != expected_base_fee {
            Err(BatchValidationError::InvalidBaseFee { expected_base_fee, base_fee })
        } else {
            Ok(())
        }
    }

    /// Reject a batch carrying an EIP-4844 transaction: the blob sidecar does not travel with it.
    fn validate_no_blob_txs(
        &self,
        transactions: &[TransactionSigned],
    ) -> BatchValidationResult<()> {
        if let Some(blob_tx) = transactions.iter().find(|tx| tx.is_eip4844()) {
            return Err(BatchValidationError::InvalidTx4844(*blob_tx.hash()));
        }
        Ok(())
    }

    /// Decode and recover one transaction, attributing a failure to the batch digest.
    fn recover_and_validate(
        tx: &[u8],
        digest: BlockHash,
    ) -> BatchValidationResult<TransactionSigned> {
        recover_signed_transaction(tx)
            .map_err(|e| BatchValidationError::RecoverTransaction(digest, e.to_string()))
    }
}

/// Validator that accepts every batch and admits nothing.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Default, Clone, Debug)]
pub struct NoopBatchValidator;

#[cfg(any(test, feature = "test-utils"))]
#[async_trait::async_trait]
impl BatchValidation for NoopBatchValidator {
    async fn validate_batch(&self, _batch: &SealedBatch) -> Result<(), BatchValidationError> {
        Ok(())
    }

    fn submit_batch_if_mine(
        &self,
        _tx_bytes: &[Bytes],
        _slots: &CommitteeSlots,
    ) -> Result<(), SubmitBatchError> {
        Ok(())
    }

    async fn submit_forwarded_txns(&self, _tx_bytes: Vec<Bytes>) -> Vec<BlockHash> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use rayls_execution_evm::{test_utils::TransactionFactory, RethChainSpec};
    use rayls_infrastructure_types::{
        max_batch_gas, test_genesis, Address, Batch, Bytes, Encodable2718 as _, FromHex,
        GenesisAccount, TaskManager, B256, ETHEREUM_BLOCK_GAS_LIMIT_56BITS, MIN_PROTOCOL_BASE_FEE,
        U256,
    };
    use serial_test::serial;
    use std::{path::Path, str::FromStr, sync::Arc};
    use tempfile::TempDir;

    // Pinned-mapping tests for committee-slot dispatch. If rustc-hash changes
    // its algorithm, these constants mismatch.

    const FIXED_TX_BYTES: [u8; 16] = [
        0x02, 0xf8, 0x6c, 0x80, 0x80, 0x84, 0x77, 0x35, 0x94, 0x00, 0x82, 0x52, 0x08, 0x94, 0xff,
        0xff,
    ];

    #[test]
    fn legacy_slot_digest_pins_to_known_value() {
        let expected = u64::from_le_bytes([0x02, 0xf8, 0x6c, 0x80, 0x80, 0x84, 0x77, 0x35]);
        assert_eq!(legacy_slot_digest(&FIXED_TX_BYTES), expected);
    }

    #[test]
    fn fxhash_slot_digest_pins_to_known_value() {
        // If this fails, rustc-hash changed its algorithm.
        let actual = fxhash_slot_digest(&FIXED_TX_BYTES);
        assert_eq!(actual, 6_289_104_099_094_390_010_u64);
    }

    #[test]
    fn slot_digests_are_deterministic_across_calls() {
        for _ in 0..16 {
            assert_eq!(legacy_slot_digest(&FIXED_TX_BYTES), legacy_slot_digest(&FIXED_TX_BYTES));
            assert_eq!(fxhash_slot_digest(&FIXED_TX_BYTES), fxhash_slot_digest(&FIXED_TX_BYTES));
        }
    }

    #[test]
    fn fxhash_slot_digest_is_deterministic_on_empty_input() {
        assert_eq!(fxhash_slot_digest(&[]), fxhash_slot_digest(&[]));
    }

    #[test]
    fn fxhash_slot_digest_handles_single_byte() {
        let a = fxhash_slot_digest(&[0x42]);
        let b = fxhash_slot_digest(&[0x42]);
        assert_eq!(a, b);
        assert_ne!(a, fxhash_slot_digest(&[]));
    }

    #[test]
    fn fxhash_and_legacy_diverge_on_exact_8_bytes() {
        // legacy reads the bytes as little-endian u64, fxhash hashes the full slice.
        let eight: [u8; 8] = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        assert_ne!(legacy_slot_digest(&eight), fxhash_slot_digest(&eight));
    }

    #[serial]
    #[tokio::test]
    async fn slot_digest_falls_back_to_legacy_without_fork_schedule() {
        // default test helper does not apply rayls_hardforks; the fork is
        // inactive and the gate returns the legacy prefix-u64 digest.
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { validator, .. } = test_tools(tmp_dir.path(), &task_manager).await;

        let digest = validator.slot_digest(&FIXED_TX_BYTES);
        assert_eq!(digest, legacy_slot_digest(&FIXED_TX_BYTES));
        assert_ne!(digest, fxhash_slot_digest(&FIXED_TX_BYTES));
    }

    #[serial]
    #[tokio::test]
    async fn slot_digest_uses_fxhash_when_local_schedule_active() {
        use rayls_execution_evm::RaylsChainSpec;
        use rayls_infrastructure_types::RaylsNetwork;

        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let rayls_chain_spec = Arc::new(
            RaylsChainSpec::builder(chain.clone()).rayls_hardforks(RaylsNetwork::Local).build(),
        );
        let reth_env = RethEnv::new_for_temp_chain_with_rayls_spec(
            chain,
            rayls_chain_spec,
            tmp_dir.path(),
            &task_manager,
            None,
        )
        .await
        .unwrap();
        let tx_pool = reth_env.init_txn_pool().unwrap();
        let validator = BatchValidator::new(
            reth_env,
            Some(tx_pool),
            0,
            BaseFeeContainer::default(),
            0,
            ETHEREUM_BLOCK_GAS_LIMIT_56BITS,
        );

        let digest = validator.slot_digest(&FIXED_TX_BYTES);
        assert_eq!(digest, fxhash_slot_digest(&FIXED_TX_BYTES));
        assert_ne!(digest, legacy_slot_digest(&FIXED_TX_BYTES));
    }

    #[serial]
    #[tokio::test]
    async fn submit_batch_if_mine_rejects_short_first_tx() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { validator, .. } = test_tools(tmp_dir.path(), &task_manager).await;

        let txs = vec![Bytes::from(vec![0u8; 4])];
        assert_matches!(
            validator.submit_batch_if_mine(&txs, &CommitteeSlots::all_live(4, 0)),
            Err(SubmitBatchError::InvalidTransactionBytes)
        );
    }

    #[serial]
    #[tokio::test]
    async fn submit_batch_if_mine_skips_on_committee_slot_mismatch() {
        // Default test setup has the fork inactive, so the active algorithm is legacy.
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { validator, .. } = test_tools(tmp_dir.path(), &task_manager).await;

        let committee_size = 4_u64;
        let matching_slot = legacy_slot_digest(&FIXED_TX_BYTES) % committee_size;
        let mismatching_slot = (matching_slot + 1) % committee_size;
        let txs = vec![Bytes::from(FIXED_TX_BYTES.to_vec())];
        assert_matches!(
            validator.submit_batch_if_mine(
                &txs,
                &CommitteeSlots::all_live(committee_size as usize, mismatching_slot)
            ),
            Ok(())
        );
    }

    #[serial]
    #[tokio::test]
    async fn submit_batch_if_mine_accepts_on_committee_slot_match() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { validator, .. } = test_tools(tmp_dir.path(), &task_manager).await;

        let committee_size = 4_u64;
        let matching_slot = legacy_slot_digest(&FIXED_TX_BYTES) % committee_size;
        let txs = vec![Bytes::from(FIXED_TX_BYTES.to_vec())];
        assert_matches!(
            validator.submit_batch_if_mine(
                &txs,
                &CommitteeSlots::all_live(committee_size as usize, matching_slot)
            ),
            Ok(())
        );
    }

    #[serial]
    #[tokio::test]
    async fn submit_batch_if_mine_handles_empty_batch() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { validator, .. } = test_tools(tmp_dir.path(), &task_manager).await;

        let txs: Vec<Bytes> = Vec::new();
        assert_matches!(
            validator.submit_batch_if_mine(&txs, &CommitteeSlots::all_live(4, 0)),
            Ok(())
        );
    }

    #[serial]
    #[tokio::test]
    async fn submit_batch_if_mine_is_noop_without_tx_pool() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let reth_env =
            RethEnv::new_for_temp_chain(chain, tmp_dir.path(), &task_manager, None).await.unwrap();
        let validator = BatchValidator::new(
            reth_env,
            None,
            0,
            BaseFeeContainer::default(),
            0,
            ETHEREUM_BLOCK_GAS_LIMIT_56BITS,
        );

        // tx_pool=None short-circuits before any slot computation, so even a too-short tx
        // is silently ignored.
        let txs = vec![Bytes::from(vec![0u8; 4])];
        assert_matches!(
            validator.submit_batch_if_mine(&txs, &CommitteeSlots::all_live(4, 0)),
            Ok(())
        );
    }

    /// Return the next valid sealed batch
    fn next_valid_sealed_batch(chain: Arc<RethChainSpec>) -> SealedBatch {
        // create valid transactions
        let mut tx_factory = TransactionFactory::new();
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");
        let gas_price = 7;

        // create 3 transactions
        let transaction1 = tx_factory.create_eip1559_encoded(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 RLS
            Bytes::new(),
        );

        let transaction2 = tx_factory.create_eip1559_encoded(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 RLS
            Bytes::new(),
        );

        let transaction3 = tx_factory.create_eip1559_encoded(
            chain,
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 RLS
            Bytes::new(),
        );

        let valid_txs = vec![transaction1, transaction2, transaction3];
        let batch = Batch {
            transactions: valid_txs,
            epoch: 0,
            beneficiary: Address::ZERO,
            base_fee_per_gas: MIN_PROTOCOL_BASE_FEE,
            worker_id: 0,
            seq: 0,
            received_at: None,
        };

        batch.seal_slow()
    }

    /// Convenience type for creating test assets.
    struct TestTools {
        /// The expected sealed batch.
        valid_batch: SealedBatch,
        /// Validator
        validator: BatchValidator,
    }

    /// Create an instance of block validator for tests.
    async fn test_tools(path: &Path, task_manager: &TaskManager) -> TestTools {
        // genesis with default TransactionFactory funded
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), path, task_manager, None).await.unwrap();
        let tx_pool = reth_env.init_txn_pool().unwrap();
        let validator = BatchValidator::new(
            reth_env,
            Some(tx_pool),
            0,
            BaseFeeContainer::default(),
            0,
            ETHEREUM_BLOCK_GAS_LIMIT_56BITS,
        );
        let valid_batch = next_valid_sealed_batch(chain);

        // block validator
        TestTools { valid_batch, validator }
    }

    #[serial]
    #[tokio::test]
    async fn dedup_cache_sweep_keeps_only_the_last_minute() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { valid_batch, validator } = test_tools(tmp_dir.path(), &task_manager).await;

        // Seed an entry two minutes old; the sweep promises to keep only the last minute.
        let stale = FixedBytes::<32>::random();
        validator.validated_batches.insert(stale, rayls_infrastructure_types::now() - 120);

        validator.validate_batch(&valid_batch).await.unwrap();

        assert!(
            !validator.validated_batches.contains_key(&stale),
            "two-minute-old dedup entry survived the keep-last-minute sweep"
        );
    }

    #[serial]
    #[tokio::test]
    async fn test_valid_batch() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { valid_batch, validator } = test_tools(tmp_dir.path(), &task_manager).await;
        let result = validator.validate_batch(&valid_batch.clone()).await;
        assert!(result.is_ok());

        // ensure non-serialized data does not affect validity
        let (mut batch, _) = valid_batch.split();
        batch.received_at = Some(rayls_infrastructure_types::now());
        let different_block = batch.seal_slow();
        let result = validator.validate_batch(&different_block).await;
        assert!(result.is_ok());
    }

    //#[tokio::test]
    // This is not checked currently, leaving test for bit to make sure we want this.
    // This check will lead to occasional false errors and should not be critical since
    // we should be validating parentage when building actual blocks (including any
    // needed waits for execution).
    async fn _test_invalid_batch_wrong_parent_hash() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { valid_batch, validator } = test_tools(tmp_dir.path(), &task_manager).await;
        let (batch, _) = valid_batch.split();
        let Batch { transactions, beneficiary, base_fee_per_gas, received_at, .. } = batch;
        let wrong_parent_hash = B256::random();
        let invalid_batch = Batch {
            transactions,
            beneficiary,
            epoch: 0,
            base_fee_per_gas,
            worker_id: 0,
            seq: 0,
            received_at,
        };
        assert_matches!(
            validator.validate_batch(&invalid_batch.seal_slow()).await,
            Err(BatchValidationError::CanonicalChain { block_hash }) if block_hash == wrong_parent_hash
        );
    }

    #[serial]
    #[tokio::test]
    async fn test_invalid_batch_wrong_epoch() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { valid_batch, validator } = test_tools(tmp_dir.path(), &task_manager).await;
        let (mut batch, _) = valid_batch.split();

        batch.epoch += 1;

        assert_matches!(
        validator.validate_batch(&batch.clone().seal_slow()).await,
        Err(BatchValidationError::InvalidEpoch{expected, found}) if expected == 0 && found == 1
        );
    }

    #[serial]
    #[tokio::test]
    async fn test_invalid_batch_excess_gas_used() {
        // Set excessive gas limit.
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { valid_batch, validator } = test_tools(tmp_dir.path(), &task_manager).await;
        let (batch, _) = valid_batch.split();

        // sign excessive transaction
        let mut tx_factory = TransactionFactory::new();
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");
        let gas_price = 7;
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());

        // create transaction with max gas limit above the max allowed
        let invalid_transaction = tx_factory.create_eip1559_encoded(
            chain.clone(),
            Some(max_batch_gas(batch.epoch) + 1),
            gas_price,
            Some(Address::ZERO),
            value, // 1 RLS
            Bytes::new(),
        );

        let Batch { beneficiary, epoch, base_fee_per_gas, received_at, .. } = batch;
        let invalid_batch = Batch {
            transactions: vec![invalid_transaction],
            epoch,
            beneficiary,
            base_fee_per_gas,
            worker_id: 0,
            seq: 0,
            received_at,
        };

        let decoded_txs = validator
            .decode_transactions(invalid_batch.transactions(), invalid_batch.digest())
            .expect("txs decode correctly");

        assert_matches!(
            validator.validate_batch_gas(&decoded_txs),
            Err(BatchValidationError::HeaderMaxGasExceedsGasLimit {
                total_possible_gas: _,
                gas_limit: _
            })
        );
    }

    #[serial]
    #[tokio::test]
    async fn test_invalid_batch_gas_overflow() {
        // Set excessive gas limit.
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { valid_batch, validator } = test_tools(tmp_dir.path(), &task_manager).await;
        let (batch, _) = valid_batch.split();

        // sign excessive transaction
        let mut tx_factory = TransactionFactory::new();
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");
        let gas_price = 7;
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());

        // create transaction with max gas limit above the max allowed
        let u64_max_transaction = tx_factory.create_eip1559_encoded(
            chain.clone(),
            Some(u64::MAX),
            gas_price,
            Some(Address::ZERO),
            value, // 1 RLS
            Bytes::new(),
        );

        let overflow_transaction = tx_factory.create_eip1559_encoded(
            chain.clone(),
            Some(1_000),
            gas_price,
            Some(Address::ZERO),
            value, // 1 RLS
            Bytes::new(),
        );

        let Batch { beneficiary, epoch, base_fee_per_gas, received_at, .. } = batch;
        let invalid_batch = Batch {
            transactions: vec![u64_max_transaction, overflow_transaction],
            beneficiary,
            epoch,
            base_fee_per_gas,
            worker_id: 0,
            seq: 0,
            received_at,
        };

        let decoded_txs = validator
            .decode_transactions(invalid_batch.transactions(), invalid_batch.digest())
            .expect("txs decode correctly");

        assert_matches!(
            validator.validate_batch_gas(&decoded_txs),
            Err(BatchValidationError::GasOverflow)
        );
    }

    #[serial]
    #[tokio::test]
    async fn test_invalid_batch_wrong_size_in_bytes() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { valid_batch, validator } = test_tools(tmp_dir.path(), &task_manager).await;
        // create enough transactions to exceed 1MB
        // because validator uses provided with same genesis
        // and tx_factory needs funds
        let genesis = test_genesis();

        // use new tx factory to ensure correct nonces are tracked
        let mut tx_factory = TransactionFactory::new();
        let factory_address = tx_factory.address();

        // fund factory with 99mil RLS
        let account = vec![(
            factory_address,
            GenesisAccount::default().with_balance(
                U256::from_str("0x51E410C0F93FE543000000").expect("account balance is parsed"),
            ),
        )];

        let genesis = genesis.extend_accounts(account);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        // currently: 19424 txs
        let mut too_many_txs = Vec::new();
        let mut total_bytes = 0;
        while total_bytes < max_batch_size(0) {
            let tx = tx_factory
                .create_explicit_eip1559(
                    Some(chain.chain.id()),
                    None,                    // default nonce
                    None,                    // no tip
                    Some(7),                 // min basefee for block 1
                    Some(1),                 // low gas limit to prevent excess gas used error
                    Some(Address::random()), // send to random address
                    Some(U256::from(100)),   // send low amount
                    None,                    // no input
                    None,                    // no access list
                )
                .encoded_2718();

            // track totals
            total_bytes += tx.len();
            too_many_txs.push(tx.into());
        }

        // NOTE: these assertions aren't important but want to know if tx size changes
        assert_eq!(too_many_txs.len(), 19424);

        // update header so tx root is correct
        let (mut block, _hash) = valid_batch.split();
        block.transactions = too_many_txs;
        let invalid_batch = block.clone().seal_slow();

        assert_matches!(
            validator.validate_batch(&invalid_batch).await,
            Err(BatchValidationError::HeaderTransactionBytesExceedsMax(wrong)) if wrong == total_bytes
        );

        // Generate 2MB vec of 1s - total bytes are: 1_000_213
        let big_input = vec![1u8; 2_000_000];

        // create giant tx
        let max_gas = max_batch_gas(0);
        let giant_tx = tx_factory.create_explicit_eip1559(
            Some(chain.chain.id()),
            Some(0),                      // make this first tx in block 1
            None,                         // no tip
            Some(7),                      // min basefee for block 1
            Some(max_gas),                // high gas limit bc this is a lot of data
            None,                         // create tx
            Some(U256::ZERO),             // no transfer
            Some(Bytes::from(big_input)), // no input
            None,                         // no access list
        );

        // NOTE: the actual size just needs to be above 1MB but want to know if tx size ever changes
        let too_big = giant_tx.encoded_2718();
        let expected_len = too_big.len();
        assert_eq!(expected_len, 2_000_090);

        let invalid_txs = vec![too_big.into()];
        block.transactions = invalid_txs;
        // ensure size method correctly accounts for struct+txs
        assert_eq!(block.size(), 2_000_178);
        let invalid_batch = block.seal_slow();
        // ensure size method correct accounts for struct+txs+digest
        assert_eq!(invalid_batch.size(), 2_000_210);
        assert_matches!(
            validator.validate_batch(&invalid_batch).await,
            Err(BatchValidationError::HeaderTransactionBytesExceedsMax(wrong)) if wrong == expected_len
        );
    }

    #[serial]
    #[tokio::test]
    async fn test_invalid_batch_empty_transactions() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { valid_batch, validator } = test_tools(tmp_dir.path(), &task_manager).await;
        let (mut batch, _) = valid_batch.split();

        // test batch with no transactions
        batch.transactions = Vec::new();
        assert_matches!(
            validator.validate_batch(&batch.clone().seal_slow()).await,
            Err(BatchValidationError::EmptyBatch)
        );
    }

    #[serial]
    #[tokio::test]
    async fn test_invalid_batch_decode_transactions() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { valid_batch, validator } = test_tools(tmp_dir.path(), &task_manager).await;
        let (mut batch, _) = valid_batch.split();

        // test batch with bad decode
        batch.transactions = vec![b"this is a bad batch".to_vec().into()];

        assert_matches!(
            validator.validate_batch(&batch.clone().seal_slow()).await,
            Err(BatchValidationError::RecoverTransaction(_, _))
        );
    }

    #[serial]
    #[tokio::test]
    async fn test_invalid_batch_base_fee_for_gas() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { valid_batch, validator } = test_tools(tmp_dir.path(), &task_manager).await;
        // Note validator will use MIN_PROTOCOL_BASE_FEE.
        let (mut batch, _) = valid_batch.split();

        assert_matches!(validator.validate_batch(&batch.clone().seal_slow()).await, Ok(()));

        batch.base_fee_per_gas = 0;
        assert_matches!(
            validator.validate_batch(&batch.clone().seal_slow()).await,
            Err(BatchValidationError::InvalidBaseFee { expected_base_fee: _, base_fee: _ })
        );

        let badfee = MIN_PROTOCOL_BASE_FEE * 100;
        batch.base_fee_per_gas = badfee;
        assert_matches!(
            validator.validate_batch(&batch.clone().seal_slow()).await,
            Err(BatchValidationError::InvalidBaseFee { expected_base_fee: _, base_fee: _ })
        );
    }

    #[serial]
    #[tokio::test]
    async fn test_invalid_tx_eip4844() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { valid_batch, validator } = test_tools(tmp_dir.path(), &task_manager).await;
        let (mut batch, _) = valid_batch.split();

        // eip4844 transaction
        let mut tx_factory = TransactionFactory::new_random();
        // known versioned hash for zero blob `c00...000`
        let blob_versioned_hash = vec![B256::from_hex(
            "010657f37554c781402a22917dee2f75def7ab966d7b770905398eba3c444014",
        )
        .expect("known versioned hash is valid")];

        // create signed tx
        let signed_tx = tx_factory.create_eip4844(
            validator.reth_env.chainspec().chain_id(),
            None,
            7,
            blob_versioned_hash,
        );

        // test batch with eip4844 tx
        batch.transactions = vec![signed_tx.encoded_2718().into()];

        assert_matches!(
            validator.validate_batch(&batch.clone().seal_slow()).await,
            Err(BatchValidationError::InvalidTx4844(_))
        );
    }

    #[serial]
    #[tokio::test]
    async fn sender_affinity_routes_a_sender_nonce_chain_to_one_slot() {
        use rayls_execution_evm::RaylsChainSpec;
        use rayls_infrastructure_types::RaylsNetwork;

        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        // Local activates SenderAffinityLoadBalancing at block 0.
        let rayls_chain_spec = Arc::new(
            RaylsChainSpec::builder(chain.clone()).rayls_hardforks(RaylsNetwork::Local).build(),
        );
        let reth_env = RethEnv::new_for_temp_chain_with_rayls_spec(
            chain.clone(),
            rayls_chain_spec,
            tmp_dir.path(),
            &task_manager,
            None,
        )
        .await
        .unwrap();
        let gas_price = reth_env.get_gas_price().unwrap();
        let validator = BatchValidator::new(
            reth_env,
            None,
            0,
            BaseFeeContainer::default(),
            0,
            ETHEREUM_BLOCK_GAS_LIMIT_56BITS,
        );

        // two transactions from one sender at consecutive nonces
        let mut factory = TransactionFactory::new();
        let value = U256::from(1_000_000_000u64);
        let tx0 = factory
            .create_eip1559(
                chain.clone(),
                None,
                gas_price,
                Some(Address::ZERO),
                value,
                Bytes::new(),
            )
            .encoded_2718();
        let tx1 = factory
            .create_eip1559(
                chain.clone(),
                None,
                gas_price,
                Some(Address::ZERO),
                value,
                Bytes::new(),
            )
            .encoded_2718();

        // the two encodings genuinely differ (different nonces)...
        assert_ne!(fxhash_slot_digest(&tx0), fxhash_slot_digest(&tx1));
        // ...yet sender-affinity keys the slot on the shared sender, so both route to one owner
        assert_eq!(validator.slot_digest(&tx0), validator.slot_digest(&tx1));

        // and that slot is exactly the fxhash of the sender address, not of the tx bytes
        let sender = recover_pooled_transaction(&tx0).unwrap().sender();
        assert_eq!(validator.slot_digest(&tx0), fxhash_slot_digest(sender.as_slice()));
    }

    #[serial]
    #[tokio::test]
    async fn submit_forwarded_txns_reports_only_nonce_too_low_as_stale() {
        use rayls_infrastructure_types::{GenesisAccount, U256};

        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let mut factory = TransactionFactory::new();
        // Seed the sender at nonce 1, so its nonce-0 transaction is already executed (stale) while
        // its nonce-1 transaction is the next valid one.
        let genesis = test_genesis().extend_accounts([(
            factory.address(),
            GenesisAccount::default().with_balance(U256::MAX).with_nonce(Some(1)),
        )]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .await
                .unwrap();
        let tx_pool = reth_env.init_txn_pool().unwrap();
        let gas_price = reth_env.get_gas_price().unwrap();
        let validator = BatchValidator::new(
            reth_env,
            Some(tx_pool),
            0,
            BaseFeeContainer::default(),
            0,
            ETHEREUM_BLOCK_GAS_LIMIT_56BITS,
        );

        let value = U256::from(1);
        factory.set_nonce(0);
        let stale_tx = factory.create_eip1559(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value,
            Bytes::new(),
        );
        factory.set_nonce(1);
        let ok_tx = factory.create_eip1559(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value,
            Bytes::new(),
        );

        let payloads =
            vec![Bytes::from(stale_tx.encoded_2718()), Bytes::from(ok_tx.encoded_2718())];
        let stale = validator.submit_forwarded_txns(payloads).await;

        assert_eq!(stale, vec![*stale_tx.hash()], "only the nonce-too-low tx is reported stale");
    }
}
