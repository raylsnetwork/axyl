//! Block validator

use rayls_execution_evm::{
    bytes_to_txn, chainspec::RaylsHardforks, recover_signed_transaction, reth_env::RethEnv,
    EthPooledTransaction, FixedBytes, PoolErrorKind, WorkerTxPool,
};
use rayls_infrastructure_types::{
    gas_accumulator::BaseFeeContainer, max_batch_size, BatchValidation, BatchValidationError,
    BlockHash, Epoch, SealedBatch, SubmitBatchError, TransactionSigned, TransactionTrait as _,
    WorkerId,
};
use rayon::iter::{IntoParallelRefIterator as _, ParallelIterator as _};

use dashmap::DashMap;
use rustc_hash::FxHasher;
use std::hash::Hasher;
use tracing::{debug, trace, warn};

/// Type convenience for implementing block validation errors.
type BatchValidationResult<T> = Result<T, BatchValidationError>;

/// Pre-`TransactionLoadBalancing` slot digest: read the first 8 bytes as little-endian u64.
/// Caller must ensure `tx.len() >= 8`.
fn legacy_slot_digest(tx: &[u8]) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&tx[0..8]);
    u64::from_le_bytes(bytes)
}

/// Post-`TransactionLoadBalancing` slot digest: `FxHasher` over the full transaction bytes.
/// Do NOT replace with `FxBuildHasher::hash_one(tx)`: that path writes a slice-length
/// prefix and changes the digest.
fn fxhash_slot_digest(tx: &[u8]) -> u64 {
    let mut hasher = FxHasher::default();
    hasher.write(tx);
    hasher.finish()
}

/// Batch validator
/// Important note about batch validation, we rely on libp2p to verify that
/// batches came from a committee member.  This means we do not generate or
/// check our own signatures for batches since they all came from current
/// committee members.
#[derive(Clone, Debug)]
pub struct BatchValidator {
    /// Database provider to encompass tree and provider factory.
    reth_env: RethEnv,
    /// A handle to the transaction pool for submitting gossipped transactions.
    tx_pool: Option<WorkerTxPool>,
    /// Worker id for this validator.
    worker_id: WorkerId,
    /// Current base fee for this validators worker.
    base_fee: BaseFeeContainer,
    /// Epoch we are validating for.
    epoch: Epoch,
    /// holds recently validated batches to prevent re-validation
    validated_batches: DashMap<FixedBytes<32>, u64>,
    /// Block gas limit.
    gas_limit: u64,
}

#[async_trait::async_trait]
impl BatchValidation for BatchValidator {
    /// Validate a peer's batch.
    ///
    /// Workers do not execute full batches. This method validates the required information.
    async fn validate_batch(&self, sealed_batch: SealedBatch) -> Result<(), BatchValidationError> {
        // ensure digest matches batch
        let (batch, digest) = sealed_batch.split();

        let verified_hash = batch.clone().seal_slow().digest();
        if digest != verified_hash {
            return Err(BatchValidationError::InvalidDigest);
        }
        if self.validated_batches.contains_key(&digest) {
            // already validated recently
            return Ok(());
        }

        // A validator belongs to a worker and that worker only handles batches with it's id.
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

        // validate batch size (bytes)
        // Use the parent timestamp for consistency with the batch builder.
        self.validate_batch_size_bytes(transactions, batch.epoch)?;

        // validate txs decode
        let decoded_txs = self.decode_transactions(transactions, digest)?;

        // validate no txs are eip4844
        self.validate_no_blob_txs(&decoded_txs)?;

        // validate gas limit
        // Use the parent timestamp for consistency with the batch builder.
        self.validate_batch_gas(&decoded_txs)?;

        // validate base fee- all batches for a worker and epoch have the same base fee.
        self.validate_basefee(batch.base_fee_per_gas)?;

        self.validated_batches.retain(|_, v| *v > rayls_infrastructure_types::now() - 60_000); // keep last minute
        self.validated_batches.insert(digest, rayls_infrastructure_types::now());

        Ok(())
    }

    /// Submit a transaction received from the gossip pool to the worker's transaction pool.
    /// This method is only active if the node is part of the committee.
    fn submit_batch_if_mine(
        &self,
        txs_bytes: &[Vec<u8>],
        committee_size: u64,
        committee_slot: u64,
    ) -> Result<(), SubmitBatchError> {
        if let Some(tx_pool) = &self.tx_pool {
            // loop to check if the batch is for this validator because some txns may be errors
            if let Some(tx) = txs_bytes.iter().next() {
                if tx.len() < 8 {
                    return Err(SubmitBatchError::InvalidTransactionBytes);
                }
                let digest = self.slot_digest(tx);
                if (digest % committee_size) != committee_slot {
                    return Ok(());
                }
                trace!(target: "worker::validator", ?digest, "tx accepted as committee owner");
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
                for tx in parsed_txns.into_iter().flatten() {
                    match tx_pool.add_raw_transaction_external(tx).await {
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
}

impl BatchValidator {
    /// Create a new instance of [Self]
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
        transactions: &[Vec<u8>],
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
        transactions: &Vec<Vec<u8>>,
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

    /// Compute the committee-slot dispatch digest for a single transaction.
    /// Branches on the `TransactionLoadBalancing` hardfork at the next block: pre-fork
    /// uses [`legacy_slot_digest`], post-fork uses [`fxhash_slot_digest`]. Caller must
    /// ensure `tx.len() >= 8`.
    ///
    /// Gate reads the local canonical tip, so validators can briefly disagree on the
    /// algorithm across the fork block. Worst case is more than one validator includes
    /// the tx in a batch; the duplicate executions then fail with `nonce too low`. No
    /// consensus fork.
    fn slot_digest(&self, tx: &[u8]) -> u64 {
        let chain_spec = self.reth_env.rayls_chain_spec();
        let next_block = self.reth_env.canonical_tip().number + 1;
        if chain_spec.is_transaction_load_balancing_active_at_block(next_block) {
            fxhash_slot_digest(tx)
        } else {
            legacy_slot_digest(tx)
        }
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
            // per-block EIP-1559 active - skip exact match
            return Ok(());
        }
        let expected_base_fee = self.base_fee.base_fee();
        if base_fee != expected_base_fee {
            Err(BatchValidationError::InvalidBaseFee { expected_base_fee, base_fee })
        } else {
            Ok(())
        }
    }

    /// Validate the block's basefee
    fn validate_no_blob_txs(
        &self,
        transactions: &[TransactionSigned],
    ) -> BatchValidationResult<()> {
        if let Some(blob_tx) = transactions.iter().find(|tx| tx.is_eip4844()) {
            return Err(BatchValidationError::InvalidTx4844(*blob_tx.hash()));
        }
        Ok(())
    }

    /// Helper function for decoding and recovering transactions.
    fn recover_and_validate(
        tx: &[u8],
        digest: BlockHash,
    ) -> BatchValidationResult<TransactionSigned> {
        recover_signed_transaction(tx)
            .map_err(|e| BatchValidationError::RecoverTransaction(digest, e.to_string()))
    }
}

/// Noop validation struct that validates any block.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Default, Clone, Debug)]
pub struct NoopBatchValidator;

#[cfg(any(test, feature = "test-utils"))]
#[async_trait::async_trait]
impl BatchValidation for NoopBatchValidator {
    async fn validate_batch(&self, _batch: SealedBatch) -> Result<(), BatchValidationError> {
        Ok(())
    }

    fn submit_batch_if_mine(
        &self,
        _tx_bytes: &[Vec<u8>],
        _committee_size: u64,
        _committee_slot: u64,
    ) -> Result<(), SubmitBatchError> {
        Ok(())
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

        let txs = vec![vec![0u8; 4]];
        assert_matches!(
            validator.submit_batch_if_mine(&txs, 4, 0),
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
        let txs = vec![FIXED_TX_BYTES.to_vec()];
        assert_matches!(
            validator.submit_batch_if_mine(&txs, committee_size, mismatching_slot),
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
        let txs = vec![FIXED_TX_BYTES.to_vec()];
        assert_matches!(
            validator.submit_batch_if_mine(&txs, committee_size, matching_slot),
            Ok(())
        );
    }

    #[serial]
    #[tokio::test]
    async fn submit_batch_if_mine_handles_empty_batch() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { validator, .. } = test_tools(tmp_dir.path(), &task_manager).await;

        let txs: Vec<Vec<u8>> = Vec::new();
        assert_matches!(validator.submit_batch_if_mine(&txs, 4, 0), Ok(()));
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
        let txs = vec![vec![0u8; 4]];
        assert_matches!(validator.submit_batch_if_mine(&txs, 4, 0), Ok(()));
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
    async fn test_valid_batch() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { valid_batch, validator } = test_tools(tmp_dir.path(), &task_manager).await;
        let result = validator.validate_batch(valid_batch.clone()).await;
        assert!(result.is_ok());

        // ensure non-serialized data does not affect validity
        let (mut batch, _) = valid_batch.split();
        batch.received_at = Some(rayls_infrastructure_types::now());
        let different_block = batch.seal_slow();
        let result = validator.validate_batch(different_block).await;
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
            validator.validate_batch(invalid_batch.seal_slow()).await,
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
        validator.validate_batch(batch.clone().seal_slow()).await,
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
            too_many_txs.push(tx);
        }

        // NOTE: these assertions aren't important but want to know if tx size changes
        assert_eq!(too_many_txs.len(), 19424);

        // update header so tx root is correct
        let (mut block, _hash) = valid_batch.split();
        block.transactions = too_many_txs;
        let invalid_batch = block.clone().seal_slow();

        assert_matches!(
            validator.validate_batch(invalid_batch).await,
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

        let invalid_txs = vec![too_big];
        block.transactions = invalid_txs;
        // ensure size method correctly accounts for struct+txs
        assert_eq!(block.size(), 2_000_178);
        let invalid_batch = block.seal_slow();
        // ensure size method correct accounts for struct+txs+digest
        assert_eq!(invalid_batch.size(), 2_000_210);
        assert_matches!(
            validator.validate_batch(invalid_batch).await,
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
            validator.validate_batch(batch.clone().seal_slow()).await,
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
        batch.transactions = vec![b"this is a bad batch".to_vec()];

        assert_matches!(
            validator.validate_batch(batch.clone().seal_slow()).await,
            Err(BatchValidationError::RecoverTransaction(_, _))
        );
    }

    #[serial]
    #[tokio::test]
    async fn test_invalid_batch_base_fee_for_gas() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { valid_batch, validator } = test_tools(tmp_dir.path(), &task_manager).await;
        // Note validator will use MIN_PROROCOL_BASE_FEE.
        let (mut batch, _) = valid_batch.split();

        assert_matches!(validator.validate_batch(batch.clone().seal_slow()).await, Ok(()));

        batch.base_fee_per_gas = 0;
        assert_matches!(
            validator.validate_batch(batch.clone().seal_slow()).await,
            Err(BatchValidationError::InvalidBaseFee { expected_base_fee: _, base_fee: _ })
        );

        let badfee = MIN_PROTOCOL_BASE_FEE * 100;
        batch.base_fee_per_gas = badfee;
        assert_matches!(
            validator.validate_batch(batch.clone().seal_slow()).await,
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
        batch.transactions = vec![signed_tx.encoded_2718()];

        assert_matches!(
            validator.validate_batch(batch.clone().seal_slow()).await,
            Err(BatchValidationError::InvalidTx4844(_))
        );
    }
}
