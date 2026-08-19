//! Worker-side wrapper around the reth transaction pool.
//!
//! Keeps reth pool internals out of the batch builder and RPC paths, and owns the pieces reth
//! does not: the in-flight mark tracker and the shutdown backup in `backup`.

use eyre::Error;
use futures::StreamExt as _;
use rayls_infrastructure_types::{
    Address, B256Set, EnvKzgSettings, Recovered, TaskSpawner, TransactionSigned, TxHash,
    MIN_PROTOCOL_BASE_FEE,
};
use reth::transaction_pool::{
    blobstore::DiskFileBlobStore, BlockInfo as RethBlockInfo, TransactionValidationTaskExecutor,
};
use reth_chainspec::ChainSpec;
use reth_node_builder::{NodeConfig, RethTransactionPoolConfig};
use reth_primitives_traits::SignedTransaction;
use reth_provider::{
    providers::BlockchainProvider, CanonStateSubscriptions as _, ChainSpecProvider,
};
use reth_rpc_eth_types::utils::recover_raw_transaction as reth_recover_raw_transaction;
use reth_transaction_pool::{
    error::{Eip4844PoolTransactionError, InvalidPoolTransactionError, PoolError},
    identifier::TransactionId,
    maintain::{maintain_transaction_pool_future, MaintainPoolConfig},
    AddedTransactionOutcome, BestTransactions, CoinbaseTipOrdering, EthPooledTransaction, Pool,
    PoolSize, PoolTransaction, TransactionEvents, TransactionListenerKind, TransactionOrigin,
    TransactionPool as _, TransactionPoolExt as _, ValidPoolTransaction,
};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::{debug, info};

use crate::{error::RaylsRethResult, in_flight::InFlightTracker, traits::RaylsNode};

mod backup;

/// A pooled transaction id.
pub type PoolTxnId = TransactionId;
/// A pooled transaction.
pub type PoolTxn = ValidPoolTransaction<EthPooledTransaction>;
/// A recovered pooled transaction.
pub type RecoveredPoolTxn = Recovered<EthPooledTransaction>;

pub use reth_primitives_traits::InMemorySize as TxnSize;

/// Builds a pooled transaction from an eth transaction and id.
pub fn new_pool_txn(transaction: EthPooledTransaction, transaction_id: PoolTxnId) -> PoolTxn {
    ValidPoolTransaction {
        transaction,
        transaction_id,
        propagate: false,
        timestamp: Instant::now(),
        origin: TransactionOrigin::External,
        authority_ids: None,
    }
}

/// Decodes EIP-2718 transaction bytes into a pooled transaction.
pub fn bytes_to_txn(tx_bytes: &[u8]) -> eyre::Result<EthPooledTransaction> {
    let transaction = decode_transaction::<TransactionSigned>(&tx_bytes)
        .map_err(|_| eyre::eyre!("failed to recover transaction"))?;
    let tx_hash = *transaction.hash();
    let pooled_tx = transaction
        .try_into_pooled()
        .map_err(|_| PoolError::other(tx_hash, "Not into pooled".to_string()))?;
    let recovered = pooled_tx
        .try_into_recovered()
        .map_err(|_| PoolError::other(tx_hash, "Failed to recover ec tx".to_string()))?;
    let eth_tx = EthPooledTransaction::from_pooled(recovered);

    Ok(eth_tx)
}

fn decode_transaction<T: SignedTransaction>(mut data: &[u8]) -> Result<T, Error> {
    if data.is_empty() {
        return Err(eyre::eyre!("empty transaction data"));
    }

    let transaction = T::decode_2718(&mut data)
        .map_err(|_| eyre::eyre!("failed to decode signed transaction"))?;

    Ok(transaction)
}

/// The pool surface the batch builder seals from.
pub trait TxPool {
    /// Returns an iterator over the best transactions in the pool.
    fn best_transactions(&self) -> BestTxns;
    /// Returns the pending base fee.
    fn get_pending_base_fee(&self) -> u64;
    /// Removes EIP-4844 blob transactions from the pool and deletes their sidecars from the blob
    /// store.
    fn remove_eip4844_txs(&mut self, blobs: Vec<TxHash>);
    /// Returns whether the hash is sealed into a batch still in flight, so the sealer skips it.
    fn is_in_flight(&self, tx_hash: &TxHash) -> bool;
}

/// The reth transaction validator used by this pool.
type EthValidator = reth_transaction_pool::EthTransactionValidator<
    BlockchainProvider<RaylsNode>,
    EthPooledTransaction,
    crate::evm::RaylsEvmConfig,
>;

/// The validator wrapped in reth's validation task executor.
type RaylsValidator = TransactionValidationTaskExecutor<EthValidator>;

/// The concrete pool type used by Rayls workers.
pub type RaylsTransactionPool =
    Pool<RaylsValidator, CoinbaseTipOrdering<EthPooledTransaction>, DiskFileBlobStore>;

/// A worker's transaction pool, kept canonical by reth's maintenance task.
#[derive(Clone, Debug)]
pub struct WorkerTxPool {
    /// The reth pool.
    pool: RaylsTransactionPool,
    /// Hashes sealed into a batch but not yet observed mined, so the sealer skips re-sealing
    /// them while their batch is in flight; shared with the batch builder via
    /// [`WorkerTxPool::in_flight`].
    in_flight_tracker: InFlightTracker,
    /// File the pending and queued transactions are snapshotted to on graceful shutdown and
    /// reloaded from on boot.
    backup_path: Arc<PathBuf>,
}

impl From<WorkerTxPool> for RaylsTransactionPool {
    fn from(value: WorkerTxPool) -> Self {
        value.pool
    }
}

impl WorkerTxPool {
    /// Builds the pool and spawns its node-scoped maintenance and in-flight release tasks.
    pub fn new(
        node_config: &NodeConfig<ChainSpec>,
        task_spawner: &TaskSpawner,
        blockchain_provider: &BlockchainProvider<RaylsNode>,
        in_flight: InFlightTracker,
    ) -> eyre::Result<Self> {
        let data_dir = node_config.datadir();
        let pool_config = node_config.txpool.pool_config();
        let blob_store = DiskFileBlobStore::open(data_dir.blobstore(), Default::default())?;
        let evm_config =
            crate::evm::RaylsEvmConfig::new(blockchain_provider.chain_spec(), Default::default());
        let task_executor =
            TransactionValidationTaskExecutor::eth_builder(blockchain_provider.clone(), evm_config)
                .kzg_settings(EnvKzgSettings::Default)
                .with_local_transactions_config(pool_config.local_transactions_config.clone())
                .with_additional_tasks(node_config.txpool.additional_validation_tasks)
                .with_max_tx_input_bytes(node_config.txpool.max_tx_input_bytes)
                .build_with_tasks(task_spawner.clone(), blob_store.clone());

        let transaction_pool =
            Pool::new(task_executor, CoinbaseTipOrdering::default(), blob_store, pool_config);

        info!(target: "rayls::execution", "Transaction pool initialized");

        let this = Self {
            pool: transaction_pool,
            in_flight_tracker: in_flight,
            backup_path: Arc::new(data_dir.txpool_transactions()),
        };

        // reth's maintenance future drains mined transactions, reloads changed accounts, and
        // updates the pending base fee on every commit. `no_local_exemptions` holds locally
        // submitted transactions to the same fee and eviction rules as external ones.
        let maintain_fut = maintain_transaction_pool_future(
            blockchain_provider.clone(),
            this.pool.clone(),
            blockchain_provider.canonical_state_stream(),
            task_spawner.clone(),
            MaintainPoolConfig {
                max_tx_lifetime: Duration::from_mins(5),
                no_local_exemptions: true,
                ..Default::default()
            },
        );
        task_spawner.spawn_critical_task("maintain txn pool", maintain_fut);

        // Release in-flight marks once maintenance drops the sealed transactions from the
        // pending sub-pool. A separate subscription: the maintenance future consumes its own
        // stream, and this one must see the same commits.
        let mut release_stream = blockchain_provider.canonical_state_stream();
        let release_pool = this.clone();
        task_spawner.spawn_critical_task("in-flight release", async move {
            while release_stream.next().await.is_some() {
                release_pool.reconcile_in_flight();
            }
        });
        Ok(this)
    }

    /// Returns the pending transactions.
    pub fn pending_transactions(&self) -> Vec<Arc<PoolTxn>> {
        self.pool.pending_transactions()
    }

    /// Returns the queued transactions (not yet executable).
    pub fn queued_transactions(&self) -> Vec<Arc<PoolTxn>> {
        self.pool.queued_transactions()
    }

    /// Subscribes to hashes as transactions enter the pending sub-pool, so the builder wakes
    /// promptly on new candidates instead of waiting for the next batching-window tick.
    pub fn pending_transactions_listener(&self) -> tokio::sync::mpsc::Receiver<TxHash> {
        self.pool.pending_transactions_listener_for(TransactionListenerKind::All)
    }

    /// Returns the in-flight tracker shared with this pool's batch builder.
    ///
    /// The builder marks a batch's hashes here on quorum so the next sealing round skips them
    /// while the batch is in flight; the pool releases them again via [`Self::reconcile_in_flight`]
    /// once execution removes them from the pending sub-pool.
    pub fn in_flight(&self) -> InFlightTracker {
        self.in_flight_tracker.clone()
    }

    /// Releases in-flight marks for hashes no longer in the pending sub-pool.
    ///
    /// A sealed transaction stays pending (and RPC-visible) until it executes; once execution
    /// drops it from pending, its mark is stale and released so a later resubmission of the same
    /// nonce is not skipped.
    pub fn reconcile_in_flight(&self) {
        if self.in_flight_tracker.is_empty() {
            return;
        }
        let pending: B256Set =
            self.pool.pending_transactions().iter().map(|tx| *tx.hash()).collect();
        let released = self.in_flight_tracker.release_mined(&pending);
        if released > 0 {
            debug!(target: "rayls::txpool", released, "released in-flight marks against pending sub-pool");
        }
    }

    /// Returns the pool's view of the canonical tip.
    pub fn block_info(&self) -> BlockInfo {
        self.pool.block_info()
    }

    /// Sets the pool's view of the canonical tip.
    pub fn set_block_info(&self, block_info: BlockInfo) {
        self.pool.set_block_info(block_info);
    }

    /// Returns the pooled transactions sent by `address`.
    pub fn get_transactions_by_sender(&self, address: Address) -> Vec<Arc<PoolTxn>> {
        self.pool.get_transactions_by_sender(address)
    }

    /// Adds a transaction with local origin.
    pub async fn add_transaction_local(
        &self,
        recovered: EthPooledTransaction,
    ) -> Result<AddedTransactionOutcome, crate::PoolError> {
        self.pool.add_transaction(TransactionOrigin::Local, recovered).await
    }

    /// Adds a transaction with external origin.
    pub async fn add_raw_transaction_external(
        &self,
        tx: EthPooledTransaction,
    ) -> Result<AddedTransactionOutcome, crate::PoolError> {
        self.pool.add_transaction(TransactionOrigin::External, tx).await
    }

    /// Adds a transaction with local origin and subscribes to its events.
    pub async fn add_transaction_and_subscribe_local(
        &self,
        recovered: EthPooledTransaction,
    ) -> Result<TransactionEvents, crate::EthApiError> {
        Ok(self.pool.add_transaction_and_subscribe(TransactionOrigin::Local, recovered).await?)
    }

    /// Returns the pooled transaction with this hash, if any.
    pub fn get(&self, tx: &TxHash) -> Option<Arc<PoolTxn>> {
        self.pool.get(tx)
    }

    /// Returns the pool size stats.
    pub fn pool_size(&self) -> PoolSize {
        self.pool.pool_size()
    }

    /// Removes a list of transactions from the pool.
    pub fn remove_transactions(&self, txs: Vec<TxHash>) {
        self.pool.remove_transactions(txs);
    }
}

/// The pool's view of the canonical tip.
pub type BlockInfo = RethBlockInfo;

impl TxPool for WorkerTxPool {
    fn best_transactions(&self) -> BestTxns {
        BestTxns { inner: self.pool.best_transactions() }
    }

    fn get_pending_base_fee(&self) -> u64 {
        // TODO(issue 114): compute the next base fee for the whole round; until then the pool
        // seals at the protocol minimum.
        MIN_PROTOCOL_BASE_FEE
    }

    fn remove_eip4844_txs(&mut self, blobs: Vec<TxHash>) {
        self.pool.remove_transactions_and_descendants(blobs.clone());
        self.pool.delete_blobs(blobs);
    }

    fn is_in_flight(&self, tx_hash: &TxHash) -> bool {
        self.in_flight_tracker.is_in_flight(tx_hash)
    }
}

/// An iterator over the best transactions of a pool.
pub struct BestTxns {
    /// The reth iterator this wraps.
    inner: Box<dyn BestTransactions<Item = Arc<PoolTxn>>>,
}

impl std::fmt::Debug for BestTxns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BestTxns iterator")
    }
}

impl BestTxns {
    /// Wraps a reth iterator directly; tests only, production goes through [`TxPool`].
    pub fn new_for_test(inner: Box<dyn BestTransactions<Item = Arc<PoolTxn>>>) -> Self {
        Self { inner }
    }
}

impl BestTxns {
    /// Disables live transaction updates from the pool during iteration.
    ///
    /// Without this the iterator keeps receiving new pending transactions while iterating; one
    /// whose predecessor is not in the snapshot starts an independent chain, leaving an
    /// intra-sender nonce gap that fails `nonce_too_high` at execution.
    pub fn no_updates(&mut self) {
        self.inner.no_updates();
    }

    /// Marks the transaction invalid for exceeding the batch gas limit.
    pub fn exceeds_gas_limit(&mut self, pool_tx: &Arc<PoolTxn>, gas_limit: u64) {
        self.inner.mark_invalid(
            pool_tx,
            &InvalidPoolTransactionError::ExceedsGasLimit(pool_tx.gas_limit(), gas_limit),
        );
    }

    /// Marks the transaction invalid for exceeding the batch size limit.
    pub fn max_batch_size(&mut self, pool_tx: &Arc<PoolTxn>, tx_size: usize, max_size: usize) {
        self.inner.mark_invalid(
            pool_tx,
            &InvalidPoolTransactionError::OversizedData { size: tx_size, limit: max_size },
        );
    }

    /// Marks the EIP-4844 transaction invalid; batches carry no blob sidecars.
    pub fn ignore_eip4844(&mut self, pool_tx: &Arc<PoolTxn>) {
        self.inner.mark_invalid(
            pool_tx,
            &InvalidPoolTransactionError::Eip4844(Eip4844PoolTransactionError::NoEip4844Blobs),
        );
    }
}

impl Iterator for BestTxns {
    type Item = Arc<PoolTxn>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

/// Recovers the signer of EIP-2718 transaction bytes.
pub fn recover_raw_transaction(tx: &[u8]) -> RaylsRethResult<Recovered<TransactionSigned>> {
    let recovered = reth_recover_raw_transaction::<TransactionSigned>(tx)?;
    Ok(recovered)
}

/// Decodes EIP-2718 transaction bytes, verifying the signature but dropping the signer.
pub fn recover_signed_transaction(tx: &[u8]) -> RaylsRethResult<TransactionSigned> {
    let recovered = reth_recover_raw_transaction::<TransactionSigned>(tx)?;
    Ok(recovered.into_inner())
}

/// Recovers EIP-2718 transaction bytes into a pooled transaction.
pub fn recover_pooled_transaction(
    tx: &[u8],
) -> eyre::Result<EthPooledTransaction<TransactionSigned>> {
    let recovered = reth_recover_raw_transaction::<TransactionSigned>(tx)?;
    let pooled = EthPooledTransaction::try_from_consensus(recovered)?;
    Ok(pooled)
}
