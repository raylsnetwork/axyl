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
    error::{Eip4844PoolTransactionError, InvalidPoolTransactionError, PoolError, PoolErrorKind},
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

/// Interval between re-checks of executed hashes against the pool, i.e. the bound on how long an
/// executed transaction's in-flight mark outlives its removal from the pool.
const EXECUTED_RELEASE_INTERVAL: Duration = Duration::from_secs(1);

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
    /// Node-scoped in-flight marks shared with the role the node runs: the batch builder marks
    /// what it sealed so the next round skips it, the forwarder marks what it sent so it does not
    /// re-send before the mark is due. Handed out via [`WorkerTxPool::in_flight`].
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
    ///
    /// The `in_flight` tracker is node-scoped and shared with whichever role the node runs (the
    /// batch builder's sealing marks or the forwarder's dissemination marks), so its
    /// forwarding-role paths only come alive once a forwarder arms them.
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
                // bounds the queued (non-executable) sub-pool only; a pending tx is never
                // lifetime-evicted, so this caps nonce-gapped stranding, not seal latency
                max_tx_lifetime: Duration::from_mins(5),
                no_local_exemptions: true,
                ..Default::default()
            },
        );
        task_spawner.spawn_critical_task("maintain txn pool", maintain_fut);

        // Maintenance drops mined transactions asynchronously, so committed hashes are re-checked
        // on a fixed tick rather than per commit: release latency stays bounded after the last
        // commit and pool-lock time does not grow with blocks per second. Demotions out of pending
        // have no commit to hook; the engine-tick membership reconcile covers those.
        let mut release_stream = blockchain_provider.canonical_state_stream();
        let release_pool = this.clone();
        task_spawner.spawn_critical_task("in-flight release", async move {
            let mut executed: Vec<TxHash> = Vec::new();
            let mut release_tick = tokio::time::interval(EXECUTED_RELEASE_INTERVAL);
            // Delay, not Burst: a stalled tick has nothing to catch up on, since the next lookup
            // sees every hash the skipped ones would have.
            release_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    notification = release_stream.next() => {
                        let Some(notification) = notification else { break };
                        executed.extend(notification.committed().inner().0.transaction_hashes());
                    }
                    _ = release_tick.tick() => {
                        if !executed.is_empty() {
                            executed = release_pool.release_executed(std::mem::take(&mut executed));
                        }
                    },
                }
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

    /// Returns the node-scoped in-flight tracker shared with the batch builder or the forwarder.
    ///
    /// The role marks hashes here (on quorum for a sealed batch, on send for a forwarded one) and
    /// the pool releases them via [`Self::reconcile_in_flight`] once execution removes them from
    /// the pending sub-pool.
    pub fn in_flight(&self) -> InFlightTracker {
        self.in_flight_tracker.clone()
    }

    /// Releases the marks of the given executed hashes the pool has already dropped, returning the
    /// tracked ones it still holds so a later call re-checks them.
    fn release_executed(&self, executed: Vec<TxHash>) -> Vec<TxHash> {
        // only this node's marks matter: most committed transactions were sealed by peers
        let tracked = self.in_flight_tracker.tracked_among(executed);
        if tracked.is_empty() {
            return tracked;
        }
        let still_pooled: B256Set =
            self.pool.get_all(tracked.clone()).iter().map(|tx| *tx.hash()).collect();
        let (carried, gone): (Vec<_>, Vec<_>) =
            tracked.into_iter().partition(|hash| still_pooled.contains(hash));
        let released = self.in_flight_tracker.release_executed(gone);
        if released > 0 {
            debug!(target: "rayls::txpool", released, carried = carried.len(), "released in-flight marks for executed transactions");
        }
        carried
    }

    /// Releases in-flight marks for hashes no longer in the pending sub-pool.
    ///
    /// A sealed transaction stays pending (and RPC-visible) until it executes; once it leaves
    /// pending its mark is stale and released so a later resubmission of the same nonce is not
    /// skipped. Snapshots the whole pending sub-pool under the pool lock, so it runs only on the
    /// fixed-rate engine tick.
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

    /// Adds external transactions to the pool in one call, one outcome per input in order.
    pub async fn add_raw_transactions_external(
        &self,
        txs: Vec<EthPooledTransaction>,
    ) -> Vec<Result<AddedTransactionOutcome, crate::PoolError>> {
        self.pool.add_transactions(TransactionOrigin::External, txs).await
    }

    /// Admit forwarded transactions and return the hashes the pool rejected as stale.
    ///
    /// Stale means `nonce too low`: the sender already executed that nonce, so the forwarder must
    /// stop re-sending it. Every other rejection (fee caps, pool limits, already-imported) is not
    /// stale: those transactions are still wanted, so they are omitted from the ack and stay
    /// eligible for a later resend.
    pub async fn add_forwarded_txns(&self, txs: Vec<EthPooledTransaction>) -> Vec<TxHash> {
        self.pool
            .add_transactions(TransactionOrigin::External, txs)
            .await
            .into_iter()
            .filter_map(|res| match res {
                Err(err)
                    if matches!(
                        &err.kind,
                        PoolErrorKind::InvalidTransaction(invalid) if invalid.is_nonce_too_low()
                    ) =>
                {
                    Some(err.hash)
                }
                _ => None,
            })
            .collect()
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

#[cfg(test)]
mod tests {
    use crate::{reth_env::RethEnv, test_utils::TransactionFactory};
    use rayls_infrastructure_types::{test_genesis, Address, TaskManager, TxHash, U256};
    use reth_chainspec::ChainSpec as RethChainSpec;
    use reth_transaction_pool::TransactionPool as _;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// An executed hash keeps its mark until the pool drops the transaction.
    #[tokio::test]
    async fn executed_mark_is_released_only_after_the_pool_drops_the_transaction() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .await
                .unwrap();
        let pool = reth_env.init_txn_pool().unwrap();
        let gas_price = reth_env.get_gas_price().unwrap();
        let tx = TransactionFactory::new().create_eip1559_encoded(
            chain,
            None,
            gas_price,
            Some(Address::ZERO),
            U256::from(1),
            Default::default(),
        );
        let tx = super::recover_pooled_transaction(&tx).unwrap();
        let hash = pool.add_transaction_local(tx).await.unwrap().hash;
        let tracker = pool.in_flight();
        tracker.mark_in_flight([hash]);

        // a peer-sealed hash this node never marked is dropped from the carry, not looked up
        let executed = pool.release_executed(vec![hash, TxHash::repeat_byte(0xee)]);
        assert!(tracker.is_in_flight(&hash), "pooled transaction must keep its mark");
        assert_eq!(executed, [hash], "only the tracked hash is carried to the next check");

        pool.pool.remove_transactions(vec![hash]);
        let executed = pool.release_executed(executed);
        assert!(!tracker.is_in_flight(&hash));
        assert!(executed.is_empty());
    }
}
