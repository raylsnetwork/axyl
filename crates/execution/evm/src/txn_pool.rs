//! Implement an abstraction around the Reth transaction pool.
//! This should insulate from shifting Reth internals, etc.

use eyre::Error;
use futures::StreamExt as _;
use rayls_infrastructure_types::{
    Address, EnvKzgSettings, Recovered, SealedBlock, TaskSpawner, TransactionSigned, TxHash,
    MIN_PROTOCOL_BASE_FEE,
};
use reth::transaction_pool::{
    blobstore::DiskFileBlobStore, BlockInfo as RethBlockInfo, TransactionValidationTaskExecutor,
};
use reth_chainspec::ChainSpec;
use reth_node_builder::{NodeConfig, RethTransactionPoolConfig};
use reth_primitives_traits::SignedTransaction;
use reth_provider::{
    providers::BlockchainProvider, AccountReader as _, CanonStateNotification,
    CanonStateSubscriptions as _, Chain, ChainSpecProvider, ChangedAccount, StateProviderFactory,
};
use reth_rpc_eth_types::utils::recover_raw_transaction as reth_recover_raw_transaction;
use reth_transaction_pool::{
    error::{Eip4844PoolTransactionError, InvalidPoolTransactionError, PoolError},
    identifier::TransactionId,
    AddedTransactionOutcome, BestTransactions, CanonicalStateUpdate, CoinbaseTipOrdering,
    EthPooledTransaction, Pool, PoolSize, PoolTransaction, PoolUpdateKind, TransactionEvents,
    TransactionOrigin, TransactionPool as _, TransactionPoolExt as _, ValidPoolTransaction,
};
use std::{collections::HashMap, sync::Arc, time::Instant};
use tracing::{debug, info, trace, warn};

use crate::{
    bypass_validator::{BypassHandle, BypassableValidator},
    error::RaylsRethResult,
    traits::RaylsNode,
};

/// Owned version of CanonicalStateUpdate for use in blocking tasks.
///
/// The original CanonicalStateUpdate holds references, but we need owned data
/// to move into a blocking task.
struct OwnedCanonicalStateUpdate {
    new_tip: SealedBlock,
    pending_block_base_fee: u64,
    pending_block_blob_fee: Option<u128>,
    changed_accounts: Vec<ChangedAccount>,
    mined_transactions: Vec<TxHash>,
}

/// A pooled transaction id.
pub type PoolTxnId = TransactionId;
/// A pooled transaction.
pub type PoolTxn = ValidPoolTransaction<EthPooledTransaction>;
/// A recovered pooled transaction.
pub type RecoveredPoolTxn = Recovered<EthPooledTransaction>;

pub use reth_primitives_traits::InMemorySize as TxnSize;

/// Generate a new pooled transaction from an eth transaction and id.
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

/// Decode transaction bytes back to a ['TransactionSigned'].
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

/// Trait on a transaction pool to produce the best transaction.
pub trait TxPool {
    /// Return an iterator over the best transactions in a pool.
    fn best_transactions(&self) -> BestTxns;
    /// Return the pending txn base fee.
    fn get_pending_base_fee(&self) -> u64;
    /// Remove EIP-4844 blob transactions from the pool and delete the sidecars from blob store.
    fn remove_eip4844_txs(&mut self, blobs: Vec<TxHash>);
}

/// The reth EthTransactionValidator type used by this pool.
type EthValidator = reth_transaction_pool::EthTransactionValidator<
    BlockchainProvider<RaylsNode>,
    EthPooledTransaction,
    crate::evm::RaylsEvmConfig,
>;

/// The full validator stack: bypass wrapper around the task executor.
type RaylsValidator = BypassableValidator<TransactionValidationTaskExecutor<EthValidator>>;

/// The concrete pool type used by Rayls workers.
pub type RaylsTransactionPool =
    Pool<RaylsValidator, CoinbaseTipOrdering<EthPooledTransaction>, DiskFileBlobStore>;

/// A rayls network transaction pool.
#[derive(Clone, Debug)]
pub struct WorkerTxPool {
    pool: RaylsTransactionPool,
    task_spawner: TaskSpawner,
    bypass_handle: BypassHandle,
    blockchain_provider: BlockchainProvider<RaylsNode>,
}

impl From<WorkerTxPool> for RaylsTransactionPool {
    fn from(value: WorkerTxPool) -> Self {
        value.pool
    }
}

impl WorkerTxPool {
    /// Create a new instance of `Self`.
    pub fn new(
        node_config: &NodeConfig<ChainSpec>,
        task_spawner: &TaskSpawner,
        blockchain_provider: &BlockchainProvider<RaylsNode>,
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

        // wrap in BypassableValidator so orphan transactions can skip re-validation
        let (bypass_validator, bypass_handle) = BypassableValidator::new(task_executor);

        let transaction_pool =
            Pool::new(bypass_validator, CoinbaseTipOrdering::default(), blob_store, pool_config);

        info!(target: "rayls::execution", "Transaction pool initialized");

        // TODO- save/load txn pool on start/stop (reth's backup_local_transactions_task
        // interface does not work with custom TaskManager, needs upstream PR)

        let mut state_stream = blockchain_provider.canonical_state_stream();
        let this = Self {
            pool: transaction_pool,
            task_spawner: task_spawner.clone(),
            bypass_handle,
            blockchain_provider: blockchain_provider.clone(),
        };
        let txn_pool_clone = this.clone();
        // Update the txn pool as the canonical tip changes.
        task_spawner.spawn_critical_task("canonical txn pool", async move {
            while let Some(update) = state_stream.next().await {
                match update {
                    CanonStateNotification::Commit { new } => {
                        txn_pool_clone.process_canon_state_update(new);
                    }
                    _ => unreachable!("Rayls reorgs are impossible"),
                }
            }
        });
        Ok(this)
    }

    /// Update pool to remove mined transactions synchronously.
    ///
    /// Use this when immediate deduplication is required (e.g., in BatchBuilder to prevent
    /// duplicate transactions in consecutive batches).
    pub fn update_canonical_state(
        &self,
        new_tip: &SealedBlock,
        pending_block_base_fee: u64,
        pending_block_blob_fee: Option<u128>,
        mined_transactions: Vec<TxHash>,
        changed_accounts: Vec<ChangedAccount>,
    ) {
        let mined_count = mined_transactions.len();
        let changed_count = changed_accounts.len();
        let start = Instant::now();

        // create canonical state update
        let update = CanonicalStateUpdate {
            new_tip,
            pending_block_base_fee,
            pending_block_blob_fee,
            changed_accounts,
            mined_transactions,
            update_kind: PoolUpdateKind::Commit,
        };

        self.pool.on_canonical_state_change(update);

        let elapsed = start.elapsed();
        debug!(
            target: "rayls::txpool",
            mined_txs = mined_count,
            changed_accounts = changed_count,
            elapsed_ms = elapsed.as_millis(),
            "pool canonical state update completed (sync)"
        );
    }

    /// Update pool to remove mined transactions asynchronously.
    ///
    /// This spawns a blocking task to avoid blocking the async runtime during high load.
    /// Use this for canonical stream updates where immediate deduplication is not required.
    /// The pool handles its own internal locking, so fire-and-forget is safe here.
    pub fn update_canonical_state_async(
        &self,
        new_tip: &SealedBlock,
        pending_block_base_fee: u64,
        pending_block_blob_fee: Option<u128>,
        mined_transactions: Vec<TxHash>,
        changed_accounts: Vec<ChangedAccount>,
    ) {
        let mined_count = mined_transactions.len();
        let changed_count = changed_accounts.len();

        // create owned canonical state update for the blocking task
        let update = OwnedCanonicalStateUpdate {
            new_tip: new_tip.clone(),
            pending_block_base_fee,
            pending_block_blob_fee,
            changed_accounts,
            mined_transactions,
        };

        let pool = self.pool.clone();

        // spawn blocking task to avoid blocking the async runtime
        // this prevents RPC degradation under high transaction load
        self.task_spawner.spawn_blocking_task("pool-canonical-update", move || {
            let start = Instant::now();

            // create the borrowed update from owned data
            let canonical_update = CanonicalStateUpdate {
                new_tip: &update.new_tip,
                pending_block_base_fee: update.pending_block_base_fee,
                pending_block_blob_fee: update.pending_block_blob_fee,
                changed_accounts: update.changed_accounts,
                mined_transactions: update.mined_transactions,
                update_kind: PoolUpdateKind::Commit,
            };

            pool.on_canonical_state_change(canonical_update);

            let elapsed = start.elapsed();
            debug!(
                target: "rayls::txpool",
                mined_txs = mined_count,
                changed_accounts = changed_count,
                elapsed_ms = elapsed.as_millis(),
                "pool canonical state update completed (async)"
            );
        });
    }

    /// Return pending transactions.
    pub fn pending_transactions(&self) -> Vec<Arc<PoolTxn>> {
        self.pool.pending_transactions()
    }

    /// Return queued transaction (not able to execute yet).
    pub fn queued_transactions(&self) -> Vec<Arc<PoolTxn>> {
        self.pool.queued_transactions()
    }

    /// This method is called when a canonical state update is received.
    ///
    /// Trigger the maintenance task to update pool before building the next block.
    fn process_canon_state_update(&self, update: Arc<Chain>) {
        trace!(target: "worker::block-builder", ?update, "canon state update from engine");

        // update pool based with canonical tip update
        let (blocks, state) = update.inner();
        let tip = blocks.tip();

        // collect all accounts that changed in last round of consensus
        let changed_accounts: Vec<ChangedAccount> = state
            .accounts_iter()
            .filter_map(|(addr, acc)| acc.map(|acc| (addr, acc)))
            .map(|(address, acc)| ChangedAccount {
                address,
                nonce: acc.nonce,
                balance: acc.balance,
            })
            .collect();

        debug!(target: "block-builder", ?changed_accounts);

        // collect tx hashes to remove any transactions from this pool that were mined
        let mined_transactions: Vec<TxHash> = blocks.transaction_hashes().collect();

        debug!(target: "block-builder", ?mined_transactions);

        let base_fee_per_gas = tip.base_fee_per_gas.unwrap_or_else(|| self.get_pending_base_fee());
        // async pool update spawned as blocking task to prevent RPC degradation
        // this is safe because canonical stream updates don't require immediate deduplication
        self.update_canonical_state(
            tip.sealed_block(),
            base_fee_per_gas,
            Some(u128::MAX), // set max fee for blobs
            mined_transactions,
            changed_accounts,
        );
    }

    /// Return the current status of the pool.
    pub fn block_info(&self) -> BlockInfo {
        self.pool.block_info()
    }

    /// Set the current status of the pool.
    pub fn set_block_info(&self, block_info: BlockInfo) {
        self.pool.set_block_info(block_info);
    }

    /// Return the transactions for an address from the pool.
    pub fn get_transactions_by_sender(&self, address: Address) -> Vec<Arc<PoolTxn>> {
        self.pool.get_transactions_by_sender(address)
    }

    /// Adds a local (NOT external) transaction to the pool.
    pub async fn add_transaction_local(
        &self,
        recovered: EthPooledTransaction,
    ) -> Result<AddedTransactionOutcome, crate::PoolError> {
        self.pool.add_transaction(TransactionOrigin::Local, recovered).await
    }

    /// Adds a local (NOT external) transaction to the pool.
    pub async fn add_transactions_local(
        &self,
        recovered: Vec<EthPooledTransaction>,
    ) -> Vec<Result<AddedTransactionOutcome, crate::PoolError>> {
        self.pool.add_transactions(TransactionOrigin::Local, recovered).await
    }

    /// Add orphan transactions that were already validated in a previous epoch.
    ///
    /// This bypasses the expensive per-transaction state validation by:
    /// 1. Reading sender state once per unique sender (batch read)
    /// 2. Pre-populating the bypass map so the validator returns `Valid` immediately
    /// 3. Calling the normal `add_transactions` path (which hits the bypass → instant)
    pub async fn add_orphan_transactions(
        &self,
        transactions: Vec<EthPooledTransaction>,
    ) -> Vec<Result<AddedTransactionOutcome, crate::PoolError>> {
        if transactions.is_empty() {
            return Vec::new();
        }

        let start = Instant::now();

        // collect unique senders
        let unique_senders: Vec<Address> = {
            let mut seen = std::collections::HashSet::with_capacity(transactions.len());
            transactions
                .iter()
                .filter_map(|tx| {
                    let sender = tx.sender();
                    if seen.insert(sender) {
                        Some(sender)
                    } else {
                        None
                    }
                })
                .collect()
        };

        // batch-read sender state from a single snapshot
        let sender_accounts = match self.blockchain_provider.latest() {
            Ok(state) => {
                let mut accounts = HashMap::with_capacity(unique_senders.len());
                for sender in &unique_senders {
                    match state.basic_account(sender) {
                        Ok(Some(account)) => {
                            accounts.insert(*sender, account);
                        }
                        Ok(None) => {
                            // account not found - use defaults (balance=0, nonce=0)
                            accounts.insert(*sender, reth_primitives_traits::Account::default());
                        }
                        Err(e) => {
                            warn!(
                                target: "orphan-batches",
                                ?sender,
                                ?e,
                                "Failed to read sender account for orphan bypass"
                            );
                        }
                    }
                }
                accounts
            }
            Err(e) => {
                warn!(
                    target: "orphan-batches",
                    ?e,
                    "Failed to get state provider for orphan bypass, falling back to normal validation"
                );
                return self.pool.add_transactions(TransactionOrigin::Local, transactions).await;
            }
        };

        let state_read_elapsed = start.elapsed();

        // activate bypass: the validator will return Valid immediately for these txs
        self.bypass_handle.activate(&transactions, &sender_accounts);

        let tx_count = transactions.len();
        let sender_count = sender_accounts.len();

        // insert through normal path - validator short-circuits via bypass map
        let results = self.pool.add_transactions(TransactionOrigin::Local, transactions).await;

        // deactivate bypass
        self.bypass_handle.deactivate();

        let ok_count = results.iter().filter(|r| r.is_ok()).count();
        let total_elapsed = start.elapsed();
        warn!(
            target: "orphan-batches",
            tx_count,
            sender_count,
            ok_count,
            err_count = tx_count - ok_count,
            state_read_ms = state_read_elapsed.as_millis(),
            total_ms = total_elapsed.as_millis(),
            "Orphan transactions inserted via bypass"
        );

        results
    }

    /// Adds an external transaction to the pool.
    pub async fn add_raw_transaction_external(
        &self,
        tx: EthPooledTransaction,
    ) -> Result<AddedTransactionOutcome, crate::PoolError> {
        self.pool.add_transaction(TransactionOrigin::External, tx).await
    }

    /// Adds a local (NOT external) transaction to the pool and subscribes to transaction events.
    pub async fn add_transaction_and_subscribe_local(
        &self,
        recovered: EthPooledTransaction,
    ) -> Result<TransactionEvents, crate::EthApiError> {
        Ok(self.pool.add_transaction_and_subscribe(TransactionOrigin::Local, recovered).await?)
    }

    /// Retrieves a transaction by hash from the pool.
    pub fn get(&self, tx: &TxHash) -> Option<Arc<PoolTxn>> {
        self.pool.get(tx)
    }

    /// Retrieve the pool size stats for the pool.
    pub fn pool_size(&self) -> PoolSize {
        self.pool.pool_size()
    }

    /// Removes a list of transactions from the pool.
    pub fn remove_transactions(&self, txs: Vec<TxHash>) {
        self.pool.remove_transactions(txs);
    }
}

/// Block info defining a transaction pool status.
pub type BlockInfo = RethBlockInfo;

impl TxPool for WorkerTxPool {
    fn best_transactions(&self) -> BestTxns {
        BestTxns { inner: self.pool.best_transactions() }
    }

    /// Return the pending txn base fee.  Currently just the min protocol base fee.
    fn get_pending_base_fee(&self) -> u64 {
        // TODO issue 114: calculate the next basefee HERE for the entire round
        //
        // for now, always use lowest base fee possible
        MIN_PROTOCOL_BASE_FEE
    }

    fn remove_eip4844_txs(&mut self, blobs: Vec<TxHash>) {
        self.pool.remove_transactions_and_descendants(blobs.clone());
        self.pool.delete_blobs(blobs);
    }
}

/// An iterator that produces the best transactions from a pool.
pub struct BestTxns {
    inner: Box<dyn BestTransactions<Item = Arc<PoolTxn>>>,
}

impl std::fmt::Debug for BestTxns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BestTxns iterator")
    }
}

impl BestTxns {
    /// Create a new BestTxns (for testing only- normally this comes from a call on the pool).
    pub fn new_for_test(inner: Box<dyn BestTransactions<Item = Arc<PoolTxn>>>) -> Self {
        Self { inner }
    }
}

impl BestTxns {
    /// Disable live transaction updates from the pool during iteration.
    ///
    /// Without this, the iterator receives new pending transactions via a broadcast
    /// channel while iterating. If a transaction arrives whose predecessor is not in the
    /// snapshot, it becomes a new independent chain — creating intra-sender nonce gaps
    /// that cause `nonce_too_high` at execution time.
    pub fn no_updates(&mut self) {
        self.inner.no_updates();
    }

    /// When the best transactions exceed our gas limit notify the pool.
    pub fn exceeds_gas_limit(&mut self, pool_tx: &Arc<PoolTxn>, gas_limit: u64) {
        self.inner.mark_invalid(
            pool_tx,
            &InvalidPoolTransactionError::ExceedsGasLimit(pool_tx.gas_limit(), gas_limit),
        );
    }

    /// When the best transactions are too large for a batch notify the pool.
    pub fn max_batch_size(&mut self, pool_tx: &Arc<PoolTxn>, tx_size: usize, max_size: usize) {
        self.inner.mark_invalid(
            pool_tx,
            &InvalidPoolTransactionError::OversizedData { size: tx_size, limit: max_size },
        );
    }

    /// Mark the EIP-4844 transaction as invalid.
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

/// Recover bytes into a transaction.
pub fn recover_raw_transaction(tx: &[u8]) -> RaylsRethResult<Recovered<TransactionSigned>> {
    let recovered = reth_recover_raw_transaction::<TransactionSigned>(tx)?;
    Ok(recovered)
}

/// Recover bytes into a signed transaction.
pub fn recover_signed_transaction(tx: &[u8]) -> RaylsRethResult<TransactionSigned> {
    let recovered = reth_recover_raw_transaction::<TransactionSigned>(tx)?;
    Ok(recovered.into_inner())
}

/// Recover a pooled transaction.
pub fn recover_pooled_transaction(
    tx: &[u8],
) -> eyre::Result<EthPooledTransaction<TransactionSigned>> {
    let recovered = reth_recover_raw_transaction::<TransactionSigned>(tx)?;
    let pooled = EthPooledTransaction::try_from_consensus(recovered)?;
    Ok(pooled)
}
