//! Worker-side wrapper around the reth transaction pool.
//!
//! Keeps reth pool internals out of the batch builder and RPC paths, and owns the pieces reth
//! does not: the in-flight mark tracker.

use crate::rls_flavour;
use alloy::{
    consensus::{
        constants::{
            EIP1559_TX_TYPE_ID, EIP2930_TX_TYPE_ID, EIP7702_TX_TYPE_ID, LEGACY_TX_TYPE_ID,
        },
        BlockHeader as _, Transaction as _,
    },
    eips::Typed2718 as _,
};
use eyre::Error;
use futures::StreamExt as _;
use rayls_infrastructure_types::{
    Address, B256Set, EnvKzgSettings, Recovered, TaskSpawner, TransactionSigned, TxHash,
    MIN_PROTOCOL_BASE_FEE, U256,
};
use reth::transaction_pool::{
    blobstore::DiskFileBlobStore, BlockInfo as RethBlockInfo, TransactionValidationTaskExecutor,
};
use reth_chainspec::ChainSpec;
use reth_node_builder::{NodeConfig, RethTransactionPoolConfig};
use reth_primitives_traits::{
    transaction::error::InvalidTransactionError, GotExpected, SealedBlock, SignedTransaction,
};
use reth_provider::{
    providers::BlockchainProvider, AccountReader as _, BlockReaderIdExt as _,
    CanonStateSubscriptions as _, ChainSpecProvider, StateProvider as _, StateProviderFactory as _,
};
use reth_revm::primitives::KECCAK_EMPTY;
use reth_rpc_eth_types::utils::recover_raw_transaction as reth_recover_raw_transaction;
use reth_transaction_pool::{
    error::{
        Eip4844PoolTransactionError, Eip7702PoolTransactionError, InvalidPoolTransactionError,
        PoolError, PoolErrorKind, PoolTransactionError,
    },
    identifier::TransactionId,
    maintain::{maintain_transaction_pool_future, MaintainPoolConfig},
    validate::{ensure_intrinsic_gas, ValidTransaction},
    AddedTransactionOutcome, BestTransactions, CoinbaseTipOrdering, EthPooledTransaction, Pool,
    PoolSize, PoolTransaction, TransactionEvents, TransactionListenerKind, TransactionOrigin,
    TransactionPool as _, TransactionPoolExt as _, TransactionValidationOutcome,
    TransactionValidator, ValidPoolTransaction,
};
use std::{
    sync::{atomic::Ordering, Arc, OnceLock},
    time::{Duration, Instant},
};
use tracing::{debug, info};

use crate::{error::RaylsRethResult, in_flight::InFlightTracker, traits::RaylsNode};

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
type RaylsValidator = TransactionValidationTaskExecutor<RlsFlavourValidator>;

/// Validates RLS-flavour transactions itself and hands every other transaction to reth's
/// validator, which rejects any chain id but the chain's own.
#[derive(Debug)]
pub struct RlsFlavourValidator {
    inner: EthValidator,
    /// The pool this validator feeds, set once the pool exists. Used to refuse a replacement
    /// that switches flavour at the same nonce.
    pool: Arc<OnceLock<RaylsTransactionPool>>,
}

/// Why an RLS-flavour transaction was refused, where reth has no matching error.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct RlsFlavourError(&'static str);

impl PoolTransactionError for RlsFlavourError {
    fn is_bad_transaction(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// The most RLS an RLS-flavour transaction can take: max fee for its gas limit plus its value.
/// `U256::MAX` if that overflows, which no balance covers.
fn rls_max_cost(transaction: &EthPooledTransaction) -> U256 {
    // gas_limit (u64) times max fee (u128) fits in a U256; adding the value can overflow
    (U256::from(transaction.gas_limit()) * U256::from(transaction.max_fee_per_gas()))
        .checked_add(transaction.value())
        .unwrap_or(U256::MAX)
}

/// Drops waiting RLS-flavour transactions that can no longer execute: those their sender can no
/// longer pay for (in nonce order), and those whose fee cap fell under the current RLS base fee.
/// Reth's pool re-checks balances and fee caps after each block, but against USDr only.
fn drop_unpayable_rls_txns(
    pool: &RaylsTransactionPool,
    provider: &BlockchainProvider<RaylsNode>,
) -> eyre::Result<usize> {
    let chain_id = provider.chain_spec().chain().id();
    let mut by_sender: std::collections::BTreeMap<Address, Vec<Arc<PoolTxn>>> = Default::default();
    for tx in pool.pending_transactions().into_iter().chain(pool.queued_transactions()) {
        if rls_flavour::is_rls(tx.transaction.chain_id(), chain_id) {
            by_sender.entry(tx.sender()).or_default().push(tx);
        }
    }
    if by_sender.is_empty() {
        return Ok(0);
    }
    let usdr_base_fee = provider
        .latest_header()?
        .and_then(|h| h.base_fee_per_gas())
        .unwrap_or(MIN_PROTOCOL_BASE_FEE);
    let rls_base_fee = rls_flavour::rls_base_fee(usdr_base_fee);
    let state = provider.latest()?;
    let mut unpayable = Vec::new();
    for (sender, mut txs) in by_sender {
        txs.sort_by_key(|t| t.nonce());
        let balance = state
            .storage(rls_flavour::RLS_TOKEN, rls_flavour::balance_slot(sender).into())?
            .unwrap_or_default();
        let mut spent = U256::ZERO;
        for tx in txs {
            spent = spent.saturating_add(rls_max_cost(&tx.transaction));
            if spent > balance || tx.transaction.max_fee_per_gas() < rls_base_fee {
                unpayable.push(*tx.hash());
            }
        }
    }
    let dropped = unpayable.len();
    if dropped > 0 {
        pool.remove_transactions(unpayable);
    }
    Ok(dropped)
}

impl RlsFlavourValidator {
    /// The stateless checks reth's validator runs (`EthTransactionValidator::validate_stateless`)
    /// except the chain id and the local USDr fee cap. A transaction that passes the pool but fails
    /// in revm is skipped without paying, so these must match.
    fn stateless_checks(
        &self,
        origin: TransactionOrigin,
        transaction: &EthPooledTransaction,
    ) -> Result<(), InvalidPoolTransactionError> {
        let inner = &self.inner;
        // EIP-7702 works as in the USDr flavour: inside an RLS-flavour transaction revm checks
        // each authorization's chain id against the RLS chain id (or 0)
        if !matches!(
            transaction.ty(),
            LEGACY_TX_TYPE_ID | EIP2930_TX_TYPE_ID | EIP1559_TX_TYPE_ID | EIP7702_TX_TYPE_ID
        ) {
            return Err(InvalidTransactionError::TxTypeNotSupported.into());
        }
        let tx_size = transaction.encoded_length();
        if tx_size > inner.max_tx_input_bytes() {
            return Err(InvalidPoolTransactionError::OversizedData {
                size: tx_size,
                limit: inner.max_tx_input_bytes(),
            });
        }
        let fork_tracker = inner.fork_tracker();
        if fork_tracker.is_shanghai_activated() {
            let max_initcode_size = fork_tracker.max_initcode_size.load(Ordering::Relaxed);
            transaction.ensure_max_init_code_size(max_initcode_size)?;
        }
        let gas_limit = transaction.gas_limit();
        if gas_limit > inner.block_gas_limit() {
            return Err(InvalidPoolTransactionError::ExceedsGasLimit(
                gas_limit,
                inner.block_gas_limit(),
            ));
        }
        let tx_gas_limit_cap = fork_tracker.tx_gas_limit_cap.load(Ordering::Relaxed);
        if tx_gas_limit_cap != 0 && gas_limit > tx_gas_limit_cap {
            return Err(InvalidPoolTransactionError::MaxTxGasLimitExceeded(
                gas_limit,
                tx_gas_limit_cap,
            ));
        }
        if transaction.max_priority_fee_per_gas() > Some(transaction.max_fee_per_gas()) {
            return Err(InvalidTransactionError::TipAboveFeeCap.into());
        }
        if !matches!(origin, TransactionOrigin::Local)
            && transaction.is_dynamic_fee()
            && transaction.max_priority_fee_per_gas() < *inner.minimum_priority_fee()
        {
            return Err(InvalidPoolTransactionError::PriorityFeeBelowMinimum {
                minimum_priority_fee: inner.minimum_priority_fee().unwrap_or_default(),
            });
        }
        if transaction.ty() == EIP7702_TX_TYPE_ID {
            if !fork_tracker.is_prague_activated() {
                return Err(InvalidTransactionError::TxTypeNotSupported.into());
            }
            if transaction.authorization_list().is_none_or(|list| list.is_empty()) {
                return Err(Eip7702PoolTransactionError::MissingEip7702AuthorizationList.into());
            }
        }
        ensure_intrinsic_gas(transaction, fork_tracker)
    }

    /// Checks an RLS-flavour transaction: the same nonce, gas and fee rules, with the cost
    /// checked against the sender's RLS instead of USDr.
    fn validate_rls(
        &self,
        origin: TransactionOrigin,
        mut transaction: EthPooledTransaction,
    ) -> TransactionValidationOutcome<EthPooledTransaction> {
        let inner = &self.inner;
        let invalid = |transaction, err: InvalidPoolTransactionError| {
            TransactionValidationOutcome::Invalid(transaction, err)
        };

        if let Err(err) = self.stateless_checks(origin, &transaction) {
            return invalid(transaction, err);
        }
        // The pool ranks fees against the USDr base fee, so an RLS fee under the RLS base fee
        // would sit as executable and then fail. Refuse it here instead.
        let usdr_base_fee = match inner.client().latest_header() {
            Ok(header) => {
                header.and_then(|h| h.base_fee_per_gas()).unwrap_or(MIN_PROTOCOL_BASE_FEE)
            }
            Err(err) => {
                return TransactionValidationOutcome::Error(*transaction.hash(), Box::new(err))
            }
        };
        if transaction.max_fee_per_gas() < rls_flavour::rls_base_fee(usdr_base_fee) {
            return invalid(transaction, InvalidTransactionError::FeeCapTooLow.into());
        }

        let state = match inner.client().latest() {
            Ok(state) => state,
            Err(err) => {
                return TransactionValidationOutcome::Error(*transaction.hash(), Box::new(err))
            }
        };
        let account = match state.basic_account(transaction.sender_ref()) {
            Ok(account) => account.unwrap_or_default(),
            Err(err) => {
                return TransactionValidationOutcome::Error(*transaction.hash(), Box::new(err))
            }
        };
        match inner.validate_sender_bytecode(&transaction, &account, &state) {
            Err(outcome) => return outcome,
            Ok(Err(err)) => return invalid(transaction, err),
            Ok(Ok(())) => {}
        }
        if let Err(err) = inner.validate_sender_nonce(&transaction, &account) {
            return invalid(transaction, err);
        }

        let slot = rls_flavour::balance_slot(*transaction.sender_ref());
        let rls_balance = match state.storage(rls_flavour::RLS_TOKEN, slot.into()) {
            Ok(balance) => balance.unwrap_or_default(),
            Err(err) => {
                return TransactionValidationOutcome::Error(*transaction.hash(), Box::new(err))
            }
        };
        // The sender's other waiting RLS-flavour transactions are paid from the same balance,
        // which the pool cannot see (it follows USDr). Count them too.
        let chain_id = inner.chain_id();
        let waiting = self.pool.get().map_or(U256::ZERO, |pool| {
            pool.get_transactions_by_sender(*transaction.sender_ref())
                .iter()
                .filter(|t| {
                    rls_flavour::is_rls(t.transaction.chain_id(), chain_id)
                        && t.transaction.nonce() != transaction.nonce()
                })
                .fold(U256::ZERO, |sum, t| sum.saturating_add(rls_max_cost(&t.transaction)))
        });
        let needed = rls_max_cost(&transaction).saturating_add(waiting);
        if rls_balance < needed || needed == U256::MAX {
            return invalid(
                transaction,
                InvalidTransactionError::InsufficientFunds(
                    GotExpected { got: rls_balance, expected: needed }.into(),
                )
                .into(),
            );
        }

        if !transaction.value().is_zero() {
            // msg.value stays USDr inside the EVM, so RLS value only goes to accounts without code
            // that are not precompiles: the same rule the handler applies at execution
            let refused = match transaction.to() {
                None => true,
                Some(to) if rls_flavour::is_precompile_range(to) => true,
                Some(to) => match state.basic_account(&to) {
                    Ok(account) => account
                        .and_then(|a| a.bytecode_hash)
                        .is_some_and(|hash| hash != KECCAK_EMPTY),
                    Err(err) => {
                        return TransactionValidationOutcome::Error(
                            *transaction.hash(),
                            Box::new(err),
                        )
                    }
                },
            };
            if refused {
                return invalid(
                    transaction,
                    InvalidPoolTransactionError::Other(Box::new(RlsFlavourError(
                        "RLS value can only go to an account without code that is not a precompile",
                    ))),
                );
            }
        }

        // the pool tracks the accounts a 7702 transaction delegates, as reth's validator does
        let authorities = transaction.authorization_list().map(|auths| {
            auths.iter().filter_map(|auth| auth.recover_authority().ok()).collect::<Vec<_>>()
        });

        // the pool weighs `cost` against the sender's USDr; this transaction spends none
        transaction.cost = U256::ZERO;
        TransactionValidationOutcome::Valid {
            balance: account.balance,
            state_nonce: account.nonce,
            bytecode_hash: account.bytecode_hash,
            transaction: ValidTransaction::Valid(transaction),
            propagate: matches!(origin, TransactionOrigin::External),
            authorities,
        }
    }
}

impl TransactionValidator for RlsFlavourValidator {
    type Transaction = EthPooledTransaction;
    type Block = <EthValidator as TransactionValidator>::Block;

    async fn validate_transaction(
        &self,
        origin: TransactionOrigin,
        transaction: Self::Transaction,
    ) -> TransactionValidationOutcome<Self::Transaction> {
        // Both flavours share one nonce per account, and a wallet keeps a separate network entry
        // for each. A transaction waiting at this nonce in the other flavour would otherwise be
        // replaced (or this one silently dropped), so refuse with a clear error instead.
        let other_flavour_waiting = self
            .pool
            .get()
            .and_then(|pool| {
                pool.get_transaction_by_sender_and_nonce(
                    *transaction.sender_ref(),
                    transaction.nonce(),
                )
            })
            .is_some_and(|waiting| {
                let chain_id = self.inner.chain_id();
                rls_flavour::is_rls(waiting.transaction.chain_id(), chain_id)
                    != rls_flavour::is_rls(transaction.chain_id(), chain_id)
            });
        // Check-then-insert: two such transactions validated at the same moment can both pass.
        // A production version moves this guard into the pool's insert path.
        if other_flavour_waiting {
            return TransactionValidationOutcome::Invalid(
                transaction,
                InvalidPoolTransactionError::Other(Box::new(RlsFlavourError(
                    "a transaction with this nonce is waiting on the other Rayls network; wait for it to land or replace it from that network",
                ))),
            );
        }
        if rls_flavour::is_rls(transaction.chain_id(), self.inner.chain_id()) {
            return self.validate_rls(origin, transaction);
        }
        self.inner.validate_transaction(origin, transaction).await
    }

    fn on_new_head_block(&self, new_tip_block: &SealedBlock<Self::Block>) {
        self.inner.on_new_head_block(new_tip_block)
    }
}

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
        let pool_handle = Arc::new(OnceLock::new());
        let task_executor =
            TransactionValidationTaskExecutor::eth_builder(blockchain_provider.clone(), evm_config)
                .kzg_settings(EnvKzgSettings::Default)
                .with_local_transactions_config(pool_config.local_transactions_config.clone())
                .with_additional_tasks(node_config.txpool.additional_validation_tasks)
                .with_max_tx_input_bytes(node_config.txpool.max_tx_input_bytes)
                .build_with_tasks(task_spawner.clone(), blob_store.clone())
                .map(|inner| RlsFlavourValidator { inner, pool: pool_handle.clone() });

        let transaction_pool =
            Pool::new(task_executor, CoinbaseTipOrdering::default(), blob_store, pool_config);
        let _ = pool_handle.set(transaction_pool.clone());

        info!(target: "rayls::execution", "Transaction pool initialized");

        let this = Self { pool: transaction_pool, in_flight_tracker: in_flight };

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

        // After each block, drop RLS-flavour transactions their senders can no longer pay for.
        let mut rls_stream = blockchain_provider.canonical_state_stream();
        let rls_pool = this.pool.clone();
        let rls_provider = blockchain_provider.clone();
        task_spawner.spawn_critical_task("rls flavour pool re-check", async move {
            while rls_stream.next().await.is_some() {
                match drop_unpayable_rls_txns(&rls_pool, &rls_provider) {
                    Ok(0) => {}
                    Ok(dropped) => debug!(target: "rayls::execution", dropped, "dropped unpayable RLS-flavour transactions"),
                    Err(err) => debug!(target: "rayls::execution", %err, "RLS-flavour pool re-check failed"),
                }
            }
        });

        // Release in-flight marks once maintenance drops the sealed transactions from the
        // pending sub-pool. A separate subscription: the maintenance future consumes its own
        // stream, and this one must see the same commits.
        let mut release_stream = blockchain_provider.canonical_state_stream();
        let release_pool = this.clone();
        task_spawner.spawn_critical_task("in-flight release", async move {
            while let Some(notification) = release_stream.next().await {
                // Release marks for exactly the txns this commit mined, straight from the
                // notification: O(mined), in-flight lock only, NO pool lock and no pending
                // snapshot - so it can neither tax execution nor gap the heartbeat regardless of
                // pool size. Move the committed chain into the blocking closure so the mined-hash
                // iterator is built there, with no intermediate Vec.
                let committed = notification.committed();
                let pool = release_pool.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    let (blocks, _) = committed.inner();
                    pool.reconcile_in_flight(Some(blocks.transaction_hashes()));
                })
                .await;
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

    /// Releases in-flight marks for hashes that have left the pending sub-pool.
    ///
    /// A sealed transaction stays pending (and RPC-visible) until it executes; once execution
    /// drops it from pending, its mark is stale and released so a later resubmission of the same
    /// nonce is not skipped.
    ///
    /// `Some(mined)` — the cheap per-block path: release exactly the txns a commit mined (from the
    /// canonical notification). Touches only the in-flight tracker's own lock, is O(mined), and
    /// takes NO pool lock — so it can't tax execution or gap the execution heartbeat.
    ///
    /// `None` — the periodic backstop: snapshot the whole pending sub-pool under the pool lock and
    /// release marks no longer pending. O(pending) and pool-locked, so keep it off any hot path;
    /// it exists to reap marks for txns that left pending WITHOUT mining
    /// (dropped/replaced/evicted), which the mined set can't see.
    pub fn reconcile_in_flight(&self, mined: Option<impl IntoIterator<Item = TxHash>>) {
        if self.in_flight_tracker.is_empty() {
            return;
        }
        let released = match mined {
            Some(hashes) => self.in_flight_tracker.release_hashes(hashes),
            None => {
                let pending: B256Set =
                    self.pool.pending_transactions().iter().map(|tx| *tx.hash()).collect();
                self.in_flight_tracker.release_mined(&pending)
            }
        };
        if released > 0 {
            debug!(target: "rayls::txpool", released, "released in-flight marks");
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
