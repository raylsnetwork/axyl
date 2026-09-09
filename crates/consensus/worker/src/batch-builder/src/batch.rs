//! The logic for building batches.
//!
//! Transactions are pulled from the worker's pending pool and added to the block without being
//! executed. Block size is measured in bytes and a transaction's max gas limit. The block is sealed
//! when the pending pool devoid of transactions or the max block size is reached (wei or bytes).
//!
//! The mined transactions are returned with the built block so the worker can update the pool.

use rayls_execution_evm::{TxPool, TxnSize};
use rayls_infrastructure_types::{
    max_batch_size, Batch, BatchBuilderArgs, Encodable2718 as _, NonceRange, SenderNonceRanges,
    TransactionTrait as _, TxHash, WorkerId,
};
use tracing::debug;

/// The transactions selected into a sealed batch, to be marked in flight on quorum.
///
/// `#[must_use]` so a build's selection cannot be silently dropped: the caller either marks it in
/// flight on quorum or explicitly discards it.
#[must_use = "mark the selection in flight on quorum, or explicitly drop it"]
#[derive(Debug)]
pub struct SelectedForSeal(Vec<TxHash>);

impl SelectedForSeal {
    /// Returns whether the batch selected no transactions.
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Consumes the selection into the hashes to mark in flight.
    pub(crate) fn into_marks(self) -> Vec<TxHash> {
        self.0
    }
}

/// The output from building the next batch.
#[derive(Debug)]
pub struct BatchBuilderOutput {
    /// The batch for the worker to propose.
    pub(crate) batch: Batch,
    /// The transactions sealed into the batch, to mark in flight on quorum.
    ///
    /// The pool itself is left untouched: account nonce and state move only on canonical changes,
    /// so workers validate each other's batches off the same canonical tip.
    pub(crate) selected: SelectedForSeal,
    /// Per-sender nonce ranges for all transactions in this batch.
    pub sender_nonce_ranges: SenderNonceRanges,
    /// Whether the batch filled to capacity (gas or bytes), so more candidates certainly remain.
    pub(crate) at_capacity: bool,
}

/// Construct a Rayls batch using the best transactions from the pool.
///
/// Returns the [`BatchBuilderOutput`] and cannot fail. The batch continues to add
/// transactions to the proposed block until either:
/// - accumulated transaction gas limit reached (measured by tx.gas_limit())
/// - max byte size of transactions (measured by tx.size())
///
/// NOTE: it's possible to under utilize resources if users submit transactions
/// with very high gas limits. It's impossible to know the amount of gas a transaction
/// will use without executing it, and the worker does not execute transactions.
#[inline]
pub fn build_batch<P: TxPool>(
    args: BatchBuilderArgs<P>,
    worker_id: WorkerId,
    base_fee: u64,
    seq: u64,
    gas_limit: u64,
) -> BatchBuilderOutput {
    let BatchBuilderArgs { mut pool, beneficiary, epoch } = args;
    let max_size = max_batch_size(epoch);
    let base_fee_per_gas = base_fee;

    // NOTE: this obtains a `read` lock on the tx pool
    // pull best transactions and rely on watch channel to ensure basefee is current
    let mut best_txs = pool.best_transactions();

    // Disable live transaction updates to prevent intra-sender nonce gaps.
    // The default BestTransactions iterator receives new pending transactions via a
    // broadcast channel during iteration. If a transaction arrives whose predecessor
    // is not in the snapshot, it starts a new independent nonce chain, producing
    // batches with non-contiguous nonces that cause nonce_too_high at execution.
    best_txs.no_updates();

    // NOTE: batches always build off the latest finalized block

    // collect data for selected transactions
    let mut total_bytes_size = 0;
    let mut total_possible_gas = 0;
    let mut at_capacity = false;
    let mut transactions = Vec::new();
    let mut mined_transactions = Vec::new();
    let mut blob_transactions = Vec::new();
    let mut sender_nonce_ranges = SenderNonceRanges::new();

    // walk the sorted "best" transactions in the pending pool; they are selected, not executed
    while let Some(pool_tx) = best_txs.next() {
        // skip a transaction already sealed into a batch still in flight, so a stuck inclusion
        // backlog is not re-sealed batch after batch. The skip cannot open a nonce gap: an
        // in-flight tx stays pending, so its descendants still trail it here, and same-authority
        // batches execute in seq order, so the in-flight prefix and this batch stay
        // nonce-contiguous at execution.
        if pool.is_in_flight(pool_tx.hash()) {
            continue;
        }

        // ensure block has capacity (in gas) for this transaction
        if total_possible_gas + pool_tx.gas_limit() > gas_limit {
            // the tx would exceed the batch's gas cap: it and all its dependents are invalid for
            // the rest of this `BestTransactions` iteration
            best_txs.exceeds_gas_limit(&pool_tx, gas_limit);
            debug!(target: "worker::batch_builder", ?pool_tx, "marking tx invalid due to gas constraint");
            // Only a non-empty batch is "at capacity": a tx whose own gas exceeds the whole cap can
            // never fit any batch, so treating it as backlog would spin the builder on it forever.
            at_capacity |= total_possible_gas > 0;
            continue;
        }

        // convert tx to a signed transaction
        //
        // NOTE: `ValidPoolTransaction::size()` is private
        let tx = pool_tx.to_consensus();

        // ignore blob transactions EIP-4844
        if tx.is_eip4844() {
            best_txs.ignore_eip4844(&pool_tx);
            debug!(target: "worker::batch_builder", ?pool_tx, "marking eip4844 tx invalid");
            blob_transactions.push(*tx.hash());
            continue;
        }

        // ensure block has capacity (in bytes) for this transaction
        if total_bytes_size + tx.size() > max_size {
            // the tx would exceed the batch's byte cap: as with the gas branch, it and its
            // dependents are invalid for the rest of this iteration
            best_txs.max_batch_size(&pool_tx, tx.size(), max_size);
            debug!(target: "worker::batch_builder", ?pool_tx, "marking tx invalid due to bytes constraint");
            // As with the gas branch: a tx larger than the whole batch is unbatchable, not backlog.
            at_capacity |= total_bytes_size > 0;
            continue;
        }

        // txs are not executed, so use the gas_limit
        total_possible_gas += tx.gas_limit();
        total_bytes_size += tx.size();

        // track per-sender nonce range
        let sender = pool_tx.sender();
        let nonce = tx.nonce();
        sender_nonce_ranges
            .entry(sender)
            .and_modify(|r| {
                r.min = r.min.min(nonce);
                r.max = r.max.max(nonce);
            })
            .or_insert(NonceRange { min: nonce, max: nonce });

        // append transaction to the batch
        mined_transactions.push(*pool_tx.hash());
        transactions.push(tx.into_inner().encoded_2718().into());
    }

    // batch
    let batch = Batch {
        transactions,
        epoch,
        beneficiary,
        base_fee_per_gas,
        worker_id,
        seq,
        received_at: None,
    };

    // remove any blob transactions that were submitted
    pool.remove_eip4844_txs(blob_transactions);

    // return output
    BatchBuilderOutput {
        batch,
        selected: SelectedForSeal(mined_transactions),
        sender_nonce_ranges,
        at_capacity,
    }
}
