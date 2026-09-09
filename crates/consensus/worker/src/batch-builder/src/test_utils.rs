//! Types for testing only.

use crate::{build_batch, BatchBuilderOutput};
use rayls_execution_evm::{
    new_pool_txn, BestTransactions, InvalidPoolTransactionError, PoolTxn, PoolTxnId,
    SenderIdentifiers, TxPool,
};
use rayls_infrastructure_types::{
    Batch, BatchBuilderArgs, Recovered, TransactionTrait as _, TxHash,
    ETHEREUM_BLOCK_GAS_LIMIT_56BITS, MIN_PROTOCOL_BASE_FEE,
};
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    sync::Arc,
};

/// Attempt to update batch with accurate header information.
///
/// NOTE: this is loosely based on reth's auto-seal consensus
/// NOTE2: this assumes worker 0.
pub fn execute_test_batch(test_batch: &mut Batch) {
    let pool = TestPool::new(&test_batch.transactions);

    let args =
        BatchBuilderArgs { pool, beneficiary: test_batch.beneficiary, epoch: test_batch.epoch };

    let BatchBuilderOutput { batch, .. } =
        build_batch(args, 0, test_batch.base_fee_per_gas, 0, ETHEREUM_BLOCK_GAS_LIMIT_56BITS);
    test_batch.beneficiary = batch.beneficiary;
    // Don't reset base_fee_per_gas, some tests need that value to remain.
}

/// A test pool that ensures every transaction is in the pending pool
#[derive(Default, Clone, Debug)]
struct TestPool {
    transactions: Vec<Arc<PoolTxn>>,
    by_id: BTreeMap<PoolTxnId, Arc<PoolTxn>>,
}

impl TxPool for TestPool {
    fn best_transactions(&self) -> rayls_execution_evm::BestTxns {
        rayls_execution_evm::BestTxns::new_for_test(self.best_transactions_int())
    }
    fn get_pending_base_fee(&self) -> u64 {
        MIN_PROTOCOL_BASE_FEE
    }
    fn remove_eip4844_txs(&mut self, _blobs: Vec<TxHash>) {
        // remove EIP-4844 transactions from the transactions vec and btreemap
        self.transactions.retain(|tx| !tx.is_eip4844());
        self.by_id.retain(|_, tx| !tx.is_eip4844());
    }
}

impl TestPool {
    /// Create a new instance of Self.
    fn new(txs: &[Vec<u8>]) -> Self {
        let mut sender_ids = SenderIdentifiers::default();
        let mut by_id = Vec::with_capacity(txs.len());
        let transactions = txs
            .iter()
            .map(|tx| {
                let ecrecovered: Recovered<_> =
                    rayls_execution_evm::recover_raw_transaction(tx).expect("tx into ecrecovered");
                let nonce = ecrecovered.nonce();
                // add to sender ids
                let id = sender_ids.sender_id_or_create(ecrecovered.signer());
                let transaction = rayls_execution_evm::recover_pooled_transaction(tx)
                    .expect("pooled tx from recovered");

                let transaction_id = PoolTxnId::new(id, nonce);

                let valid_tx = Arc::new(new_pool_txn(transaction, transaction_id));
                // add by id
                by_id.push((transaction_id, valid_tx.clone()));

                valid_tx
            })
            .collect();
        Self { transactions, by_id: by_id.into_iter().collect() }
    }

    fn best_transactions_int(&self) -> Box<dyn BestTransactions<Item = Arc<PoolTxn>>> {
        let mut independent = VecDeque::new();

        // see reth::transaction-pool::pool::pending::update_independents_and_highest_nonces()
        //
        // if there's __no__ ancestor, then this transaction is independent
        // guaranteed because the pool is gapless
        for tx in self.transactions.iter() {
            if tx.transaction_id.unchecked_ancestor().and_then(|id| self.by_id.get(&id)).is_none() {
                independent.push_back(tx.clone())
            }
        }

        Box::new(BestTestTransactions {
            all: self.by_id.clone(),
            independent,
            invalid: Default::default(),
            skip_blobs: true,
        })
    }
}

/// Type for pulling best transactions from the pool.
///
/// An iterator that returns transactions that can be executed on the current state (*best*
/// transactions).
///
/// The [`PendingPool`](crate::pool::pending::PendingPool) contains transactions that *could* all
/// be executed on the current state, but only yields transactions that are ready to be executed
/// now. While it contains all gapless transactions of a sender, it _always_ only returns the
/// transaction with the current on chain nonce.
struct BestTestTransactions {
    /// Contains a copy of _all_ transactions of the pending pool at the point in time this
    /// iterator was created.
    all: BTreeMap<PoolTxnId, Arc<PoolTxn>>,
    /// Transactions that can be executed right away: these have the expected nonce.
    ///
    /// Once an `independent` transaction with the nonce `N` is returned, it unlocks `N+1`, which
    /// then can be moved from the `all` set to the `independent` set.
    independent: VecDeque<Arc<PoolTxn>>,
    /// There might be the case where a yielded transactions is invalid, this will track it.
    invalid: HashSet<TxHash>,
    /// Flag to control whether to skip blob transactions (EIP4844).
    skip_blobs: bool,
}

impl BestTestTransactions {
    /// Mark the transaction and it's descendants as invalid.
    fn mark_invalid(&mut self, tx: &Arc<PoolTxn>) {
        self.invalid.insert(*tx.hash());
    }
}

impl BestTransactions for BestTestTransactions {
    fn mark_invalid(&mut self, tx: &Self::Item, _kind: &InvalidPoolTransactionError) {
        Self::mark_invalid(self, tx)
    }

    fn no_updates(&mut self) {
        // does not need to implement it for tests because tests does not push data while running
    }

    fn skip_blobs(&mut self) {
        self.set_skip_blobs(true);
    }

    fn set_skip_blobs(&mut self, skip_blobs: bool) {
        self.skip_blobs = skip_blobs;
    }
}

impl Iterator for BestTestTransactions {
    type Item = Arc<PoolTxn>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // remove the next independent tx (created with `push_back`)
            let best = self.independent.pop_front()?.clone();
            let hash = best.transaction.transaction().hash();

            // skip transactions that were marked as invalid
            if self.invalid.contains(hash) {
                tracing::debug!(
                    target: "test-txpool",
                    "[{:?}] skipping invalid transaction",
                    hash
                );
                continue;
            }

            // Insert transactions that just got unlocked.
            if let Some(unlocked) = self.all.get(&best.transaction_id.descendant()) {
                self.independent.push_back(unlocked.clone());
            }

            if self.skip_blobs && best.is_eip4844() {
                // blobs should be skipped, marking the as invalid will ensure that no dependent
                // transactions are returned
                self.mark_invalid(&best)
            } else {
                return Some(best);
            }
        }
    }
}
