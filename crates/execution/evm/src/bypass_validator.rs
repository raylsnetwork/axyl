//! A validator wrapper that can bypass full validation for pre-validated transactions.
//!
//! When orphan transactions are re-introduced at epoch boundaries, they have already
//! been validated in the previous epoch. This wrapper short-circuits the expensive
//! per-transaction state validation (and the MPSC channel overhead of the
//! [`TransactionValidationTaskExecutor`]) by pre-populating a map of tx hashes
//! to sender state. Normal (non-orphan) transactions are delegated to the inner
//! validator with zero overhead beyond a single `is_none()` check.

use rayls_infrastructure_types::{Address, TransactionTrait, TxHash, U256};
use reth_primitives_traits::{Account, SealedBlock};
use reth_transaction_pool::{
    validate::ValidTransaction, EthPooledTransaction, PoolTransaction,
    TransactionValidationOutcome, TransactionValidator,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// Cached sender state used to construct pre-validated outcomes.
#[derive(Debug, Clone, Copy)]
struct SenderState {
    balance: U256,
    nonce: u64,
}

/// Map from tx hash → sender state for short-circuiting validation.
type BypassMap = HashMap<TxHash, SenderState>;

/// A [`TransactionValidator`] wrapper that can bypass validation for known-good transactions.
#[derive(Debug)]
pub struct BypassableValidator<V> {
    inner: V,
    /// When `Some`, contains pre-validated tx hashes. The validator will return
    /// `Valid` immediately for any tx whose hash is found here.
    bypass: Arc<Mutex<Option<BypassMap>>>,
}

impl<V: Clone> Clone for BypassableValidator<V> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone(), bypass: Arc::clone(&self.bypass) }
    }
}

impl<V> BypassableValidator<V> {
    /// Wrap an existing validator with bypass capability.
    pub fn new(inner: V) -> (Self, BypassHandle) {
        let bypass = Arc::new(Mutex::new(None));
        let handle = BypassHandle { bypass: Arc::clone(&bypass) };
        (Self { inner, bypass }, handle)
    }
}

/// Handle for populating the bypass map from outside the validator.
///
/// Call [`BypassHandle::activate`] before adding orphan transactions to the pool,
/// then [`BypassHandle::deactivate`] after the insertion completes.
#[derive(Debug, Clone)]
pub struct BypassHandle {
    bypass: Arc<Mutex<Option<BypassMap>>>,
}

impl BypassHandle {
    /// Populate the bypass map with sender state for the given transactions.
    ///
    /// `sender_accounts` maps sender address → (balance, nonce) from a single state snapshot.
    /// All transactions whose senders appear in the map will bypass validation.
    pub fn activate(
        &self,
        transactions: &[EthPooledTransaction],
        sender_accounts: &HashMap<Address, Account>,
    ) {
        let mut map = HashMap::with_capacity(transactions.len());
        for tx in transactions {
            let sender = tx.sender();
            if let Some(account) = sender_accounts.get(&sender) {
                map.insert(
                    *tx.hash(),
                    SenderState { balance: account.balance, nonce: account.nonce },
                );
            }
        }
        *self.bypass.lock().expect("bypass lock poisoned") = Some(map);
    }

    /// Clear the bypass map, restoring normal validation.
    pub fn deactivate(&self) {
        *self.bypass.lock().expect("bypass lock poisoned") = None;
    }
}

impl<V> BypassableValidator<V>
where
    V: TransactionValidator<Transaction = EthPooledTransaction> + 'static,
{
    /// Try to short-circuit validation for a single transaction.
    /// Returns `Some(Valid)` if the tx is in the bypass map, `None` otherwise.
    fn try_bypass(
        &self,
        tx: &EthPooledTransaction,
    ) -> Option<TransactionValidationOutcome<EthPooledTransaction>> {
        let guard = self.bypass.lock().expect("bypass lock poisoned");
        let map = guard.as_ref()?;
        let state = map.get(tx.hash())?;
        // stale nonce - tx was already executed (e.g. included in the boundary block).
        // fall through to normal validation which rejects gracefully instead of panicking.
        if tx.nonce() < state.nonce {
            return None;
        }
        Some(TransactionValidationOutcome::Valid {
            balance: state.balance,
            state_nonce: state.nonce,
            bytecode_hash: None,
            transaction: ValidTransaction::Valid(tx.clone()),
            propagate: false,
            authorities: None,
        })
    }
}

impl<V> TransactionValidator for BypassableValidator<V>
where
    V: TransactionValidator<Transaction = EthPooledTransaction> + 'static,
{
    type Transaction = EthPooledTransaction;
    type Block = V::Block;

    async fn validate_transaction(
        &self,
        origin: reth_transaction_pool::TransactionOrigin,
        transaction: Self::Transaction,
    ) -> TransactionValidationOutcome<Self::Transaction> {
        if let Some(outcome) = self.try_bypass(&transaction) {
            return outcome;
        }
        self.inner.validate_transaction(origin, transaction).await
    }

    async fn validate_transactions(
        &self,
        transactions: impl IntoIterator<
                Item = (reth_transaction_pool::TransactionOrigin, Self::Transaction),
                IntoIter: Send,
            > + Send,
    ) -> Vec<TransactionValidationOutcome<Self::Transaction>> {
        let transactions: Vec<_> = transactions.into_iter().collect();
        // Fast path: no bypass active → delegate entirely (zero overhead).
        // The lock is acquired and released before any .await.
        let is_bypass_active = {
            let guard = self.bypass.lock().expect("bypass lock poisoned");
            guard.is_some()
        };

        if !is_bypass_active {
            return self.inner.validate_transactions(transactions).await;
        }

        // Check which transactions can be bypassed.
        // try_bypass() acquires the lock briefly per call but never across an await.
        let mut results: Vec<Option<TransactionValidationOutcome<Self::Transaction>>> =
            Vec::with_capacity(transactions.len());
        let mut need_validation = Vec::new();
        let mut need_validation_indices = Vec::new();

        for (i, (origin, tx)) in transactions.into_iter().enumerate() {
            if let Some(outcome) = self.try_bypass(&tx) {
                results.push(Some(outcome));
            } else {
                results.push(None);
                need_validation.push((origin, tx));
                need_validation_indices.push(i);
            }
        }

        // Validate remaining transactions through normal pipeline
        if !need_validation.is_empty() {
            let validated = self.inner.validate_transactions(need_validation).await;
            for (idx, outcome) in need_validation_indices.into_iter().zip(validated) {
                results[idx] = Some(outcome);
            }
        }

        results.into_iter().map(|r| r.expect("all slots filled")).collect()
    }

    fn on_new_head_block(&self, new_tip_block: &SealedBlock<Self::Block>) {
        self.inner.on_new_head_block(new_tip_block)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::consensus::TxEip1559;
    use rayls_infrastructure_types::{
        SignableTransaction, Transaction, TransactionSigned, TxKind, B256, MIN_PROTOCOL_BASE_FEE,
    };
    use reth_ethereum_primitives::Block;
    use reth_primitives_traits::{crypto::secp256k1::sign_message, SignedTransaction};
    use reth_transaction_pool::TransactionOrigin;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Mock validator that records delegated tx hashes and returns a configurable outcome.
    #[derive(Debug)]
    struct MockValidator {
        delegated: Arc<Mutex<Vec<TxHash>>>,
        call_count: AtomicUsize,
    }

    impl MockValidator {
        fn new() -> Self {
            Self { delegated: Arc::new(Mutex::new(Vec::new())), call_count: AtomicUsize::new(0) }
        }

        fn delegated_hashes(&self) -> Vec<TxHash> {
            self.delegated.lock().unwrap().clone()
        }

        fn call_count(&self) -> usize {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    impl TransactionValidator for MockValidator {
        type Transaction = EthPooledTransaction;
        type Block = Block;

        async fn validate_transaction(
            &self,
            _origin: TransactionOrigin,
            transaction: Self::Transaction,
        ) -> TransactionValidationOutcome<Self::Transaction> {
            self.delegated.lock().unwrap().push(*transaction.hash());
            self.call_count.fetch_add(1, Ordering::SeqCst);
            TransactionValidationOutcome::Valid {
                balance: U256::from(1_000_000u64),
                state_nonce: transaction.nonce(),
                bytecode_hash: None,
                transaction: ValidTransaction::Valid(transaction),
                propagate: false,
                authorities: None,
            }
        }
    }

    /// Deterministic secret key for test transaction signing.
    fn test_secret() -> B256 {
        B256::from([1u8; 32])
    }

    /// Build a signed, pooled EIP-1559 transaction with the given nonce and signer key.
    fn make_pooled_tx(signer_key: B256, nonce: u64) -> EthPooledTransaction {
        let tx = Transaction::Eip1559(TxEip1559 {
            chain_id: 1,
            nonce,
            max_priority_fee_per_gas: 0,
            max_fee_per_gas: MIN_PROTOCOL_BASE_FEE as u128,
            gas_limit: 21_000,
            to: TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            input: Default::default(),
            access_list: Default::default(),
        });
        let sig = sign_message(signer_key, tx.signature_hash()).expect("sign");
        let signed = TransactionSigned::new_unhashed(tx, sig);
        let recovered = signed.try_into_recovered().expect("recover");
        EthPooledTransaction::try_from_consensus(recovered).expect("pooled")
    }

    /// Recover the sender address for a given secret key (from a dummy tx).
    fn sender_of(key: B256) -> Address {
        make_pooled_tx(key, 0).sender()
    }

    /// Build a sender_accounts map for `activate`.
    fn accounts_map(senders: &[(Address, u64, U256)]) -> HashMap<Address, Account> {
        senders
            .iter()
            .map(|(addr, nonce, balance)| {
                (*addr, Account { nonce: *nonce, balance: *balance, bytecode_hash: None })
            })
            .collect()
    }

    /// tx.nonce == state.nonce is the normal case - should bypass.
    #[tokio::test]
    async fn exact_nonce_bypasses() {
        let key = test_secret();
        let sender = sender_of(key);
        let tx = make_pooled_tx(key, 10); // nonce 10

        let mock = MockValidator::new();
        let (validator, handle) = BypassableValidator::new(mock);

        let sender_accounts = accounts_map(&[(sender, 10, U256::from(500u64))]);
        handle.activate(std::slice::from_ref(&tx), &sender_accounts);

        let outcome = validator.validate_transaction(TransactionOrigin::Local, tx).await;

        // bypassed - inner validator should NOT have been called
        assert!(validator.inner.delegated_hashes().is_empty());
        assert!(outcome.is_valid());
        if let TransactionValidationOutcome::Valid { state_nonce, balance, .. } = outcome {
            assert_eq!(state_nonce, 10);
            assert_eq!(balance, U256::from(500u64));
        }
    }

    /// tx hash not in bypass map - delegates to inner.
    #[tokio::test]
    async fn tx_not_in_map_delegates() {
        let key = test_secret();
        let sender = sender_of(key);
        let known_tx = make_pooled_tx(key, 1);
        let unknown_tx = make_pooled_tx(key, 2); // different nonce = different hash
        let unknown_hash = *unknown_tx.hash();

        let mock = MockValidator::new();
        let (validator, handle) = BypassableValidator::new(mock);

        // only known_tx is in the map
        let sender_accounts = accounts_map(&[(sender, 0, U256::from(1_000u64))]);
        handle.activate(&[known_tx], &sender_accounts);

        let outcome = validator.validate_transaction(TransactionOrigin::Local, unknown_tx).await;

        assert_eq!(validator.inner.delegated_hashes(), vec![unknown_hash]);
        assert!(outcome.is_valid());
    }

    /// bypass map is None (deactivated) - all txs go to inner.
    #[tokio::test]
    async fn bypass_inactive_delegates() {
        let key = test_secret();
        let tx = make_pooled_tx(key, 0);
        let tx_hash = *tx.hash();

        let mock = MockValidator::new();
        let (validator, _handle) = BypassableValidator::new(mock);

        // never activate - bypass map stays None
        let outcome = validator.validate_transaction(TransactionOrigin::Local, tx).await;

        assert_eq!(validator.inner.delegated_hashes(), vec![tx_hash]);
        assert!(outcome.is_valid());
    }

    /// activate populates the map, deactivate clears it.
    #[tokio::test]
    async fn activate_deactivate_lifecycle() {
        let key = test_secret();
        let sender = sender_of(key);
        let tx = make_pooled_tx(key, 5);

        let mock = MockValidator::new();
        let (validator, handle) = BypassableValidator::new(mock);

        // activate - should bypass
        let sender_accounts = accounts_map(&[(sender, 5, U256::from(1_000u64))]);
        handle.activate(std::slice::from_ref(&tx), &sender_accounts);

        let outcome = validator.validate_transaction(TransactionOrigin::Local, tx.clone()).await;
        assert!(outcome.is_valid());
        assert!(validator.inner.delegated_hashes().is_empty(), "should bypass when active");

        // deactivate - should delegate
        handle.deactivate();

        let outcome = validator.validate_transaction(TransactionOrigin::Local, tx).await;
        assert!(outcome.is_valid());
        assert_eq!(validator.inner.call_count(), 1, "should delegate after deactivate");
    }

    /// validate_transactions with empty input returns empty output.
    #[tokio::test]
    async fn empty_transactions_vec() {
        let mock = MockValidator::new();
        let (validator, _handle) = BypassableValidator::new(mock);

        let outcomes: Vec<TransactionValidationOutcome<EthPooledTransaction>> =
            validator.validate_transactions(Vec::new()).await;

        assert!(outcomes.is_empty());
        assert_eq!(validator.inner.call_count(), 0);
    }

    /// activate with tx whose sender is NOT in sender_accounts - tx not added to bypass map.
    #[tokio::test]
    async fn account_not_in_sender_map() {
        let key_a = test_secret();
        let key_b = B256::from([2u8; 32]);
        let tx_b = make_pooled_tx(key_b, 0);
        let tx_b_hash = *tx_b.hash();

        let sender_a = sender_of(key_a);

        let mock = MockValidator::new();
        let (validator, handle) = BypassableValidator::new(mock);

        // only sender_a is in accounts, but tx is from sender_b
        let sender_accounts = accounts_map(&[(sender_a, 0, U256::from(1_000u64))]);
        handle.activate(std::slice::from_ref(&tx_b), &sender_accounts);

        let outcome = validator.validate_transaction(TransactionOrigin::Local, tx_b).await;

        // tx_b's sender not in map, so it must be delegated
        assert_eq!(validator.inner.delegated_hashes(), vec![tx_b_hash]);
        assert!(outcome.is_valid());
    }

    /// Regression test for the "Invalid transaction" panic in reth's pool.
    ///
    /// Exercises all bypass invariants in a single batch:
    /// 1. Stale nonce (tx.nonce < state.nonce) must delegate to inner validator
    /// 2. Valid nonce (tx.nonce == state.nonce) must bypass
    /// 3. Batch routing must preserve order and split correctly
    /// 4. No Valid outcome may have state_nonce > tx.nonce (reth pool assert)
    #[tokio::test]
    async fn stale_nonce_mixed_batch() {
        let key = test_secret();
        let sender = sender_of(key);
        let stale_tx = make_pooled_tx(key, 5); // stale: 5 < 10
        let valid_tx = make_pooled_tx(key, 10); // valid: 10 == 10
        let stale_hash = *stale_tx.hash();

        let mock = MockValidator::new();
        let (validator, handle) = BypassableValidator::new(mock);

        let sender_accounts = accounts_map(&[(sender, 10, U256::from(1_000u64))]);
        handle.activate(&[stale_tx.clone(), valid_tx.clone()], &sender_accounts);

        let batch =
            vec![(TransactionOrigin::Local, stale_tx), (TransactionOrigin::Local, valid_tx)];
        let outcomes = validator.validate_transactions(batch).await;

        assert_eq!(outcomes.len(), 2);
        assert!(outcomes[0].is_valid());
        assert!(outcomes[1].is_valid());

        // stale tx must delegate, valid tx must bypass
        assert_eq!(
            validator.inner.delegated_hashes(),
            vec![stale_hash],
            "stale tx must delegate, valid tx must bypass"
        );

        // reth pool invariant: no Valid outcome may have state_nonce > tx.nonce
        for outcome in &outcomes {
            if let TransactionValidationOutcome::Valid { state_nonce, transaction, .. } = outcome {
                let tx_nonce = transaction.transaction().nonce();
                assert!(
                    *state_nonce <= tx_nonce,
                    "INVARIANT VIOLATED: state_nonce ({}) > tx.nonce ({}) would panic reth pool",
                    state_nonce,
                    tx_nonce
                );
            }
        }
    }
}
