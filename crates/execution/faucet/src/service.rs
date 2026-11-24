//! Faucet rpc endpoint service.
//!
//! Service for creating and signing a transaction.
//! The service submits transactions directly to the
//! transaction pool to initiate a transfer to a requesting
//! address if the address hasn't received from the faucet
//! wallet within the time period.

use crate::{Drip, FaucetWallet, GoogleKMSClient, Secp256k1PubKeyBytes, TaskSpawner};
use futures::StreamExt;
use gcloud_sdk::{
    google::cloud::kms::v1::{
        digest::Digest, key_management_service_client::KeyManagementServiceClient,
        AsymmetricSignRequest, Digest as KMSDigest,
    },
    GoogleApi,
};
use humantime::format_duration;
use lru_time_cache::LruCache;
use rayls_execution_evm::{reth_env::RethEnv, EthPooledTransaction, WorkerTxPool};
use rayls_infrastructure_types::{
    Address, EthSignature, SignableTransaction as _, SolType, Transaction, TransactionSigned,
    TransactionTrait as _, TxEip1559, TxHash, TxKind, B256, U256,
};
use reth::{
    primitives::SignedTransaction,
    rpc::server_types::eth::{EthApiError, EthResult, RpcInvalidTransactionError},
};
use reth_transaction_pool::{PoolTransaction, TransactionEvent};
use secp256k1::{
    ecdsa::{RecoverableSignature, RecoveryId, Signature},
    Message, SECP256K1,
};
use std::{
    future::Future,
    pin::Pin,
    task::{ready, Context, Poll},
    time::{Duration, SystemTime},
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, warn};

/// The mined transaction information for the faucet to track.
///
/// This struct is used when a subscribed event is received for a transaction that is mined from the
/// transaction pool. The information is used to track faucet state.
#[derive(Copy, Clone, Debug)]
pub(crate) struct MinedTxInfo {
    /// The address that received the faucet's drip.
    user: Address,
    /// The contract associated with the digital asset that the faucet dripped to the address.
    contract: Address,
}

impl MinedTxInfo {
    /// Create a new instance of Self.
    fn new(user: Address, contract: Address) -> Self {
        Self { user, contract }
    }
}

/// Service for managing requests.
///
/// The faucet receives an address from the RPC, checks the LRU cache,
/// and then submits a transaction (or returns an error). The faucet is a
/// direct address -> address transfer. The faucet address is seeded in genesis.
pub(crate) struct FaucetService<Tasks> {
    /// The faucet contract's address.
    ///
    /// The value is used to send call data to the contract that mints stablecoins.
    pub(crate) faucet_contract: Address,
    /// The channel between the RPC and the faucet.
    ///
    /// The channel contains:
    /// - the receiving user's address
    /// - an optional contract address (when requesting stablecoins)
    /// - the oneshot channel to return the result
    pub(crate) request_rx:
        ReceiverStream<(Address, Option<Address>, oneshot::Sender<EthResult<TxHash>>)>,
    /// Database impl for retrieving blockchain data.
    pub(crate) reth_env: RethEnv,
    /// The pool for submitting transactions.
    pub(crate) pool: WorkerTxPool,
    /// The cache for verifying an address hasn't exceeded time-based request limit.
    ///
    /// The cache maps a user's address with the contract's address to the time of the request.
    ///
    /// Users request faucet transfers for native tokens (zero address) and
    /// stablecoin tokens (contract address) at specific times.
    /// NOTE: only transactions that were successfully mined are included in this 24-hr cache.
    pub(crate) success_cache: LruCache<(Address, Address), SystemTime>,
    /// Short-lived pending cache.
    ///
    /// This cache is to prevent quick calls before a request has had time to reach consensus. The
    /// cache is much shorter so a user can re-request if a faucet transaction failed.
    pub(crate) pending_cache: LruCache<(Address, Address), SystemTime>,
    /// The chain id for constructing transactions.
    pub(crate) chain_id: u64,
    /// The amount of time the LRU cache retains an address (specified in `FaucetConfig`).
    pub(crate) wait_period: Duration,
    /// The type that can spawn tasks onto the runtime.
    pub(crate) executor: Tasks,
    /// The transaction signer's information.
    pub(crate) wallet: FaucetWallet,
    /// Sending half of the cache channel.
    ///
    /// The user's address and contract address are sent through this channel
    /// then added to the LRU cache.
    pub(crate) add_to_success_cache_tx: mpsc::Sender<MinedTxInfo>,
    /// Receiving half of the cache channel.
    ///
    /// Addresses received on this channel are added to the LRU cache.
    pub(crate) update_success_cache_rx: mpsc::Receiver<MinedTxInfo>,
    /// The nonce for this faucet as tracked through the highest submitted transaction nonce.
    ///
    /// The faucet service checks for the highest nonce in the transaction pool and if needd, in
    /// the database. However, the faucet also needs to nonce state for transactions that are
    /// pending in batches.
    ///
    /// The account nonce read from the database returns the account's CURRENT nonce.
    pub(crate) next_nonce: u64,
}

impl<Tasks> FaucetService<Tasks>
where
    Tasks: TaskSpawner + Clone + 'static,
{
    /// Calculate when the wait period is over
    fn calc_wait_period(&self, last_transfer: &SystemTime) -> EthResult<Duration> {
        // calc when the address is removed from cache
        let end =
            last_transfer.checked_add(self.wait_period).ok_or(EthApiError::InternalEthError)?;

        // calc the remaining duration
        end.duration_since(SystemTime::now()).map_err(|e| EthApiError::InvalidParams(e.to_string()))
    }

    /// Process the transfer request for the faucet service.
    ///
    /// This method intentionally uses `&mut self` to ensure nonce is incremented correctly.
    fn process_transfer_request(
        &mut self,
        user: Address,
        contract: Address,
        reply: oneshot::Sender<EthResult<TxHash>>,
    ) -> EthResult<()> {
        // create transaction based on request type
        let transaction = self.create_transaction_to_sign(user, contract)?;
        // request signature from kms
        let kms_name = self.wallet.name();
        let public_key = self.wallet.kms_public_key();
        let chain_id = self.chain_id;
        let pool = self.pool.clone();
        let add_to_success_cache = self.add_to_success_cache_tx.clone();

        debug!(target: "faucet", ?transaction, "processing transfer request");

        // update tracked nonce
        let nonce = transaction.nonce();
        self.next_nonce = nonce + 1;

        // request signature and submit to txpool
        self.executor.spawn_task(Box::pin(async move {
            let digest = transaction.signature_hash();
            let response =
                Self::request_kms_signature(kms_name, digest, chain_id, public_key).await;

            // submit tx to pool
            match response {
                Ok(signature) => {
                    let tx_for_pool = TransactionSigned::new_unhashed(transaction, signature);
                    let res =
                        submit_transaction(pool, tx_for_pool, add_to_success_cache, user, contract)
                            .await;
                    // reply to rpc
                    let _ = reply.send(res);
                }
                Err(e) => error!(target: "faucet", ?e, "Error requesting KMS signature"),
            }
        }));

        Ok(())
    }

    /// Create a EIP1559 transaction with max fee per gas set to 1 RLS.
    ///
    /// This method intentionally uses `&mut self` to ensure nonce is incremented correctly.
    /// TODO: use AtomicU64 for thread safe nonce increments
    fn create_transaction_to_sign(&self, to: Address, contract: Address) -> EthResult<Transaction> {
        let nonce = self.next_nonce()?;
        let gas_price = self.gas_price();

        // RLFaucet.sol will drip native RLS when called with RPC param `contract == address(0)`
        let transaction = {
            // hardcoded selector: keccak256("drip(address,address)")[0..4] == 0xeb3839a7
            let selector = [235, 56, 57, 167];
            // encode params
            let params: Vec<u8> = Drip::abi_encode_params(&(&contract, &to));
            // combine params with selector to create input for contract call
            let input = [&selector, &params[..]].concat().into();

            // stablecoin transaction - call faucet contract
            Transaction::Eip1559(TxEip1559 {
                chain_id: self.chain_id,
                nonce,
                max_priority_fee_per_gas: gas_price,
                max_fee_per_gas: gas_price,
                gas_limit: 1_000_000,
                to: TxKind::Call(self.faucet_contract),
                value: U256::ZERO,
                input,
                access_list: Default::default(),
            })
        };

        debug!(target: "faucet", ?transaction);
        Ok(transaction)
    }

    /// Calculate the next nonce to use.
    ///
    /// This method looks at the transaction pool first because the pool is gapless. If no
    /// faucet transactions in the pool, compare the highest nonce in the database and the
    /// highest nonce the faucet has seen. It is still possible for a transaction to fail after it
    /// was mined. This would require restarting the faucet service.
    ///
    /// The account nonce read from the database returns the account's CURRENT nonce.
    fn next_nonce(&self) -> EthResult<u64> {
        let address = self.wallet.address;
        debug!(?address, "Faucet address");
        // lookup transactions in pool
        let address_txs = self.pool.get_transactions_by_sender(address);

        // use highest nonce in tx pool bc this is most recent transaction
        if !address_txs.is_empty() {
            // get max transaction with the highest nonce
            let highest_nonce_tx = address_txs
                .into_iter()
                .reduce(|accum, item| {
                    if item.transaction.nonce() > accum.transaction.nonce() {
                        item
                    } else {
                        accum
                    }
                })
                .ok_or(EthApiError::InvalidParams(
                    "Failed to reduce the highest nonce transaction in the pool".to_string(),
                ))?;

            let tx_count = highest_nonce_tx
                .transaction
                .nonce()
                .checked_add(1)
                .ok_or(RpcInvalidTransactionError::NonceMaxValue)?;
            return Ok(tx_count);
        }

        // lookup account nonce in db and compare it last known tx nonce mined by worker
        let state = self.reth_env.latest()?;
        let db_account_nonce = state.account_nonce(&address)?.unwrap_or_default();
        debug!(target: "faucet", ?db_account_nonce, tracked_nonce=?self.next_nonce, "comparing faucet nonces");
        let highest_nonce = std::cmp::max(db_account_nonce, self.next_nonce);

        Ok(highest_nonce)
    }

    /// Taken from rpc/src/eth/api/fees.rs
    ///
    /// Estimate gas price for legacy transactions
    fn gas_price(&self) -> u128 {
        let pool_info = self.pool.block_info();
        debug!(target: "faucet", ?pool_info, "checking gas price");
        pool_info.pending_basefee.into()
    }

    /// Send a request to Google KMS and convert it to EVM compatible.
    async fn request_kms_signature(
        name: String,
        digest: B256,
        chain_id: u64,
        public_key_bytes: Secp256k1PubKeyBytes,
    ) -> eyre::Result<EthSignature> {
        // create client
        //
        // note: this is reusable, but challenging to figure out how
        // to call the .await from inside sync function (spawn, create, etc.)
        let client: GoogleKMSClient = GoogleApi::from_function(
            KeyManagementServiceClient::new,
            "https://cloudkms.googleapis.com",
            None,
        )
        .await?;

        // create message from slice before consuming digest
        // this is needed to calculate `v` below
        let message = Message::from_digest(digest.0);

        // assemble digest for signature
        let digest = Some(Digest::Sha256(digest.0.to_vec()));
        let digest = Some(KMSDigest { digest });
        let signed_data = client
            .get()
            .asymmetric_sign(AsymmetricSignRequest {
                name: name.clone(),
                digest,
                ..Default::default()
            })
            .await?
            .into_inner()
            .signature;

        debug!(target: "faucet", ?signed_data, "signed data returned from kms client");

        // ensure signature is compatible with ethereum (see EIP-155)
        let mut signature = Signature::from_der(&signed_data)?;
        signature.normalize_s();
        // retrieve r, s, and v values for EthSignature
        let compact = signature.serialize_compact();

        debug!(target: "faucet", ?compact, "compact serialized signature");

        // calculate `v` for eth signature's `y_parity`
        let y_parity = Self::calculate_v(message, chain_id, &compact, &public_key_bytes)?;

        // r and s are 32 bytes each
        let (r, s) = compact.split_at(32);

        let r = U256::from_be_slice(r);
        let s = U256::from_be_slice(s);
        let eth_signature = EthSignature::new(r, s, y_parity);

        Ok(eth_signature)
    }

    /// Try both recovery ids (0 or 1) to find the correct v value
    ///
    /// NOTE: this compares the compressed public keys for convenience
    ///       uncompressed approach is commented out
    ///
    /// How and why this works:
    /// Calculating the v value from r, s, the original hash, and the public key, especially for
    /// use in Ethereum transactions, involves trying to recover the public key from the
    /// signature and comparing it to the known public key to determine the correct v value.
    /// Ethereum uses the v value to encode the recovery id and some blockchain-specific
    /// information (like chain id in EIP-155). In Ethereum, v can typically be 27 or 28
    /// (or higher if adjusted for chain id as per EIP-155), corresponding to the two possible
    /// recovery ids (0 or 1) that can result from the ECDSA signature recovery process.
    /// In Ethereum, a signature consists of three components: r, s, and v. The r and s values
    /// are part of the ECDSA signature, and the v value is a recovery id that indicates which of
    /// the two possible public keys is the correct one (since a signature does not uniquely
    /// identify a public key).
    ///
    /// The signature from Google Cloud KMS using the secp256k1 curve is in DER format.
    /// This 64-byte format consists of the r and s components of the ECDSA signature, each being
    /// 32 bytes long.
    ///
    /// The `v` value is Ethereum-specific and must be calculated to use the KMS signature with
    /// EVM. The `v` value can be either 27 or 28 (or 35 or 36 when adding the chain ID to
    /// prevent replay attacks on different networks as per EIP-155).
    ///
    /// The `v` value in the signature not only indicates the chain ID (for replay
    /// protection) but also encodes information about the "y parity" of the point on the
    /// elliptic curve that corresponds to the public key recovered from the signature. The y
    /// parity (odd or even) is a critical component used to recover the correct public key from
    /// a given signature (r, s) and message hash.
    ///
    /// Relationship Between y_odd_parity and v:
    /// The v value for Ethereum signatures traditionally starts at 27 (or 28), where the
    /// difference (27 or 28) essentially encodes the y parity.
    ///
    /// Specifically:
    /// - If v = 35 + 2*chain_id), the y parity is even.
    /// - If v = 36 + 2*chain_id), the y parity is odd.
    ///
    /// The y_odd_parity is false if v is 35 (even y) and true if v is 36 (odd y).
    fn calculate_v(
        message: Message,
        chain_id: u64,
        compact_signature: &[u8; 64],
        public_key_bytes: &Secp256k1PubKeyBytes, // [u8; 33]
    ) -> EthResult<bool> {
        // recovery id must be 0 or 1
        for recovery_id in [0, 1] {
            let recid = RecoveryId::try_from(recovery_id).expect("Invalid recovery id");
            debug!(target: "faucet", recovery_id, ?recid, "recovery id");
            let recoverable_signature =
                RecoverableSignature::from_compact(compact_signature, recid).map_err(|e| {
                    EthApiError::InvalidParams(format!("failed to recover signature: {e}"))
                })?;

            debug!(target: "faucet", ?recoverable_signature, "recovered signature");

            let recovery_result = SECP256K1.recover_ecdsa(message, &recoverable_signature);
            debug!(target: "faucet", ?recovery_result, "attempt to recover ecdsa");

            if let Ok(recovered_key) = recovery_result {
                debug!(target: "faucet", ?recovered_key, "recovered ecdsa");
                let recovered_pubkey = recovered_key.serialize();
                debug!(target: "faucet", ?recovered_pubkey, "recovered pubkey");
                debug!(target: "faucet", ?public_key_bytes, "pubic key bytes");
                if recovered_pubkey == public_key_bytes.as_ref() {
                    // v is found when the recovered key matches the known public key
                    //
                    // calculate v based on EIP-155
                    let v = recovery_id as u64 + chain_id * 2 + 35;
                    let y_odd_parity = v.is_multiple_of(2);
                    return Ok(y_odd_parity);
                }
            }
        }

        debug!(target: "faucet", "failed to recover v - returning error");
        Err(EthApiError::FailedToDecodeSignedTransaction)
    }
}

impl<Tasks> Future for FaucetService<Tasks>
where
    Tasks: TaskSpawner + Clone + 'static,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // listen for cache updates
            while let Poll::Ready(Some(MinedTxInfo { user, contract })) =
                this.update_success_cache_rx.poll_recv(cx)
            {
                // insert user's address and contract address into LRU cache
                this.success_cache.insert((user, contract), SystemTime::now());
            }

            match ready!(this.request_rx.poll_next_unpin(cx)) {
                None => {
                    unreachable!("faucet request_rx can't close - always listening for addresses from the rpc")
                }
                Some((user_address, contract, reply)) => {
                    // assign token address for checking LRU cache
                    let contract_address = if let Some(address) = contract {
                        // stablecoin transfer
                        address
                    } else {
                        // native token transfer
                        Address::ZERO
                    };

                    // check the pending cache for user's address
                    //
                    // use `::peek` so cache timer doesn't reset
                    if let Some(time) = this.pending_cache.peek(&(user_address, contract_address)) {
                        // return remaining time if address combo is still cached
                        let wait_period_over = this.calc_wait_period(time);
                        let error = match wait_period_over {
                            Ok(time) => {
                                // trim off ms, us, and ns
                                let human_readable =
                                    format_duration(Duration::new(time.as_secs(), 0));
                                let msg = format!("Wait period over at: {human_readable}");
                                Err(EthApiError::InvalidParams(msg))
                            }
                            Err(e) => Err(e),
                        };

                        // return the error and check the next request
                        let _ = reply.send(error);
                        continue;
                    }

                    // check the longer-lived success cache for user's address
                    // use `::peek` so cache timer doesn't reset
                    if let Some(time) = this.success_cache.peek(&(user_address, contract_address)) {
                        // return remaining time if address combo is still cached
                        let wait_period_over = this.calc_wait_period(time);
                        let error = match wait_period_over {
                            Ok(time) => {
                                // trim off ms, us, and ns
                                let human_readable =
                                    format_duration(Duration::new(time.as_secs(), 0));
                                let msg = format!("Wait period over at: {human_readable}");
                                Err(EthApiError::InvalidParams(msg))
                            }
                            Err(e) => Err(e),
                        };

                        // return the error and check the next request
                        let _ = reply.send(error);
                        continue;
                    }

                    // add request to short-lived pending cache if not found in either cache
                    this.pending_cache.insert((user_address, contract_address), SystemTime::now());

                    // user's request not in either cache - process request
                    if let Err(e) =
                        this.process_transfer_request(user_address, contract_address, reply)
                    {
                        error!(target: "faucet", ?e, "Error creating faucet transaction")
                    }
                }
            }
        }
    }
}

/// Rayls: Submit signed transaction to pool and subscribe to mining events.
async fn submit_transaction(
    pool: WorkerTxPool,
    tx: TransactionSigned,
    add_to_success_cache: mpsc::Sender<MinedTxInfo>,
    user: Address,
    contract: Address,
) -> EthResult<TxHash> {
    let recovered =
        tx.try_into_recovered().map_err(|_| EthApiError::InvalidTransactionSignature)?;
    let pool_tx = EthPooledTransaction::try_from_consensus(recovered)
        .map_err(|e| EthApiError::InvalidParams(format!("transaction conversion error: {e}")))?;
    let mut tx_events = pool.add_transaction_and_subscribe_local(pool_tx).await?;

    let tx_hash = tx_events.hash();
    let mined_tx_info = MinedTxInfo::new(user, contract);

    // Spawn task to listen for mining event, then update lru cache.
    // MEMORY SAFETY: This task is unmanaged but has:
    // - A 2-minute timeout to ensure eventual cleanup
    // - Early exit on final events (mined, replaced, discarded)
    // - The tx_events stream is dropped when the task exits, releasing pool references
    tokio::task::spawn(async move {
        // Reduced timeout from 5 minutes to 2 minutes to limit resource holding.
        // Faucet transactions should be mined within seconds under normal conditions.
        const MINING_TIMEOUT_SECS: u64 = 120;

        let timeout_result = tokio::time::timeout(
            std::time::Duration::from_secs(MINING_TIMEOUT_SECS),
            async {
                // Process events until tx mined or final event received.
                // The loop exits on any terminal condition to release resources.
                while let Some(event) = tx_events.next().await {
                    debug!(target: "faucet", ?event, "tx event received");
                    match event {
                        TransactionEvent::Mined(block_hash) => {
                            debug!(target: "faucet", ?block_hash, ?mined_tx_info, "successfully mined");
                            let _ = add_to_success_cache.send(mined_tx_info).await;
                            return; // Exit task immediately on success
                        }
                        _ => {}
                    }

                    // Exit on any final event (replace, discard, mined)
                    if event.is_final() {
                        warn!(target: "faucet", "faucet transaction did not get mined: {event:?}");
                        return; // Exit task to release resources
                    }
                }
            },
        )
        .await;

        if timeout_result.is_err() {
            warn!(target: "faucet", "faucet transaction mining subscription timed out after {MINING_TIMEOUT_SECS}s");
        }
        // tx_events stream is dropped here, releasing pool subscription
    });

    Ok(tx_hash)
}
