// SPDX-License-Identifier: BUSL-1.1
//! Faucet components needed for the faucet to run.
//!
//! Includes config and cache for bridging rpc and faucet service.
//!
//! The faucet adds a method to the worker's RPC
//! to transfer a specified amount of RLS to a wallet based on
//! the faucet configuration. The process is automated and requests
//! are limited by a time-based LRU also specified in the faucet config.
//!
//! WARNING: DO NOT ENABLE THIS FEATURE ON MAINNET.

use gcloud_sdk::{
    google::cloud::kms::v1::key_management_service_client::KeyManagementServiceClient, GoogleApi,
    GoogleAuthMiddleware,
};
use lru_time_cache::LruCache;
use rayls_execution_evm::{reth_env::RethEnv, WorkerTxPool};
use rayls_infrastructure_types::{Address, TxHash};
use reth::rpc::server_types::eth::{EthApiError, EthResult};
use reth_tasks::{TaskSpawner, TokioTaskExecutor};
use secp256k1::constants::PUBLIC_KEY_SIZE;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
mod cli_ext;
mod rpc_ext;
mod service;
pub use cli_ext::{parse_u256_from_decimal_value, FaucetArgs};
pub use rpc_ext::{FaucetRpcExt, FaucetRpcExtApiServer};
pub(crate) use service::FaucetService;

/// Client to send API requests to Google KMS.
pub type GoogleKMSClient = GoogleApi<KeyManagementServiceClient<GoogleAuthMiddleware>>;
/// Serialized public key in bytes: `[u8; 33]`
pub type Secp256k1PubKeyBytes = [u8; PUBLIC_KEY_SIZE];
/// The abi encoded type parameters for the drip method
/// of the faucet contract deployed at contract address.
/// pub for integration test
pub type Drip = rayls_infrastructure_types::sol! { (address, address) };

/// Configure the faucet with a wait period between transfers and the amount of RLS to transfer.
#[derive(Debug)]
pub struct FaucetConfig {
    /// The amount of time recipients must wait between transfers
    /// specified in seconds.
    pub wait_period: Duration,
    /// The chain id
    pub chain_id: u64,
    /// Sensitive information regarding the wallet hot-signing transactions
    pub wallet: FaucetWallet,
    /// Onchain faucet contract address for testing
    /// The faucet manages the stablecoin and native token drip amounts
    /// as well as whether or not a given stablecoin or the native token is enabled
    /// for drips and for the frontend to query
    pub contract_address: Address,
}

/// The account details used by the faucet to create and sign transactions.
///
/// The FaucetWallet sends a request to Google KMS service for a signature.
#[derive(Debug)]
pub struct FaucetWallet {
    /// The faucet's address.
    ///
    /// Used for verifying nonces and estimating gas.
    pub address: Address,

    /// The faucet's compressed (serialized) public key as bytes.
    ///
    /// The key is serialized as a byte-encoded pair of values. In compressed form the y-coordinate
    /// is represented by only a single bit, as x determines it up to one bit.
    ///
    /// Used for creating transactions signed by Google KMS.
    pub public_key_bytes: Secp256k1PubKeyBytes,

    /// The "name" used by Google KMS to identify the key.
    ///
    /// The key needs to be "global" and in the format:
    /// "projects/{}/locations/{}/keyRings/{}/cryptoKeys/{}/cryptoKeyVersions/{}".
    pub name: String,
}

impl FaucetWallet {
    /// Clone the Google KMS "name".
    ///
    /// The "name" refers to the cloud KMS.
    pub fn name(&self) -> String {
        self.name.clone()
    }
    /// Google KMS serialized public key (bytes).
    pub fn kms_public_key(&self) -> Secp256k1PubKeyBytes {
        self.public_key_bytes
    }
}

/// Maximum number of pending faucet requests to prevent DoS via channel overflow.
const MAX_FAUCET_REQUEST_CHANNEL_SIZE: usize = 1024;

/// Maximum number of pending cache update messages.
const MAX_CACHE_UPDATE_CHANNEL_SIZE: usize = 256;

/// Provides async access to the cached addresses.
///
/// This is the frontend for the async caching service which manages cached data
/// on a different task.
#[derive(Debug)]
pub(crate) struct Faucet {
    /// Channel to service task.
    to_service: mpsc::Sender<(Address, Option<Address>, oneshot::Sender<EthResult<TxHash>>)>,
}

impl Faucet {
    /// Create and return both cache's frontend and the time bound service.
    fn create<Tasks>(
        reth_env: RethEnv,
        pool: WorkerTxPool,
        executor: Tasks,
        config: FaucetConfig,
    ) -> (Self, FaucetService<Tasks>) {
        // Use bounded channel to prevent DoS via request flooding
        let (to_service, rx) = mpsc::channel(MAX_FAUCET_REQUEST_CHANNEL_SIZE);
        let FaucetConfig { wait_period, chain_id, wallet, contract_address } = config;

        // Construct an `LruCache` of `<String, SystemTime>`s, limited by 24hr expiry time
        let success_cache = LruCache::with_expiry_duration(wait_period);

        // short time-based LRU cache to stop duplicate requests while consensus is being reached
        // NOTE: 10s chosen bc defaults primary header delay - this should be configurable
        let pending_cache = LruCache::with_expiry_duration(Duration::from_secs(10));
        // Use bounded channel for cache updates to prevent memory growth
        let (add_to_success_cache_tx, update_success_cache_rx) =
            mpsc::channel(MAX_CACHE_UPDATE_CHANNEL_SIZE);

        let service = FaucetService {
            faucet_contract: contract_address,
            request_rx: ReceiverStream::new(rx),
            reth_env,
            pool,
            success_cache,
            pending_cache,
            chain_id,
            wait_period,
            executor,
            wallet,
            add_to_success_cache_tx,
            update_success_cache_rx,
            next_nonce: 0, // start at 0 - service checks db
        };
        let faucet = Self { to_service };
        (faucet, service)
    }

    /// Creates a new async LRU backed cache service task and spawns it to a new task via
    /// [tokio::spawn].
    ///
    /// See also [Self::spawn_with]
    pub(crate) fn spawn(reth_env: RethEnv, pool: WorkerTxPool, config: FaucetConfig) -> Self {
        Self::spawn_with(reth_env, pool, config, TokioTaskExecutor::default())
    }

    /// Creates a new async LRU backed cache service task and spawns it to a new task.
    pub(crate) fn spawn_with<Tasks>(
        reth_env: RethEnv,
        pool: WorkerTxPool,
        config: FaucetConfig,
        executor: Tasks,
    ) -> Self
    where
        Tasks: TaskSpawner + Clone + 'static,
    {
        let (this, service) = Self::create(reth_env, pool, executor.clone(), config);

        executor.spawn_critical_task("faucet cache", Box::pin(service));
        this
    }

    /// Requests a new transfer from faucet wallet to an address.
    pub(crate) async fn handle_request(
        &self,
        address: Address,
        contract: Option<Address>,
    ) -> EthResult<TxHash> {
        let (tx, rx) = oneshot::channel();
        self.to_service
            .send((address, contract, tx))
            .await
            .map_err(|_| EthApiError::InvalidParams("faucet service unavailable".to_string()))?;
        rx.await.map_err(|e| EthApiError::InvalidParams(e.to_string())).and_then(|res| res)
    }
}

#[cfg(test)]
mod tests {
    use ecdsa::elliptic_curve::{pkcs8::DecodePublicKey as _, sec1::ToEncodedPoint};
    use gcloud_sdk::{
        google::cloud::kms::v1::{
            digest::Digest, key_management_service_client::KeyManagementServiceClient,
            AsymmetricSignRequest, Digest as KMSDigest, GetPublicKeyRequest,
        },
        GoogleApi, GoogleAuthMiddleware, GoogleEnvironment,
    };
    use k256::PublicKey as PubKey;
    use rayls_infrastructure_types::{
        keccak256, public_key_to_address, EthSignature, RaylsNetwork, U256,
    };
    use secp256k1::{
        ecdsa::{RecoverableSignature, RecoveryId, Signature},
        Message, PublicKey, SECP256K1,
    };
    use tokio::sync::oneshot;
    use tracing::debug;

    /// Test the response from the following request to Google Cloud KMS
    /// ```rust
    /// let kms_client: GoogleApi<KeyManagementServiceClient<GoogleAuthMiddleware>> =
    ///     GoogleApi::from_function(
    ///         KeyManagementServiceClient::new,
    ///         "https://cloudkms.googleapis.com",
    ///         None,
    ///     )
    ///     .await?;
    ///
    /// let locations = "global";
    /// let key_rings = "testnet";
    /// let crypto_keys = "validator-1";
    /// let crypto_key_versions = "1";
    ///
    /// let name = format!(
    ///     "projects/{}/locations/{}/keyRings/{}/cryptoKeys/{}/cryptoKeyVersions/{}",
    ///     google_project_id, locations, key_rings, crypto_keys, crypto_key_versions
    /// );
    ///
    /// let digest_bytes = keccak256("this is a test").0.to_vec();
    /// let digest = Some(Digest::Sha256(digest_bytes));
    /// let digest = Some(KMSDigest { digest });
    ///
    /// // note: signed_data.message.signature is used as `response` in test
    /// let signed_data = kms_client
    ///     .get()
    ///     .asymmetric_sign(tonic::Request::new(AsymmetricSignRequest {
    ///         name,
    ///         digest,
    ///         ..Default::default()
    ///     }))
    ///     .await?;
    ///
    /// // note: pubkey.message.pem is used as `pem_public_key` in test
    /// let pubkey =
    ///     kms_client.get().get_public_key(tonic::Request::new(GetPublicKeyRequest { name })).await?;
    /// ```
    #[test]
    #[ignore = "should not run with a default cargo test"]
    fn test_with_creds_google_kms_signature() {
        // validator 1 kms
        // asymmetric_sign for SHA256 digest (keccak)
        let response = vec![
            48, 69, 2, 33, 0, 219, 198, 213, 75, 127, 199, 6, 132, 213, 175, 38, 79, 39, 79, 100,
            251, 226, 117, 23, 211, 53, 228, 17, 21, 7, 231, 108, 186, 188, 81, 182, 102, 2, 32,
            84, 77, 162, 193, 77, 79, 110, 165, 41, 1, 229, 222, 58, 187, 250, 188, 124, 82, 214,
            108, 78, 3, 156, 73, 108, 215, 112, 221, 24, 31, 133, 2,
        ];

        // kms service: `get_public_key`
        // Your PEM-formatted public key as a string
        let pem_public_key = "-----BEGIN PUBLIC KEY-----\nMFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEqzv8pSIJXo3PJZsGv+feaCZJFQoG3ed5\ngl0o/dpBKtwT+yajMYTCravDiqW/g62W+PNVzLoCbaot1WdlwXcp4Q==\n-----END PUBLIC KEY-----\n";

        // convert from pem format
        let pubkey_from_pem =
            PubKey::from_public_key_pem(pem_public_key).expect("public key from pem");
        let public_key = PublicKey::from_slice(pubkey_from_pem.to_encoded_point(false).as_bytes())
            .expect("converted to Pkey");

        // calculate wallet's address
        let wallet_address = public_key_to_address(public_key);
        let mut sig = Signature::from_der(&response).expect("valid signature from der");

        // ensure lower half of curve for `s`
        sig.normalize_s();

        // retrieve `r` and `s` values
        let compact = sig.serialize_compact();
        let (r, s) = compact.split_at(32);

        // the message used to create test data
        let data = keccak256("this is a test");
        let message_hash = data.0.as_slice();

        // Try both recovery ids (0 or 1) to find the correct v value
        //
        // NOTE: this compares the compressed public keys for convenience
        //       uncompressed approach is commented out
        //
        // How and why this works:
        // Calculating the v value from r, s, the original hash, and the public key, especially for
        // use in Ethereum transactions, involves trying to recover the public key from the
        // signature and comparing it to the known public key to determine the correct v value.
        // Ethereum uses the v value to encode the recovery id and some blockchain-specific
        // information (like chain id in EIP-155). In Ethereum, v can typically be 27 or 28
        // (or higher if adjusted for chain id as per EIP-155), corresponding to the two possible
        // recovery ids (0 or 1) that can result from the ECDSA signature recovery process.
        // In Ethereum, a signature consists of three components: r, s, and v. The r and s values
        // are part of the ECDSA signature, and the v value is a recovery id that indicates which of
        // the two possible public keys is the correct one (since a signature does not uniquely
        // identify a public key).
        //
        // The signature from Google Cloud KMS using the secp256k1 curve is in DER format.
        // This 64-byte format consists of the r and s components of the ECDSA signature, each being
        // 32 bytes long.
        //
        // The `v` value is Ethereum-specific and must be calculated to use the KMS signature with
        // EVM. The `v` value can be either 27 or 28 (or 35 or 36 when adding the chain ID to
        // prevent replay attacks on different networks as per EIP-155).
        //
        // The `v` value in the signature not only indicates the chain ID (for replay
        // protection) but also encodes information about the "y parity" of the point on the
        // elliptic curve that corresponds to the public key recovered from the signature. The y
        // parity (odd or even) is a critical component used to recover the correct public key from
        // a given signature (r, s) and message hash.
        //
        // Relationship Between y_odd_parity and v:
        // The v value for Ethereum signatures traditionally starts at 27 (or 28), where the
        // difference (27 or 28) essentially encodes the y parity.
        //
        // Specifically:
        // - If v = 35 + 2*chain_id), the y parity is even.
        // - If v = 36 + 2*chain_id), the y parity is odd.
        //
        // The y_odd_parity boolean would thus be false if v is 35 (even y) and true if v is 36 (odd
        // y).
        // let pubkey_bytes = pubkey_from_pem.to_sec1_bytes();
        let pubkey_bytes = public_key.serialize();
        let chain_id = RaylsNetwork::Local.chain_id();

        // alternative approach:
        // compare uncompressed public keys
        // let public_key_uncompressed = pubkey.to_encoded_point(false);

        let (tx, rx) = oneshot::channel();
        for recovery_id in [0, 1] {
            let recid = RecoveryId::try_from(recovery_id).expect("Invalid recovery id");
            let recoverable_signature = RecoverableSignature::from_compact(&compact, recid)
                .expect("creating recoverable signature");

            let slice: [u8; 32] = message_hash.try_into().expect("32 byte message hash");
            let digest = Message::from_digest(slice);
            if let Ok(recovered_key) = SECP256K1.recover_ecdsa(digest, &recoverable_signature) {
                let recovered_pubkey = recovered_key.serialize();
                // alternative approach:
                // let uncomp_pubkey = recovered_key.serialize_uncompressed();

                if recovered_pubkey == pubkey_bytes.as_ref() {
                    // alternative approach:
                    // if uncomp_pubkey == public_key_uncompressed.as_bytes() {

                    // v is found when the recovered key matches the known public key
                    //
                    // calculate v based on EIP-155
                    // recovery_id is 0 or 1; widened to u64 to match chain_id's type (no data lost)
                    let v = recovery_id as u64 + chain_id * 2 + 35;
                    let y_odd_parity = v % 2 == 0;
                    tx.send(y_odd_parity).expect("tx sent odd_y_parity");
                    break;
                }
            }
        }

        let y_parity = rx.blocking_recv().expect("y odd parity");

        let r = U256::from_be_slice(r);
        let s = U256::from_be_slice(s);
        let eth_signature = EthSignature::new(r, s, y_parity);

        let signer = eth_signature.recover_address_from_prehash(&data).expect("signer recoverable");

        assert_eq!(signer, wallet_address);
    }

    #[tokio::test]
    #[ignore = "should not run with a default cargo test"]
    async fn test_with_creds_gcloud_sdk() {
        // Debug logging

        std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", "./gcloud-credentials.json");

        // Detect Google project ID using environment variables PROJECT_ID/GCP_PROJECT_ID
        // or GKE metadata server when the app runs inside GKE
        let google_project_id = GoogleEnvironment::detect_google_project_id()
            .await
            // .expect("No Google Project ID detected. Please specify it explicitly using env
            // variable: PROJECT_ID");
            .unwrap_or("rayls-network".to_string());

        let kms_client: GoogleApi<KeyManagementServiceClient<GoogleAuthMiddleware>> =
            GoogleApi::from_function(
                KeyManagementServiceClient::new,
                "https://cloudkms.googleapis.com",
                None,
            )
            .await
            .expect("kms client created");

        let locations = "global";
        let key_rings = "tests";
        let crypto_keys = "key-for-unit-tests";
        let crypto_key_versions = "1";

        let name = format!(
            "projects/{google_project_id}/locations/{locations}/keyRings/{key_rings}/cryptoKeys/{crypto_keys}/cryptoKeyVersions/{crypto_key_versions}"
        );

        let digest_bytes = keccak256("this is a test").0.to_vec();
        let digest = Some(Digest::Sha256(digest_bytes));
        let digest = Some(KMSDigest { digest });

        let signed_data = kms_client
            .get()
            .asymmetric_sign(AsymmetricSignRequest {
                name: name.clone(),
                digest,
                ..Default::default()
            })
            .await
            .expect("kms response with signed data")
            .into_inner()
            .signature;

        debug!("kms response:\n {:?}", signed_data);

        let pem_pubkey = kms_client
            .get()
            .get_public_key(GetPublicKeyRequest { name })
            .await
            .expect("kms response with public key")
            .into_inner()
            .pem;

        debug!("public key:\n {:?}", pem_pubkey);
    }
}
