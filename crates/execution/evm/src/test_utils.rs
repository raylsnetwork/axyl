//! Transaction factory to create legit transactions for execution.

use crate::{
    error::RaylsRethResult,
    evm::RaylsEvm,
    recover_raw_transaction,
    reth_env::RethEnv,
    system_calls::{ConsensusRegistry, EpochState},
    WorkerTxPool,
};
// re-exports for engine tests
pub use alloy::eips::{
    eip2935::HISTORY_STORAGE_ADDRESS, eip4788::BEACON_ROOTS_ADDRESS, eip7685::EMPTY_REQUESTS_HASH,
};
use alloy::{
    consensus::{SignableTransaction as _, TxEip4844, TxEip4844Variant},
    eips::eip7594::BlobTransactionSidecarVariant,
    hex,
    primitives::ChainId,
    signers::{
        k256::sha2::{Digest as _, Sha256},
        local::PrivateKeySigner,
    },
    sol_types::SolCall as _,
};
use rayls_infrastructure_types::{
    address, calculate_transaction_root, keccak256, now, test_chain_spec_arc, test_genesis,
    AccessList, Address, Batch, BlobTransactionSidecar, Block, BlockBody, BlockHash, BlsPublicKey,
    Bytes, Committee, CommitteeBuilder, Encodable2718, EthSignature, ExecHeader, ExecutionKeypair,
    Genesis, GenesisAccount, RecoveredBlock, SealedHeader, TaskManager, Transaction,
    TransactionSigned, TxEip1559, TxHash, TxKind, WorkerId, B256, EMPTY_OMMER_ROOT_HASH,
    EMPTY_TRANSACTIONS, EMPTY_WITHDRAWALS, ETHEREUM_BLOCK_GAS_LIMIT_56BITS, MIN_PROTOCOL_BASE_FEE,
    U256,
};
use rayls_middleware_rewards::RewardsCounter;
use reth_chainspec::{ChainSpec as RethChainSpec, EthChainSpec};
use reth_engine_tree::tree::{precompile_cache::PrecompileCacheMap, PayloadProcessor};
use reth_evm::{execute::Executor as _, ConfigureEvm, EvmFactory as _};
use reth_primitives::{sign_message, Account};
pub use reth_primitives_traits::proofs::calculate_withdrawals_root;
use reth_primitives_traits::SignerRecoverable;
use reth_provider::{AccountReader as _, StateProviderBox, StateProviderFactory};
use reth_revm::{database::StateProviderDatabase, db::BundleState, State};
use reth_transaction_pool::{EthPoolTransaction, EthPooledTransaction, PoolTransaction};
use reth_trie_db::ChangesetCache;
use secp256k1::{
    rand::{rngs::StdRng, Rng, SeedableRng as _},
    Secp256k1,
};
use std::{collections::HashMap, path::Path, str::FromStr, sync::Arc};

// methods for tests
impl RethEnv {
    /// Create a new RethEnv for testing only.
    pub async fn new_for_test<P: AsRef<Path>>(
        db_path: P,
        task_manager: &TaskManager,
        rewards: Option<RewardsCounter>,
    ) -> eyre::Result<Self> {
        Self::new_for_temp_chain(test_chain_spec_arc(), db_path, task_manager, rewards).await
    }

    /// Create a new RethEnv with a custom RaylsChainSpec for testing hardfork behavior.
    pub async fn new_for_temp_chain_with_rayls_spec<P: AsRef<Path>>(
        chain: Arc<RethChainSpec>,
        rayls_chain_spec: Arc<crate::RaylsChainSpec>,
        db_path: P,
        task_manager: &TaskManager,
        rewards: Option<RewardsCounter>,
    ) -> eyre::Result<Self> {
        use crate::{
            evm::{initialize_erc20_precompile, RaylsEvmConfig},
            native_erc20::{Erc20Precompile, Erc20TokenConfig, ERC20_PRECOMPILE_ADDRESS},
            persistence,
            reth_env::RethConfig,
        };
        use reth_node_core::{
            args::{DatadirArgs, StorageArgs},
            node_config::NodeConfig,
        };
        use reth_provider::providers::BlockchainProvider;
        use reth_storage_api::{BlockNumReader, DatabaseProviderFactory};

        let node_config = NodeConfig {
            datadir: DatadirArgs {
                datadir: reth_node_core::dirs::MaybePlatformPath::from(
                    db_path.as_ref().to_path_buf(),
                ),
                static_files_path: None,
                rocksdb_path: None,
                pprof_dumps_path: None,
            },
            chain,
            storage: StorageArgs { v2: true },
            ..NodeConfig::default()
        };
        let reth_config = RethConfig(node_config.clone());
        let database = Self::new_database(&reth_config, db_path)?;
        let rewards_counter = rewards.unwrap_or_default();
        let evm_config = RaylsEvmConfig::new(rayls_chain_spec.clone(), rewards_counter.clone());
        let task_spawner = task_manager.get_spawner();
        let runtime = reth_tasks::Runtime::with_existing_handle(tokio::runtime::Handle::current())?;
        let provider_factory = Self::init_provider_factory(
            &node_config,
            rayls_chain_spec,
            database.clone(),
            &task_spawner,
            runtime.clone(),
            rewards_counter,
        )
        .await?;
        let blockchain_provider = BlockchainProvider::new(provider_factory.clone())?;
        let chain_id = node_config.chain.chain_id();
        let erc20_precompile =
            Erc20Precompile::new(Erc20TokenConfig::default(), ERC20_PRECOMPILE_ADDRESS, chain_id);
        let _ = initialize_erc20_precompile(erc20_precompile);
        let last_persisted = blockchain_provider.database_provider_ro()?.best_block_number()?;
        let (persistence_handle, _) =
            persistence::spawn_persistence(provider_factory.clone(), node_config.prune_config());
        let persistence_state =
            Arc::new(parking_lot::Mutex::new(persistence::PersistenceState::new(
                last_persisted,
                node_config.engine.persistence_threshold,
                database.clone(),
            )));
        let tree_config = node_config.engine.tree_config();
        let payload_processor = Arc::new(parking_lot::Mutex::new(PayloadProcessor::new(
            runtime,
            evm_config.clone(),
            &tree_config,
            PrecompileCacheMap::default(),
        )));
        Ok(Self {
            node_config,
            blockchain_provider,
            #[cfg(feature = "archive-replay")]
            provider_factory,
            evm_config,
            task_spawner,
            persistence_handle,
            persistence_state,
            payload_processor,
            tree_config,
            ancestor_trie_cache: Arc::new(parking_lot::Mutex::new(None)),
            changeset_cache: ChangesetCache::new(),
            #[cfg(feature = "archive-replay")]
            canonical_root_oracle: Arc::new(std::sync::OnceLock::new()),
            #[cfg(feature = "archive-replay")]
            ancestor_sorted_cache: Arc::new(parking_lot::Mutex::new(None)),
        })
    }

    /// Retrieve the state at the provided block hash.
    pub fn state_by_block_hash(&self, hash: BlockHash) -> RaylsRethResult<StateProviderBox> {
        Ok(self.blockchain_provider.state_by_block_hash(hash)?)
    }

    /// Retrieve the account balance.
    pub fn retrieve_account(&self, address: &Address) -> RaylsRethResult<Option<Account>> {
        Ok(self.blockchain_provider.basic_account(address)?)
    }

    /// Create an EVM-environment from state provider.
    pub fn rayls_evm(
        &self,
        hash: BlockHash,
    ) -> eyre::Result<RaylsEvm<State<StateProviderDatabase<StateProviderBox>>>> {
        let header = self.header(hash)?.expect("provided hash in header table");
        let state = self.state_by_block_hash(hash)?;
        let db = State::builder()
            .with_database(StateProviderDatabase::new(state))
            .with_bundle_update()
            .build();
        Ok(self.evm_config.evm_factory().create_evm(db, self.evm_config.evm_env(&header)?))
    }

    /// Test utility to execute batch and return execution outcome.
    ///
    /// This is useful for simulating execution results for account state changes.
    /// Currently only used by faucet tests to obtain faucet contract account info
    /// by simulating deploying proxy contract. The results are then put into genesis.
    pub fn execution_outcome_for_tests(
        &self,
        txs: Vec<Vec<u8>>,
        parent: &SealedHeader,
    ) -> BundleState {
        // create "empty" header with default values
        let mut header = ExecHeader {
            parent_hash: parent.hash(),
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            beneficiary: Address::ZERO,
            state_root: Default::default(),
            transactions_root: Default::default(),
            receipts_root: Default::default(),
            withdrawals_root: Some(EMPTY_WITHDRAWALS),
            logs_bloom: Default::default(),
            difficulty: U256::ZERO,
            number: parent.number + 1,
            gas_limit: ETHEREUM_BLOCK_GAS_LIMIT_56BITS,
            gas_used: 0,
            timestamp: now(),
            mix_hash: B256::random(),
            nonce: 0_u64.into(),
            base_fee_per_gas: Some(MIN_PROTOCOL_BASE_FEE),
            blob_gas_used: Some(0),
            excess_blob_gas: Some(0),
            extra_data: Default::default(),
            parent_beacon_block_root: Some(B256::ZERO),
            requests_hash: None,
        };

        // decode transactions
        let mut decoded_txs = Vec::with_capacity(txs.len());
        let mut signers = Vec::with_capacity(txs.len());
        for tx_bytes in &txs {
            let tx = recover_raw_transaction(tx_bytes)
                .expect("raw transaction recovered for test")
                .into_inner();
            signers.push(tx.recover_signer().expect("recover signer for test tx"));
            decoded_txs.push(tx);
        }

        // update header's transactions root
        header.transactions_root = if txs.is_empty() {
            EMPTY_TRANSACTIONS
        } else {
            calculate_transaction_root(&decoded_txs)
        };

        // recover senders from block
        let block = Block {
            header,
            body: BlockBody {
                transactions: decoded_txs,
                ommers: vec![],
                withdrawals: Some(Default::default()),
            },
        };

        // create execution db
        let mut db = StateProviderDatabase::new(
            self.latest().expect("provider retrieves latest during test batch execution"),
        );
        let executor = self.evm_config.executor(&mut db);
        let res = executor
            .execute(&RecoveredBlock::new_unhashed(block, signers))
            .expect("execute one block");

        res.state
    }

    /// Retrieve validator rewards.
    pub fn get_validator_rewards(&self, hash: BlockHash, address: Address) -> eyre::Result<U256> {
        let mut rayls_evm = self.rayls_evm(hash)?;
        let calldata =
            ConsensusRegistry::getRewardsCall { validatorAddress: address }.abi_encode().into();
        let rewards = self.call_consensus_registry::<_, U256>(&mut rayls_evm, calldata)?;
        Ok(rewards)
    }
}

/// Transaction factory
#[derive(Clone, Copy, Debug)]
pub struct TransactionFactory {
    /// Keypair for signing transactions
    keypair: ExecutionKeypair,
    /// The nonce for the next transaction constructed.
    nonce: u64,
}

impl Default for TransactionFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionFactory {
    /// Create a new instance of self from a [0; 32] seed.
    ///
    /// Address: 0xb14d3c4f5fbfbcfb98af2d330000d49c95b93aa7
    /// Secret: 9bf49a6a0755f953811fce125f2683d50429c3bb49e074147e0089a52eae155f
    pub fn new() -> Self {
        let mut rng = StdRng::from_seed([0; 32]);
        let secp = Secp256k1::new();
        let (secret_key, _public_key) = secp.generate_keypair(&mut rng);
        let keypair = ExecutionKeypair::from_secret_key(&secp, &secret_key);
        Self { keypair, nonce: 0 }
    }

    /// create a new instance of self from a provided seed.
    pub fn new_random_from_seed<R: Rng + ?Sized>(rand: &mut R) -> Self {
        let secp = Secp256k1::new();
        let (secret_key, _public_key) = secp.generate_keypair(rand);
        let keypair = ExecutionKeypair::from_secret_key(&secp, &secret_key);
        Self { keypair, nonce: 0 }
    }

    /// create a new instance of self from a random seed.
    pub fn new_random() -> Self {
        let secp = Secp256k1::new();
        let (secret_key, _public_key) = secp.generate_keypair(&mut StdRng::from_os_rng());
        let keypair = ExecutionKeypair::from_secret_key(&secp, &secret_key);
        Self { keypair, nonce: 0 }
    }

    /// Return the address of the signer.
    pub fn address(&self) -> Address {
        let public_key = self.keypair.public_key();
        // strip out the first byte because that should be the SECP256K1_TAG_PUBKEY_UNCOMPRESSED
        // tag returned by libsecp's uncompressed pubkey serialization
        let hash = keccak256(&public_key.serialize_uncompressed()[1..]);
        Address::from_slice(&hash[12..])
    }

    /// Change the nonce for the next transaction.
    pub fn set_nonce(&mut self, nonce: u64) {
        self.nonce = nonce;
    }

    /// Increment nonce after a transaction was created and signed.
    pub fn inc_nonce(&mut self) {
        self.nonce += 1;
    }

    /// Create a signed EIP1559 transaction and encode it.
    pub fn create_eip1559_encoded(
        &mut self,
        chain: Arc<RethChainSpec>,
        gas_limit: Option<u64>,
        gas_price: u128,
        to: Option<Address>,
        value: U256,
        input: Bytes,
    ) -> Vec<u8> {
        self.create_eip1559(chain, gas_limit, gas_price, to, value, input).encoded_2718()
    }

    /// Create and sign an EIP1559 transaction.
    pub fn create_eip1559(
        &mut self,
        chain: Arc<RethChainSpec>,
        gas_limit: Option<u64>,
        gas_price: u128,
        to: Option<Address>,
        value: U256,
        input: Bytes,
    ) -> TransactionSigned {
        let gas_limit = gas_limit.unwrap_or(1_000_000);
        let tx_kind = match to {
            Some(address) => TxKind::Call(address),
            None => TxKind::Create,
        };

        // Eip1559
        let transaction = Transaction::Eip1559(TxEip1559 {
            chain_id: chain.chain.id(),
            nonce: self.nonce,
            max_priority_fee_per_gas: 0,
            max_fee_per_gas: gas_price,
            gas_limit,
            to: tx_kind,
            value,
            input,
            access_list: Default::default(),
        });

        let tx_signature_hash = transaction.signature_hash();
        let signature = self.sign_hash(tx_signature_hash);

        // increase nonce for next tx
        self.inc_nonce();

        TransactionSigned::new_unhashed(transaction, signature)
    }

    /// Create a signed EIP4844 transaction using empty bytes.
    pub fn create_eip4844(
        &mut self,
        chain_id: ChainId,
        gas_limit: Option<u64>,
        gas_price: u128,
        blob_versioned_hashes: Vec<B256>,
    ) -> TransactionSigned {
        let gas_limit = gas_limit.unwrap_or(1_000_000);

        // blob transaction
        let tx = TxEip4844 {
            chain_id,
            nonce: self.nonce,
            max_priority_fee_per_gas: 0,
            max_fee_per_gas: gas_price,
            gas_limit,
            to: address!("a8cb082a5a689e0d594d7da1e2d72a3d63adc1bd"),
            value: U256::ZERO,
            input: Bytes::new(),
            access_list: Default::default(),
            blob_versioned_hashes,
            max_fee_per_blob_gas: 1,
        };
        let variant = TxEip4844Variant::<BlobTransactionSidecar>::TxEip4844(tx);
        let tx_signature_hash = variant.signature_hash();

        // construct transaction and sign
        let signature = self.sign_hash(tx_signature_hash);

        // increase nonce for next tx
        self.inc_nonce();

        TransactionSigned::new_unhashed(variant.into(), signature)
    }

    /// Create and sign an EIP4844 transaction.
    pub async fn create_and_submit_eip4844(
        &mut self,
        chain: Arc<RethChainSpec>,
        gas_limit: Option<u64>,
        gas_price: u128,
        pool: WorkerTxPool,
    ) -> TxHash {
        // Use the "zero blob" - a blob filled with zeros
        // This has known valid KZG commitments and proofs
        let blob_data = [0u8; 131072]; // 128KB of zeros

        // Known valid KZG commitment for zero blob (from consensus tests)
        let commitment: [u8; 48] = hex::decode("c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")
            .expect("valid hex commitment").try_into().expect("valid commitment length");

        // Known valid KZG proof for zero blob (from consensus tests)
        let proof: [u8; 48] = hex::decode("c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")
            .expect("valid hex proof").try_into().expect("valid proof length");

        // Compute the versioned hash from the commitment
        // EIP-4844: versioned_hash = 0x01 + sha256(commitment)[1:]
        let mut hasher = Sha256::new();
        hasher.update(commitment);
        let commitment_hash = hasher.finalize();
        let mut versioned_hash = [0u8; 32];
        versioned_hash[0] = 0x01; // Version byte for KZG commitments
        versioned_hash[1..].copy_from_slice(&commitment_hash[1..]);

        // dummy blob data - not used anywhere
        let sidecar = BlobTransactionSidecar {
            blobs: vec![blob_data.into()],
            commitments: vec![commitment.into()],
            proofs: vec![proof.into()],
        };
        let sidecar: BlobTransactionSidecarVariant =
            BlobTransactionSidecarVariant::Eip4844(sidecar);

        // construct transaction, sign, and submit to pool
        let blob_versioned_hashes = vec![versioned_hash.into()]; // use computed hash
        let signed_tx =
            self.create_eip4844(chain.chain_id(), gas_limit, gas_price, blob_versioned_hashes);
        let recovered = signed_tx.try_into_recovered().expect("recovered tx");
        let pooled_tx = EthPooledTransaction::try_from_eip4844(recovered, sidecar)
            .expect("recovered into eth pooled tx");
        let hash = pool.add_transaction_local(pooled_tx).await.expect("recovered tx added to pool");
        hash.hash
    }

    /// Create and sign an EIP1559 transaction with all possible parameters passed.
    ///
    /// All arguments are optional and default to:
    /// - chain_id: 2017 (testnet)
    /// - nonce: `Self::nonce` (correctly incremented)
    /// - max_priority_fee_per_gas: 0 (no tip)
    /// - max_fee_per_gas: basefee minimum (7 wei)
    /// - gas_limit: 1_000_000 wei
    /// - to: None (results in `TxKind::Create`)
    /// - value: 1 coin (1^10*18 wei)
    /// - input: empty bytes (`Bytes::default()`)
    /// - access_list: None
    ///
    /// NOTE: the nonce is still incremented to track the number of signed transactions for `Self`.
    #[allow(clippy::too_many_arguments)]
    pub fn create_explicit_eip1559(
        &mut self,
        chain_id: Option<u64>,
        nonce: Option<u64>,
        max_priority_fee_per_gas: Option<u128>,
        max_fee_per_gas: Option<u128>,
        gas_limit: Option<u64>,
        to: Option<Address>,
        value: Option<U256>,
        input: Option<Bytes>,
        access_list: Option<AccessList>,
    ) -> TransactionSigned {
        let tx_kind = match to {
            Some(address) => TxKind::Call(address),
            None => TxKind::Create,
        };

        // Eip1559
        let transaction = Transaction::Eip1559(TxEip1559 {
            chain_id: chain_id.unwrap_or(2017),
            nonce: nonce.unwrap_or(self.nonce),
            max_priority_fee_per_gas: max_priority_fee_per_gas.unwrap_or(0),
            max_fee_per_gas: max_fee_per_gas.unwrap_or(MIN_PROTOCOL_BASE_FEE.into()),
            gas_limit: gas_limit.unwrap_or(1_000_000),
            to: tx_kind,
            value: value.unwrap_or_else(|| {
                U256::from(10).checked_pow(U256::from(18)).expect("1x10^18 does not overflow")
            }),
            input: input.unwrap_or_default(),
            access_list: access_list.unwrap_or_default(),
        });

        let tx_signature_hash = transaction.signature_hash();
        let signature = self.sign_hash(tx_signature_hash);

        // increase nonce for self
        self.inc_nonce();

        TransactionSigned::new_unhashed(transaction, signature)
    }

    /// Sign the transaction hash with the key in memory
    fn sign_hash(&self, hash: B256) -> EthSignature {
        let secret = B256::from_slice(&self.keypair.secret_bytes());
        let signature = sign_message(secret, hash);
        signature.expect("failed to sign transaction")
    }

    /// Helper to instantiate an `alloy-signer-local::PrivateKeySigner` wrapping the default account
    pub fn get_default_signer(&self) -> eyre::Result<PrivateKeySigner> {
        // circumvent Secp256k1 <> k256 type incompatibility via FieldBytes intermediary
        let binding = self.keypair.secret_key().secret_bytes();
        let signer = PrivateKeySigner::from_bytes(&binding.into())?;
        Ok(signer)
    }

    /// Create and submit the next transaction to the provided [TransactionPool].
    pub async fn create_and_submit_eip1559_pool_tx(
        &mut self,
        chain: Arc<RethChainSpec>,
        gas_price: u128,
        to: Address,
        value: U256,
        pool: WorkerTxPool,
    ) -> TxHash {
        let tx = self.create_eip1559(chain, None, gas_price, Some(to), value, Bytes::new());
        let recovered = tx.try_into_recovered().expect("recovered tx");
        let pooled_tx = EthPooledTransaction::try_from_consensus(recovered)
            .expect("recovered into eth pooled tx");

        pool.add_transaction_local(pooled_tx).await.expect("recovered tx added to pool").hash
    }

    /// Submit a transaction to the provided pool.
    pub async fn submit_tx_to_pool(&self, tx: TransactionSigned, pool: WorkerTxPool) -> TxHash {
        let recovered = tx.try_into_recovered().expect("recovered tx");
        let pooled_tx = EthPooledTransaction::try_from_consensus(recovered)
            .expect("recovered into eth pooled tx");

        pool.add_transaction_local(pooled_tx).await.expect("recovered tx added to pool").hash
    }
}

/// Helper to get the gas price based on the provider's latest header.
pub fn get_gas_price(reth_env: &RethEnv) -> u128 {
    reth_env.get_gas_price().expect("gas price")
}

/// Create a random encoded transaction.
pub fn transaction(chain: Arc<RethChainSpec>) -> Vec<u8> {
    let mut tx_factory = TransactionFactory::new_random();
    let gas_price = 100_000;
    let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

    // random transaction
    tx_factory.create_eip1559_encoded(
        chain,
        None,
        gas_price,
        Some(Address::ZERO),
        value,
        Bytes::new(),
    )
}

/// will create a batch with randomly formed transactions
/// dictated by the parameter number_of_transactions
pub fn fixture_batch_with_transactions(number_of_transactions: u32) -> Batch {
    let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
    let transactions = (0..number_of_transactions).map(|_v| transaction(chain.clone())).collect();

    // Put some random bytes in the header so that tests will have unique headers.
    Batch { transactions, beneficiary: Address::random(), ..Default::default() }
}

/// Create a batch with two random, valid transactions. The rest of the [Batch] uses defaults.
pub fn batch(chain: Arc<RethChainSpec>) -> Batch {
    let transactions = vec![transaction(chain.clone()), transaction(chain)];
    Batch { transactions, ..Default::default() }
}

/// generate multiple fixture batches. The number of generated batches
/// are dictated by the parameter num_of_batches.
pub fn batches(chain: Arc<RethChainSpec>, num_of_batches: usize) -> Vec<Batch> {
    let mut batches = Vec::new();

    for i in 1..num_of_batches + 1 {
        batches.push(batch_with_transactions(chain.clone(), i, 0));
    }

    batches
}

/// Create a batch with the specified number of transactions.
pub fn batch_with_transactions(
    chain: Arc<RethChainSpec>,
    num_of_transactions: usize,
    worker_id: WorkerId,
) -> Batch {
    let mut transactions = Vec::new();

    for _ in 0..num_of_transactions {
        transactions.push(transaction(chain.clone()));
    }

    Batch::new_for_test(transactions, ExecHeader::default(), worker_id, 0, 0)
}

/// Helper function to seed an instance of Genesis with accounts from a random batch.
pub fn seeded_genesis_from_random_batch(
    genesis: Genesis,
    batch: &Batch,
) -> (Genesis, Vec<TransactionSigned>, Vec<Address>) {
    let max_capacity = batch.transactions.len();
    let mut decoded_txs = Vec::with_capacity(max_capacity);
    let mut senders = Vec::with_capacity(max_capacity);
    let mut accounts_to_seed = Vec::with_capacity(max_capacity);

    // loop through the transactions
    for tx_bytes in &batch.transactions {
        let (tx, address) =
            recover_raw_transaction(tx_bytes).expect("raw transaction recovered").into_parts();
        decoded_txs.push(tx);
        senders.push(address);
        // fund account with 99mil
        let account = (
            address,
            GenesisAccount::default().with_balance(
                U256::from_str("0x51E410C0F93FE543000000").expect("account balance is parsed"),
            ),
        );
        accounts_to_seed.push(account);
    }
    (genesis.extend_accounts(accounts_to_seed), decoded_txs, senders)
}

/// Helper function to seed an instance of Genesis with random batches.
///
/// The transactions in the randomly generated batches are decoded and their signers are recovered.
///
/// The function returns the new Genesis, the signed transactions by batch, and the addresses for
/// further use it testing.
pub fn seeded_genesis_from_random_batches<'a>(
    mut genesis: Genesis,
    batches: impl IntoIterator<Item = &'a Batch>,
) -> (Genesis, Vec<Vec<TransactionSigned>>, Vec<Vec<Address>>) {
    let mut txs = vec![];
    let mut senders = vec![];
    for batch in batches {
        let (g, t, s) = seeded_genesis_from_random_batch(genesis, batch);
        genesis = g;
        txs.push(t);
        senders.push(s);
    }
    (genesis, txs, senders)
}

/// Helper function to create a committee for tests from on-chain data.
pub async fn create_committee_from_state(epoch_state: EpochState) -> eyre::Result<Committee> {
    // deconstruct epoch information
    let EpochState { epoch, validators, .. } = epoch_state;
    let validators = validators
        .iter()
        .map(|v| {
            let decoded_bls = BlsPublicKey::from_literal_bytes(v.blsPubkey.as_ref());
            decoded_bls.map(|decoded| (decoded, v))
        })
        .collect::<Result<HashMap<_, _>, _>>()
        .map_err(|err| eyre::eyre!("failed to create bls key from on-chain bytes: {err:?}"))?;
    let mut committee_builder = CommitteeBuilder::new(epoch);
    for (bls_key, info) in validators {
        committee_builder.add_authority(bls_key, 1, info.validatorAddress);
    }
    let committee = committee_builder.build();
    committee.load();
    Ok(committee)
}
