//! Integration test for RPC Faucet feature.
//!
//! The faucet receives an rpc request containing an address and submits
//! a direct transfer to the address if it is not found in the LRU time-based
//! cache. The signing process is handled by an API call to Google KMS using
//! secp256k1 algorithm. However, additional information is needed for the
//! signature to be EVM compatible. The faucet service does all of this and
//! then submits the transaction to the RPC Transaction Pool for the next batch.

use alloy::{network::EthereumWallet, providers::ProviderBuilder};
use e2e_tests::{ensure_account_balance_infinite_loop, spawn_local_testnet, IT_TEST_MUTEX};
use futures::{stream::FuturesUnordered, StreamExt};
use gcloud_sdk::{
    google::cloud::kms::v1::{
        key_management_service_client::KeyManagementServiceClient, GetPublicKeyRequest,
    },
    GoogleApi, GoogleAuthMiddleware, GoogleEnvironment,
};
use jsonrpsee::{core::client::ClientT, http_client::HttpClientBuilder, rpc_params};
use k256::{elliptic_curve::sec1::ToEncodedPoint, pkcs8::DecodePublicKey, PublicKey as PubKey};
use rayls_execution_evm::{reth_env::RethEnv, test_utils::TransactionFactory, RethChainSpec};
use rayls_execution_rpc::{EngineToPrimary, NodeRole, NodeStatus};
use rayls_infrastructure_config::{
    fetch_file_content_relative_to_manifest, Config, ConfigFmt, ConfigTrait,
};
use rayls_infrastructure_types::{
    hex, public_key_to_address, sol, testnet_genesis, Address, BlockHash, ConsensusHeader,
    Encodable2718 as _, Epoch, EpochCertificate, EpochRecord, Genesis, GenesisAccount, SolValue,
    TaskManager, B256, U256,
};

use secp256k1::PublicKey;
use std::{str::FromStr, sync::Arc, time::Duration};
use tokio::{task::JoinHandle, time::timeout};
use tracing::{debug, info};

struct EmptyEngToPrimary();
impl EngineToPrimary for EmptyEngToPrimary {
    fn get_latest_consensus_block(&self) -> ConsensusHeader {
        ConsensusHeader::default()
    }
    fn consensus_block_by_number(&self, _number: u64) -> Option<ConsensusHeader> {
        None
    }
    fn consensus_block_by_hash(&self, _hash: BlockHash) -> Option<ConsensusHeader> {
        None
    }

    fn epoch(
        &self,
        _epoch: Option<Epoch>,
        _hash: Option<BlockHash>,
    ) -> Option<(EpochRecord, EpochCertificate)> {
        None
    }

    fn node_status(&self) -> NodeStatus {
        NodeStatus {
            role: NodeRole::Observer,
            is_caught_up: true,
            epoch: 0,
            committed_round: 0,
            primary_round: 0,
            gc_round: 0,
            last_canonical_block: 0,
        }
    }
}

#[ignore = "internal test for devops - credentials required"]
#[tokio::test]
async fn test_faucet_transfers_rls_and_xyz_with_google_kms_e2e() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();

    // create google env and temp chain spec for state initialization
    let (tmp_chain, kms_address) = prepare_google_kms_env().await?;

    // faucet interface
    sol!(
        #[allow(clippy::too_many_arguments)]
        #[sol(rpc)]
        contract StablecoinManager {
            struct StablecoinManagerInitParams {
                address admin_;
                address maintainer_;
                address[] tokens_;
                uint256 initMaxLimit;
                uint256 initMinLimit;
                address[] authorizedFaucets_;
                uint256 dripAmount_;
                uint256 nativeDripAmount_;
            }

            function initialize(StablecoinManagerInitParams calldata initParams) external;
            function grantRole(bytes32 role, address account) external;
        }
    );

    // stablecoin interface
    sol!(
        #[allow(clippy::too_many_arguments)]
        #[sol(rpc)]
        contract Stablecoin {
            function initialize(
                string memory name_,
                string memory symbol_,
                uint8 decimals_
            ) external;
            function decimals() external view returns (uint8);
            function balanceOf(address account) external view returns (uint256);
            function mint(uint256 value) external;
            function mintTo(
                address account,
                uint256 value
            ) external;
            function burn(uint256 value) external;
            function burnFrom(
                address account,
                uint256 value
            ) external;
        }
    );

    // set random addresses on which to etch contract bytecodes
    let faucet_impl_address = Address::random();
    let stablecoin_impl_address = Address::random();
    // fetch bytecode attributes from compiled jsons in rayls-contracts repo
    let faucet_standard_json = fetch_file_content_relative_to_manifest(
        "../../rayls-contracts/artifacts/StablecoinManager.json",
    );
    let faucet_deployed_bytecode =
        RethEnv::fetch_value_from_json_str(&faucet_standard_json, Some("deployedBytecode.object"))?
            .as_str()
            .map(hex::decode)
            .unwrap()?;
    let stablecoin_json =
        fetch_file_content_relative_to_manifest("../../rayls-contracts/artifacts/Stablecoin.json");
    let stablecoin_impl_bytecode =
        RethEnv::fetch_value_from_json_str(&stablecoin_json, Some("deployedBytecode.object"))?
            .as_str()
            .map(hex::decode)
            .unwrap()?;

    // extend genesis accounts to fund factory_address, etch bytecodes, construct proxy creation txs
    let mut tx_factory = TransactionFactory::new();
    let factory_address = tx_factory.address();
    let tmp_genesis = tmp_chain.genesis.clone().extend_accounts(
        vec![
            (factory_address, GenesisAccount::default().with_balance(U256::MAX)),
            (
                faucet_impl_address,
                GenesisAccount::default().with_code(Some(faucet_deployed_bytecode.clone().into())),
            ),
            (
                stablecoin_impl_address,
                GenesisAccount::default().with_code(Some(stablecoin_impl_bytecode.clone().into())),
            ),
        ]
        .into_iter(),
    );

    // ERC1967Proxy interface
    sol!(
        #[allow(clippy::too_many_arguments)]
        #[sol(rpc)]
        contract ERC1967Proxy {
            constructor(address implementation, bytes memory _data);
        }
    );

    // get data for faucet proxy deployment w/ initdata
    let faucet_init_selector = [22, 173, 166, 177];
    let deployed_token_bytes = vec![];
    let init_max_limit = U256::MAX;
    let init_min_limit = U256::from(1_000);
    let kms_faucets = vec![kms_address];
    let xyz_amount = U256::from(10).checked_pow(U256::from(6)).expect("1e6 doesn't overflow U256"); // 1
                                                                                                    // $XYZ
    let rls_amount =
        U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256"); // 1 $RLS

    // encode initialization struct (prevents stack too deep)
    let init_params = StablecoinManager::StablecoinManagerInitParams {
        admin_: factory_address,
        maintainer_: factory_address,
        tokens_: deployed_token_bytes,
        initMaxLimit: init_max_limit,
        initMinLimit: init_min_limit,
        authorizedFaucets_: kms_faucets,
        dripAmount_: xyz_amount,
        nativeDripAmount_: rls_amount,
    }
    .abi_encode();

    // construct create data for faucet proxy address
    let init_call = [&faucet_init_selector, &init_params[..]].concat();
    let constructor_params = (faucet_impl_address, init_call.clone()).abi_encode_params();
    let proxy_json = fetch_file_content_relative_to_manifest(
        "../../rayls-contracts/artifacts/ERC1967Proxy.json",
    );
    let proxy_initcode = RethEnv::fetch_value_from_json_str(&proxy_json, Some("bytecode.object"))?
        .as_str()
        .map(hex::decode)
        .unwrap()?;
    let proxy_bytecode =
        RethEnv::fetch_value_from_json_str(&proxy_json, Some("deployedBytecode.object"))?
            .as_str()
            .map(hex::decode)
            .unwrap()?;
    let faucet_create_data = [proxy_initcode.clone().as_slice(), &constructor_params[..]].concat();

    // construct `grantRole(faucet)` data
    let grant_role_selector = [47, 47, 241, 93];
    let grant_role_params = (
        B256::from_str("0xaecf5761d3ba769b4631978eb26cb84eae66bcaca9c3f0f4ecde3feb2f4cf144")?,
        kms_address,
    )
        .abi_encode_params();

    let grant_role_call = [&grant_role_selector, &grant_role_params[..]].concat().into();

    // construct create data for stablecoin proxy
    let stablecoin_init_selector = [22, 36, 246, 198];
    let stablecoin_init_params = ("name", "symbol", 6).abi_encode_params();
    let stablecoin_init_call = [&stablecoin_init_selector, &stablecoin_init_params[..]].concat();
    let stablecoin_constructor_params =
        (stablecoin_impl_address, stablecoin_init_call.clone()).abi_encode_params();
    let stablecoin_create_data =
        [proxy_initcode.as_slice(), &stablecoin_constructor_params[..]].concat();

    // faucet deployment will be `factory_address`'s first tx, stablecoin will be second tx
    let faucet_proxy_address = factory_address.create(0);
    let stablecoin_address = factory_address.create(1);

    // construct `updateXYZ()` data
    let updatexyz_selector = [233, 174, 163, 150];
    let updatexyz_params = (stablecoin_address, true, U256::MAX, U256::ZERO).abi_encode_params();
    let updatexyz_call = [&updatexyz_selector, &updatexyz_params[..]].concat().into();

    // construct `grantRole(minter_role)` data
    let minter_role_params = (
        B256::from_str("0x9f2df0fed2c77648de5860a4cc508cd0818c85b8b8a1ab4ceeef8d981c8956a6")?,
        faucet_proxy_address,
    )
        .abi_encode_params();
    let minter_role_call = [&grant_role_selector, &minter_role_params[..]].concat().into();

    // assemble eip1559 transactions using constructed datas
    let pre_genesis_chain: Arc<RethChainSpec> = Arc::new(tmp_genesis.into());
    let gas_price = 100;
    let faucet_tx_raw = tx_factory.create_eip1559_encoded(
        pre_genesis_chain.clone(),
        None,
        gas_price,
        None,
        U256::ZERO,
        faucet_create_data.clone().into(),
    );

    let stablecoin_tx_raw = tx_factory.create_eip1559_encoded(
        pre_genesis_chain.clone(),
        None,
        gas_price,
        None,
        U256::ZERO,
        stablecoin_create_data.clone().into(),
    );

    let role_tx_raw = tx_factory.create_eip1559_encoded(
        pre_genesis_chain.clone(),
        None,
        gas_price,
        Some(faucet_proxy_address),
        U256::ZERO,
        grant_role_call,
    );

    let updatexyz_tx_raw = tx_factory.create_eip1559_encoded(
        pre_genesis_chain.clone(),
        None,
        gas_price,
        Some(faucet_proxy_address),
        U256::ZERO,
        updatexyz_call,
    );

    let minter_tx_raw = tx_factory.create_eip1559_encoded(
        pre_genesis_chain.clone(),
        None,
        gas_price,
        Some(stablecoin_address),
        U256::ZERO,
        minter_role_call,
    );

    let raw_txs =
        vec![faucet_tx_raw, stablecoin_tx_raw, role_tx_raw, updatexyz_tx_raw, minter_tx_raw];

    let tmp_dir = tempfile::TempDir::new().unwrap();
    let task_manager = TaskManager::new("Temp Task Manager");
    let tmp_reth_env =
        RethEnv::new_for_temp_chain(pre_genesis_chain.clone(), tmp_dir.path(), &task_manager, None)
            .await?;
    // fetch state to be set on the faucet proxy address
    let execution_bundle = tmp_reth_env
        .execution_outcome_for_tests(raw_txs, &pre_genesis_chain.sealed_genesis_header());
    let execution_storage_faucet = &execution_bundle
        .state
        .get(&faucet_proxy_address)
        .expect("faucet address missing from bundle state")
        .storage;
    // fetch state to be set on the stablecoin address
    let execution_storage_stablecoin = &execution_bundle
        .state
        .get(&stablecoin_address)
        .expect("stablecoin address missing from bundle state")
        .storage;

    // real genesis: configure genesis accounts for proxy deployment & faucet_role
    let genesis_accounts = vec![
        (factory_address, GenesisAccount::default().with_balance(U256::MAX)),
        (kms_address, GenesisAccount::default().with_balance(U256::MAX)),
        (
            stablecoin_impl_address,
            GenesisAccount::default().with_code(Some(stablecoin_impl_bytecode.into())),
        ),
        (
            stablecoin_address,
            GenesisAccount::default().with_code(Some(proxy_bytecode.clone().into())).with_storage(
                Some(
                    execution_storage_stablecoin
                        .iter()
                        .map(|(k, v)| ((*k).into(), v.present_value.into()))
                        .collect(),
                ),
            ),
        ),
        (
            faucet_impl_address,
            GenesisAccount::default().with_code(Some(faucet_deployed_bytecode.into())),
        ),
        // convert U256 HashMap to B256 for BTreeMap
        (
            faucet_proxy_address,
            GenesisAccount::default()
                .with_code(Some(proxy_bytecode.into()))
                .with_balance(U256::MAX)
                .with_storage(Some(
                    execution_storage_faucet
                        .iter()
                        .map(|(k, v)| ((*k).into(), v.present_value.into()))
                        .collect(),
                )),
        ),
    ];

    // create and launch validator nodes on local network,
    // use expected faucet contract address from `TransactionFactory::default` with nonce == 0
    let faucet_tmp_dir = tempfile::TempDir::new().unwrap();
    spawn_local_testnet(
        faucet_tmp_dir.path(),
        &faucet_proxy_address.to_string(),
        Some(genesis_accounts),
    )?;
    let genesis_file = faucet_tmp_dir.path().join("shared-genesis/genesis/genesis.yaml");
    let genesis: Genesis = Config::load_from_path(&genesis_file, ConfigFmt::YAML)?;
    let chain: Arc<RethChainSpec> = Arc::new(genesis.clone().into());

    info!(target: "faucet-test", "nodes started - sleeping for 10s...");

    tokio::time::sleep(Duration::from_secs(15)).await;

    let rpc_url = "http://127.0.0.1:8545".to_string();
    let client = HttpClientBuilder::default().build(&rpc_url)?;

    // assert deployer starting balance is properly seeded
    let tx_factory = TransactionFactory::new();
    let default_deployer_address = tx_factory.address();
    let deployer_balance: String =
        client.request("eth_getBalance", rpc_params!(default_deployer_address)).await?;
    debug!(target: "faucet-test", "Deployer starting balance: {deployer_balance:?}");
    assert_eq!(U256::from_str(&deployer_balance)?, U256::MAX);

    // note: response is different each time bc KMS
    //
    // assert starting balance is 0
    let mut random_tx_factory = TransactionFactory::new_random();
    let random_address = random_tx_factory.address();
    let starting_rls_balance: String =
        client.request("eth_getBalance", rpc_params!(random_address)).await?;
    debug!(target: "faucet-test", "starting balance: {starting_rls_balance:?}");
    assert_eq!(U256::from_str(&starting_rls_balance)?, U256::ZERO);

    let rls_tx_hash: String =
        client.request("faucet_transfer", rpc_params![random_address]).await?;
    info!(target: "faucet-test", ?rls_tx_hash, "valid faucet transfer tx hash");

    // more than enough time for the nodes to launch RPCs
    let duration = Duration::from_secs(30);

    // ensure account balance increased
    let expected_rls_balance = U256::from_str("0xde0b6b3a7640000")?; // 1*10^18 (1 RLS)
    let _ = timeout(
        duration,
        ensure_account_balance_infinite_loop(&client, random_address, expected_rls_balance),
    )
    .await?
    .expect("expected balance timeout");

    // duplicate request is err
    assert!(client
        .request::<String, _>("faucet_transfer", rpc_params![random_address])
        .await
        .is_err());

    // NOW:
    // submit another tx from the account that just got dripped
    // so the the worker's watch channel updates to a new batch that doesn't have
    // the faucet's address in the state
    //
    // this creates scenario for faucet to rely on provider.latest() for accuracy
    let tx = random_tx_factory.create_eip1559(
        chain,
        None,
        1_000_000_000,
        Some(Address::random()),
        U256::from_str("0xaffffffffffffff").expect("U256 from str for tx factory"),
        Default::default(),
    );

    info!(target: "faucet-test", ?tx, "submitting new tx to clear worker's watch channel...");

    // submit tx through rpc
    let tx_bytes = tx.encoded_2718();
    let tx_hash: String = client.request("eth_sendRawTransaction", rpc_params![tx_bytes]).await?;
    info!(target: "faucet-test", ?tx_hash, "tx submitted :D");

    // ensure account balance decreased
    let expected_balance = U256::from_str("0x2e0b6b3a761c1c9")?;
    let _ = timeout(
        duration,
        ensure_account_balance_infinite_loop(&client, random_address, expected_balance),
    )
    .await?
    .expect("expected balance timeout");

    // request another faucet drip for random address
    //
    // assert starting balance is 0
    info!(target: "faucet-test", ?expected_balance, "account balance decreased. requesting from faucet again...");
    let new_random_address = Address::random();
    let starting_balance: String =
        client.request("eth_getBalance", rpc_params!(new_random_address)).await?;
    assert_eq!(U256::from_str(&starting_balance)?, U256::ZERO);

    let tx_hash: String =
        client.request("faucet_transfer", rpc_params![new_random_address]).await?;
    info!(target: "faucet-test", ?tx_hash, "new random faucet request success. waiting for balance increase...");

    // ensure account balance increased
    //
    // account balance is only updates on final execution
    // finding the expected balance in time means the faucet successfully used the correct nonce
    let _ = timeout(
        duration,
        ensure_account_balance_infinite_loop(&client, new_random_address, expected_rls_balance),
    )
    .await?
    .expect("expected balance random account timeout");

    // duplicate request is err
    info!(target: "faucet-test", "account balance updated. submitting duplicate request and shutting down...");
    assert!(client
        .request::<String, _>("faucet_transfer", rpc_params![new_random_address])
        .await
        .is_err());

    // assert starting stablecoin balance is 0
    let signer = random_tx_factory.get_default_signer()?;
    let wallet = EthereumWallet::from(signer);
    let provider = ProviderBuilder::new().wallet(wallet).connect_http(rpc_url.parse()?);
    let stablecoin_contract = Stablecoin::new(stablecoin_address, provider.clone());
    let starting_xyz_balance: U256 =
        U256::from(stablecoin_contract.balanceOf(new_random_address).call().await?);
    debug!(target: "faucet-test", "starting balance: {starting_xyz_balance:?}");
    assert_eq!(starting_xyz_balance, U256::ZERO);

    // drip XYZ to new_random_address
    let xyz_tx_hash: String = client
        .request("faucet_transfer", rpc_params![new_random_address, stablecoin_address])
        .await?;
    info!(target: "faucet-test", ?xyz_tx_hash, "valid faucet XYZ transfer tx hash");

    // ensure account balance increased
    let expected_xyz_balance = U256::from(1_000_000); // 1e6 (1 XYZ)

    let result = timeout(duration, async {
        loop {
            let actual_xyz_balance: U256 =
                stablecoin_contract.balanceOf(new_random_address).call().await?;
            debug!(target: "faucet-test", "actual balance: {:?}", actual_xyz_balance);

            if actual_xyz_balance == expected_xyz_balance {
                return Ok::<_, eyre::Report>(actual_xyz_balance);
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
    .await;

    match result {
        Ok(Ok(balance)) => {
            info!(target: "faucet-test", "Balance check completed successfully: {}", balance);
        }
        Ok(Err(e)) => {
            panic!("Error while checking balance: {e:?}");
        }
        Err(_) => {
            panic!("Balance check timed out");
        }
    }

    // duplicate XYZ request is err
    info!(target: "faucet-test", "account balance updated. submitting duplicate request and shutting down...");
    assert!(client
        .request::<String, _>(
            "faucet_transfer",
            rpc_params![new_random_address, stablecoin_address]
        )
        .await
        .is_err());

    // submit 100 txs
    let random_addresses: Vec<Address> = (0..50).map(|_| Address::random()).collect();
    let mut requests: FuturesUnordered<JoinHandle<String>> = random_addresses
        .clone()
        .into_iter()
        .map(|address| {
            tokio::spawn({
                let client = client.clone();
                async move {
                    client
                        .clone()
                        .request::<String, _>("faucet_transfer", rpc_params![address])
                        .await
                        .expect("request successful")
                }
            })
        })
        .collect();

    while let Some(res) = requests.next().await {
        assert!(res.is_ok());
    }

    // wait for all account balances to update
    let mut check_account_balances: FuturesUnordered<JoinHandle<()>> = random_addresses
        .into_iter()
        .map(|address| {
            tokio::spawn({
                let client = client.clone();
                async move {
                    // ensure account balance increased
                    //
                    // account balance is only updates on final execution
                    // finding the expected balance in time means the faucet successfully used the
                    // correct nonce
                    let _ = timeout(
                        duration,
                        ensure_account_balance_infinite_loop(
                            &client,
                            address,
                            expected_rls_balance,
                        ),
                    )
                    .await
                    .expect("account balance okay")
                    .expect("expected balance random account timeout");
                }
            })
        })
        .collect();

    while let Some(res) = check_account_balances.next().await {
        assert!(res.is_ok());
    }

    Ok(())
}

/// Retrieve the public key from KMS.
///
/// This simulates what the startup script should do on deployed nodes:
/// - set an env variable to the PEM formatted key.
async fn set_google_kms_public_key_env_var() -> eyre::Result<()> {
    // Detect Google project ID using environment variables PROJECT_ID/GCP_PROJECT_ID
    // or GKE metadata server when the app runs inside GKE
    let google_project_id = GoogleEnvironment::detect_google_project_id().await
        .expect("No Google Project ID detected. Please specify it explicitly using env variable: PROJECT_ID");

    let kms_client: GoogleApi<KeyManagementServiceClient<GoogleAuthMiddleware>> =
        GoogleApi::from_function(
            KeyManagementServiceClient::new,
            "https://cloudkms.googleapis.com",
            None,
        )
        .await?;

    // retrieve api information from env
    let locations = std::env::var("KMS_KEY_LOCATIONS")
        .expect("KMS_KEY_LOCATIONS must be set in the environment");
    let key_rings =
        std::env::var("KMS_KEY_RINGS").expect("KMS_KEY_RINGS must be set in the environment");
    let crypto_keys =
        std::env::var("KMS_CRYPTO_KEYS").expect("KMS_CRYPTO_KEYS must be set in the environment");
    let crypto_key_versions = std::env::var("KMS_CRYPTO_KEY_VERSIONS")
        .expect("KMS_CRYPTO_KEY_VERSIONS must be set in the environment");

    // construct api endpoint for Google KMS requests
    let name = format!(
        "projects/{google_project_id}/locations/{locations}/keyRings/{key_rings}/cryptoKeys/{crypto_keys}/cryptoKeyVersions/{crypto_key_versions}"
    );

    // request KMS public key
    let kms_pubkey_response =
        kms_client.get().get_public_key(GetPublicKeyRequest { name: name.clone() }).await?;

    // convert pem pubkey format
    let kms_pem_pubkey = kms_pubkey_response.into_inner().pem;
    // store to env
    std::env::set_var("FAUCET_PUBLIC_KEY", kms_pem_pubkey);

    Ok(())
}

/// Use Google KMS credentials json to fetch public key, seed account at genesis, and set env vars
/// for faucet signature requests.
async fn prepare_google_kms_env() -> eyre::Result<(Arc<RethChainSpec>, Address)> {
    // set application credentials for accessing Google KMS API
    std::env::set_var(
        "GOOGLE_APPLICATION_CREDENTIALS",
        "../../crates/execution/faucet/gcloud-credentials.json",
    );
    // set Project ID for google_sdk
    std::env::set_var("PROJECT_ID", "rayls-network");
    // set env vars for faucet cli
    std::env::set_var("KMS_KEY_LOCATIONS", "global");
    std::env::set_var("KMS_KEY_RINGS", "tests");
    std::env::set_var("KMS_CRYPTO_KEYS", "key-for-unit-tests");
    std::env::set_var("KMS_CRYPTO_KEY_VERSIONS", "1");

    // fetch kms address from google and set env
    set_google_kms_public_key_env_var().await?;
    let kms_pem_pubkey = std::env::var("FAUCET_PUBLIC_KEY")?;
    // k256 public key to convert from pem
    let pubkey_from_pem = PubKey::from_public_key_pem(&kms_pem_pubkey)?;
    // secp256k1 public key from uncompressed k256 variation
    let public_key = PublicKey::from_slice(pubkey_from_pem.to_encoded_point(false).as_bytes())?;
    // calculate address from uncompressed public key
    let kms_address = public_key_to_address(public_key);

    // create genesis and fund relevant accounts
    let genesis = testnet_genesis();
    let faucet_account = vec![(kms_address, GenesisAccount::default().with_balance(U256::MAX))];
    let default_deployer_address = TransactionFactory::default().address();
    let default_deployer_account =
        vec![(default_deployer_address, GenesisAccount::default().with_balance(U256::MAX))];

    let accounts_to_fund = faucet_account.into_iter().chain(default_deployer_account.into_iter());
    let genesis = genesis.extend_accounts(accounts_to_fund);

    Ok((Arc::new(genesis.into()), kms_address))
}
