//! Tests with RPC calls for ConsensusRegistry in genesis (through CLI).
//!
//! NOTE: this test contains code for executing a proxy/impl pre-genesis
//! however, the RPC calls don't work. The beginning of the test is left
//! because the proxy version may be re-prioritized later.

use alloy::{
    network::EthereumWallet,
    providers::{Provider, ProviderBuilder},
};
use core::panic;
use e2e_tests::{spawn_local_testnet, IT_TEST_MUTEX};
use eyre::OptionExt;
use jsonrpsee::{core::client::ClientT, http_client::HttpClientBuilder, rpc_params};
use rayls_execution_evm::{
    reth_env::RethEnv,
    system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS},
    test_utils::TransactionFactory,
};
use rayls_infrastructure_config::{NetworkGenesis, BLSG1_JSON, DEPLOYMENTS_JSON};
use rayls_infrastructure_types::{Address, Bytes, FromHex};
use serde_json::Value;
use std::time::Duration;
use tokio::time::timeout;
use tracing::debug;

#[tokio::test(flavor = "multi_thread")]
async fn test_genesis_with_precompiles() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    // sleep for other tests to cleanup
    std::thread::sleep(std::time::Duration::from_secs(5));
    // spawn testnet for RPC calls
    let temp_path =
        tempfile::TempDir::with_suffix("genesis_with_precompiles").expect("tempdir is okay");
    let _testnet = spawn_local_testnet(
        temp_path.path(),
        #[cfg(feature = "faucet")]
        "0x0000000000000000000000000000000000000000",
        None,
    )
    .expect("failed to spawn testnet");
    // wait for node rpc to become available
    let rpc_url = "http://127.0.0.1:8545".to_string();
    let provider_check = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    timeout(Duration::from_secs(30), async {
        let mut result = provider_check.get_chain_id().await;
        while let Err(e) = result {
            debug!(target: "genesis-test", "waiting for rpc: {e:?}");
            tokio::time::sleep(Duration::from_secs(1)).await;
            result = provider_check.get_chain_id().await;
        }
    })
    .await?;

    let client = HttpClientBuilder::default().build(&rpc_url).expect("couldn't build rpc client");

    let precompiles =
        NetworkGenesis::fetch_precompile_genesis_accounts().expect("precompiles not found");

    // Verify each precompile account has bytecode deployed on-chain.
    // We only check code presence (not storage slot values) because the genesis
    // ceremony may legitimately overwrite storage for accounts that overlap with
    // consensus registry execution (e.g. DelegationPool owner slot).
    for (address, genesis_account) in precompiles {
        if let Some(_expected_code) = genesis_account.code {
            let returned_code: String = client.request("eth_getCode", rpc_params!(address)).await?;
            let on_chain_code = Bytes::from_hex(&returned_code)?;
            assert!(!on_chain_code.is_empty(), "precompile {address} should have code deployed");
            debug!(target: "genesis-test", ?address, code_len = on_chain_code.len(), "precompile deployed");
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_precompile_genesis_accounts() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    // sleep for other tests to cleanup
    std::thread::sleep(std::time::Duration::from_secs(5));

    let precompiles =
        NetworkGenesis::fetch_precompile_genesis_accounts().expect("precompiles not found");

    // Verify we have precompile accounts
    assert!(!precompiles.is_empty(), "precompiles should not be empty");

    // Verify key addresses from deployments.json are present
    let expected_deployments = RethEnv::fetch_value_from_json_str(DEPLOYMENTS_JSON, None)?;
    let is_address_present = |address: &str| {
        let target = Address::from_hex(address).expect("valid hex address");
        precompiles.iter().any(|(addr, _)| *addr == target)
    };

    // Check that key precompile addresses are in the genesis accounts
    for key in &["Safe", "SafeImpl", "FeeAggregator", "NativeTokenController"] {
        if let Some(addr) = expected_deployments.get(*key).and_then(Value::as_str) {
            assert!(is_address_present(addr), "{key} ({addr}) is not present in precompiles");
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_genesis_with_consensus_registry() -> eyre::Result<()> {
    let _guard = IT_TEST_MUTEX.lock();
    // sleep for other tests to cleanup
    std::thread::sleep(std::time::Duration::from_secs(5));
    // fetch blsg1 bytecode and derive blsg1 address
    let tao_address_binding = RethEnv::fetch_value_from_json_str(DEPLOYMENTS_JSON, Some("Safe"))?;
    let tao_address =
        Address::from_hex(tao_address_binding.as_str().ok_or_eyre("Safe owner address")?)?;
    let blsg1_address = tao_address.create(0).to_string();

    let blsg1_runtimecode_binding =
        RethEnv::fetch_value_from_json_str(BLSG1_JSON, Some("deployedBytecode.object"))?;
    let blsg1_deployed_bytecode =
        blsg1_runtimecode_binding.as_str().ok_or_eyre("invalid blsg1 json")?;

    // spawn testnet for RPC calls
    let temp_path =
        tempfile::TempDir::with_suffix("genesis_with_consensus_registry").expect("tempdir is okay");
    let _testnet = spawn_local_testnet(
        temp_path.path(),
        #[cfg(feature = "faucet")]
        "0x0000000000000000000000000000000000000000",
        None,
    )
    .expect("failed to spawn testnet");
    // wait for node rpc to become available
    let rpc_url = "http://127.0.0.1:8545".to_string();
    let provider_check = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    timeout(Duration::from_secs(30), async {
        let mut result = provider_check.get_chain_id().await;
        while let Err(e) = result {
            debug!(target: "genesis-test", "waiting for rpc: {e:?}");
            tokio::time::sleep(Duration::from_secs(1)).await;
            result = provider_check.get_chain_id().await;
        }
    })
    .await?;

    let client = HttpClientBuilder::default().build(&rpc_url).expect("couldn't build rpc client");

    // sanity check onchain spawned contracts in genesis
    // Registry bytecode has immutables (e.g. _rls address) filled in by the constructor,
    // so we only verify it is non-empty rather than comparing against the artifact.
    let returned_registry_bytecode: String = client
        .request("eth_getCode", rpc_params!(CONSENSUS_REGISTRY_ADDRESS))
        .await
        .expect("Failed to fetch registry bytecode");
    assert!(
        !Bytes::from_hex(&returned_registry_bytecode)?.is_empty(),
        "ConsensusRegistry should be deployed"
    );

    let returned_blsg1_bytecode: String = client
        .request("eth_getCode", rpc_params!(blsg1_address))
        .await
        .expect("Failed to fetch BLS G1 bytecode");
    assert_eq!(
        Bytes::from_hex(&returned_blsg1_bytecode)?,
        Bytes::from_hex(blsg1_deployed_bytecode)?
    );

    let tx_factory = TransactionFactory::default();
    let signer = tx_factory.get_default_signer().expect("failed to fetch signer");
    let wallet = EthereumWallet::from(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(rpc_url.parse().expect("rpc url parse error"));

    // test rpc calls for registry in genesis - this is not the one deployed for the test
    let consensus_registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, provider.clone());
    let current_epoch_info =
        consensus_registry.getCurrentEpochInfo().call().await.expect("get current epoch result");

    debug!(target: "genesis-test", "consensus_registry: {:#?}", current_epoch_info);
    let ConsensusRegistry::EpochInfo { committee, blockHeight, epochDuration, stakeVersion } =
        current_epoch_info;
    assert_eq!(blockHeight, 0);
    assert_eq!(epochDuration, 86400);
    assert_eq!(stakeVersion, 0);

    let validators = consensus_registry
        .getValidators(ConsensusRegistry::ValidatorStatus::Active.into())
        .call()
        .await
        .expect("failed active validators read");

    let validator_addresses: Vec<_> = validators.iter().map(|v| v.validatorAddress).collect();
    assert_eq!(committee, validator_addresses);
    debug!(target: "genesis-test", "active validators??\n{:?}", validators);

    Ok(())
}
