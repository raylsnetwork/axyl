// SPDX-License-Identifier: BUSL-1.1
//! Utilities for it tests.

// ignore for IT test
#![allow(unused_crate_dependencies)]
use clap::Parser;
use escargot::{CargoBuild, CargoRun};
use rayls_infrastructure_config::{Config, ConfigFmt, ConfigTrait};
use rayls_infrastructure_types::{test_utils::CommandParser, Address, Genesis, GenesisAccount};
use rayls_middleware_orchestrator::launch_node;
use rayls_network_cli::{genesis::GenesisArgs, keytool::KeyArgs, node::NodeCommand};
use std::{
    path::{Path, PathBuf},
    sync::OnceLock,
};
use tracing::{error, info};
// unused deps warnings

/// Limit potential for port collisions.
pub static IT_TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());
/// Only compile main bin once for all tests.
pub static RAYLS_BINARY: OnceLock<CargoRun> = OnceLock::new();

const NODE_PASSWORD: &str = "sup3rsecuur";

/// Execute genesis ceremony inside tempdir
pub fn create_validator_info(dir: &Path, address: &str, passphrase: String) -> eyre::Result<()> {
    let datadir = dir.to_path_buf();

    // keytool
    let keys_command =
        CommandParser::<KeyArgs>::parse_from(["rl", "generate", "validator", "--address", address]);
    keys_command.args.execute(datadir, passphrase)?;

    Ok(())
}

/// Execute observer config inside tempdir
fn create_observer_info(datadir: PathBuf, passphrase: String) -> eyre::Result<()> {
    // keytool
    let keys_command = CommandParser::<KeyArgs>::parse_from([
        "rl",
        "generate",
        "observer",
        "--address",
        "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
    ]);
    keys_command.args.execute(datadir, passphrase)
}

/// Create validator info, genesis ceremony, and spawn node command with faucet active.
pub fn config_local_testnet(
    temp_path: &Path,
    passphrase: String,
    accounts: Option<Vec<(Address, GenesisAccount)>>,
) -> eyre::Result<()> {
    let validators = [
        ("validator-1", "0x1111111111111111111111111111111111111111"),
        ("validator-2", "0x2222222222222222222222222222222222222222"),
        ("validator-3", "0x3333333333333333333333333333333333333333"),
        ("validator-4", "0x4444444444444444444444444444444444444444"),
    ];

    // create shared genesis dir
    let shared_genesis_dir = temp_path.join("shared-genesis");
    let copy_path = shared_genesis_dir.join("genesis/validators");
    std::fs::create_dir_all(&copy_path)?;
    // create validator info and copy to shared genesis dir
    for (v, addr) in validators.iter() {
        let dir = temp_path.join(v);
        // init genesis ceremony to create committee files
        create_validator_info(&dir, addr, passphrase.clone())?;

        // copy to shared genesis dir
        std::fs::copy(dir.join("node-info.yaml"), copy_path.join(format!("{v}.yaml")))?;
    }

    // Create an observer config.
    let dir = temp_path.join("observer");
    // init config ceremony for observer
    create_observer_info(dir, passphrase.clone())?;

    // create committee from shared genesis dir
    let create_committee_command = CommandParser::<GenesisArgs>::parse_from([
        "rl",
        "--basefee-address",
        "0x9999999999999999999999999999999999999999",
        "--consensus-registry-owner",
        "0x00000000000000000000000000000000000007a0",
        "--dev-funded-account",
        "test-source",
        "--max-header-delay-ms",
        "1000",
        "--min-header-delay-ms",
        "500",
    ]);
    create_committee_command.args.execute(shared_genesis_dir.clone())?;
    // If provided optional accounts then hack them into genesis now...
    if let Some(accounts) = accounts {
        let data_dir = shared_genesis_dir.join("genesis/genesis.yaml");
        let genesis: Genesis = Config::load_from_path(&data_dir, ConfigFmt::YAML)?;
        let genesis = genesis.extend_accounts(accounts);
        Config::write_to_path(&data_dir, &genesis, ConfigFmt::YAML)?;
    }

    for (v, _addr) in validators.iter() {
        let dir = temp_path.join(v);
        std::fs::create_dir_all(dir.join("genesis"))?;
        // copy genesis files back to validator dirs
        std::fs::copy(
            shared_genesis_dir.join("genesis/committee.yaml"),
            dir.join("genesis/committee.yaml"),
        )?;
        std::fs::copy(
            shared_genesis_dir.join("genesis/genesis.yaml"),
            dir.join("genesis/genesis.yaml"),
        )?;
        std::fs::copy(shared_genesis_dir.join("parameters.yaml"), dir.join("parameters.yaml"))?;
    }

    let dir = temp_path.join("observer");
    // copy genesis files back to observer dirs
    std::fs::create_dir_all(dir.join("genesis"))?;
    std::fs::copy(
        shared_genesis_dir.join("genesis/committee.yaml"),
        dir.join("genesis/committee.yaml"),
    )?;
    std::fs::copy(
        shared_genesis_dir.join("genesis/genesis.yaml"),
        dir.join("genesis/genesis.yaml"),
    )?;
    std::fs::copy(shared_genesis_dir.join("parameters.yaml"), dir.join("parameters.yaml"))?;
    Ok(())
}

/// Create validator info, genesis ceremony, and spawn node command with faucet active.
pub fn spawn_local_testnet(
    temp_path: &Path,
    #[cfg(feature = "faucet")] faucet_contract_address: &str,
    accounts: Option<Vec<(Address, GenesisAccount)>>,
) -> eyre::Result<()> {
    config_local_testnet(temp_path, NODE_PASSWORD.to_owned(), accounts)?;

    let validators = ["validator-1", "validator-2", "validator-3", "validator-4"];
    for v in validators.into_iter() {
        let dir = temp_path.join(v);
        let instance = v.chars().last().expect("validator instance").to_string();

        #[cfg(feature = "faucet")]
        let command = NodeCommand::<rayls_execution_faucet::FaucetArgs>::parse_from([
            "rl",
            "--http",
            "--storage.v2",
            "--instance",
            &instance,
            "--google-kms",
            "--faucet-contract",
            faucet_contract_address,
        ]);
        #[cfg(not(feature = "faucet"))]
        let command =
            NodeCommand::parse_from(["rl", "--http", "--storage.v2", "--instance", &instance]);

        std::thread::spawn(|| {
            let err = command.execute(
                dir,
                NODE_PASSWORD.to_string(),
                |mut builder, faucet_args, rl_datadir, passphrase| {
                    builder.opt_faucet_args = Some(faucet_args);
                    launch_node(builder, rl_datadir, passphrase)
                },
            );
            error!("{:?}", err);
        });
    }

    Ok(())
}

/// Helper to retrieve and build the main project binary.
pub fn get_rayls_network_binary() -> &'static CargoRun {
    info!("building main binary for e2e tests");
    RAYLS_BINARY.get_or_init(|| {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
        let path = PathBuf::from(manifest_dir);
        let workspace_root = path
            .ancestors()
            .find(|p| p.join("Cargo.toml").exists() && p.join("crates").exists())
            .expect("Cannot find workspace root");

        let build = CargoBuild::new()
            .bin("rayls-network")
            .manifest_path(workspace_root.join("Cargo.toml"))
            .target_dir(workspace_root.join("target"))
            .current_target();
        // The dev-mode e2e test needs the binary built with the `dev` feature so the
        // `dev` subcommand exists; other e2e tests don't, so only add it when this
        // crate is itself built with `--features dev`.
        #[cfg(feature = "dev-single-node-setup")]
        let build = build.features("dev-single-node-setup");
        build.run().expect("Failed to build rayls-network binary")
    })
}

// imports for traits used in faucet tests only
#[cfg(feature = "faucet")]
use jsonrpsee::core::client::ClientT;
#[cfg(feature = "faucet")]
use rayls_infrastructure_types::U256;
#[cfg(feature = "faucet")]
use std::str::FromStr as _;

/// RPC request to continually check until an account balance is above 0.
///
/// Warning: this should only be called with a timeout - could result in infinite loop otherwise.
#[cfg(feature = "faucet")]
pub async fn ensure_account_balance_infinite_loop(
    client: &jsonrpsee::http_client::HttpClient,
    address: Address,
    expected_bal: U256,
) -> eyre::Result<U256> {
    while let Ok(bal) =
        client.request::<String, _>("eth_getBalance", jsonrpsee::rpc_params!(address)).await
    {
        tracing::debug!(target: "faucet-test", "{address} bal: {bal:?}");
        let balance = U256::from_str(&bal)?;

        // return Ok if expected bal
        if balance == expected_bal {
            return Ok(balance);
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    Ok(U256::ZERO)
}
