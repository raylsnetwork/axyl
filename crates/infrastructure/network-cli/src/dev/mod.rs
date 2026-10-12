//! Dev mode: one-command local chain bootstrap.
//!
//! This module powers two entrypoints (issue #590):
//!
//! - `rayls-network dev` — the all-in-one [`DevCommand`]: auto-bootstrap an empty datadir, then
//!   start the node with dev-friendly RPC defaults.
//! - `rayls-network node --dev` — the lower-level path; [`NodeCommand::execute`] calls
//!   [`bootstrap_dev_datadir_if_empty`] before booting.
//!
//! Bootstrapping runs the single-validator genesis ceremony in-process (no manual
//! `keytool generate` / `genesis` steps): generate the validator key, write its
//! `node-info.yaml` into `genesis/validators/`, run [`GenesisArgs::dev`], then
//! pre-fund the well-known dev accounts below.
//!
//! NOT FOR PRODUCTION USE — the dev accounts' private keys are public, the chain is
//! gasless, and the network is a 1-of-1 committee with no Byzantine fault tolerance.

mod dashboard;

use crate::{genesis::GenesisArgs, node::NodeCommand, NoArgs};
use core::fmt;
use rayls_infrastructure_config::{Config, ConfigFmt, ConfigTrait as _, RaylsDirs as _};
use rayls_infrastructure_types::{Address, Genesis, GenesisAccount, RaylsNetwork, U256};
use rayls_middleware_orchestrator::engine::RaylsBuilder;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

/// Default BLS-key passphrase used by the `dev` subcommand when none is supplied,
/// so `rayls-network dev` is truly one-command. The key never leaves the local
/// datadir and protects nothing of value on a throwaway dev chain.
pub const DEV_PASSPHRASE: &str = "rayls-dev";

/// Well-known development accounts (the standard Hardhat / Anvil test keys).
///
/// Their private keys are PUBLIC and identical across every Hardhat/Anvil install,
/// so they import into any wallet (MetaMask, cast, ethers) without ceremony. Each is
/// pre-funded with native USDr in the dev genesis. NEVER use these on a real network.
///
/// `(address, private_key)`:
pub const DEV_ACCOUNTS: &[(&str, &str)] = &[
    (
        "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
    ),
    (
        "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d",
    ),
    (
        "0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC",
        "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a",
    ),
    (
        "0x90F79bf6EB2c4f870365E785982E1f101E93b906",
        "0x7c852118294e51e653712a81e05800f419141751be58f605c371e15141b007a6",
    ),
];

/// Execution / fee-recipient address for the single dev validator (dev account #0),
/// so block rewards land in a known, importable account.
pub const DEV_VALIDATOR_FEE_ADDRESS: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";

/// Native balance pre-funded to each dev account at genesis (1,000,000 USDr).
fn dev_account_balance() -> U256 {
    U256::from(1_000_000u64) * U256::from(10u64).pow(U256::from(18u64))
}

/// Sentinel file written as the very last step of a successful bootstrap.
///
/// Using the sentinel instead of `committee.yaml` (written in step 3) as the
/// idempotency guard prevents a partially-bootstrapped datadir: if step 4
/// (`fund_dev_accounts`) fails after step 3 has already written `committee.yaml`,
/// every subsequent call would see `committee.yaml`, return `Ok(false)`, and
/// silently skip account funding — leaving a zero-balance datadir that looks healthy
/// but never mines transactions. The sentinel is written last, so a partial failure
/// at any step leaves the guard unset and the next call retries the full ceremony.
const BOOTSTRAP_SENTINEL: &str = ".dev-bootstrap-complete";

/// Bootstrap an empty datadir into a working single-validator dev chain.
///
/// Idempotent: if the datadir already contains a completed bootstrap sentinel it is
/// left untouched and `Ok(false)` is returned. On a fresh datadir it performs the
/// full ceremony and returns `Ok(true)`.
pub fn bootstrap_dev_datadir_if_empty(datadir: &Path, passphrase: &str) -> eyre::Result<bool> {
    // Owned PathBuf so the `RaylsDirs` blanket impl (which requires `'static`)
    // resolves to `PathBuf` rather than a borrowed `&Path`.
    let datadir = datadir.to_path_buf();

    let sentinel = datadir.join(BOOTSTRAP_SENTINEL);
    if sentinel.exists() {
        return Ok(false);
    }

    info!(target: "rl::dev", datadir = %datadir.display(), "empty datadir — bootstrapping single-validator dev chain");

    // 1. Generate the validator key + node-info.yaml (fee recipient = dev account #0).
    let fee_recipient: Address =
        DEV_VALIDATOR_FEE_ADDRESS.parse().expect("DEV_VALIDATOR_FEE_ADDRESS is a valid address");
    crate::keytool::generate_validator_keys(&datadir, fee_recipient, passphrase.to_string())?;

    // 2. Place node-info.yaml where the genesis ceremony reads validators from.
    let validators_dir = datadir.genesis_path().join("validators");
    std::fs::create_dir_all(&validators_dir)?;
    std::fs::copy(datadir.node_info_path(), validators_dir.join("validator.yaml"))?;

    // 3. Run the single-validator dev genesis ceremony (gasless, local chain-id, fast headers).
    //    Writes genesis.yaml, committee.yaml, parameters.yaml. The node boot selects the schedule
    //    explicitly (`--dev` implies `--network local`), so no stamping is needed.
    GenesisArgs::dev().execute(datadir.clone())?;

    // 4. Pre-fund the well-known dev accounts so txs can be sent immediately.
    fund_dev_accounts(&datadir)?;

    // Write the sentinel last so any failure above leaves the guard unset and the
    // next `bootstrap_dev_datadir_if_empty` call retries the full ceremony.
    std::fs::write(&sentinel, b"")?;

    warn!(
        target: "rl::dev",
        "DEV chain bootstrapped (chain-id {}, gasless, 1-of-1 committee) — NOT FOR PRODUCTION",
        RaylsNetwork::Local.chain_id()
    );
    Ok(true)
}

/// Extend the freshly-written genesis with the pre-funded dev accounts.
fn fund_dev_accounts(datadir: &Path) -> eyre::Result<()> {
    let datadir = datadir.to_path_buf();
    let genesis_path = datadir.genesis_file_path();
    let genesis: Genesis = Config::load_from_path(&genesis_path, ConfigFmt::YAML)?;

    let balance = dev_account_balance();
    let accounts: Vec<(Address, GenesisAccount)> = DEV_ACCOUNTS
        .iter()
        .map(|(addr, _key)| {
            let addr: Address = addr.parse().expect("DEV_ACCOUNTS address is valid");
            (addr, GenesisAccount::default().with_balance(balance))
        })
        .collect();

    let genesis = genesis.extend_accounts(accounts);
    Config::write_to_path(&genesis_path, &genesis, ConfigFmt::YAML)?;
    Ok(())
}

/// All-in-one local dev chain: bootstrap (if needed) and start the node with
/// dev-friendly RPC defaults.
///
/// Flattens [`NodeCommand`] so every node/reth flag is still available as a
/// pass-through (e.g. `--instance`, `--http.port`, `--metrics`); the dev defaults
/// below are applied on top before launch.
#[derive(Debug, clap::Parser)]
pub struct DevCommand<Ext: clap::Args + fmt::Debug = NoArgs> {
    /// Port for the embedded dev dashboard (block explorer + chain status UI).
    #[arg(long = "dashboard-port", default_value_t = 8550)]
    pub dashboard_port: u16,

    /// Disable the embedded dev dashboard.
    #[arg(long = "no-dashboard", default_value_t = false)]
    pub no_dashboard: bool,

    /// All standard node arguments are accepted and override the dev defaults.
    #[clap(flatten)]
    pub node: NodeCommand<Ext>,
}

impl<Ext: clap::Args + fmt::Debug> DevCommand<Ext> {
    /// Execute the `dev` command: apply dev defaults and launch (the node's
    /// `execute` auto-bootstraps an empty datadir because `dev` is set).
    pub fn execute<L>(
        mut self,
        rl_datadir: PathBuf,
        passphrase: String,
        launcher: L,
    ) -> eyre::Result<()>
    where
        L: FnOnce(RaylsBuilder, Ext, PathBuf, String) -> eyre::Result<()>,
    {
        // `dev` is the single-node opt-in.
        self.node.dev = true;

        // RPC on by default with local-friendly host/CORS so wallets/dApps/scripts
        // connect out of the box. (`--dev` only auto-enables HTTP at parse time, so
        // we set these explicitly here for the `dev` subcommand path.)
        let rpc = &mut self.node.reth.rpc;
        rpc.http = true;
        rpc.ws = true;
        if rpc.http_corsdomain.is_none() {
            rpc.http_corsdomain = Some("*".to_string());
        }
        if rpc.ws_allowed_origins.is_none() {
            rpc.ws_allowed_origins = Some("*".to_string());
        }
        // Disable IPC so repeated/parallel dev nodes don't collide on the socket path.
        rpc.ipcdisable = true;

        // V1 (plain) storage is banned at boot, and the e2e harness passes the flag
        // explicitly — apply the default here so a plain `rayls-network dev` works
        // out of the box (there is no CLI way to select V1, so this can't override
        // an explicit choice).
        self.node.reth.storage.v2 = true;

        // Serve the embedded dashboard (block explorer + chain status) so the user
        // can see the chain is live. Runs on background threads; the node launch
        // below blocks for the process lifetime.
        if !self.no_dashboard {
            let rpc_url = format!("http://127.0.0.1:{}", self.node.reth.rpc.http_port);
            dashboard::spawn_dashboard(self.dashboard_port, rpc_url);
        }

        self.node.execute(rl_datadir, passphrase, launcher)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dev_accounts_parse() {
        // Every well-known dev account address and key must be valid.
        for (addr, key) in DEV_ACCOUNTS {
            addr.parse::<Address>().unwrap_or_else(|e| panic!("bad dev address {addr}: {e}"));
            assert!(key.starts_with("0x") && key.len() == 66, "bad dev key {key}");
        }
        DEV_VALIDATOR_FEE_ADDRESS.parse::<Address>().expect("validator fee address valid");
    }

    #[test]
    fn dev_genesis_is_gasless_local() {
        let g = GenesisArgs::dev();
        assert_eq!(g.chain_id, RaylsNetwork::Local.chain_id(), "dev must use the local chain-id");
        assert_eq!(g.base_fee, 0, "dev chain is gasless");
        assert_eq!(g.min_base_fee, 0, "dev chain has no base-fee floor");
    }
}
