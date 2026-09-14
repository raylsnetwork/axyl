//! Genesis ceremony command.
//!
//! The genesis ceremony is how networks are started.

use clap::Args;
use rayls_execution_evm::{
    reth_env::{genesis::apply_greenfield_fixes, RethEnv},
    system_calls::ConsensusRegistry,
    RethChainSpec,
};
use rayls_infrastructure_config::{
    Config, ConfigFmt, ConfigTrait, NetworkGenesis, Parameters, RaylsDirs as _,
    DELEGATION_POOL_ADDRESS, FEE_AGGREGATOR_ADDRESS, GOVERNANCE_SAFE_ADDRESS,
    NATIVE_TOKEN_CONTROLLER_ADDRESS, REWARD_DISTRIBUTOR_ADDRESS, RLS_ACCUMULATOR_ADDRESS,
};
use rayls_infrastructure_types::{
    keccak256, set_genesis_defaults, Address, GenesisAccount, RaylsNetwork,
    ETHEREUM_BLOCK_GAS_LIMIT_56BITS, MIN_RAYLS_PROTOCOL_BASE_FEE, U256,
};
use secp256k1::{
    rand::{rngs::StdRng, SeedableRng},
    Secp256k1,
};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};
use tracing::info;

use crate::args::{clap_address_parser, clap_u256_parser_to_18_decimals, maybe_hex};

/// Generate a new chain genesis.
#[derive(Debug, Args)]
pub struct GenesisArgs {
    /// The owner's address for initializing the `ConsensusRegistry` in genesis.
    ///
    /// This address is used to initialize the owner for `ConsensusRegistry`.
    /// This should be a governance-controller, multisig address in production.
    ///
    /// Address doesn't have to start with "0x", but the CLI supports the "0x" format too.
    #[arg(
        long = "consensus-registry-owner",
        alias = "consensus_registry_owner",
        help_heading = "The owner for ConsensusRegistry",
        value_parser = clap_address_parser,
        default_value_t = GOVERNANCE_SAFE_ADDRESS,
        verbatim_doc_comment
    )]
    pub consensus_registry_owner: Address,

    /// The address receives all transaction base fees.
    ///
    /// This is the FeeAggregator contract that collects and distributes fees.
    ///
    /// Address doesn't have to start with "0x", but the CLI supports the "0x" format too.
    #[arg(
        long = "basefee-address",
        alias = "basefee_address",
        help_heading = "The recipient of base fees",
        value_parser = clap_address_parser,
        default_value_t = FEE_AGGREGATOR_ADDRESS,
        verbatim_doc_comment
    )]
    pub basefee_address: Address,

    /// The admin address for the FeeAggregator contract.
    ///
    /// This address controls upgrades, configuration, and emergency functions.
    /// Should be a governance multisig (Safe) in production.
    ///
    /// Address doesn't have to start with "0x", but the CLI supports the "0x" format too.
    #[arg(
        long = "fee-aggregator-admin",
        alias = "fee_aggregator_admin",
        help_heading = "The admin for FeeAggregator (upgrades, config, emergency)",
        value_parser = clap_address_parser,
        default_value_t = GOVERNANCE_SAFE_ADDRESS,
        verbatim_doc_comment
    )]
    pub fee_aggregator_admin: Address,

    /// The network admin address for all precompile contracts.
    ///
    /// This address replaces the default deployer admin in genesis storage
    /// for DelegationPool, FeeAggregator, RewardDistributor, and NativeTokenController.
    /// This gives the network admin control over upgrades, configuration, minting roles,
    /// and emergency functions.
    ///
    /// Address doesn't have to start with "0x", but the CLI supports the "0x" format too.
    #[arg(
        long = "network-admin",
        alias = "network_admin",
        help_heading = "The admin for all precompile contracts (replaces Foundry deployer admin in genesis)",
        value_parser = clap_address_parser,
        default_value_t = GOVERNANCE_SAFE_ADDRESS,
        verbatim_doc_comment
    )]
    pub network_admin: Address,

    /// The initial stake credited to each validator in genesis.
    #[arg(
        long = "initial-stake-per-validator",
        alias = "stake",
        help_heading = "The initial stake credited to each validator in genesis. The default is 5mil RLS.",
        value_parser = clap_u256_parser_to_18_decimals,
        default_value = "5_000_000",
        verbatim_doc_comment
    )]
    pub initial_stake: U256,

    /// The minimum amount a validator can withdraw.
    #[arg(
        long = "min-withdraw-amount",
        alias = "min_withdraw",
        help_heading = "The minimal amount a validator can withdraw. The default is 1_000 RLS.",
        value_parser = clap_u256_parser_to_18_decimals,
        default_value = "1_000",
        verbatim_doc_comment
    )]
    pub min_withdrawal: U256,

    /// The duration of each epoch (in secs) starting in genesis.
    #[arg(
        long = "epoch-duration-in-secs",
        alias = "epoch_length",
        help_heading = "The length of each epoch in seconds.",
        default_value_t = 60 * 60 * 24, // 24-hours
        verbatim_doc_comment
    )]
    pub epoch_duration: u32,

    /// Used to add a funded account (by simple text string).  Use this on a dev cluster
    /// to have an account with a deterministically derived key. This is ONLY for dev
    /// testing, never use this for other chains.
    #[arg(long)]
    pub dev_funded_account: Option<String>,
    /// Max delay for a node to produce a new header.
    #[arg(long)]
    pub max_header_delay_ms: Option<u64>,
    /// Min delay for a node to produce a new header.
    #[arg(long)]
    pub min_header_delay_ms: Option<u64>,
    /// Numeric chain id that will go in the genesis.
    /// Defaults to the `local` network's chain-id (487). Any value is accepted: the resulting
    /// datadir carries no baked-in network profile ("external"), so nodes must be started with
    /// a schedule matching this chain-id — either `--network <devnet|testnet|mainnet|local>`
    /// (when the chain-id matches that network) or `--config-file <path> --subnet <name>`.
    #[arg(long, default_value_t = RaylsNetwork::Local.chain_id(), value_parser=maybe_hex)]
    pub chain_id: u64,
    /// YAML file containing accounts to merge into genesis.
    /// This is intended for dev and test nets.
    #[arg(long, value_name = "YAML_FILE", verbatim_doc_comment)]
    pub accounts: Option<PathBuf>,

    /// Base fee per gas for the genesis block (in wei).
    /// Set to 0 for a gasless network. Default: 48 Gwei.
    #[arg(long = "base-fee", default_value_t = MIN_RAYLS_PROTOCOL_BASE_FEE)]
    pub base_fee: u64,

    /// Minimum base fee floor (in wei). The EIP-1559 base fee can never drop below this.
    /// Set to 0 for a gasless network. Default: 48 Gwei.
    #[arg(long = "min-base-fee", default_value_t = MIN_RAYLS_PROTOCOL_BASE_FEE)]
    pub min_base_fee: u64,

    /// Block gas limit (in gas units). Default: 30 billion.
    #[arg(long = "gas-limit", default_value_t = ETHEREUM_BLOCK_GAS_LIMIT_56BITS)]
    pub gas_limit: u64,

    /// YAML file mapping addresses to an RLS ERC-20 balance to pre-fund at genesis.
    #[arg(long = "rls-accounts", value_name = "YAML_FILE", verbatim_doc_comment)]
    pub rls_accounts: Option<PathBuf>,
}

/// Take a string and return the deterministic account derived from it.  This is be used
/// with similar functionality in the test client to allow easy testing using simple strings
/// for accounts.
pub(crate) fn account_from_word(key_word: &str) -> Address {
    if key_word.starts_with("0x") {
        key_word.parse().expect("not a valid account!")
    } else {
        let seed = keccak256(key_word.as_bytes());
        let mut rand = <StdRng as SeedableRng>::from_seed(seed.0);
        let secp = Secp256k1::new();
        let (_, public_key) = secp.generate_keypair(&mut rand);
        // strip out the first byte because that should be the SECP256K1_TAG_PUBKEY_UNCOMPRESSED
        // tag returned by libsecp's uncompressed pubkey serialization
        let hash = keccak256(&public_key.serialize_uncompressed()[1..]);
        Address::from_slice(&hash[12..])
    }
}

impl GenesisArgs {
    /// Genesis arguments preset for a local `--dev` chain.
    ///
    /// The `local` network's chain-id, gasless (base fee and floor both 0), fast
    /// header timing for quick local iteration, and otherwise the standard
    /// precompile / governance-safe defaults. Pre-funded dev accounts are added
    /// separately (see [`crate::dev`]) after the ceremony runs.
    #[cfg(feature = "dev-single-node-setup")]
    pub fn dev() -> Self {
        let ten_pow_18 = U256::from(10u64).pow(U256::from(18u64));
        Self {
            consensus_registry_owner: GOVERNANCE_SAFE_ADDRESS,
            basefee_address: FEE_AGGREGATOR_ADDRESS,
            fee_aggregator_admin: GOVERNANCE_SAFE_ADDRESS,
            network_admin: GOVERNANCE_SAFE_ADDRESS,
            initial_stake: U256::from(5_000_000u64) * ten_pow_18,
            min_withdrawal: U256::from(1_000u64) * ten_pow_18,
            epoch_duration: 60 * 60 * 24,
            dev_funded_account: None,
            // Fast headers so a single dev node produces blocks quickly.
            max_header_delay_ms: Some(250),
            min_header_delay_ms: Some(125),
            chain_id: RaylsNetwork::Local.chain_id(),
            accounts: None,
            // Gasless local chain: no base fee and no floor, so dev txs cost nothing.
            base_fee: 0,
            min_base_fee: 0,
            gas_limit: ETHEREUM_BLOCK_GAS_LIMIT_56BITS,
            rls_accounts: None,
        }
    }

    /// Execute command
    pub fn execute(&self, data_dir: PathBuf) -> eyre::Result<()> {
        info!(target: "genesis::ceremony", "Creating a new chain genesis with initial validators");

        let chain: RethChainSpec = RethChainSpec::default();
        // load network genesis
        let mut network_genesis =
            NetworkGenesis::new_from_path_and_genesis(&data_dir, chain.genesis().clone())?;

        // validate only checks proof of possession for now
        //
        // the signatures must match the expected genesis file before consensus registry is added
        network_genesis.validate()?;

        // execute data so committee is on-chain and in genesis
        let validators: Vec<_> = network_genesis.validators().values().cloned().collect();

        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: self.initial_stake,
            minWithdrawAmount: self.min_withdrawal,
            epochDuration: self.epoch_duration,
        };

        let mut genesis = network_genesis.genesis().clone();
        set_genesis_defaults(&mut genesis);
        genesis.config.chain_id = self.chain_id;
        genesis.gas_limit = self.gas_limit;

        // Load optional RLS ERC-20 pre-funded accounts.
        let rls_prefunds: Vec<(Address, U256)> = if let Some(path) = &self.rls_accounts {
            let f = std::fs::File::open(path)?;
            let raw: BTreeMap<Address, GenesisAccount> = serde_yaml::from_reader(f)?;
            raw.into_iter()
                .filter(|(_, acct)| !acct.balance.is_zero())
                .map(|(addr, acct)| (addr, acct.balance))
                .collect()
        } else {
            Vec::new()
        };

        // Pre-genesis sim is pure in-memory revm; no tokio runtime needed.
        let genesis_with_consensus_registry = RethEnv::create_consensus_registry_genesis_accounts(
            validators.clone(),
            genesis,
            initial_stake_config.clone(),
            self.consensus_registry_owner,
            self.network_admin,
            rls_prefunds.clone(),
        )?;
        // use embedded precompile config from submodule for genesis accounts.
        let precompiles =
            NetworkGenesis::fetch_precompile_genesis_accounts().expect("precompile fetch error");

        // Pull the five dynamically-simulated precompile proxy
        let sim_proxy_overrides: Vec<(Address, GenesisAccount)> = [
            NATIVE_TOKEN_CONTROLLER_ADDRESS,
            FEE_AGGREGATOR_ADDRESS,
            REWARD_DISTRIBUTOR_ADDRESS,
            DELEGATION_POOL_ADDRESS,
            RLS_ACCUMULATOR_ADDRESS,
        ]
        .iter()
        .filter_map(|addr| {
            genesis_with_consensus_registry.alloc.get(addr).map(|account| (*addr, account.clone()))
        })
        .collect();
        let mut updated_genesis = genesis_with_consensus_registry.extend_accounts(precompiles);

        // Patch UUPS impl bytecodes and the RLS allowance slot.
        apply_greenfield_fixes(&mut updated_genesis.alloc);

        // Re-apply the sim's proxy storage on top of the YAML entries when --network-admin was
        // passed.
        for (addr, account) in sim_proxy_overrides {
            updated_genesis.alloc.insert(addr, account);
        }

        // Changed a default config setting so update and save.
        if let Some(acct_str) = &self.dev_funded_account {
            let addr = crate::genesis::account_from_word(acct_str);
            // Owner nonce after the pre-genesis ceremony:
            //  0: CREATE BlsG1, 1: CREATE RLS impl, 2: CREATE RLS proxy, 3: CALL transfer RLS to
            // ConsensusRegistry, 4..(4+N-1): CALL transfer RLS to each --rls-accounts entry (N
            // entries)   (4+N): CREATE ConsensusRegistry
            // Final nonce = 5 + N.
            let owner_post_nonce = 5u64 + rls_prefunds.len() as u64;
            updated_genesis.alloc.insert(
                addr,
                GenesisAccount::default()
                    .with_nonce(Some(owner_post_nonce))
                    .with_balance(U256::from(10).pow(U256::from(27))), // One Billion RLS
            );
        }
        // Fund network admin with 1M native tokens
        if self.network_admin != Address::ZERO {
            let one_million_native = U256::from(10).pow(U256::from(18)) * U256::from(1_000_000);
            updated_genesis
                .alloc
                .entry(self.network_admin)
                .and_modify(|account| account.balance += one_million_native)
                .or_insert_with(|| GenesisAccount::default().with_balance(one_million_native));
        }

        // Extend genesis accounts with option account file.
        if let Some(accounts) = &self.accounts {
            let f = std::fs::File::open(accounts)?;
            let accounts: BTreeMap<Address, GenesisAccount> = serde_yaml::from_reader(f)?;
            updated_genesis.alloc.extend(accounts);
        }

        // Set the final base fee per gas for the genesis block.
        // This must happen after pre-genesis contract deployments which run at the default base
        // fee.
        updated_genesis.base_fee_per_gas = Some(self.base_fee as u128);

        // updated genesis with registry information
        network_genesis.update_genesis(updated_genesis);

        // update the config with new genesis information
        let mut parameters = Parameters::default();
        if let Some(max_header_delay_ms) = self.max_header_delay_ms {
            parameters.max_header_delay = Duration::from_millis(max_header_delay_ms);
        }
        if let Some(min_header_delay_ms) = self.min_header_delay_ms {
            parameters.min_header_delay = Duration::from_millis(min_header_delay_ms);
        }
        parameters.basefee_address = Some(self.basefee_address);
        parameters.fee_aggregator_admin = Some(self.fee_aggregator_admin);
        parameters.min_base_fee = self.min_base_fee;
        parameters.gas_limit = self.gas_limit;

        // write genesis and config to file
        Config::write_to_path(
            data_dir.genesis_file_path(),
            network_genesis.genesis(),
            ConfigFmt::YAML,
        )?;
        Config::write_to_path(data_dir.node_config_parameters_path(), parameters, ConfigFmt::YAML)?;

        // generate committee and worker cache
        let committee = network_genesis.create_committee()?;

        // write to file
        Config::write_to_path(data_dir.committee_path(), committee, ConfigFmt::YAML)?;

        Ok(())
    }
}
