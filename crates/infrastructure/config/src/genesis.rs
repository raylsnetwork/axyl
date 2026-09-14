//! Genesis information used when configuring a node.
use crate::RaylsDirs;
use eyre::Context;
use rayls_infrastructure_types::{
    address, test_genesis, verify_proof_of_possession_bls, Address, BlsPublicKey, BlsSignature,
    Committee, CommitteeBuilder, Genesis, GenesisAccount, Multiaddr, NetworkPublicKey, NodeP2pInfo,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, ffi::OsStr, fs, path::Path};
use tracing::{info, warn};

/// The validators directory used to create genesis
pub const GENESIS_VALIDATORS_DIR: &str = "validators";
/// The observers directory used to create genesis
pub const GENESIS_OBSERVERS_DIR: &str = "observers";
/// Precompile info for genesis, read from current submodule commit
pub const DEPLOYMENTS_JSON: &str =
    include_str!("../../../../rayls-contracts/deployments/deployments.json");
/// The path to consensus registry json (rl-contracts submodule).
pub const CONSENSUS_REGISTRY_JSON: &str =
    include_str!("../../../../rayls-contracts/artifacts/ConsensusRegistry.json");
/// The path to erc1967proxy json (rl-contracts submodule).
pub const ERC1967PROXY_JSON: &str =
    include_str!("../../../../rayls-contracts/artifacts/ERC1967Proxy.json");
/// The path to blsg1 json (rl-contracts submodule).
pub const BLSG1_JSON: &str = include_str!("../../../../rayls-contracts/artifacts/BlsG1.json");
/// The path to RLS ERC-20 token json (rl-contracts submodule).
pub const RLS_JSON: &str = include_str!("../../../../rayls-contracts/artifacts/RLS.json");
/// The path to DelegationPool json (rl-contracts submodule).
pub const DELEGATION_POOL_JSON: &str =
    include_str!("../../../../rayls-contracts/artifacts/DelegationPool.json");
/// The path to FeeAggregator json (rl-contracts submodule).
pub const FEE_AGGREGATOR_JSON: &str =
    include_str!("../../../../rayls-contracts/artifacts/FeeAggregator.json");
/// The path to RewardDistributor json (rl-contracts submodule).
pub const REWARD_DISTRIBUTOR_JSON: &str =
    include_str!("../../../../rayls-contracts/artifacts/RewardDistributor.json");
/// The path to NativeTokenController json (rl-contracts submodule).
pub const NATIVE_TOKEN_CONTROLLER_JSON: &str =
    include_str!("../../../../rayls-contracts/artifacts/NativeTokenController.json");
/// The path to RLSAccumulator json (rl-contracts submodule).
pub const RLS_ACCUMULATOR_JSON: &str =
    include_str!("../../../../rayls-contracts/artifacts/RLSAccumulator.json");
/// Precompile genesis configuration from the contracts submodule.
pub const PRECOMPILE_CFG_YAML: &str =
    include_str!("../../../../rayls-contracts/deployments/genesis/precompile-config.yaml");
/// The default governance safe address.
pub const GOVERNANCE_SAFE_ADDRESS: Address = address!("00000000000000000000000000000000000007a0");
/// ConsensusRegistry address (used as the validator stake holder + committee source of truth).
pub const CONSENSUS_REGISTRY_ADDRESS: Address =
    address!("07e17e17e17e17e17e17e17e17e17e17e17e17e1");
/// DelegationPool proxy address.
pub const DELEGATION_POOL_ADDRESS: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17e2");
/// The fee aggregator address for collecting and distributing transaction fees.
pub const FEE_AGGREGATOR_ADDRESS: Address = address!("07E17e17E17e17E17e17E17E17E17e17e17E17e3");
/// FeeAggregator implementation address (impl behind the FEE_AGGREGATOR_ADDRESS proxy).
pub const FEE_AGGREGATOR_IMPL_ADDRESS: Address =
    address!("07e17e17e17e17e17e17e17e17e17e17e17e17e4");
/// RewardDistributor proxy address.
pub const REWARD_DISTRIBUTOR_ADDRESS: Address =
    address!("07e17e17e17e17e17e17e17e17e17e17e17e17e5");
/// NativeTokenController proxy address.
pub const NATIVE_TOKEN_CONTROLLER_ADDRESS: Address =
    address!("07e17e17e17e17e17e17e17e17e17e17e17e17e6");
/// NativeTokenController implementation address.
pub const NATIVE_TOKEN_CONTROLLER_IMPL_ADDRESS: Address =
    address!("07e17e17e17e17e17e17e17e17e17e17e17e17e7");
/// RewardDistributor implementation address.
pub const REWARD_DISTRIBUTOR_IMPL_ADDRESS: Address =
    address!("07e17e17e17e17e17e17e17e17e17e17e17e17e8");
/// DelegationPool implementation address.
pub const DELEGATION_POOL_IMPL_ADDRESS: Address =
    address!("07e17e17e17e17e17e17e17e17e17e17e17e17e9");
/// The address for the RLS ERC-20 token proxy.
pub const RLS_ADDRESS: Address = address!("07E17e17E17e17E17e17E17E17E17e17e17E17eA");
/// The address for the RLS ERC-20 token implementation.
pub const RLS_IMPL_ADDRESS: Address = address!("07E17e17E17e17E17e17E17E17E17e17e17E17eB");
/// RLSAccumulator proxy address.
pub const RLS_ACCUMULATOR_ADDRESS: Address = address!("07e17e17e17e17e17e17e17e17e17e17e17e17ec");
/// RLSAccumulator implementation address.
pub const RLS_ACCUMULATOR_IMPL_ADDRESS: Address =
    address!("07e17e17e17e17e17e17e17e17e17e17e17e17ed");
/// Native ERC-20 precompile (USDr) address — implemented in the node, no EVM bytecode.
pub const USDR_PRECOMPILE_ADDRESS: Address = address!("0000000000000000000000000000000000000400");

/// The struct for starting a network at genesis.
#[derive(Debug)]
pub struct NetworkGenesis {
    /// Execution data
    genesis: Genesis,
    /// Validator signatures
    validators: BTreeMap<BlsPublicKey, NodeInfo>,
    /// Validator signatures
    observers: BTreeMap<BlsPublicKey, NodeInfo>,
}

impl NetworkGenesis {
    /// Create new version of [NetworkGenesis] using the testnet genesis [ChainSpec].
    pub fn new_for_test() -> Self {
        Self {
            genesis: test_genesis(),
            validators: Default::default(),
            observers: Default::default(),
        }
    }

    /// Return the current genesis.
    pub fn genesis(&self) -> &Genesis {
        &self.genesis
    }

    /// Add validator information to the genesis directory.
    ///
    /// Adding [ValidatorInfo] to the genesis directory allows other
    /// validators to discover peers using VCS (ie - github).
    #[cfg(test)]
    fn add_validator(&mut self, validator: NodeInfo) {
        self.validators.insert(*validator.public_key(), validator);
    }

    /// Update chain spec with executed values for genesis.
    pub fn update_genesis(&mut self, genesis: Genesis) {
        self.genesis = genesis;
    }

    /// Load a list of validators by reading files in a directory.
    fn load_validators_from_path<P>(rayls_paths: &P) -> eyre::Result<Vec<(BlsPublicKey, NodeInfo)>>
    where
        P: RaylsDirs,
    {
        let path = rayls_paths.genesis_path();
        info!(target: "genesis::ceremony", ?path, "Loading Network Genesis");

        if !path.is_dir() {
            eyre::bail!("path must be a directory");
        }

        // Load validator information
        let mut validators = Vec::new();
        for entry in fs::read_dir(path.join(GENESIS_VALIDATORS_DIR))? {
            let entry = entry?;
            let path = entry.path();

            // Check if it's a file and has the .yaml extension and does not start with '.'
            if path.is_file()
                && path.file_name().and_then(OsStr::to_str).is_none_or(|s| !s.starts_with('.'))
            {
                let info_bytes = fs::read(&path)?;
                let validator: NodeInfo = serde_yaml::from_slice(&info_bytes)
                    .with_context(|| format!("validator failed to load from {}", path.display()))?;
                validators.push((validator.bls_public_key, validator));
            } else {
                warn!("skipping dir: {}\ndirs should not be in validators dir", path.display());
            }
        }
        Ok(validators)
    }

    /// Load a list of observers by reading files in a directory.
    fn load_observers_from_path<P>(rayls_paths: &P) -> eyre::Result<Vec<(BlsPublicKey, NodeInfo)>>
    where
        P: RaylsDirs,
    {
        let path = rayls_paths.genesis_path();
        info!(target: "genesis::ceremony", ?path, "Loading Network Genesis");

        if !path.is_dir() {
            eyre::bail!("path must be a directory");
        }

        // Load validator information
        let mut observers = Vec::new();
        if let Ok(observers_dir_iter) = fs::read_dir(path.join(GENESIS_OBSERVERS_DIR)) {
            for entry in observers_dir_iter {
                let entry = entry?;
                let path = entry.path();

                // Check if it's a file and has the .yaml extension and does not start with '.'
                if path.is_file()
                    && path.file_name().and_then(OsStr::to_str).is_none_or(|s| !s.starts_with('.'))
                {
                    let info_bytes = fs::read(&path)?;
                    let validator: NodeInfo =
                        serde_yaml::from_slice(&info_bytes).with_context(|| {
                            format!("validator failed to load from {}", path.display())
                        })?;
                    observers.push((validator.bls_public_key, validator));
                } else {
                    warn!("skipping dir: {}\ndirs should not be in observers dir", path.display());
                }
            }
        }
        Ok(observers)
    }

    /// Generate a [NetworkGenesis] by reading validators from files in a directory with genesis.
    pub fn new_from_path_and_genesis<P>(rayls_paths: &P, genesis: Genesis) -> eyre::Result<Self>
    where
        P: RaylsDirs,
    {
        // Load validator information
        let validators = Self::load_validators_from_path(rayls_paths)?;
        let validators = BTreeMap::from_iter(validators);

        // Load observers information
        let observers = Self::load_observers_from_path(rayls_paths)?;
        let observers = BTreeMap::from_iter(observers);

        Ok(Self { genesis, validators, observers })
    }

    /// Validate each validator:
    /// - verify proof of possession
    pub fn validate(&self) -> eyre::Result<()> {
        for (pubkey, validator) in self.validators.iter() {
            info!(target: "genesis::validate", "verifying validator: {}", pubkey);
            verify_proof_of_possession_bls(
                &validator.proof_of_possession,
                pubkey,
                &validator.execution_address,
            )?;
        }
        info!(target: "genesis::validate", "all validators valid for genesis");
        Ok(())
    }

    /// Create a [Committee] from the validators in [NetworkGenesis].
    pub fn create_committee(&self) -> eyre::Result<Committee> {
        let mut committee_builder = CommitteeBuilder::new(0);
        for (pubkey, validator) in self.validators.iter() {
            committee_builder.add_authority_and_bootstrap(
                *pubkey,
                1,
                (
                    validator.primary_network_address().clone(),
                    validator.primary_network_key().clone(),
                )
                    .into(),
                (
                    validator.worker_network_address().clone(),
                    validator.worker_network_key().clone(),
                )
                    .into(),
                validator.execution_address,
            );
        }
        for (pubkey, validator) in self.observers.iter() {
            committee_builder.add_bootstrap_server(
                *pubkey,
                (
                    validator.primary_network_address().clone(),
                    validator.primary_network_key().clone(),
                )
                    .into(),
                (
                    validator.worker_network_address().clone(),
                    validator.worker_network_key().clone(),
                )
                    .into(),
            );
        }
        Ok(committee_builder.build())
    }

    /// Return a reference to the validators.
    pub fn validators(&self) -> &BTreeMap<BlsPublicKey, NodeInfo> {
        &self.validators
    }

    /// Precompile genesis accounts parsed from the committed per-network
    /// `precompile-config.yaml`.
    pub fn fetch_precompile_genesis_accounts() -> eyre::Result<Vec<(Address, GenesisAccount)>> {
        let config: std::collections::HashMap<Address, GenesisAccount> =
            serde_yaml::from_str(PRECOMPILE_CFG_YAML).expect("yaml parsing failure");

        Ok(config
            .into_iter()
            .map(|(address, precompile)| {
                (
                    address,
                    GenesisAccount::default()
                        .with_nonce(precompile.nonce)
                        .with_balance(precompile.balance)
                        .with_code(precompile.code)
                        .with_storage(precompile.storage),
                )
            })
            .collect())
    }
}

/// Information needed for every validator:
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct NodeInfo {
    /// The name for the validator. The default value
    /// is the hashed value of the validator's
    /// execution address. The operator can overwrite
    /// this value since it is not used when writing to file.
    ///
    /// Keccak256(Address)
    pub name: String,
    /// [BlsPublicKey] to verify signature.
    pub bls_public_key: BlsPublicKey,
    /// Information for this validator's primary,
    /// including worker details.
    pub p2p_info: NodeP2pInfo,
    /// The address for suggested fee recipient.
    ///
    /// Validator rewards are sent to this address.
    /// Note, non-validators can also have an address but do not earn rewards (it is informational
    /// only).
    pub execution_address: Address,
    /// Proof
    pub proof_of_possession: BlsSignature,
}

impl NodeInfo {
    /// Return public key bytes.
    pub fn public_key(&self) -> &BlsPublicKey {
        &self.bls_public_key
    }

    /// Return the primary's public network key.
    pub fn primary_network_key(&self) -> &NetworkPublicKey {
        &self.p2p_info.primary.network_key
    }

    /// Return the primary's network address.
    pub fn primary_network_address(&self) -> &Multiaddr {
        &self.p2p_info.primary.network_address
    }

    /// Return the primary's public network key.
    pub fn worker_network_key(&self) -> &NetworkPublicKey {
        &self.p2p_info.worker.network_key
    }

    /// Return the primary's network address.
    pub fn worker_network_address(&self) -> &Multiaddr {
        &self.p2p_info.worker.network_address
    }
}

impl Default for NodeInfo {
    fn default() -> Self {
        let bls_public_key = BlsPublicKey::default();
        let name = format!("node-{}", bs58::encode(&bls_public_key.to_bytes()[0..8]).into_string());
        Self {
            name,
            bls_public_key,
            p2p_info: Default::default(),
            execution_address: Address::ZERO,
            proof_of_possession: BlsSignature::default(),
        }
    }
}

/// Fetch a file with a path relative to the CARGO MANIFEST dir and return it as a string.
///
/// Note this will ONLY work in tests or during builds, otherwise the required env variable
/// will not be set.
pub fn fetch_file_content_relative_to_manifest<P: AsRef<Path>>(relative_path: P) -> String {
    let mut file_path = std::path::PathBuf::from(
        std::env::var("CARGO_MANIFEST_DIR").expect("Missing CARGO_MANIFEST_DIR!"),
    );
    file_path.push(relative_path);

    fs::read_to_string(file_path).expect("unable to read file")
}

#[cfg(test)]
mod tests {
    use super::NetworkGenesis;
    use crate::NodeInfo;
    use rand::{rngs::StdRng, SeedableRng};
    use rayls_infrastructure_types::{
        generate_proof_of_possession_bls, Address, BlsKeypair, Multiaddr, NetworkKeypair,
        NodeP2pInfo,
    };

    #[test]
    fn test_validate_genesis() {
        let mut network_genesis = NetworkGenesis::new_for_test();
        // create keys and information for validators
        for v in 0..4 {
            let bls_keypair = BlsKeypair::generate(&mut StdRng::from_seed([0; 32]));
            let network_keypair = NetworkKeypair::generate_ed25519();
            let worker_network_keypair = NetworkKeypair::generate_ed25519();
            let address = Address::from_raw_public_key(&[0; 64]);
            let proof_of_possession =
                generate_proof_of_possession_bls(&bls_keypair, &address).unwrap();
            let primary_network_address = Multiaddr::empty();
            let worker_network_address = Multiaddr::empty();
            let primary_info = NodeP2pInfo::new(
                (network_keypair.public().clone().into(), primary_network_address).into(),
                (worker_network_keypair.public().clone().into(), worker_network_address).into(),
            );
            let name = format!("validator-{v}");
            // create validator
            let validator = NodeInfo {
                name,
                bls_public_key: *bls_keypair.public(),
                p2p_info: primary_info,
                execution_address: address,
                proof_of_possession,
            };
            // add validator
            network_genesis.add_validator(validator.clone());
        }
        // validate
        assert!(network_genesis.validate().is_ok())
    }

    #[test]
    fn test_validate_genesis_fails() {
        // this uses `test_genesis_yaml`
        let mut network_genesis = NetworkGenesis::new_for_test();
        // create keys and information for validators
        for v in 0..4 {
            let bls_keypair = BlsKeypair::generate(&mut StdRng::from_seed([0; 32]));
            let network_keypair = NetworkKeypair::generate_ed25519();
            let worker_network_keypair = NetworkKeypair::generate_ed25519();
            let address = Address::from_raw_public_key(&[0; 64]);
            let wrong_address = Address::from_raw_public_key(&[1; 64]);

            // generate proof with wrong chain spec
            let proof_of_possession =
                generate_proof_of_possession_bls(&bls_keypair, &wrong_address).unwrap();
            let primary_network_address = Multiaddr::empty();
            let worker_network_address = Multiaddr::empty();
            let primary_info = NodeP2pInfo::new(
                (network_keypair.public().clone().into(), primary_network_address).into(),
                (worker_network_keypair.public().clone().into(), worker_network_address).into(),
            );
            let name = format!("validator-{v}");
            // create validator
            let validator = NodeInfo {
                name,
                bls_public_key: *bls_keypair.public(),
                p2p_info: primary_info,
                execution_address: address,
                proof_of_possession,
            };
            // add validator
            network_genesis.add_validator(validator.clone());
        }
        // validate should fail
        assert!(network_genesis.validate().is_err(), "proof of possession should fail")
    }
}
