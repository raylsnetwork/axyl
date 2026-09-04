//! External hardfork configuration: one file per client, holding any number
//! of named subnets.
//!
//! A node started with `--config-file` / `--subnet` loads the hardfork
//! schedule of the selected subnet from such a file instead of the schedule
//! baked into the binary. Everything else — genesis, parameters, committee,
//! node identity — still comes from the node's datadir, exactly as before.
//!
//! The selected subnet is stored in a process-wide [`OnceLock`] so the
//! execution layer can reach it without threading it through every
//! constructor.

use std::{collections::BTreeMap, sync::OnceLock};

use reth_chainspec::ForkCondition;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::chainspec::RaylsHardFork;

/// The activation condition of a single hardfork in a network config file.
///
/// Serialized as a plain block number (`Eip1559: 0`) or the string
/// `never` (`AdminTransfer: never`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForkActivation {
    /// The fork activates at the given block number.
    Block(u64),
    /// The fork never activates.
    Never,
}

impl ForkActivation {
    fn from_str_lossy(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case("never") {
            Some(Self::Never)
        } else {
            None
        }
    }
}

impl Serialize for ForkActivation {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Block(block) => serializer.serialize_u64(*block),
            Self::Never => serializer.serialize_str("never"),
        }
    }
}

impl<'de> Deserialize<'de> for ForkActivation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Block(u64),
            Text(String),
        }
        match Raw::deserialize(deserializer)? {
            Raw::Block(block) => Ok(Self::Block(block)),
            Raw::Text(text) => Self::from_str_lossy(&text)
                .ok_or_else(|| serde::de::Error::custom(format!(
                    "invalid fork activation {text:?}; expected a block number or \"never\""
                ))),
        }
    }
}

impl From<ForkActivation> for ForkCondition {
    fn from(activation: ForkActivation) -> Self {
        match activation {
            ForkActivation::Block(block) => ForkCondition::Block(block),
            ForkActivation::Never => ForkCondition::Never,
        }
    }
}

/// The hardfork configuration of a single subnet of a client.
///
/// This is what one `networks.<name>` entry of a config file holds. The rest
/// of the network's configuration (genesis, parameters, committee) lives in
/// the node's datadir as before; only the chain-id and the hardfork schedule
/// are externalized, because they used to be baked into the binary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkProfile {
    /// The chain-id a node's datadir must carry to run this subnet. The node
    /// verifies the datadir's genesis chain-id against it at boot and refuses
    /// to start on a mismatch (wrong datadir for this subnet/client).
    pub chain_id: u64,
    /// The hardfork schedule: Rayls hardfork name -> activation block or
    /// `never`. Forks absent from the map stay `Never`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub hardforks: BTreeMap<String, ForkActivation>,
}

impl NetworkProfile {
    /// Validate the `hardforks` map: every key must be a known Rayls hardfork.
    pub fn validate_hardforks(&self) -> eyre::Result<()> {
        for name in self.hardforks.keys() {
            let known =
                RaylsHardFork::VARIANTS.iter().any(|fork| fork.name().eq_ignore_ascii_case(name));
            if !known {
                let known_forks = RaylsHardFork::VARIANTS
                    .iter()
                    .map(|fork| fork.name())
                    .collect::<Vec<_>>()
                    .join(", ");
                eyre::bail!("unknown hardfork '{name}' in network config; known forks: {known_forks}")
            }
        }
        Ok(())
    }

    /// Resolve the `hardforks` map into a schedule. Forks absent from the map
    /// are omitted (they resolve to `Never` at lookup time).
    pub fn schedule(&self) -> Vec<(RaylsHardFork, ForkCondition)> {
        RaylsHardFork::VARIANTS
            .iter()
            .filter_map(|fork| {
                self.hardforks
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(fork.name()))
                    .map(|(_, activation)| (*fork, ForkCondition::from(*activation)))
            })
            .collect()
    }
}

/// A client's network configuration file: any number of named subnets.
///
/// ```yaml
/// networks:
///   mainnet:
///     chain_id: 487
///     hardforks: { Eip1559: 0, AdminTransfer: never, ... }
///   testnet:
///     chain_id: 2017
///     hardforks: { Eip1559: 281800, ... }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfigFile {
    /// The subnets this client operates, keyed by name. The number of
    /// subnets is not fixed — each client defines its own.
    pub networks: BTreeMap<String, NetworkProfile>,
}

impl NetworkConfigFile {
    /// Look up a subnet by name.
    pub fn subnet(&self, name: &str) -> Option<&NetworkProfile> {
        self.networks.get(name)
    }
}

/// The hardfork schedule selected at node start via `--config-file` /
/// `--subnet`.
///
/// `None` when the node runs without an external config file — then the
/// baked-in hardfork schedule selected by `parameters.network` applies,
/// exactly as before.
static ACTIVE_PROFILE: OnceLock<NetworkProfile> = OnceLock::new();

/// Install the active hardfork schedule. Called exactly once, at node start,
/// before the execution layer is built.
pub fn set_active_profile(profile: NetworkProfile) -> eyre::Result<()> {
    ACTIVE_PROFILE
        .set(profile)
        .map_err(|_| eyre::eyre!("active network profile is already set"))
}

/// The active hardfork schedule, if an external config file was provided.
pub fn active_profile() -> Option<&'static NetworkProfile> {
    ACTIVE_PROFILE.get()
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROFILE_YAML: &str = r#"
chain_id: 2017
hardforks:
  Eip1559: 0
  BatchDigestV2: 100
  AdminTransfer: never
"#;

    #[test]
    fn fork_activation_from_block_number() {
        let activation: ForkActivation = serde_yaml::from_str("1234").unwrap();
        assert_eq!(activation, ForkActivation::Block(1234));
        assert_eq!(ForkCondition::from(activation), ForkCondition::Block(1234));
    }

    #[test]
    fn fork_activation_from_never() {
        let activation: ForkActivation = serde_yaml::from_str("\"never\"").unwrap();
        assert_eq!(activation, ForkActivation::Never);
        assert_eq!(ForkCondition::from(activation), ForkCondition::Never);
    }

    #[test]
    fn fork_activation_roundtrip() {
        for activation in [ForkActivation::Block(0), ForkActivation::Block(999), ForkActivation::Never] {
            let s = serde_yaml::to_string(&activation).unwrap();
            assert_eq!(serde_yaml::from_str::<ForkActivation>(&s).unwrap(), activation);
        }
    }

    #[test]
    fn fork_activation_rejects_garbage() {
        let err = serde_yaml::from_str::<ForkActivation>("\"someday\"").unwrap_err();
        assert!(err.to_string().contains("invalid fork activation"), "{err}");
    }

    #[test]
    fn profile_parses() {
        let profile: NetworkProfile = serde_yaml::from_str(PROFILE_YAML).unwrap();
        assert_eq!(profile.chain_id, 2017);
        assert_eq!(profile.hardforks.len(), 3);
        assert_eq!(profile.hardforks.get("Eip1559"), Some(&ForkActivation::Block(0)));
        assert_eq!(profile.hardforks.get("AdminTransfer"), Some(&ForkActivation::Never));
    }

    #[test]
    fn profile_requires_chain_id() {
        let err =
            serde_yaml::from_str::<NetworkProfile>("hardforks: { Eip1559: 0 }\n").unwrap_err();
        assert!(err.to_string().contains("chain_id"), "{err}");
    }

    #[test]
    fn profile_schedule_resolves_known_forks_only() {
        let profile: NetworkProfile = serde_yaml::from_str(PROFILE_YAML).unwrap();
        let schedule = profile.schedule();
        let by_name = |name: &str| schedule.iter().find(|(fork, _)| fork.name() == name);
        assert_eq!(by_name("Eip1559").map(|(_, c)| *c), Some(ForkCondition::Block(0)));
        assert_eq!(
            by_name("BatchDigestV2").map(|(_, c)| *c),
            Some(ForkCondition::Block(100))
        );
        assert_eq!(by_name("AdminTransfer").map(|(_, c)| *c), Some(ForkCondition::Never));
        // Absent forks are omitted from the schedule (lookup yields Never).
        assert!(by_name("Tokenomics").is_none());
        assert!(schedule.len() <= RaylsHardFork::VARIANTS.len());
    }

    #[test]
    fn validate_hardforks_accepts_known_names_case_insensitively() {
        let mut profile: NetworkProfile = serde_yaml::from_str(PROFILE_YAML).unwrap();
        profile.hardforks.insert("batchdigestv2".to_string(), ForkActivation::Block(1));
        profile.validate_hardforks().unwrap();
    }

    #[test]
    fn validate_hardforks_rejects_unknown_names() {
        let mut profile: NetworkProfile = serde_yaml::from_str(PROFILE_YAML).unwrap();
        profile.hardforks.insert("MyFork".to_string(), ForkActivation::Block(1));
        let err = profile.validate_hardforks().unwrap_err();
        assert!(err.to_string().contains("unknown hardfork 'MyFork'"), "{err}");
    }

    #[test]
    fn example_client1_yaml_parses() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../../examples/client1.yaml");
        let text = std::fs::read_to_string(path).expect("examples/client1.yaml is committed");
        let file: NetworkConfigFile = serde_yaml::from_str(&text).unwrap();
        assert_eq!(file.networks.len(), 2);

        let mainnet = file.subnet("mainnet").unwrap();
        mainnet.validate_hardforks().unwrap();
        assert_eq!(mainnet.chain_id, 487);

        let testnet = file.subnet("testnet").unwrap();
        testnet.validate_hardforks().unwrap();
        assert_eq!(testnet.chain_id, 2017);

        let by = |p: &NetworkProfile, name: &str| {
            p.schedule().iter().find(|(fork, _)| fork.name() == name).map(|(_, c)| *c)
        };
        assert_eq!(by(mainnet, "Eip1559"), Some(ForkCondition::Block(0)));
        assert_eq!(
            by(mainnet, "UsdrSupplyCorrection"),
            Some(ForkCondition::Block(3_569_194))
        );
        assert_eq!(by(testnet, "Tokenomics"), Some(ForkCondition::Block(1_879_000)));
        assert_eq!(by(testnet, "Erc20PrecompileBytecode"), Some(ForkCondition::Never));
    }

    #[test]
    fn file_parses_multiple_subnets() {
        let yaml = format!(
            r#"
networks:
  mainnet:
{mainnet}
  testnet:
{testnet}
"#,
            mainnet = indent(PROFILE_YAML),
            testnet = indent(PROFILE_YAML).replace("Eip1559: 0", "Eip1559: 281800"),
        );
        let file: NetworkConfigFile = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(file.networks.len(), 2);
        assert_eq!(
            file.subnet("mainnet").unwrap().hardforks.get("Eip1559"),
            Some(&ForkActivation::Block(0))
        );
        assert_eq!(
            file.subnet("testnet").unwrap().hardforks.get("Eip1559"),
            Some(&ForkActivation::Block(281800))
        );
        assert!(file.subnet("devnet").is_none());
    }

    fn indent(block: &str) -> String {
        block
            .lines()
            .map(|line| format!("    {line}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}
