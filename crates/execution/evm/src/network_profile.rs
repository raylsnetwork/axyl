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

use std::{collections::BTreeMap, path::Path, sync::OnceLock};

use rayls_infrastructure_types::RaylsNetwork;
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
            Raw::Text(text) => {
                if text.eq_ignore_ascii_case("never") {
                    Ok(Self::Never)
                } else {
                    Err(serde::de::Error::custom(format!(
                        "invalid fork activation {text:?}; expected a block number or \"never\""
                    )))
                }
            }
        }
    }
}

impl ForkActivation {
    /// The activation block; `None` for a fork that never activates.
    pub fn block(&self) -> Option<u64> {
        match self {
            Self::Block(block) => Some(*block),
            Self::Never => None,
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
    /// `never`. A file loaded at boot must define every known fork
    /// ([`NetworkProfile::validate_hardforks`] refuses the boot otherwise);
    /// a fork absent from the map resolves to `Never` at lookup time.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub hardforks: BTreeMap<String, ForkActivation>,
}

impl NetworkProfile {
    /// Validate the `hardforks` map: every key must be a known Rayls hardfork,
    /// and every known hardfork must have an entry.
    ///
    /// A fork absent from the map would silently run as `never`, so a node
    /// could diverge from a network that activates it (e.g. after a binary
    /// upgrade that added a fork and left the file stale). Refusing the boot
    /// until every fork is set deliberately — a block number or `never` — is
    /// the intended per-network decision point for each new fork.
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
                eyre::bail!(
                    "unknown hardfork '{name}' in network config; known forks: {known_forks}"
                )
            }
        }
        let missing: Vec<&str> = RaylsHardFork::VARIANTS
            .iter()
            .filter(|fork| {
                !self.hardforks.keys().any(|name| name.eq_ignore_ascii_case(fork.name()))
            })
            .map(|fork| fork.name())
            .collect();
        if !missing.is_empty() {
            eyre::bail!(
                "hardforks map does not define: {}; add each one with a block number or \
                 \"never\"",
                missing.join(", ")
            );
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

    /// The baked-in hardfork schedule for a built-in network, as a profile.
    ///
    /// This is what `--network <name>` selects: the network's chain-id plus the
    /// full 15-fork schedule baked into the binary, expressed with the same
    /// profile shape a config-file subnet resolves to, so both schedule
    /// sources flow through one code path.
    pub fn from_builtin(network: RaylsNetwork) -> Self {
        let hardforks = RaylsHardFork::for_network(network)
            .iter()
            .map(|(fork, condition)| match condition {
                ForkCondition::Block(block) => {
                    (fork.name().to_string(), ForkActivation::Block(*block))
                }
                ForkCondition::Never => (fork.name().to_string(), ForkActivation::Never),
                other => unreachable!("built-in schedules are block-based, got {other:?}"),
            })
            .collect();
        Self { chain_id: network.chain_id(), hardforks }
    }
}

/// A client's network configuration file: any number of named subnets.
///
/// ```yaml
/// networks:
///   mainnet:
///     chain_id: 72957
///     hardforks: { Eip1559: 0, AdminTransfer: never, ... }
///   testnet:
///     chain_id: 7295799
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

/// The hardfork schedule selected at node start: the subnet profile from
/// `--config-file` / `--subnet`, or the baked-in schedule selected by
/// `--network`.
///
/// `None` only in processes that never ran the CLI's boot gate (in-process
/// test engines), which run with an all-`Never` schedule.
static ACTIVE_PROFILE: OnceLock<NetworkProfile> = OnceLock::new();

/// Install the active hardfork schedule. Called exactly once, at node start,
/// after the boot gates pass, before the execution layer is built.
pub fn set_active_profile(profile: NetworkProfile) -> eyre::Result<()> {
    ACTIVE_PROFILE.set(profile).map_err(|_| eyre::eyre!("active network profile is already set"))
}

/// The active hardfork schedule, if the CLI boot gate installed one.
pub fn active_profile() -> Option<&'static NetworkProfile> {
    ACTIVE_PROFILE.get()
}

/// The hardfork schedule a node's datadir records as the one that produced the
/// chain's executed blocks.
///
/// Written at boot by the CLI's schedule gate: it captures the schedule
/// selected for the run and the chain head at that moment. Since every block up
/// to the head was executed under that schedule, a later boot whose selected
/// schedule disagrees with the record on an already-executed fork would
/// re-interpret the chain's history and is refused. Forks whose differing
/// boundary is still in the future may be moved (that is how agreed schedule
/// updates ship) and are reported by [`verify_schedule`] so the caller can
/// warn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleRecord {
    /// The chain-id the recorded schedule applies to.
    pub chain_id: u64,
    /// The chain head when this record was last written.
    pub as_of_block: u64,
    /// The recorded fork schedule; `never` forks are stored explicitly, so a
    /// fork absent from the map means the record predates that fork (it reads
    /// as `never` at verification time).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub hardforks: BTreeMap<String, ForkActivation>,
}

impl ScheduleRecord {
    /// Build a record from the schedule selected for this boot, storing
    /// never-activating forks explicitly: the record is a complete snapshot
    /// of the schedule, so only a record written before a fork existed lacks
    /// it.
    pub fn from_schedule(
        chain_id: u64,
        as_of_block: u64,
        schedule: &[(RaylsHardFork, ForkCondition)],
    ) -> Self {
        assert_block_based(schedule);
        let hardforks = schedule
            .iter()
            .filter_map(|(fork, condition)| match condition {
                ForkCondition::Block(block) => {
                    Some((fork.name().to_string(), ForkActivation::Block(*block)))
                }
                ForkCondition::Never => Some((fork.name().to_string(), ForkActivation::Never)),
                // Rayls schedules only use block-based (or absent) activations;
                // `assert_block_based` above fires in dev builds otherwise.
                ForkCondition::TTD { .. } | ForkCondition::Timestamp(_) => None,
            })
            .collect();
        Self { chain_id, as_of_block, hardforks }
    }

    /// The recorded activation of a fork: its block number, or `None` when the
    /// fork is absent from the record or recorded as `never`. Fork names match
    /// case-insensitively, as everywhere else in the schedule.
    pub fn activation(&self, name: &str) -> Option<u64> {
        self.hardforks
            .iter()
            .find(|(recorded, _)| recorded.eq_ignore_ascii_case(name))
            .and_then(|(_, activation)| activation.block())
    }

    /// The recorded entry of a fork: `Some(Block(n))`, `Some(Never)`, or
    /// `None` when the fork is absent from the record (the record predates
    /// that fork). Fork names match case-insensitively, as everywhere else in
    /// the schedule.
    pub fn entry(&self, name: &str) -> Option<&ForkActivation> {
        self.hardforks
            .iter()
            .find(|(recorded, _)| recorded.eq_ignore_ascii_case(name))
            .map(|(_, activation)| activation)
    }
}

/// A fork whose boundary differs between the recorded and selected schedules
/// but is still in the future (not yet executed at verification time).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FutureForkMove {
    /// The fork whose boundary moved.
    pub fork: RaylsHardFork,
    /// The recorded boundary; `None` when the record has the fork as `never`.
    pub recorded: Option<u64>,
    /// The boundary selected for this boot; `None` when never.
    pub selected: Option<u64>,
}

/// Verify the hardfork schedule selected for this boot against the datadir's
/// [`ScheduleRecord`].
///
/// `head` is the chain's current highest block. For every fork whose boundary
/// differs between record and selection, the lower of the two boundaries is the
/// first block the two schedules disagree about: when that block is `<= head`,
/// the chain already executed blocks under the recorded schedule, so the
/// selected schedule is inconsistent with the chain's history and this returns
/// an error. When it is `> head`, the move is in the future and is allowed,
/// reported in the returned list for the caller to warn about.
///
/// `record_path` is the datadir's record file, named in the error so the
/// refusal carries its remedy (add/fix the fork's entry in the record, or
/// delete the file to re-record the selected schedule).
pub fn verify_schedule(
    record: &ScheduleRecord,
    selected: &[(RaylsHardFork, ForkCondition)],
    selected_chain_id: u64,
    head: u64,
    record_path: &Path,
) -> eyre::Result<Vec<FutureForkMove>> {
    if record.chain_id != selected_chain_id {
        eyre::bail!(
            "schedule record belongs to chain-id {} but the selected schedule targets \
             chain-id {selected_chain_id}; the datadir's schedule record does not match the \
             selected schedule",
            record.chain_id
        );
    }
    for name in record.hardforks.keys() {
        let known =
            RaylsHardFork::VARIANTS.iter().any(|fork| fork.name().eq_ignore_ascii_case(name));
        if !known {
            eyre::bail!("schedule record contains unknown hardfork '{name}'");
        }
    }
    let selected_blocks: BTreeMap<&str, Option<u64>> =
        selected.iter().map(|(fork, condition)| (fork.name(), block_of(condition))).collect();
    let mut moves = Vec::new();
    for fork in RaylsHardFork::VARIANTS {
        let recorded = record.activation(fork.name());
        let selected_block = selected_blocks.get(fork.name()).copied().flatten();
        if recorded == selected_block {
            continue;
        }
        // `recorded != selected_block`, so at least one of the two is `Some`.
        let first_disagreement = [recorded, selected_block]
            .into_iter()
            .flatten()
            .min()
            .expect("at least one boundary is Some when the schedules differ");
        if first_disagreement <= head {
            let detail = match (record.entry(fork.name()), selected_block) {
                // The record has no entry for the fork: it was written before
                // the fork existed, and the selected schedule back-dates the
                // fork into the executed history.
                (None, Some(block)) => format!(
                    "hardfork '{}' is absent from the schedule record (the datadir predates \
                     this fork), but the selected schedule activates it at block {block} while \
                     the chain has already executed block {head}",
                    fork.name()
                ),
                // The record pins the fork as never, and the selected schedule
                // back-dates its activation into the executed history.
                (Some(ForkActivation::Never), Some(block)) => format!(
                    "hardfork '{}' is recorded as never, but the selected schedule activates \
                     it at block {block} while the chain has already executed block {head}",
                    fork.name()
                ),
                // The record activated the fork within the executed history at
                // a different boundary.
                (Some(ForkActivation::Block(from_block)), Some(to_block)) => format!(
                    "hardfork '{}' boundary changed from {from_block} to {to_block} while the \
                     chain has already executed block {head}",
                    fork.name()
                ),
                // The record activated the fork within the executed history;
                // the selected schedule never activates it.
                (Some(ForkActivation::Block(from_block)), None) => format!(
                    "hardfork '{}' was recorded at block {from_block}, but the selected \
                     schedule never activates it while the chain has already executed block \
                     {head}",
                    fork.name()
                ),
                // `activation()` reads both as `None`, so these cannot differ.
                (None, None) | (Some(ForkActivation::Never), None) => {
                    unreachable!("a never-activating selection cannot disagree with the record")
                }
            };
            eyre::bail!(
                "{detail}; an executed fork's activation block cannot change and a new fork \
                 cannot be back-dated into the executed history. Refusing to start with a \
                 schedule inconsistent with the chain's history. Remedy: add or fix the fork's \
                 entry in {record_path:?}, or delete {record_path:?} to re-record the selected \
                 schedule (the executed-history check then starts from the current head)",
            );
        }
        moves.push(FutureForkMove { fork: *fork, recorded, selected: selected_block });
    }
    Ok(moves)
}

/// Rayls schedules only use block-based (or absent) activations. Fire in dev
/// builds if a TTD- or timestamp-based fork is ever added: the record format
/// (and its verification) must learn to represent it first.
fn assert_block_based(schedule: &[(RaylsHardFork, ForkCondition)]) {
    debug_assert!(
        schedule
            .iter()
            .all(|(_, condition)| matches!(condition, ForkCondition::Block(_) | ForkCondition::Never)),
        "Rayls schedules are block-based only; extend ScheduleRecord before adding TTD/timestamp forks"
    );
}

/// The activation block of a fork condition; `None` for `Never`.
fn block_of(condition: &ForkCondition) -> Option<u64> {
    debug_assert!(
        matches!(condition, ForkCondition::Block(_) | ForkCondition::Never),
        "Rayls schedules are block-based only; extend ScheduleRecord before adding TTD/timestamp forks"
    );
    match condition {
        ForkCondition::Block(block) => Some(*block),
        ForkCondition::Never => None,
        // Rayls schedules only use block-based (or absent) activations; the
        // debug assert above fires in dev builds otherwise.
        ForkCondition::TTD { .. } | ForkCondition::Timestamp(_) => None,
    }
}

#[cfg(test)]
const EXAMPLE_CLIENT1_YAML: &str = include_str!("testdata/client1.yaml");

#[cfg(test)]
mod tests {
    use super::*;

    const PROFILE_YAML: &str = r#"
chain_id: 7295799
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
        for activation in
            [ForkActivation::Block(0), ForkActivation::Block(999), ForkActivation::Never]
        {
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
        assert_eq!(profile.chain_id, 7295799);
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
        assert_eq!(by_name("BatchDigestV2").map(|(_, c)| *c), Some(ForkCondition::Block(100)));
        assert_eq!(by_name("AdminTransfer").map(|(_, c)| *c), Some(ForkCondition::Never));
        // Absent forks are omitted from the schedule (lookup yields Never).
        assert!(by_name("Tokenomics").is_none());
        assert!(schedule.len() <= RaylsHardFork::VARIANTS.len());
    }

    #[test]
    fn from_builtin_roundtrips_the_baked_in_schedule() {
        for network in [
            RaylsNetwork::Devnet,
            RaylsNetwork::Testnet,
            RaylsNetwork::Mainnet,
            RaylsNetwork::Local,
        ] {
            let profile = NetworkProfile::from_builtin(network);
            assert_eq!(profile.chain_id, network.chain_id());
            // A complete map (passes the completeness gate) resolving to exactly
            // the baked-in schedule.
            profile.validate_hardforks().unwrap();
            let baked = RaylsHardFork::for_network(network);
            let schedule = profile.schedule();
            assert_eq!(schedule.len(), baked.len());
            for (fork, condition) in baked {
                let resolved =
                    schedule.iter().find(|(f, _)| f.name() == fork.name()).map(|(_, c)| *c);
                assert_eq!(resolved, Some(condition), "fork {fork} of {network}");
            }
        }
    }

    /// A complete profile: every known fork pinned to `never`.
    fn complete_profile() -> NetworkProfile {
        let hardforks = RaylsHardFork::VARIANTS
            .iter()
            .map(|fork| (fork.name().to_string(), ForkActivation::Never))
            .collect();
        NetworkProfile { chain_id: 7295799, hardforks }
    }

    #[test]
    fn validate_hardforks_accepts_complete_map() {
        complete_profile().validate_hardforks().unwrap();
    }

    #[test]
    fn validate_hardforks_accepts_known_names_case_insensitively() {
        let mut profile = complete_profile();
        profile.hardforks.insert("batchdigestv2".to_string(), ForkActivation::Block(1));
        profile.validate_hardforks().unwrap();
    }

    #[test]
    fn validate_hardforks_rejects_unknown_names() {
        let mut profile = complete_profile();
        profile.hardforks.insert("MyFork".to_string(), ForkActivation::Block(1));
        let err = profile.validate_hardforks().unwrap_err();
        assert!(err.to_string().contains("unknown hardfork 'MyFork'"), "{err}");
    }

    #[test]
    fn validate_hardforks_rejects_missing_forks() {
        let mut profile = complete_profile();
        profile.hardforks.remove("HybridRewards");
        profile.hardforks.remove("SenderAffinityLoadBalancing");
        let err = profile.validate_hardforks().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("HybridRewards"), "{msg}");
        assert!(msg.contains("SenderAffinityLoadBalancing"), "{msg}");
        assert!(msg.contains("\"never\""), "{msg}");
    }

    #[test]
    fn example_client1_yaml_parses() {
        let file: NetworkConfigFile = serde_yaml::from_str(EXAMPLE_CLIENT1_YAML).unwrap();
        assert_eq!(file.networks.len(), 2);

        let mainnet = file.subnet("mainnet").unwrap();
        mainnet.validate_hardforks().unwrap();
        assert_eq!(mainnet.chain_id, 72957);

        let testnet = file.subnet("testnet").unwrap();
        testnet.validate_hardforks().unwrap();
        assert_eq!(testnet.chain_id, 7295799);

        let by = |p: &NetworkProfile, name: &str| {
            p.schedule().iter().find(|(fork, _)| fork.name() == name).map(|(_, c)| *c)
        };
        assert_eq!(by(mainnet, "Eip1559"), Some(ForkCondition::Block(0)));
        assert_eq!(by(mainnet, "UsdrSupplyCorrection"), Some(ForkCondition::Block(3_569_194)));
        assert_eq!(by(testnet, "Tokenomics"), Some(ForkCondition::Block(1_879_000)));
        assert_eq!(by(testnet, "Erc20PrecompileBytecode"), Some(ForkCondition::Never));
        // Both subnets define every known fork (the completeness gate).
        assert_eq!(by(mainnet, "SenderAffinityLoadBalancing"), Some(ForkCondition::Never));
        assert_eq!(by(testnet, "OutputSeqNormalization"), Some(ForkCondition::Never));
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
        block.lines().map(|line| format!("    {line}")).collect::<Vec<_>>().join("\n")
    }

    fn record(chain_id: u64, head: u64, hardforks: &[(&str, ForkActivation)]) -> ScheduleRecord {
        ScheduleRecord {
            chain_id,
            as_of_block: head,
            hardforks: hardforks
                .iter()
                .map(|(name, activation)| (name.to_string(), *activation))
                .collect(),
        }
    }

    fn eip1559(block: u64) -> (RaylsHardFork, ForkCondition) {
        (RaylsHardFork::Eip1559, ForkCondition::Block(block))
    }

    /// The record path passed to `verify_schedule` in tests (the refusal
    /// message names it in its remedy).
    const RECORD_PATH: &str = "schedule-record.yaml";

    #[test]
    fn record_from_schedule_stores_never_forks() {
        let record = ScheduleRecord::from_schedule(
            487,
            10,
            &[eip1559(5), (RaylsHardFork::Tokenomics, ForkCondition::Never)],
        );
        assert_eq!(record.chain_id, 487);
        assert_eq!(record.as_of_block, 10);
        assert_eq!(record.hardforks.len(), 2);
        assert_eq!(record.hardforks.get("Eip1559"), Some(&ForkActivation::Block(5)));
        // `never` is stored explicitly so the record is a complete snapshot;
        // only a record that predates a fork lacks an entry for it.
        assert_eq!(record.hardforks.get("Tokenomics"), Some(&ForkActivation::Never));
    }

    #[test]
    fn record_yaml_roundtrip() {
        let record = record(
            72957,
            123,
            &[("Eip1559", ForkActivation::Block(0)), ("Uups", ForkActivation::Never)],
        );
        let yaml = serde_yaml::to_string(&record).unwrap();
        let parsed: ScheduleRecord = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed, record);
    }

    #[test]
    fn fork_activation_block_accessor() {
        assert_eq!(ForkActivation::Block(5).block(), Some(5));
        assert_eq!(ForkActivation::Never.block(), None);
    }

    #[test]
    fn record_activation_lookup() {
        let record = record(
            487,
            0,
            &[("Eip1559", ForkActivation::Block(7)), ("Uups", ForkActivation::Never)],
        );
        assert_eq!(record.activation("Eip1559"), Some(7));
        // Fork names match case-insensitively.
        assert_eq!(record.activation("eip1559"), Some(7));
        // An explicit `never` and an absent fork both read as `None`.
        assert_eq!(record.activation("Uups"), None);
        assert_eq!(record.activation("AdminTransfer"), None);
        // `entry` keeps the distinction an `activation` lookup collapses:
        // explicit `never` vs absent (record predates the fork).
        assert_eq!(record.entry("Uups"), Some(&ForkActivation::Never));
        assert_eq!(record.entry("uups"), Some(&ForkActivation::Never));
        assert_eq!(record.entry("AdminTransfer"), None);
    }

    #[test]
    fn verify_identical_schedule_passes() {
        let record = record(487, 0, &[("Eip1559", ForkActivation::Block(5))]);
        let moves = verify_schedule(&record, &[eip1559(5)], 487, 100, Path::new(RECORD_PATH))
            .expect("identical schedule");
        assert!(moves.is_empty());
    }

    #[test]
    fn verify_future_move_is_allowed_and_reported() {
        let record = record(487, 0, &[("Eip1559", ForkActivation::Block(1000))]);
        let moves = verify_schedule(&record, &[eip1559(2000)], 487, 500, Path::new(RECORD_PATH))
            .expect("future move");
        assert_eq!(moves.len(), 1);
        assert_eq!(moves[0].fork, RaylsHardFork::Eip1559);
        assert_eq!(moves[0].recorded, Some(1000));
        assert_eq!(moves[0].selected, Some(2000));
    }

    #[test]
    fn verify_executed_move_is_refused() {
        let record = record(487, 0, &[("Eip1559", ForkActivation::Block(1000))]);
        let err = verify_schedule(&record, &[eip1559(2000)], 487, 1500, Path::new(RECORD_PATH))
            .unwrap_err();
        assert!(err.to_string().contains("Eip1559"), "{err}");
        assert!(err.to_string().contains("inconsistent with the chain's history"), "{err}");
    }

    #[test]
    fn verify_boundary_at_head_is_refused() {
        // A fork boundary exactly at the head has already affected block `head`.
        let record = record(487, 0, &[("Eip1559", ForkActivation::Block(500))]);
        assert!(
            verify_schedule(&record, &[eip1559(1000)], 487, 500, Path::new(RECORD_PATH)).is_err()
        );
    }

    #[test]
    fn verify_boundary_just_after_head_is_allowed() {
        let record = record(487, 0, &[("Eip1559", ForkActivation::Block(1000))]);
        let moves = verify_schedule(&record, &[eip1559(501)], 487, 500, Path::new(RECORD_PATH))
            .expect("future move");
        assert_eq!(moves.len(), 1);
    }

    #[test]
    fn verify_backdated_fork_is_refused() {
        // The record never activated the fork; the selection back-dates it into
        // the executed history.
        let record = record(487, 0, &[]);
        assert!(
            verify_schedule(&record, &[eip1559(100)], 487, 500, Path::new(RECORD_PATH)).is_err()
        );
    }

    #[test]
    fn verify_removed_executed_fork_is_refused() {
        // The record activated the fork in the executed history; the selection
        // removes it.
        let record = record(487, 0, &[("Eip1559", ForkActivation::Block(100))]);
        let err = verify_schedule(&record, &[], 487, 500, Path::new(RECORD_PATH)).unwrap_err();
        assert!(err.to_string().contains("Eip1559"), "{err}");
    }

    #[test]
    fn verify_chain_id_mismatch_is_refused() {
        let record = record(487, 0, &[("Eip1559", ForkActivation::Block(5))]);
        let err = verify_schedule(&record, &[eip1559(5)], 72957, 100, Path::new(RECORD_PATH))
            .unwrap_err();
        assert!(err.to_string().contains("chain-id"), "{err}");
    }

    #[test]
    fn verify_unknown_record_fork_is_refused() {
        let record = record(487, 0, &[("MyFork", ForkActivation::Block(5))]);
        let err = verify_schedule(&record, &[], 487, 100, Path::new(RECORD_PATH)).unwrap_err();
        assert!(err.to_string().contains("unknown hardfork 'MyFork'"), "{err}");
    }

    #[test]
    fn verify_never_record_entry_matches_never_selection() {
        // An explicit `never` in the record is the same as the selection's
        // `Never` (or an absent fork).
        let record = record(487, 0, &[("Eip1559", ForkActivation::Never)]);
        let moves = verify_schedule(&record, &[], 487, 100, Path::new(RECORD_PATH))
            .expect("never == absent");
        assert!(moves.is_empty());
    }

    #[test]
    fn record_as_of_block_is_not_verified() {
        // `as_of_block` is informational: the gate compares the schedules
        // against the live `head` passed in, not the head stored in the
        // record. A stale stored head must not refuse an identical schedule.
        let record = record(487, 999, &[("Eip1559", ForkActivation::Block(5))]);
        let moves = verify_schedule(&record, &[eip1559(5)], 487, 100, Path::new(RECORD_PATH))
            .expect("the stored head is not verified");
        assert!(moves.is_empty());
    }

    #[test]
    fn verify_predated_fork_is_refused_with_remedy() {
        // The record has no entry for the fork (it was written before the fork
        // existed); the selection activates it inside the executed history.
        let record = record(487, 0, &[]);
        let err =
            verify_schedule(&record, &[eip1559(0)], 487, 100, Path::new(RECORD_PATH)).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("predates"), "{msg}");
        assert!(msg.contains(RECORD_PATH), "{msg}");
        assert!(msg.contains("delete"), "{msg}");
    }

    #[test]
    fn verify_recorded_never_fork_is_refused_distinctly() {
        // The record pins the fork as never; the selection activates it inside
        // the executed history. The message must say "recorded as never", not
        // "predates".
        let record = record(487, 0, &[("Eip1559", ForkActivation::Never)]);
        let err =
            verify_schedule(&record, &[eip1559(50)], 487, 100, Path::new(RECORD_PATH)).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("recorded as never"), "{msg}");
        assert!(!msg.contains("predates"), "{msg}");
        assert!(msg.contains("delete"), "{msg}");
    }

    #[test]
    fn verify_future_activation_from_never_is_allowed() {
        let record = record(487, 0, &[("Eip1559", ForkActivation::Never)]);
        let moves = verify_schedule(&record, &[eip1559(2000)], 487, 500, Path::new(RECORD_PATH))
            .expect("future activation from never");
        assert_eq!(moves.len(), 1);
        assert_eq!(moves[0].recorded, None);
        assert_eq!(moves[0].selected, Some(2000));
    }
}
