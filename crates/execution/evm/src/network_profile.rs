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
                eyre::bail!(
                    "unknown hardfork '{name}' in network config; known forks: {known_forks}"
                )
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
    ACTIVE_PROFILE.set(profile).map_err(|_| eyre::eyre!("active network profile is already set"))
}

/// The active hardfork schedule, if an external config file was provided.
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
    /// The recorded fork schedule; forks absent from the map never activate.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub hardforks: BTreeMap<String, ForkActivation>,
}

impl ScheduleRecord {
    /// Build a record from the schedule selected for this boot, omitting
    /// never-activating forks.
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
                ForkCondition::Never => None,
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
pub fn verify_schedule(
    record: &ScheduleRecord,
    selected: &[(RaylsHardFork, ForkCondition)],
    selected_chain_id: u64,
    head: u64,
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
            eyre::bail!(
                "hardfork '{}' boundary changed from {} to {} but the chain has already \
                 executed block {head}; an executed fork's activation block cannot change and \
                 a new fork cannot be back-dated into the executed history. Refusing to start \
                 with a schedule inconsistent with the chain's history",
                fork.name(),
                fmt_block(recorded),
                fmt_block(selected_block)
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

/// Render an optional activation block for log/error messages.
fn fmt_block(block: Option<u64>) -> String {
    block.map_or_else(|| "never".to_string(), |block| block.to_string())
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

    #[test]
    fn record_from_schedule_omits_never_forks() {
        let record = ScheduleRecord::from_schedule(
            487,
            10,
            &[eip1559(5), (RaylsHardFork::Tokenomics, ForkCondition::Never)],
        );
        assert_eq!(record.chain_id, 487);
        assert_eq!(record.as_of_block, 10);
        assert_eq!(record.hardforks.len(), 1);
        assert_eq!(record.hardforks.get("Eip1559"), Some(&ForkActivation::Block(5)));
        assert!(!record.hardforks.contains_key("Tokenomics"));
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
    }

    #[test]
    fn verify_identical_schedule_passes() {
        let record = record(487, 0, &[("Eip1559", ForkActivation::Block(5))]);
        let moves = verify_schedule(&record, &[eip1559(5)], 487, 100).expect("identical schedule");
        assert!(moves.is_empty());
    }

    #[test]
    fn verify_future_move_is_allowed_and_reported() {
        let record = record(487, 0, &[("Eip1559", ForkActivation::Block(1000))]);
        let moves = verify_schedule(&record, &[eip1559(2000)], 487, 500).expect("future move");
        assert_eq!(moves.len(), 1);
        assert_eq!(moves[0].fork, RaylsHardFork::Eip1559);
        assert_eq!(moves[0].recorded, Some(1000));
        assert_eq!(moves[0].selected, Some(2000));
    }

    #[test]
    fn verify_executed_move_is_refused() {
        let record = record(487, 0, &[("Eip1559", ForkActivation::Block(1000))]);
        let err = verify_schedule(&record, &[eip1559(2000)], 487, 1500).unwrap_err();
        assert!(err.to_string().contains("Eip1559"), "{err}");
        assert!(err.to_string().contains("inconsistent with the chain's history"), "{err}");
    }

    #[test]
    fn verify_boundary_at_head_is_refused() {
        // A fork boundary exactly at the head has already affected block `head`.
        let record = record(487, 0, &[("Eip1559", ForkActivation::Block(500))]);
        assert!(verify_schedule(&record, &[eip1559(1000)], 487, 500).is_err());
    }

    #[test]
    fn verify_boundary_just_after_head_is_allowed() {
        let record = record(487, 0, &[("Eip1559", ForkActivation::Block(1000))]);
        let moves = verify_schedule(&record, &[eip1559(501)], 487, 500).expect("future move");
        assert_eq!(moves.len(), 1);
    }

    #[test]
    fn verify_backdated_fork_is_refused() {
        // The record never activated the fork; the selection back-dates it into
        // the executed history.
        let record = record(487, 0, &[]);
        assert!(verify_schedule(&record, &[eip1559(100)], 487, 500).is_err());
    }

    #[test]
    fn verify_removed_executed_fork_is_refused() {
        // The record activated the fork in the executed history; the selection
        // removes it.
        let record = record(487, 0, &[("Eip1559", ForkActivation::Block(100))]);
        let err = verify_schedule(&record, &[], 487, 500).unwrap_err();
        assert!(err.to_string().contains("Eip1559"), "{err}");
    }

    #[test]
    fn verify_chain_id_mismatch_is_refused() {
        let record = record(487, 0, &[("Eip1559", ForkActivation::Block(5))]);
        let err = verify_schedule(&record, &[eip1559(5)], 72957, 100).unwrap_err();
        assert!(err.to_string().contains("chain-id"), "{err}");
    }

    #[test]
    fn verify_unknown_record_fork_is_refused() {
        let record = record(487, 0, &[("MyFork", ForkActivation::Block(5))]);
        let err = verify_schedule(&record, &[], 487, 100).unwrap_err();
        assert!(err.to_string().contains("unknown hardfork 'MyFork'"), "{err}");
    }

    #[test]
    fn verify_never_record_entry_matches_never_selection() {
        // An explicit `never` in the record is the same as the selection's
        // `Never` (or an absent fork).
        let record = record(487, 0, &[("Eip1559", ForkActivation::Never)]);
        let moves = verify_schedule(&record, &[], 487, 100).expect("never == absent");
        assert!(moves.is_empty());
    }
}
