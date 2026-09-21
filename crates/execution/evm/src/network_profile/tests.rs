use std::path::Path;

use rayls_infrastructure_types::RaylsNetwork;
use reth_chainspec::ForkCondition;

use super::*;
use crate::chainspec::RaylsHardFork;

const EXAMPLE_CLIENT1_YAML: &str = include_str!("testdata/client1.yaml");

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
    for activation in [ForkActivation::Block(0), ForkActivation::Block(999), ForkActivation::Never]
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
    assert_eq!(profile.hardforks.get(&ForkName::from("Eip1559")), Some(&ForkActivation::Block(0)));
    assert_eq!(
        profile.hardforks.get(&ForkName::from("AdminTransfer")),
        Some(&ForkActivation::Never)
    );
}

#[test]
fn profile_requires_chain_id() {
    let err = serde_yaml::from_str::<NetworkProfile>("hardforks: { Eip1559: 0 }\n").unwrap_err();
    assert!(err.to_string().contains("chain_id"), "{err}");
}

#[test]
fn profile_schedule_resolves_known_forks_only() {
    let profile: NetworkProfile = serde_yaml::from_str(PROFILE_YAML).unwrap();
    let schedule = profile.schedule();
    let by_name = |name: &str| schedule.iter().find(|entry| entry.fork.name() == name);
    assert_eq!(by_name("Eip1559").map(|entry| entry.condition), Some(ForkCondition::Block(0)));
    assert_eq!(
        by_name("BatchDigestV2").map(|entry| entry.condition),
        Some(ForkCondition::Block(100))
    );
    assert_eq!(by_name("AdminTransfer").map(|entry| entry.condition), Some(ForkCondition::Never));
    // Absent forks are omitted from the schedule (lookup yields Never).
    assert!(by_name("Tokenomics").is_none());
    assert!(schedule.len() <= RaylsHardFork::VARIANTS.len());
}

#[test]
fn from_builtin_roundtrips_the_baked_in_schedule() {
    for network in
        [RaylsNetwork::Devnet, RaylsNetwork::Testnet, RaylsNetwork::Mainnet, RaylsNetwork::Local]
    {
        let profile = NetworkProfile::from_builtin(network);
        assert_eq!(profile.chain_id, network.chain_id());
        // A complete map (passes the completeness gate) resolving to exactly
        // the baked-in schedule.
        profile.validate_hardforks().unwrap();
        let baked = RaylsHardFork::for_network(network);
        let schedule = profile.schedule();
        assert_eq!(schedule.len(), baked.len());
        for entry in baked {
            let resolved =
                schedule.iter().find(|e| e.fork.name() == entry.fork.name()).map(|e| e.condition);
            assert_eq!(resolved, Some(entry.condition), "fork {} of {network}", entry.fork);
        }
    }
}

/// A complete profile: every known fork pinned to `never`.
fn complete_profile() -> NetworkProfile {
    let hardforks = RaylsHardFork::VARIANTS
        .iter()
        .map(|fork| (ForkName::from(fork.name()), ForkActivation::Never))
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
    profile.hardforks.insert(ForkName::from("batchdigestv2"), ForkActivation::Block(1));
    profile.validate_hardforks().unwrap();
}

#[test]
fn validate_hardforks_rejects_unknown_names() {
    let mut profile = complete_profile();
    profile.hardforks.insert(ForkName::from("MyFork"), ForkActivation::Block(1));
    let err = profile.validate_hardforks().unwrap_err();
    assert!(err.to_string().contains("unknown hardfork 'myfork'"), "{err}");
}

#[test]
fn validate_hardforks_rejects_missing_forks() {
    let mut profile = complete_profile();
    profile.hardforks.remove(&ForkName::from("HybridRewards"));
    profile.hardforks.remove(&ForkName::from("SenderAffinityLoadBalancing"));
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
        p.schedule().iter().find(|entry| entry.fork.name() == name).map(|entry| entry.condition)
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
        file.subnet("mainnet").unwrap().hardforks.get(&ForkName::from("Eip1559")),
        Some(&ForkActivation::Block(0))
    );
    assert_eq!(
        file.subnet("testnet").unwrap().hardforks.get(&ForkName::from("Eip1559")),
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
            .map(|(name, activation)| (ForkName::from(*name), *activation))
            .collect(),
    }
}

fn profile(chain_id: u64, hardforks: &[(&str, ForkActivation)]) -> NetworkProfile {
    NetworkProfile {
        chain_id,
        hardforks: hardforks
            .iter()
            .map(|(name, activation)| (ForkName::from(*name), *activation))
            .collect(),
    }
}

/// The record path passed to `verify_schedule` in tests (the refusal
/// message names it in its remedy).
const RECORD_PATH: &str = "schedule-record.yaml";

#[test]
fn record_from_profile_stores_never_forks() {
    let record = ScheduleRecord::from_profile(
        &profile(
            487,
            &[("Eip1559", ForkActivation::Block(5)), ("Tokenomics", ForkActivation::Never)],
        ),
        10,
    );
    assert_eq!(record.chain_id, 487);
    assert_eq!(record.as_of_block, 10);
    assert_eq!(record.hardforks.len(), 2);
    assert_eq!(record.hardforks.get(&ForkName::from("Eip1559")), Some(&ForkActivation::Block(5)));
    // `never` is stored explicitly so the record is a complete snapshot;
    // only a record that predates a fork lacks an entry for it.
    assert_eq!(record.hardforks.get(&ForkName::from("Tokenomics")), Some(&ForkActivation::Never));
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
    let record =
        record(487, 0, &[("Eip1559", ForkActivation::Block(7)), ("Uups", ForkActivation::Never)]);
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
    let moves = verify_schedule(
        &record,
        &profile(487, &[("Eip1559", ForkActivation::Block(5))]),
        100,
        Path::new(RECORD_PATH),
    )
    .expect("identical schedule");
    assert!(moves.is_empty());
}

#[test]
fn verify_future_move_is_allowed_and_reported() {
    let record = record(487, 0, &[("Eip1559", ForkActivation::Block(1000))]);
    let moves = verify_schedule(
        &record,
        &profile(487, &[("Eip1559", ForkActivation::Block(2000))]),
        500,
        Path::new(RECORD_PATH),
    )
    .expect("future move");
    assert_eq!(moves.len(), 1);
    assert_eq!(moves[0].fork, RaylsHardFork::Eip1559);
    assert_eq!(moves[0].recorded, Some(1000));
    assert_eq!(moves[0].selected, Some(2000));
}

#[test]
fn verify_executed_move_is_refused() {
    let record = record(487, 0, &[("Eip1559", ForkActivation::Block(1000))]);
    let err = verify_schedule(
        &record,
        &profile(487, &[("Eip1559", ForkActivation::Block(2000))]),
        1500,
        Path::new(RECORD_PATH),
    )
    .unwrap_err();
    assert!(err.to_string().contains("Eip1559"), "{err}");
    assert!(err.to_string().contains("inconsistent with the chain's history"), "{err}");
}

#[test]
fn verify_reports_all_executed_moves_together() {
    // Two forks whose boundaries moved within the executed history must both
    // appear in the single refusal, not just the first (one fix, one restart).
    let record = record(
        487,
        0,
        &[("Eip1559", ForkActivation::Block(100)), ("BatchDigestV2", ForkActivation::Block(200))],
    );
    let err = verify_schedule(
        &record,
        &profile(
            487,
            &[
                ("Eip1559", ForkActivation::Block(300)),
                ("BatchDigestV2", ForkActivation::Block(400)),
            ],
        ),
        500,
        Path::new(RECORD_PATH),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("schedule inconsistencies"), "{msg}");
    assert!(msg.contains("Eip1559"), "{msg}");
    assert!(msg.contains("BatchDigestV2"), "{msg}");
    assert!(msg.contains(RECORD_PATH), "{msg}");
    assert!(msg.contains("delete"), "{msg}");
}

#[test]
fn verify_boundary_at_head_is_refused() {
    // A fork boundary exactly at the head has already affected block `head`.
    let record = record(487, 0, &[("Eip1559", ForkActivation::Block(500))]);
    assert!(verify_schedule(
        &record,
        &profile(487, &[("Eip1559", ForkActivation::Block(1000))]),
        500,
        Path::new(RECORD_PATH),
    )
    .is_err());
}

#[test]
fn verify_boundary_just_after_head_is_allowed() {
    let record = record(487, 0, &[("Eip1559", ForkActivation::Block(1000))]);
    let moves = verify_schedule(
        &record,
        &profile(487, &[("Eip1559", ForkActivation::Block(501))]),
        500,
        Path::new(RECORD_PATH),
    )
    .expect("future move");
    assert_eq!(moves.len(), 1);
}

#[test]
fn verify_backdated_fork_is_refused() {
    // The record never activated the fork; the selection back-dates it into
    // the executed history.
    let record = record(487, 0, &[]);
    assert!(verify_schedule(
        &record,
        &profile(487, &[("Eip1559", ForkActivation::Block(100))]),
        500,
        Path::new(RECORD_PATH),
    )
    .is_err());
}

#[test]
fn verify_removed_executed_fork_is_refused() {
    // The record activated the fork in the executed history; the selection
    // removes it.
    let record = record(487, 0, &[("Eip1559", ForkActivation::Block(100))]);
    let err =
        verify_schedule(&record, &profile(487, &[]), 500, Path::new(RECORD_PATH)).unwrap_err();
    assert!(err.to_string().contains("Eip1559"), "{err}");
}

#[test]
fn verify_chain_id_mismatch_is_refused() {
    let record = record(487, 0, &[("Eip1559", ForkActivation::Block(5))]);
    let err = verify_schedule(
        &record,
        &profile(72957, &[("Eip1559", ForkActivation::Block(5))]),
        100,
        Path::new(RECORD_PATH),
    )
    .unwrap_err();
    assert!(err.to_string().contains("chain-id"), "{err}");
}

#[test]
fn verify_unknown_record_fork_is_refused() {
    let record = record(487, 0, &[("MyFork", ForkActivation::Block(5))]);
    let err =
        verify_schedule(&record, &profile(487, &[]), 100, Path::new(RECORD_PATH)).unwrap_err();
    assert!(err.to_string().contains("unknown hardfork 'myfork'"), "{err}");
}

#[test]
fn verify_never_record_entry_matches_never_selection() {
    // An explicit `never` in the record is the same as the selection's
    // `Never` (or an absent fork).
    let record = record(487, 0, &[("Eip1559", ForkActivation::Never)]);
    let moves = verify_schedule(&record, &profile(487, &[]), 100, Path::new(RECORD_PATH))
        .expect("never == absent");
    assert!(moves.is_empty());
}

#[test]
fn record_as_of_block_is_not_verified() {
    // `as_of_block` is informational: the gate compares the schedules
    // against the live `head` passed in, not the head stored in the
    // record. A stale stored head must not refuse an identical schedule.
    let record = record(487, 999, &[("Eip1559", ForkActivation::Block(5))]);
    let moves = verify_schedule(
        &record,
        &profile(487, &[("Eip1559", ForkActivation::Block(5))]),
        100,
        Path::new(RECORD_PATH),
    )
    .expect("the stored head is not verified");
    assert!(moves.is_empty());
}

#[test]
fn verify_predated_fork_is_refused_with_remedy() {
    // The record has no entry for the fork (it was written before the fork
    // existed); the selection activates it inside the executed history.
    let record = record(487, 0, &[]);
    let err = verify_schedule(
        &record,
        &profile(487, &[("Eip1559", ForkActivation::Block(0))]),
        100,
        Path::new(RECORD_PATH),
    )
    .unwrap_err();
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
    let err = verify_schedule(
        &record,
        &profile(487, &[("Eip1559", ForkActivation::Block(50))]),
        100,
        Path::new(RECORD_PATH),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("recorded as never"), "{msg}");
    assert!(!msg.contains("predates"), "{msg}");
    assert!(msg.contains("delete"), "{msg}");
}

#[test]
fn verify_future_activation_from_never_is_allowed() {
    let record = record(487, 0, &[("Eip1559", ForkActivation::Never)]);
    let moves = verify_schedule(
        &record,
        &profile(487, &[("Eip1559", ForkActivation::Block(2000))]),
        500,
        Path::new(RECORD_PATH),
    )
    .expect("future activation from never");
    assert_eq!(moves.len(), 1);
    assert_eq!(moves[0].recorded, None);
    assert_eq!(moves[0].selected, Some(2000));
}
