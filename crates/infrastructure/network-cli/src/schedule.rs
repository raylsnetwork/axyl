//! Hardfork schedule selection and datadir verification at boot.
//!
//! A datadir carries no hardfork schedule: every `node` boot selects one
//! explicitly (a `--config-file`/`--subnet` profile, or the built-in schedule
//! behind `--network`), and these gates verify the selection against the
//! datadir before the node starts — refusing to boot when the datadir and the
//! selected schedule disagree.
use std::path::{Path, PathBuf};

use eyre::Context;
use rayls_execution_evm::{
    reth_env::{RethConfig, RethEnv},
    verify_schedule, NetworkConfigFile, NetworkProfile, ScheduleRecord,
};
use rayls_infrastructure_config::RaylsDirs;
use rayls_infrastructure_types::RaylsNetwork;
use tracing::{info, warn};

/// Load the client's network config file and select the requested subnet.
///
/// Validates the profile's `hardforks` map before the node starts: every entry
/// must be a known fork and every known fork must be defined (a stale file
/// that omits a newly added fork would otherwise run that fork as `never`). A
/// broken or stale file fails fast with an actionable message.
pub fn load_subnet_profile(config_file: &Path, subnet: &str) -> eyre::Result<NetworkProfile> {
    let yaml = std::fs::read_to_string(config_file)
        .wrap_err_with(|| format!("failed to read network config file {config_file:?}"))?;
    let file: NetworkConfigFile = serde_yaml::from_str(&yaml)
        .wrap_err_with(|| format!("failed to parse network config file {config_file:?}"))?;
    let profile = file.subnet(subnet).cloned().ok_or_else(|| {
        let known = file.networks.keys().cloned().collect::<Vec<_>>().join(", ");
        eyre::eyre!("subnet '{subnet}' not found in {config_file:?}; available subnets: {known}")
    })?;
    if profile.hardforks.is_empty() {
        eyre::bail!(
            "subnet '{subnet}' in {config_file:?} defines no `hardforks`; every subnet must \
             define its hardfork schedule (a block number or \"never\" per fork)"
        );
    }
    profile
        .validate_hardforks()
        .wrap_err_with(|| format!("subnet '{subnet}' in {config_file:?}"))?;
    Ok(profile)
}

/// Verify that the datadir's genesis chain-id matches the chain-id of the
/// selected schedule source (a config-file subnet, or the baked-in network
/// profile). A mismatch means the datadir belongs to a different network or
/// client, and running it would apply the wrong hardfork schedule — refuse to
/// boot.
pub fn verify_datadir_chain_id(actual: u64, expected: u64, source: &str) -> eyre::Result<()> {
    if actual != expected {
        eyre::bail!(
            "datadir chain-id {actual} does not match the expected chain-id {expected} \
             from {source}. The datadir appears to belong to a different network or client. \
             Use a datadir whose genesis chain-id is {expected}, or select a schedule source \
             whose chain-id is {actual}."
        );
    }
    Ok(())
}

/// The hardfork schedule selected for this boot: the resolved profile plus a
/// human-readable description of where it came from (used in refusal messages
/// and the boot log).
#[derive(Debug)]
pub struct SelectedSchedule {
    /// The selected profile (chain-id + hardfork schedule).
    pub profile: NetworkProfile,
    /// Description of the schedule source, e.g. `subnet 'mainnet' of
    /// "/x/client.yaml"` or `network 'local'`.
    pub source: String,
}

/// Select the hardfork schedule for this boot.
///
/// Precedence: the `--config-file`/`--subnet` profile (already loaded and
/// validated) wins, then the built-in schedule selected by `--network`.
/// Bails when neither is given — a datadir carries no schedule, so the source
/// must be explicit at every boot.
pub fn select_schedule(
    file_schedule: Option<&FileSchedule>,
    network: Option<RaylsNetwork>,
) -> eyre::Result<SelectedSchedule> {
    if let Some(file_schedule) = file_schedule {
        return Ok(SelectedSchedule {
            profile: file_schedule.profile.clone(),
            source: format!("subnet '{}' of {:?}", file_schedule.subnet, file_schedule.path),
        });
    }
    match network {
        Some(network) => Ok(SelectedSchedule {
            profile: NetworkProfile::from_builtin(network),
            source: format!("network '{network}'"),
        }),
        None => eyre::bail!(
            "no hardfork schedule source: start with `--network <devnet|testnet|mainnet|local>` \
             (the chain-id must match the genesis) or `--config-file <path> --subnet <name>`"
        ),
    }
}

/// The hardfork schedule selected from a `--config-file`: the file path, the
/// subnet chosen with `--subnet`, and the subnet's resolved profile.
#[derive(Debug)]
pub struct FileSchedule {
    /// Path of the network config file.
    pub path: PathBuf,
    /// The subnet selected from it.
    pub subnet: String,
    /// The subnet's resolved profile.
    pub profile: NetworkProfile,
}

/// Verify the hardfork schedule selected for this boot against the datadir's
/// schedule record, then update the record for this boot.
///
/// The record (see [`ScheduleRecord`]) pins the schedule this chain's blocks
/// were produced under: a selected schedule that disagrees with the record on
/// an already-executed fork is refused (it would re-interpret the chain's
/// history), while a differing fork boundary still in the future is allowed —
/// that is how agreed schedule updates ship — but warned about. A datadir
/// without a record (a fresh chain) gets one written for the selected schedule.
pub fn verify_schedule_record<P: RaylsDirs>(
    datadir: &P,
    node_config: &RethConfig,
    profile: &NetworkProfile,
) -> eyre::Result<()> {
    let (schedule, chain_id) = (profile.schedule(), profile.chain_id);

    // The chain's highest executed block: reth's `Finish` stage checkpoint
    // (the node re-opens the DB right after).
    let head = RethEnv::best_block_number(node_config, datadir.reth_db_path())?;

    // Read (rather than `exists()`-then-read): a record deleted concurrently
    // is treated as "no record" — the trust-and-record path — not a hard error.
    let path = datadir.schedule_record_path();
    let existing: Option<ScheduleRecord> = match std::fs::read_to_string(&path) {
        Ok(raw) => Some(
            serde_yaml::from_str::<ScheduleRecord>(&raw)
                .with_context(|| format!("failed to parse schedule record {path:?}"))?,
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if head > 0 {
                warn!(
                    target: "cli",
                    ?path,
                    %head,
                    "datadir has no schedule record; trusting the selected schedule and \
                     recording it (the executed-history check starts from now)"
                );
            }
            None
        }
        Err(e) => {
            return Err(e).with_context(|| format!("failed to read schedule record {path:?}"))
        }
    };

    let moves = match &existing {
        Some(record) => verify_schedule(record, &schedule, chain_id, head, &path)?,
        None => Vec::new(),
    };
    for move_ in &moves {
        warn!(
            target: "cli",
            fork = move_.fork.name(),
            ?move_.recorded,
            ?move_.selected,
            %head,
            "future hardfork boundary changed; verify this matches the network-agreed schedule"
        );
    }

    let record = ScheduleRecord::from_schedule(chain_id, head, &schedule);
    let yaml =
        serde_yaml::to_string(&record).wrap_err("failed to serialize the schedule record")?;
    // Write to a sibling temp file and rename into place (atomic on POSIX): a
    // crash mid-write must leave the previous, still-parseable record — never
    // a torn file that bricks the next boot. A leftover temp file after a
    // crash is harmless; the next write overwrites it.
    let tmp = path.with_file_name("schedule-record.yaml.tmp");
    std::fs::write(&tmp, yaml)
        .with_context(|| format!("failed to write schedule record temp file {tmp:?}"))?;
    std::fs::rename(&tmp, &path)
        .with_context(|| format!("failed to move schedule record into place at {path:?}"))?;
    info!(
        target: "cli",
        ?path,
        %chain_id,
        %head,
        "hardfork schedule verified against the chain's executed history"
    );
    Ok(())
}

#[cfg(test)]
mod chain_id_tests {
    use super::{select_schedule, verify_datadir_chain_id, FileSchedule};
    use rayls_execution_evm::NetworkProfile;
    use rayls_infrastructure_types::RaylsNetwork;
    use std::{collections::BTreeMap, path::PathBuf};

    /// A file-schedule standing in for a loaded `--config-file` subnet.
    fn file_schedule(chain_id: u64) -> FileSchedule {
        FileSchedule {
            path: PathBuf::from("/x/y.yaml"),
            subnet: "mainnet".to_string(),
            profile: NetworkProfile { chain_id, hardforks: BTreeMap::new() },
        }
    }

    #[test]
    fn matching_chain_id_passes() {
        assert!(verify_datadir_chain_id(7295799, 7295799, "network 'testnet'").is_ok());
        assert!(verify_datadir_chain_id(72957, 72957, "subnet 'mainnet' of \"/x/y.yaml\"").is_ok());
    }

    #[test]
    fn mismatched_chain_id_is_refused() {
        let err = verify_datadir_chain_id(487, 72957, "network 'mainnet'").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("487"), "{msg}");
        assert!(msg.contains("72957"), "{msg}");
        assert!(msg.contains("network 'mainnet'"), "{msg}");
    }

    #[test]
    fn mismatched_chain_id_names_the_config_file() {
        // A file-schedule source names the subnet and the file, so the refusal
        // tells the operator which config file carries the wrong chain-id.
        let err = verify_datadir_chain_id(487, 72957, "subnet 'mainnet' of \"/x/client.yaml\"")
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("487"), "{msg}");
        assert!(msg.contains("72957"), "{msg}");
        assert!(msg.contains("subnet 'mainnet'"), "{msg}");
        assert!(msg.contains("client.yaml"), "{msg}");
    }

    #[test]
    fn file_schedule_wins_over_network() {
        let file = file_schedule(72957);
        let selected = select_schedule(Some(&file), Some(RaylsNetwork::Testnet)).unwrap();
        assert_eq!(selected.profile.chain_id, 72957);
        assert_eq!(selected.source, "subnet 'mainnet' of \"/x/y.yaml\"");
    }

    #[test]
    fn network_flag_resolves_to_the_builtin_profile() {
        let selected = select_schedule(None, Some(RaylsNetwork::Local)).unwrap();
        assert_eq!(selected.profile.chain_id, 487);
        assert_eq!(selected.source, "network 'local'");
        // The built-in profile is a complete schedule (passes the completeness gate).
        selected.profile.validate_hardforks().unwrap();
    }

    #[test]
    fn no_schedule_source_is_refused() {
        let err = select_schedule(None, None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("no hardfork schedule source"), "{msg}");
        assert!(msg.contains("--network"), "{msg}");
        assert!(msg.contains("--config-file"), "{msg}");
    }
}

#[cfg(test)]
mod config_file_tests {
    use super::load_subnet_profile;
    use rayls_execution_evm::{
        network_profile::{ForkActivation, NetworkConfigFile, NetworkProfile},
        RaylsHardFork,
    };
    use std::collections::BTreeMap;

    /// A valid profile: every known fork pinned, so only the case under test
    /// can make the load fail.
    fn complete_local_profile() -> NetworkProfile {
        let hardforks = RaylsHardFork::VARIANTS
            .iter()
            .map(|fork| (fork.name().to_string(), ForkActivation::Never))
            .collect();
        NetworkProfile { chain_id: 487, hardforks }
    }

    fn write_config(dir: &std::path::Path, name: &str, yaml: &str) -> std::path::PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, yaml).expect("config file written");
        path
    }

    fn write_profile(
        dir: &std::path::Path,
        name: &str,
        profile: &NetworkProfile,
    ) -> std::path::PathBuf {
        let file = NetworkConfigFile {
            networks: BTreeMap::from([("local".to_string(), profile.clone())]),
        };
        write_config(dir, name, &serde_yaml::to_string(&file).expect("profile serializes"))
    }

    #[test]
    fn config_file_complete_profile_loads() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_profile(dir.path(), "client.yaml", &complete_local_profile());
        let profile = load_subnet_profile(&path, "local").expect("complete file loads");
        assert_eq!(profile.chain_id, 487);
        assert_eq!(profile.hardforks.len(), RaylsHardFork::VARIANTS.len());
    }

    #[test]
    fn config_file_missing_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let err = load_subnet_profile(&dir.path().join("absent.yaml"), "local").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("failed to read"), "{msg}");
        assert!(msg.contains("absent.yaml"), "{msg}");
    }

    #[test]
    fn config_file_unparseable_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_config(dir.path(), "broken.yaml", "not: [yaml");
        let err = load_subnet_profile(&path, "local").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("failed to parse"), "{msg}");
        assert!(msg.contains("broken.yaml"), "{msg}");
    }

    #[test]
    fn config_file_unknown_subnet_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = NetworkConfigFile {
            networks: BTreeMap::from([
                ("local".to_string(), complete_local_profile()),
                ("mainnet".to_string(), complete_local_profile()),
            ]),
        };
        let path = write_config(dir.path(), "client.yaml", &serde_yaml::to_string(&file).unwrap());
        let err = load_subnet_profile(&path, "stagenet").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("stagenet"), "{msg}");
        // The refusal lists the file's subnets so the operator can pick one.
        assert!(msg.contains("local"), "{msg}");
        assert!(msg.contains("mainnet"), "{msg}");
    }

    #[test]
    fn config_file_empty_hardforks_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path =
            write_config(dir.path(), "empty.yaml", "networks:\n  local:\n    chain_id: 487\n");
        let err = load_subnet_profile(&path, "local").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("defines no `hardforks`"), "{msg}");
        assert!(msg.contains("empty.yaml"), "{msg}");
    }

    #[test]
    fn config_file_unknown_fork_error_names_subnet_and_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut profile = complete_local_profile();
        profile.hardforks.insert("MyFork".to_string(), ForkActivation::Block(1));
        let path = write_profile(dir.path(), "client.yaml", &profile);
        let err = load_subnet_profile(&path, "local").unwrap_err();
        // `{:#}` renders the whole error chain (eyre's plain Display shows only
        // the outermost context).
        let msg = format!("{err:#}");
        assert!(msg.contains("unknown hardfork 'MyFork'"), "{msg}");
        assert!(msg.contains("subnet 'local'"), "{msg}");
        assert!(msg.contains("client.yaml"), "{msg}");
    }

    #[test]
    fn config_file_missing_fork_error_names_subnet_and_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut profile = complete_local_profile();
        profile.hardforks.remove("HybridRewards");
        let path = write_profile(dir.path(), "client.yaml", &profile);
        let err = load_subnet_profile(&path, "local").unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("does not define"), "{msg}");
        assert!(msg.contains("HybridRewards"), "{msg}");
        assert!(msg.contains("subnet 'local'"), "{msg}");
        assert!(msg.contains("client.yaml"), "{msg}");
    }

    #[test]
    fn config_file_invalid_activation_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_config(
            dir.path(),
            "bad.yaml",
            "networks:\n  local:\n    chain_id: 487\n    hardforks:\n      Eip1559: someday\n",
        );
        let err = load_subnet_profile(&path, "local").unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("invalid fork activation"), "{msg}");
        assert!(msg.contains("bad.yaml"), "{msg}");
    }
}

#[cfg(test)]
mod schedule_record_tests {
    use super::{verify_schedule_record, FileSchedule};
    use clap::Parser;
    use rayls_execution_evm::{
        network_profile::{ForkActivation, NetworkProfile},
        reth_env::{RethCommand, RethConfig, RethEnv},
        ForkCondition, RaylsHardFork, RethChainSpec, ScheduleRecord,
    };
    use rayls_infrastructure_types::RaylsNetwork;
    use reth_db::{tables::StageCheckpoints, transaction::DbTxMut, Database};
    use reth_stages::{StageCheckpoint, StageId};
    use std::{collections::BTreeMap, path::Path, sync::Arc};

    fn node_config(datadir: &Path) -> RethConfig {
        let reth = RethCommand::parse_from(["rayls-test"]);
        RethConfig::new(reth, None, datadir, false, Arc::new(RethChainSpec::default()))
    }

    /// Fake the chain head the way a real node records it: commit the `Finish`
    /// stage checkpoint at `block` (reth tracks the executed head there, not in
    /// `CanonicalHeaders`).
    fn set_head(node_config: &RethConfig, datadir: &Path, block: u64) {
        let db = RethEnv::new_database(node_config, datadir.join("db")).expect("db opens");
        db.update(|tx| {
            tx.put::<StageCheckpoints>(
                StageId::Finish.as_str().to_string(),
                StageCheckpoint::new(block),
            )
            .expect("head checkpoint written");
        })
        .expect("db update");
    }

    /// The full local schedule with one fork's activation replaced.
    fn local_profile_moving(fork: &str, to: u64) -> NetworkProfile {
        let mut hardforks = BTreeMap::new();
        for (fork, condition) in RaylsHardFork::for_network(RaylsNetwork::Local) {
            if let ForkCondition::Block(block) = condition {
                hardforks.insert(fork.name().to_string(), ForkActivation::Block(block));
            }
        }
        hardforks.insert(fork.to_string(), ForkActivation::Block(to));
        NetworkProfile { chain_id: 487, hardforks }
    }

    fn boot(dir: &Path, config: &RethConfig, network: RaylsNetwork) -> eyre::Result<()> {
        let dir = dir.to_path_buf();
        verify_schedule_record(&dir, config, &NetworkProfile::from_builtin(network))
    }

    #[test]
    fn fresh_datadir_records_selected_schedule() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = node_config(dir.path());
        boot(dir.path(), &config, RaylsNetwork::Local).expect("first boot passes");
        let raw = std::fs::read_to_string(dir.path().join("schedule-record.yaml"))
            .expect("record written");
        let record: ScheduleRecord = serde_yaml::from_str(&raw).expect("record parses");
        assert_eq!(record.chain_id, 487);
        assert_eq!(record.as_of_block, 0);
        assert_eq!(record.hardforks.get("Eip1559"), Some(&ForkActivation::Block(0)));
        assert_eq!(record.hardforks.get("UsdrSupplyCorrection"), Some(&ForkActivation::Block(100)));
        // `never` forks are recorded explicitly: the record is a complete
        // snapshot of the selected schedule.
        assert_eq!(record.hardforks.get("Uups"), Some(&ForkActivation::Never));
    }

    #[test]
    fn second_boot_records_the_chain_head() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = node_config(dir.path());
        boot(dir.path(), &config, RaylsNetwork::Local).expect("first boot passes");
        set_head(&config, dir.path(), 10);
        boot(dir.path(), &config, RaylsNetwork::Local).expect("second boot passes");
        let raw = std::fs::read_to_string(dir.path().join("schedule-record.yaml"))
            .expect("record re-written");
        let record: ScheduleRecord = serde_yaml::from_str(&raw).expect("record parses");
        assert_eq!(record.as_of_block, 10);
    }

    #[test]
    fn executed_fork_move_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = node_config(dir.path());
        boot(dir.path(), &config, RaylsNetwork::Local).expect("first boot passes");
        set_head(&config, dir.path(), 10);
        let profile = local_profile_moving("Eip1559", 20);
        let file_schedule = FileSchedule {
            path: std::path::PathBuf::from("/x/y.yaml"),
            subnet: "local".to_string(),
            profile,
        };
        let err = {
            let dir = dir.path().to_path_buf();
            verify_schedule_record(&dir, &config, &file_schedule.profile)
        }
        .expect_err("moving an executed fork is refused");
        let msg = err.to_string();
        assert!(msg.contains("Eip1559"), "{msg}");
        assert!(msg.contains("0"), "{msg}");
        assert!(msg.contains("20"), "{msg}");
        // The refusal names the record and its remedy.
        assert!(msg.contains("schedule-record.yaml"), "{msg}");
        assert!(msg.contains("delete"), "{msg}");
    }

    #[test]
    fn recorded_never_fork_activated_in_executed_history_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = node_config(dir.path());
        // A complete record (the new format) pinning Uups as `never`, the rest
        // at their built-in local boundaries.
        let mut hardforks = BTreeMap::new();
        for (fork, condition) in RaylsHardFork::for_network(RaylsNetwork::Local) {
            hardforks.insert(
                fork.name().to_string(),
                match condition {
                    ForkCondition::Block(block) => ForkActivation::Block(block),
                    ForkCondition::Never => ForkActivation::Never,
                    _ => ForkActivation::Never,
                },
            );
        }
        std::fs::write(
            dir.path().join("schedule-record.yaml"),
            serde_yaml::to_string(&ScheduleRecord { chain_id: 487, as_of_block: 0, hardforks })
                .expect("record serializes"),
        )
        .expect("record written");
        set_head(&config, dir.path(), 10);
        let profile = local_profile_moving("Uups", 5);
        let file_schedule = FileSchedule {
            path: std::path::PathBuf::from("/x/y.yaml"),
            subnet: "local".to_string(),
            profile,
        };
        let err = {
            let dir = dir.path().to_path_buf();
            verify_schedule_record(&dir, &config, &file_schedule.profile)
        }
        .expect_err("activating a recorded-never fork in the executed history is refused");
        let msg = err.to_string();
        assert!(msg.contains("Uups"), "{msg}");
        assert!(msg.contains("recorded as never"), "{msg}");
        assert!(msg.contains("schedule-record.yaml"), "{msg}");
        assert!(msg.contains("delete"), "{msg}");
    }

    #[test]
    fn record_predating_a_block_zero_fork_is_refused_with_remedy() {
        // The comment-1 case: an old record (no entry for a fork the new
        // binary activates at block 0) is refused, and the message says the
        // datadir predates the fork and how to fix it.
        let dir = tempfile::tempdir().expect("tempdir");
        let config = node_config(dir.path());
        let mut hardforks = BTreeMap::new();
        for (fork, condition) in RaylsHardFork::for_network(RaylsNetwork::Local) {
            if fork == RaylsHardFork::HybridRewards {
                continue; // the record predates this fork: no entry at all
            }
            hardforks.insert(
                fork.name().to_string(),
                match condition {
                    ForkCondition::Block(block) => ForkActivation::Block(block),
                    _ => ForkActivation::Never,
                },
            );
        }
        std::fs::write(
            dir.path().join("schedule-record.yaml"),
            serde_yaml::to_string(&ScheduleRecord { chain_id: 487, as_of_block: 0, hardforks })
                .expect("record serializes"),
        )
        .expect("record written");
        set_head(&config, dir.path(), 10);
        let err = boot(dir.path(), &config, RaylsNetwork::Local)
            .expect_err("a record predating a block-0 fork in the executed history is refused");
        let msg = err.to_string();
        assert!(msg.contains("HybridRewards"), "{msg}");
        assert!(msg.contains("predates"), "{msg}");
        assert!(msg.contains("schedule-record.yaml"), "{msg}");
        assert!(msg.contains("delete"), "{msg}");
    }

    #[test]
    fn future_fork_move_is_allowed_and_re_recorded() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = node_config(dir.path());
        boot(dir.path(), &config, RaylsNetwork::Local).expect("first boot passes");
        set_head(&config, dir.path(), 10);
        let profile = local_profile_moving("UsdrSupplyCorrection", 200);
        let file_schedule = FileSchedule {
            path: std::path::PathBuf::from("/x/y.yaml"),
            subnet: "local".to_string(),
            profile,
        };
        {
            let dir = dir.path().to_path_buf();
            verify_schedule_record(&dir, &config, &file_schedule.profile)
        }
        .expect("moving a future fork is allowed");
        let raw = std::fs::read_to_string(dir.path().join("schedule-record.yaml"))
            .expect("record re-written");
        let record: ScheduleRecord = serde_yaml::from_str(&raw).expect("record parses");
        assert_eq!(record.hardforks.get("UsdrSupplyCorrection"), Some(&ForkActivation::Block(200)));
    }

    #[test]
    fn deleted_record_is_rewritten() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = node_config(dir.path());
        boot(dir.path(), &config, RaylsNetwork::Local).expect("first boot passes");
        set_head(&config, dir.path(), 10);
        std::fs::remove_file(dir.path().join("schedule-record.yaml")).expect("record removed");
        boot(dir.path(), &config, RaylsNetwork::Local).expect("boot passes");
        let record: ScheduleRecord = serde_yaml::from_str(
            &std::fs::read_to_string(dir.path().join("schedule-record.yaml")).expect("record"),
        )
        .expect("record parses");
        assert_eq!(record.as_of_block, 10);
    }

    #[test]
    fn chain_id_mismatch_in_record_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = node_config(dir.path());
        boot(dir.path(), &config, RaylsNetwork::Local).expect("first boot passes");
        let mut profile = local_profile_moving("Eip1559", 0);
        profile.chain_id = 99999;
        let file_schedule = FileSchedule {
            path: std::path::PathBuf::from("/x/y.yaml"),
            subnet: "local".to_string(),
            profile,
        };
        let err = {
            let dir = dir.path().to_path_buf();
            verify_schedule_record(&dir, &config, &file_schedule.profile)
        }
        .expect_err("chain-id mismatch is refused");
        let msg = err.to_string();
        assert!(msg.contains("chain-id"), "{msg}");
    }

    #[test]
    fn unparseable_record_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = node_config(dir.path());
        std::fs::write(dir.path().join("schedule-record.yaml"), "not: [yaml").expect("garbage");
        let err = boot(dir.path(), &config, RaylsNetwork::Local)
            .expect_err("unparseable record is refused");
        let msg = err.to_string();
        assert!(msg.contains("schedule-record.yaml"), "{msg}");
    }
}
