use super::*;

/// Chain-ids for the tests, taken from `RaylsNetwork::chain_id` (the same
/// source the boot gate compares against) so a changed id updates the tests
/// instead of leaving a stale literal.
const MAINNET_CHAIN_ID: u64 = RaylsNetwork::Mainnet.chain_id();
const TESTNET_CHAIN_ID: u64 = RaylsNetwork::Testnet.chain_id();
const LOCAL_CHAIN_ID: u64 = RaylsNetwork::Local.chain_id();

mod chain_id_tests {
    use super::{
        verify_datadir_chain_id, FileSchedule, SelectedSchedule, LOCAL_CHAIN_ID, MAINNET_CHAIN_ID,
        TESTNET_CHAIN_ID,
    };
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
        assert!(verify_datadir_chain_id(TESTNET_CHAIN_ID, TESTNET_CHAIN_ID, "network 'testnet'")
            .is_ok());
        assert!(verify_datadir_chain_id(
            MAINNET_CHAIN_ID,
            MAINNET_CHAIN_ID,
            "subnet 'mainnet' of \"/x/y.yaml\"",
        )
        .is_ok());
    }

    #[test]
    fn mismatched_chain_id_is_refused() {
        let err = verify_datadir_chain_id(LOCAL_CHAIN_ID, MAINNET_CHAIN_ID, "network 'mainnet'")
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains(&LOCAL_CHAIN_ID.to_string()), "{msg}");
        assert!(msg.contains(&MAINNET_CHAIN_ID.to_string()), "{msg}");
        assert!(msg.contains("network 'mainnet'"), "{msg}");
    }

    #[test]
    fn mismatched_chain_id_names_the_config_file() {
        // A file-schedule source names the subnet and the file, so the refusal
        // tells the operator which config file carries the wrong chain-id.
        let err = verify_datadir_chain_id(
            LOCAL_CHAIN_ID,
            MAINNET_CHAIN_ID,
            "subnet 'mainnet' of \"/x/client.yaml\"",
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains(&LOCAL_CHAIN_ID.to_string()), "{msg}");
        assert!(msg.contains(&MAINNET_CHAIN_ID.to_string()), "{msg}");
        assert!(msg.contains("subnet 'mainnet'"), "{msg}");
        assert!(msg.contains("client.yaml"), "{msg}");
    }

    #[test]
    fn file_schedule_wins_over_network() {
        let file = file_schedule(MAINNET_CHAIN_ID);
        let selected = SelectedSchedule::select(Some(&file), Some(RaylsNetwork::Testnet)).unwrap();
        assert_eq!(selected.profile.chain_id, MAINNET_CHAIN_ID);
        assert_eq!(selected.source, "subnet 'mainnet' of \"/x/y.yaml\"");
    }

    #[test]
    fn network_flag_resolves_to_the_builtin_profile() {
        let selected = SelectedSchedule::select(None, Some(RaylsNetwork::Local)).unwrap();
        assert_eq!(selected.profile.chain_id, LOCAL_CHAIN_ID);
        assert_eq!(selected.source, "network 'local'");
        // The built-in profile is a complete schedule (passes the completeness gate).
        selected.profile.validate_hardforks().unwrap();
    }

    #[test]
    fn no_schedule_source_is_refused() {
        let err = SelectedSchedule::select(None, None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("no hardfork schedule source"), "{msg}");
        assert!(msg.contains("--network"), "{msg}");
        assert!(msg.contains("--config-file"), "{msg}");
    }
}

mod config_file_tests {
    use super::{FileSchedule, LOCAL_CHAIN_ID, MAINNET_CHAIN_ID, TESTNET_CHAIN_ID};
    use rayls_execution_evm::{
        network_profile::{ForkActivation, ForkName, NetworkConfigFile, NetworkProfile},
        RaylsHardFork,
    };
    use rayls_infrastructure_types::RaylsNetwork;
    use std::collections::BTreeMap;

    /// A valid profile: every known fork pinned, so only the case under test
    /// can make the load fail.
    fn complete_local_profile() -> NetworkProfile {
        let hardforks = RaylsHardFork::VARIANTS
            .iter()
            .map(|fork| (ForkName::from(fork.name()), ForkActivation::Never))
            .collect();
        NetworkProfile { chain_id: LOCAL_CHAIN_ID, hardforks }
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
        let file = FileSchedule::load(&path, "local").expect("complete file loads");
        let profile = file.profile;
        assert_eq!(profile.chain_id, LOCAL_CHAIN_ID);
        assert_eq!(profile.hardforks.len(), RaylsHardFork::VARIANTS.len());
    }

    #[test]
    fn config_file_mainnet_chain_id_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut profile = complete_local_profile();
        profile.chain_id = RaylsNetwork::Mainnet.chain_id();
        let path = write_profile(dir.path(), "client.yaml", &profile);
        let err = FileSchedule::load(&path, "local").unwrap_err();
        let msg = err.to_string();
        // The refusal names the baked-in network and its remedy.
        assert!(msg.contains(&MAINNET_CHAIN_ID.to_string()), "{msg}");
        assert!(msg.contains("mainnet"), "{msg}");
        assert!(msg.contains("--network mainnet"), "{msg}");
        assert!(msg.contains("subnet 'local'"), "{msg}");
        assert!(msg.contains("client.yaml"), "{msg}");
    }

    #[test]
    fn config_file_testnet_chain_id_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut profile = complete_local_profile();
        profile.chain_id = RaylsNetwork::Testnet.chain_id();
        let path = write_profile(dir.path(), "client.yaml", &profile);
        let err = FileSchedule::load(&path, "local").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains(&TESTNET_CHAIN_ID.to_string()), "{msg}");
        assert!(msg.contains("testnet"), "{msg}");
        assert!(msg.contains("--network testnet"), "{msg}");
        assert!(msg.contains("subnet 'local'"), "{msg}");
        assert!(msg.contains("client.yaml"), "{msg}");
    }

    #[test]
    fn config_file_devnet_chain_id_still_loads() {
        // Only the baked-in mainnet/testnet chain-ids are protected; a devnet
        // (or local) subnet keeps working from a config file.
        let dir = tempfile::tempdir().expect("tempdir");
        let mut profile = complete_local_profile();
        profile.chain_id = RaylsNetwork::Devnet.chain_id();
        let path = write_profile(dir.path(), "client.yaml", &profile);
        let file = FileSchedule::load(&path, "local").expect("devnet chain-id loads");
        assert_eq!(file.profile.chain_id, RaylsNetwork::Devnet.chain_id());
    }

    #[test]
    fn config_file_missing_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let err = FileSchedule::load(&dir.path().join("absent.yaml"), "local").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("failed to read"), "{msg}");
        assert!(msg.contains("absent.yaml"), "{msg}");
    }

    #[test]
    fn config_file_unparseable_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_config(dir.path(), "broken.yaml", "not: [yaml");
        let err = FileSchedule::load(&path, "local").unwrap_err();
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
        let err = FileSchedule::load(&path, "stagenet").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("stagenet"), "{msg}");
        // The refusal lists the file's subnets so the operator can pick one.
        assert!(msg.contains("local"), "{msg}");
        assert!(msg.contains("mainnet"), "{msg}");
    }

    #[test]
    fn config_file_empty_hardforks_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_config(
            dir.path(),
            "empty.yaml",
            &format!("networks:\n  local:\n    chain_id: {LOCAL_CHAIN_ID}\n"),
        );
        let err = FileSchedule::load(&path, "local").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("defines no `hardforks`"), "{msg}");
        assert!(msg.contains("empty.yaml"), "{msg}");
    }

    #[test]
    fn config_file_unknown_fork_error_names_subnet_and_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut profile = complete_local_profile();
        profile.hardforks.insert(ForkName::from("MyFork"), ForkActivation::Block(1));
        let path = write_profile(dir.path(), "client.yaml", &profile);
        let err = FileSchedule::load(&path, "local").unwrap_err();
        // `{:#}` renders the whole error chain (eyre's plain Display shows only
        // the outermost context).
        let msg = format!("{err:#}");
        assert!(msg.contains("unknown hardfork 'myfork'"), "{msg}");
        assert!(msg.contains("subnet 'local'"), "{msg}");
        assert!(msg.contains("client.yaml"), "{msg}");
    }

    #[test]
    fn config_file_missing_fork_error_names_subnet_and_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut profile = complete_local_profile();
        profile.hardforks.remove(&ForkName::from("HybridRewards"));
        let path = write_profile(dir.path(), "client.yaml", &profile);
        let err = FileSchedule::load(&path, "local").unwrap_err();
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
            &format!("networks:\n  local:\n    chain_id: {LOCAL_CHAIN_ID}\n    hardforks:\n      Eip1559: someday\n"),
        );
        let err = FileSchedule::load(&path, "local").unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("invalid fork activation"), "{msg}");
        assert!(msg.contains("bad.yaml"), "{msg}");
    }
}

mod schedule_record_tests {
    use super::{verify_schedule_record, LOCAL_CHAIN_ID};
    use clap::Parser;
    use rayls_execution_evm::{
        network_profile::{ForkActivation, ForkName, NetworkProfile},
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

    /// The full local schedule with one fork's activation replaced. `never`
    /// forks are kept (as `Never`), so the result is a complete profile that
    /// also passes `validate_hardforks`.
    fn local_profile_moving(fork: &str, to: u64) -> NetworkProfile {
        let mut hardforks = BTreeMap::new();
        for entry in RaylsHardFork::for_network(RaylsNetwork::Local) {
            let activation = match entry.condition {
                ForkCondition::Block(block) => ForkActivation::Block(block),
                _ => ForkActivation::Never,
            };
            hardforks.insert(ForkName::from(entry.fork.name()), activation);
        }
        hardforks.insert(ForkName::from(fork), ForkActivation::Block(to));
        NetworkProfile { chain_id: LOCAL_CHAIN_ID, hardforks }
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
        assert_eq!(record.chain_id, LOCAL_CHAIN_ID);
        assert_eq!(record.as_of_block, 0);
        assert_eq!(
            record.hardforks.get(&ForkName::from("Eip1559")),
            Some(&ForkActivation::Block(0))
        );
        assert_eq!(
            record.hardforks.get(&ForkName::from("UsdrSupplyCorrection")),
            Some(&ForkActivation::Block(100))
        );
        // `never` forks are recorded explicitly: the record is a complete
        // snapshot of the selected schedule.
        assert_eq!(record.hardforks.get(&ForkName::from("Uups")), Some(&ForkActivation::Never));
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
        let err = {
            let dir = dir.path().to_path_buf();
            verify_schedule_record(&dir, &config, &profile)
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
        for entry in RaylsHardFork::for_network(RaylsNetwork::Local) {
            hardforks.insert(
                ForkName::from(entry.fork.name()),
                match entry.condition {
                    ForkCondition::Block(block) => ForkActivation::Block(block),
                    ForkCondition::Never => ForkActivation::Never,
                    _ => ForkActivation::Never,
                },
            );
        }
        std::fs::write(
            dir.path().join("schedule-record.yaml"),
            serde_yaml::to_string(&ScheduleRecord {
                chain_id: LOCAL_CHAIN_ID,
                as_of_block: 0,
                hardforks,
            })
            .expect("record serializes"),
        )
        .expect("record written");
        set_head(&config, dir.path(), 10);
        let profile = local_profile_moving("Uups", 5);
        let err = {
            let dir = dir.path().to_path_buf();
            verify_schedule_record(&dir, &config, &profile)
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
        // The record-predates-a-fork case: an old record (no entry for a fork
        // the new binary activates at block 0) is refused, and the message
        // says the datadir predates the fork and how to fix it.
        let dir = tempfile::tempdir().expect("tempdir");
        let config = node_config(dir.path());
        let mut hardforks = BTreeMap::new();
        for entry in RaylsHardFork::for_network(RaylsNetwork::Local) {
            if entry.fork == RaylsHardFork::HybridRewards {
                continue; // the record predates this fork: no entry at all
            }
            hardforks.insert(
                ForkName::from(entry.fork.name()),
                match entry.condition {
                    ForkCondition::Block(block) => ForkActivation::Block(block),
                    _ => ForkActivation::Never,
                },
            );
        }
        std::fs::write(
            dir.path().join("schedule-record.yaml"),
            serde_yaml::to_string(&ScheduleRecord {
                chain_id: LOCAL_CHAIN_ID,
                as_of_block: 0,
                hardforks,
            })
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
        {
            let dir = dir.path().to_path_buf();
            verify_schedule_record(&dir, &config, &profile)
        }
        .expect("moving a future fork is allowed");
        let raw = std::fs::read_to_string(dir.path().join("schedule-record.yaml"))
            .expect("record re-written");
        let record: ScheduleRecord = serde_yaml::from_str(&raw).expect("record parses");
        assert_eq!(
            record.hardforks.get(&ForkName::from("UsdrSupplyCorrection")),
            Some(&ForkActivation::Block(200))
        );
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
        let err = {
            let dir = dir.path().to_path_buf();
            verify_schedule_record(&dir, &config, &profile)
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
