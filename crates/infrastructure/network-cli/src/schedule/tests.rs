use super::verify_schedule_record;
use rayls_infrastructure_types::RaylsNetwork;

/// Chain-id for the tests, taken from `RaylsNetwork::chain_id` (the same
/// source the boot gate compares against) so a changed id updates the tests
/// instead of leaving a stale literal.
const LOCAL_CHAIN_ID: u64 = RaylsNetwork::Local.chain_id();

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
