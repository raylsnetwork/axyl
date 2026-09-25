//! Boot-time selection of which network profile / hardfork schedule to run.
//!
//! A datadir carries no hardfork schedule: every boot selects one explicitly
//! (a `--config-file`/`--subnet` profile, or the built-in schedule behind
//! `--network`). This module resolves that selection — loading and validating
//! a config-file subnet, applying the file-over-network precedence, and
//! gating the datadir's genesis chain-id against the selected source — before
//! the node (or a replay) installs the profile.
use std::path::{Path, PathBuf};

use eyre::Context;
use rayls_infrastructure_types::RaylsNetwork;

use crate::network_profile::{NetworkConfigFile, NetworkProfile};

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

impl SelectedSchedule {
    /// Select the hardfork schedule for this boot.
    ///
    /// Precedence: the `--config-file`/`--subnet` profile (already loaded and
    /// validated) wins, then the built-in schedule selected by `--network`.
    /// Bails when neither is given — a datadir carries no schedule, so the source
    /// must be explicit at every boot.
    pub fn select(
        file_schedule: Option<&FileSchedule>,
        network: Option<RaylsNetwork>,
    ) -> eyre::Result<Self> {
        if let Some(file_schedule) = file_schedule {
            return Ok(Self {
                profile: file_schedule.profile.clone(),
                source: format!("subnet '{}' of {:?}", file_schedule.subnet, file_schedule.path),
            });
        }
        match network {
            Some(network) => Ok(Self {
                profile: NetworkProfile::from_builtin(network),
                source: format!("network '{network}'"),
            }),
            None => eyre::bail!(
                "no hardfork schedule source: start with `--network <devnet|testnet|mainnet|local>` \
                  (the chain-id must match the genesis) or `--config-file <path> --subnet <name>`"
            ),
        }
    }
}

/// The hardfork schedule selected from a `--config-file`: the file path, the
/// subnet chosen with `--subnet`, and the subnet's resolved profile.
#[derive(Debug)]
pub struct FileSchedule {
    /// Path of the network config file.
    path: PathBuf,
    /// The subnet selected from it.
    subnet: String,
    /// The subnet's resolved profile.
    profile: NetworkProfile,
}

/// The built-in network whose chain-id is `id`, when `id` is one of the
/// networks a config file may never redefine.
///
/// Mainnet and testnet always run on the schedule baked into the binary
/// (started with `--network mainnet|testnet`); letting a client config file
/// carry their chain-id would let a per-client file redefine a shared
/// network's schedule, so a subnet declaring one of these chain-ids is
/// refused at load time. Shared with the CLI's `schedule export` renderer,
/// which marks those networks' exports as template-only.
pub fn baked_in_network(id: u64) -> Option<RaylsNetwork> {
    [RaylsNetwork::Mainnet, RaylsNetwork::Testnet]
        .into_iter()
        .find(|network| network.chain_id() == id)
}

impl FileSchedule {
    /// The subnet's resolved profile.
    pub fn profile(&self) -> &NetworkProfile {
        &self.profile
    }

    /// Load the client's network config file and select the requested subnet.
    ///
    /// Validates the profile before the node starts: the subnet's `chain_id`
    /// must not be a baked-in network's (mainnet/testnet run on `--network`,
    /// never a config file), and its `hardforks` map must name only known
    /// forks with every known fork defined (a stale file that omits a newly
    /// added fork would otherwise run that fork as `never`). A broken or
    /// stale file fails fast with an actionable message.
    pub fn load(config_file: &Path, subnet: &str) -> eyre::Result<Self> {
        let yaml = std::fs::read_to_string(config_file)
            .wrap_err_with(|| format!("failed to read network config file {config_file:?}"))?;
        let file: NetworkConfigFile = serde_yaml::from_str(&yaml)
            .wrap_err_with(|| format!("failed to parse network config file {config_file:?}"))?;
        let profile = file.subnet(subnet).cloned().ok_or_else(|| {
            let known = file.networks.keys().cloned().collect::<Vec<_>>().join(", ");
            eyre::eyre!(
                "subnet '{subnet}' not found in {config_file:?}; available subnets: {known}"
            )
        })?;
        if let Some(network) = baked_in_network(profile.chain_id) {
            eyre::bail!(
                "subnet '{subnet}' in {config_file:?} declares chain-id {}, the chain-id of the \
                  baked-in {network} network; {network} must be started with `--network {network}`, \
                  not `--config-file`",
                profile.chain_id
            );
        }
        if profile.hardforks.is_empty() {
            eyre::bail!(
                "subnet '{subnet}' in {config_file:?} defines no `hardforks`; every subnet must \
                  define its hardfork schedule (a block number or \"never\" per fork)"
            );
        }
        profile
            .validate_hardforks()
            .wrap_err_with(|| format!("subnet '{subnet}' in {config_file:?}"))?;
        Ok(Self { path: config_file.to_path_buf(), subnet: subnet.to_string(), profile })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        network_profile::{ForkActivation, ForkName, NetworkConfigFile, NetworkProfile},
        RaylsHardFork,
    };
    use rayls_infrastructure_types::RaylsNetwork;
    use std::{collections::BTreeMap, path::PathBuf};

    use super::{verify_datadir_chain_id, FileSchedule, SelectedSchedule};

    /// Chain-ids for the tests, taken from `RaylsNetwork::chain_id` (the same
    /// source the boot gate compares against) so a changed id updates the tests
    /// instead of leaving a stale literal.
    const MAINNET_CHAIN_ID: u64 = RaylsNetwork::Mainnet.chain_id();
    const TESTNET_CHAIN_ID: u64 = RaylsNetwork::Testnet.chain_id();
    const LOCAL_CHAIN_ID: u64 = RaylsNetwork::Local.chain_id();

    mod chain_id_tests {
        use super::{
            verify_datadir_chain_id, FileSchedule, SelectedSchedule, LOCAL_CHAIN_ID,
            MAINNET_CHAIN_ID, TESTNET_CHAIN_ID,
        };
        use crate::NetworkProfile;
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
            assert!(verify_datadir_chain_id(
                TESTNET_CHAIN_ID,
                TESTNET_CHAIN_ID,
                "network 'testnet'"
            )
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
            let err =
                verify_datadir_chain_id(LOCAL_CHAIN_ID, MAINNET_CHAIN_ID, "network 'mainnet'")
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
            let selected =
                SelectedSchedule::select(Some(&file), Some(RaylsNetwork::Testnet)).unwrap();
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
        use crate::{
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
            let path =
                write_config(dir.path(), "client.yaml", &serde_yaml::to_string(&file).unwrap());
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
}
