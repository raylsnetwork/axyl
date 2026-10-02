//! `schedule export`: dump a built-in hardfork schedule as a network config file.
//!
//! The output is a complete, loadable `--config-file` (one subnet, every known
//! fork defined) rendered from the schedule baked into this very binary. It is
//! the starting point for a client-defined subnet: copy, rename the subnet,
//! set its `chain_id`, adjust the activation blocks.
use clap::{Args, Subcommand};
use rayls_execution_evm::{
    baked_in_network, network_profile::ForkName, ForkActivation, NetworkProfile, RaylsHardFork,
};
use rayls_infrastructure_types::RaylsNetwork;

/// Hardfork schedule tooling.
#[derive(Debug, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct ScheduleArgs {
    /// The schedule operation to run.
    #[command(subcommand)]
    pub command: ScheduleCommand,
}

/// Operations on hardfork schedules.
#[derive(Debug, Subcommand)]
pub enum ScheduleCommand {
    /// Export a built-in hardfork schedule as a network config file on stdout.
    ///
    /// Prints one subnet holding the chain-id and the full schedule baked into
    /// this binary for `--network`, in the exact shape `node --config-file`
    /// loads. Redirect it to a file and use it as the template for a
    /// client-defined subnet.
    Export(ExportArgs),
}

/// Arguments of `schedule export`.
#[derive(Debug, Args)]
pub struct ExportArgs {
    /// The built-in network whose schedule to export (devnet, testnet, mainnet, local).
    #[arg(long, value_name = "RAYLS_NETWORK")]
    pub network: RaylsNetwork,

    /// Name of the exported subnet entry. Defaults to the network name.
    /// Letters, digits, `-` and `_` only, so it is a plain YAML key and a
    /// plain `--subnet` argument.
    #[arg(long, value_name = "NAME", value_parser = parse_subnet_name)]
    pub subnet_name: Option<String>,
}

/// Accept only names that are unambiguous both as a bare YAML map key and as
/// the `--subnet` value that selects the entry back: ASCII letters, digits,
/// `-` and `_`. A colon, quote, `#` or whitespace would need YAML quoting.
fn parse_subnet_name(name: &str) -> Result<String, String> {
    if name.is_empty() {
        return Err("subnet name must not be empty".to_string());
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        return Err(format!(
            "subnet name {name:?} may only contain ASCII letters, digits, '-' and '_'"
        ));
    }
    Ok(name.to_string())
}

impl ScheduleArgs {
    /// Execute the selected schedule operation.
    pub fn execute(self) -> eyre::Result<()> {
        match self.command {
            ScheduleCommand::Export(args) => args.execute(),
        }
    }
}

impl ExportArgs {
    /// Render the built-in schedule to stdout.
    pub fn execute(self) -> eyre::Result<()> {
        let subnet = self.subnet_name.unwrap_or_else(|| self.network.to_string());
        print!("{}", render_config_file(self.network, &subnet));
        Ok(())
    }
}

/// Render the built-in schedule of `network` as a one-subnet network config
/// file.
///
/// Rendered by hand rather than through serde so the forks appear in
/// activation-table order under their canonical names (`Eip1559`, not the
/// lower-cased map key the loader normalizes to). The result parses back into
/// exactly [`NetworkProfile::from_builtin`] and passes the completeness check
/// that `--config-file` enforces at boot.
pub(crate) fn render_config_file(network: RaylsNetwork, subnet: &str) -> String {
    let profile = NetworkProfile::from_builtin(network);
    let mut out = String::new();
    out.push_str(&format!(
        "# Hardfork schedule of the built-in `{network}` network (chain-id {}), exported by\n\
         # rayls-network {}.\n",
        profile.chain_id,
        env!("CARGO_PKG_VERSION"),
    ));
    if baked_in_network(profile.chain_id).is_some() {
        // Mainnet and testnet always run their baked-in schedule: `node --config-file`
        // refuses a subnet declaring their chain-id, so this file is a template only.
        out.push_str(&format!(
            "#\n\
             # TEMPLATE ONLY: `{network}` always runs its baked-in schedule and is started\n\
             # with `--network {network}`; `node --config-file` refuses a subnet that declares\n\
             # chain-id {}. To define a client network from this schedule, rename the\n\
             # subnet, set `chain_id` to your genesis chain-id and adjust the activation\n\
             # blocks (a block number or `never`). Every known fork must stay defined: a\n\
             # missing fork refuses the boot.\n",
            profile.chain_id
        ));
    } else {
        out.push_str(&format!(
            "# Start a node from it with:\n\
             #\n\
             #   rayls-network node --config-file <this file> --subnet {subnet}\n\
             #\n\
             # To define a client network, rename the subnet, set its `chain_id` to the\n\
             # genesis chain-id and adjust the activation blocks (a block number or\n\
             # `never`). Every known fork must stay defined: a missing fork refuses the boot.\n\
             # A built-in network itself needs no file: start it with `--network {network}`.\n"
        ));
    }
    out.push_str("networks:\n");
    out.push_str(&format!("  {subnet}:\n"));
    out.push_str(&format!("    chain_id: {}\n", profile.chain_id));
    out.push_str("    hardforks:\n");
    for fork in RaylsHardFork::VARIANTS {
        let activation = profile
            .hardforks
            .get(&ForkName::from(fork.name()))
            .copied()
            .expect("from_builtin defines every known fork");
        let value = match activation {
            ForkActivation::Block(block) => block.to_string(),
            ForkActivation::Never => "never".to_string(),
        };
        out.push_str(&format!("      {}: {value}\n", fork.name()));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayls_execution_evm::{FileSchedule, ForkCondition};

    const ALL_NETWORKS: [RaylsNetwork; 4] =
        [RaylsNetwork::Devnet, RaylsNetwork::Testnet, RaylsNetwork::Mainnet, RaylsNetwork::Local];

    /// Networks a config file may run: the exported file loads through the exact
    /// path `node --config-file` uses and resolves to the built-in profile.
    #[test]
    fn export_round_trips_through_the_config_file_loader() {
        let dir = tempfile::tempdir().expect("tempdir");
        for network in [RaylsNetwork::Devnet, RaylsNetwork::Local] {
            let subnet = network.to_string();
            let path = dir.path().join(format!("{subnet}.yaml"));
            std::fs::write(&path, render_config_file(network, &subnet)).expect("written");

            let loaded = FileSchedule::load(&path, &subnet)
                .unwrap_or_else(|e| panic!("{network}: exported file must load: {e:#}"));
            let builtin = NetworkProfile::from_builtin(network);
            assert_eq!(loaded.profile().chain_id, builtin.chain_id, "{network}");
            assert_eq!(loaded.profile().hardforks, builtin.hardforks, "{network}");
            assert!(!loaded.profile().hardforks.is_empty(), "{network}");
        }
    }

    /// Mainnet and testnet always run their baked-in schedule: their export is a
    /// template that the loader refuses as-is (protected chain-id), and the file
    /// says so in its header.
    #[test]
    fn export_of_guarded_networks_is_a_template_the_loader_refuses() {
        let dir = tempfile::tempdir().expect("tempdir");
        for network in [RaylsNetwork::Mainnet, RaylsNetwork::Testnet] {
            let subnet = network.to_string();
            let yaml = render_config_file(network, &subnet);
            assert!(yaml.contains("TEMPLATE ONLY"), "{network}:\n{yaml}");
            assert!(yaml.contains(&format!("--network {network}")), "{network}:\n{yaml}");
            assert!(!yaml.contains("Start a node from it"), "{network}:\n{yaml}");

            let path = dir.path().join(format!("{subnet}.yaml"));
            std::fs::write(&path, &yaml).expect("written");
            let err = FileSchedule::load(&path, &subnet)
                .expect_err("a protected chain-id must be refused by the loader");
            let msg = format!("{err:#}");
            assert!(msg.contains(&network.chain_id().to_string()), "{network}: {msg}");
            assert!(msg.contains(&format!("--network {network}")), "{network}: {msg}");
        }

        // Only the chain-id is protected: the same schedule under another chain-id loads.
        let mut yaml = render_config_file(RaylsNetwork::Mainnet, "client-main");
        yaml = yaml.replace(
            &format!("chain_id: {}", RaylsNetwork::Mainnet.chain_id()),
            "chain_id: 424242",
        );
        let path = dir.path().join("client-main.yaml");
        std::fs::write(&path, yaml).expect("written");
        let loaded = FileSchedule::load(&path, "client-main").expect("re-keyed template loads");
        assert_eq!(loaded.profile().chain_id, 424242);
        assert_eq!(
            loaded.profile().hardforks,
            NetworkProfile::from_builtin(RaylsNetwork::Mainnet).hardforks
        );
    }

    /// Fork values match the baked-in table exactly, including `never` entries.
    #[test]
    fn export_matches_the_baked_in_table() {
        for network in ALL_NETWORKS {
            let yaml = render_config_file(network, "x");
            for entry in RaylsHardFork::for_network(network) {
                let expected = match entry.condition {
                    ForkCondition::Block(block) => {
                        format!("      {}: {block}\n", entry.fork.name())
                    }
                    ForkCondition::Never => format!("      {}: never\n", entry.fork.name()),
                    other => panic!("built-in schedules are block-based, got {other:?}"),
                };
                assert!(yaml.contains(&expected), "{network}: missing {expected:?} in\n{yaml}");
            }
        }
    }

    /// Forks are listed under their canonical names, in activation-table order.
    #[test]
    fn export_uses_canonical_names_in_table_order() {
        let yaml = render_config_file(RaylsNetwork::Mainnet, "mainnet");
        let mut last = 0;
        for fork in RaylsHardFork::VARIANTS {
            let needle = format!("      {}: ", fork.name());
            let pos = yaml.find(&needle).unwrap_or_else(|| panic!("{needle:?} not found"));
            assert!(pos > last, "{} is out of order", fork.name());
            last = pos;
        }
        assert!(!yaml.contains("eip1559:"), "keys must not be lower-cased:\n{yaml}");
    }

    #[test]
    fn subnet_name_must_be_a_plain_yaml_key() {
        for ok in ["stagenet", "client-devnet", "net_2", "A1"] {
            assert_eq!(parse_subnet_name(ok).as_deref(), Ok(ok), "{ok:?}");
        }
        for bad in ["", "my:net", "a b", "x\ny", "\"q\"", "#c", "ünï"] {
            let err = parse_subnet_name(bad).expect_err(&format!("{bad:?} must be refused"));
            assert!(err.contains("subnet name"), "{bad:?}: {err}");
        }
    }

    #[test]
    fn export_names_the_subnet_and_chain_id() {
        let yaml = render_config_file(RaylsNetwork::Devnet, "stagenet");
        assert!(yaml.contains("  stagenet:\n"), "{yaml}");
        assert!(yaml.contains("    chain_id: 503\n"), "{yaml}");
        assert!(yaml.contains("--subnet stagenet"), "{yaml}");
    }
}
