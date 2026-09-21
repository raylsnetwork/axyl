use std::collections::BTreeMap;

use rayls_infrastructure_types::RaylsNetwork;
use reth_chainspec::ForkCondition;
use serde::{Deserialize, Serialize};

use crate::chainspec::{RaylsHardFork, ScheduledFork};

use super::{activation::ForkActivation, fork_name::ForkName};

/// The hardfork configuration of a single subnet of a client.
///
/// This is what one `networks.<name>` entry of a config file holds. The rest
/// of the network's configuration (genesis, parameters, committee) lives in
/// the node's datadir as before; only the chain-id and the hardfork schedule
/// are externalized, because they used to be baked into the binary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkProfile {
    /// The chain-id the datadir's genesis must carry to run this subnet; a
    /// mismatch refuses the boot.
    pub chain_id: u64,
    /// The hardfork schedule: fork name -> activation block or `never`. A file
    /// loaded at boot must define every known fork
    /// ([`NetworkProfile::validate_hardforks`] refuses the boot otherwise); a
    /// fork absent from the map resolves to `Never` at lookup time.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub hardforks: BTreeMap<ForkName, ForkActivation>,
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
        // Every configured key must name a known fork.
        for name in self.hardforks.keys() {
            if RaylsHardFork::from_name(name.as_str()).is_none() {
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

        // Every known fork must have an entry (reported in fork order).
        let missing: Vec<&str> = RaylsHardFork::VARIANTS
            .iter()
            .filter(|fork| !self.hardforks.contains_key(&ForkName::from(fork.name())))
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
    pub fn schedule(&self) -> Vec<ScheduledFork> {
        RaylsHardFork::VARIANTS
            .iter()
            .filter_map(|fork| {
                self.hardforks
                    .get(&ForkName::from(fork.name()))
                    .map(|activation| ScheduledFork::new(*fork, ForkCondition::from(*activation)))
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
            .map(|entry| match entry.condition {
                ForkCondition::Block(block) => {
                    (ForkName::from(entry.fork.name()), ForkActivation::Block(block))
                }
                ForkCondition::Never => (ForkName::from(entry.fork.name()), ForkActivation::Never),
                other => unreachable!("built-in schedules are block-based, got {other:?}"),
            })
            .collect();
        Self { chain_id: network.chain_id(), hardforks }
    }
}
