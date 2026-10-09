use std::{collections::BTreeMap, path::Path};

use eyre::Context;
use serde::{Deserialize, Serialize};

use super::{activation::ForkActivation, fork_name::ForkName, profile::NetworkProfile};

/// The hardfork schedule a datadir records as the one its executed blocks ran
/// under. The CLI's schedule gate writes it at boot and refuses a selected
/// schedule that disagrees with the record on an already-executed fork;
/// differing future boundaries are allowed and reported by
/// [`verify_schedule`](super::verify::verify_schedule).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleRecord {
    /// The chain-id the recorded schedule applies to.
    pub chain_id: u64,
    /// The chain head when this record was last written.
    pub as_of_block: u64,
    /// The recorded fork schedule. `never` forks are stored explicitly, so a
    /// fork absent from the map means the record predates that fork (it reads
    /// as `never` at verification time).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub hardforks: BTreeMap<ForkName, ForkActivation>,
}

impl ScheduleRecord {
    /// Build a record from the selected profile; its hardfork map is already
    /// the complete snapshot, never-activating forks included.
    pub fn from_profile(profile: &NetworkProfile, as_of_block: u64) -> Self {
        Self { chain_id: profile.chain_id, as_of_block, hardforks: profile.hardforks.clone() }
    }

    /// Read and parse the schedule record at `path`.
    ///
    /// Reads directly (rather than `exists()`-then-read): a record deleted
    /// concurrently is treated as "no record" — the caller's "trust the
    /// selected schedule" path — not a hard error. A present record that
    /// fails to parse is a hard error naming the path.
    pub fn load(path: &Path) -> eyre::Result<Option<Self>> {
        let raw = match std::fs::read_to_string(path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(e).wrap_err_with(|| format!("read schedule record {}", path.display()))
            }
        };
        Ok(Some(
            serde_yaml::from_str(&raw)
                .wrap_err_with(|| format!("parse schedule record {}", path.display()))?,
        ))
    }

    /// The recorded activation of a fork: its block number, or `None` when the
    /// fork is absent from the record or recorded as `never`.
    pub fn activation(&self, name: &str) -> Option<u64> {
        self.entry(name).and_then(ForkActivation::block)
    }

    /// The recorded entry of a fork: `Some(Block(n))`, `Some(Never)`, or
    /// `None` when the fork is absent from the record (the record predates
    /// that fork).
    pub fn entry(&self, name: &str) -> Option<&ForkActivation> {
        self.hardforks.get(&ForkName::from(name))
    }
}
