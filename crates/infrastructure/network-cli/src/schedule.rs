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

impl FileSchedule {
    /// Load the client's network config file and select the requested subnet.
    ///
    /// Validates the profile's `hardforks` map before the node starts: every entry
    /// must be a known fork and every known fork must be defined (a stale file
    /// that omits a newly added fork would otherwise run that fork as `never`). A
    /// broken or stale file fails fast with an actionable message.
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

/// Verify the schedule selected for this boot against the datadir's
/// [`ScheduleRecord`] (refusing executed-fork disagreements, reporting future
/// moves), then re-record it. A datadir without a record (a fresh chain) gets
/// one written for the selected schedule.
pub fn verify_schedule_record<P: RaylsDirs>(
    datadir: &P,
    node_config: &RethConfig,
    profile: &NetworkProfile,
) -> eyre::Result<()> {
    let chain_id = profile.chain_id;

    // The chain's executed head: reth's `Finish` stage checkpoint.
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
        Some(record) => verify_schedule(record, profile, head, &path)?,
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

    let record = ScheduleRecord::from_profile(profile, head);
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
mod tests;
