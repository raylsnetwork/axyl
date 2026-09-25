//! Hardfork-schedule record verification at node boot.
//!
//! The selection of *which* profile to run lives in
//! `rayls_execution_evm::network_schedule`. This module verifies the selected
//! schedule against the datadir's recorded history (refusing executed-fork
//! disagreements, reporting future moves) and re-records it, plus the CLI
//! `schedule export` subcommand.
use eyre::Context;
use rayls_execution_evm::{
    reth_env::RethConfig, verify_datadir_schedule_record, NetworkProfile, ScheduleRecord,
};
use rayls_infrastructure_config::RaylsDirs;
use tracing::{info, warn};

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
    let verification = verify_datadir_schedule_record(datadir, node_config, profile)?;
    let head = verification.head;
    // No record: a fresh datadir (head 0) is the normal first-boot case. A
    // datadir with executed history and no record (predating the feature, or
    // deleted) is trusted — and re-recorded below, so the executed-history
    // check starts from now.
    if verification.record.is_none() && head > 0 {
        warn!(
            target: "cli",
            path = tracing::field::debug(&verification.path),
            %head,
            "datadir has no schedule record; trusting the selected schedule and \
             recording it (the executed-history check starts from now)"
        );
    }

    let record = ScheduleRecord::from_profile(profile, head);
    let yaml =
        serde_yaml::to_string(&record).wrap_err("failed to serialize the schedule record")?;
    // Write to a sibling temp file and rename into place (atomic on POSIX): a
    // crash mid-write must leave the previous, still-parseable record — never
    // a torn file that bricks the next boot. A leftover temp file after a
    // crash is harmless; the next write overwrites it.
    let tmp = verification.path.with_file_name("schedule-record.yaml.tmp");
    std::fs::write(&tmp, yaml)
        .with_context(|| format!("failed to write schedule record temp file {tmp:?}"))?;
    std::fs::rename(&tmp, &verification.path).with_context(|| {
        format!("failed to move schedule record into place at {:?}", verification.path)
    })?;
    info!(
        target: "cli",
        path = tracing::field::debug(&verification.path),
        %chain_id,
        %head,
        "hardfork schedule verified against the chain's executed history"
    );
    Ok(())
}

pub mod export;

#[cfg(test)]
mod tests;
