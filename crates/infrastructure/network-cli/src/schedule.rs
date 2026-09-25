//! Hardfork-schedule record verification at node boot.
//!
//! The selection of *which* profile to run lives in
//! `rayls_execution_evm::network_schedule`. This module verifies the selected
//! schedule against the datadir's recorded history (refusing executed-fork
//! disagreements, reporting future moves) and re-records it, plus the CLI
//! `schedule export` subcommand.
use eyre::Context;
use rayls_execution_evm::{
    reth_env::{RethConfig, RethEnv},
    verify_schedule, NetworkProfile, ScheduleRecord,
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

pub mod export;

#[cfg(test)]
mod tests;
