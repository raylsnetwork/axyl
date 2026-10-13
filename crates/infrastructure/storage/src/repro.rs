//! Power-loss reproduction hook. Never shipped; lives only on the reproduction branch.
//!
//! A vote, header or certificate is made durable *before* it is sent: the record is committed
//! and readable, so it survives a process crash. But the consensus DB runs MDBX in `SafeNoSync`,
//! so that commit is not fsynced until the periodic sync; a power loss in the window discards it
//! even though peers already saw it. After restart the validator no longer knows what it sent,
//! which lets it build a second header / cast a second vote for a round it already acted on and
//! produce two certificates for one round: a chain halt.
//!
//! This hook makes that power loss deterministic on a live network. When a node is armed (its
//! per-node arm file exists), the next proposer header or vote write first force-syncs the DB and
//! snapshots the datafile (the last-synced on-disk image *without* this round's record), then
//! lets the write proceed. The driver kills the node, restores the snapshot (modelling the power
//! cut that drops the committed-but-unsynced tail), and restarts it. Because the snapshot is taken
//! *before* the armed write, the restored node has lost exactly this round's record regardless of
//! the kill timing, removing the race that would otherwise make the live repro probabilistic.

use rayls_infrastructure_types::Database;
use std::path::Path;

/// Takes the one-shot pre-write power-loss snapshot if this node is armed.
///
/// `what` is `"header"` or `"vote"` (for logging only). `db` is the consensus storage. Reads:
/// - `RL_REPRO_PL_ARM_FILE`: the node's arm file. The snapshot runs only while it exists and the
///   file is removed afterwards, so the snapshot is taken exactly once, before the first armed
///   write, and later rounds do not re-snapshot.
/// - `RL_REPRO_PL_DBFILE`: path to this node's `consensus-db/mdbx.dat`. The snapshot is written
///   next to it as `<dbfile>.pl-snap`, which the driver restores after `kill -9`.
///
/// Best-effort: any failure is logged and the write proceeds unsnapshotted (the run then simply
/// does not reproduce, rather than wedging the node).
pub async fn maybe_power_loss_snapshot<DB: Database>(what: &str, db: &DB) {
    let Ok(arm) = std::env::var("RL_REPRO_PL_ARM_FILE") else {
        return;
    };
    if !Path::new(&arm).exists() {
        return;
    }
    let dbfile = match std::env::var("RL_REPRO_PL_DBFILE") {
        Ok(f) => f,
        Err(_) => {
            tracing::error!(target: "repro", "RL_REPRO power-loss: RL_REPRO_PL_DBFILE unset while armed");
            return;
        }
    };
    let snap = format!("{dbfile}.pl-snap");

    // Drain the writer queue so every prior committed write is in the environment.
    if let Err(e) = db.persist().await {
        tracing::error!(target: "repro", %e, "RL_REPRO power-loss: persist before snapshot failed");
        return;
    }
    // Capture a transactionally consistent, flushed snapshot of the latest committed state. It
    // excludes this round's record (not written yet) and never tears, even under concurrent
    // consensus writes. `mdbx_env_copy` requires a non-existent destination.
    let _ = std::fs::remove_file(&snap);
    if let Err(e) = db.snapshot_datafile_to(std::path::Path::new(&snap)) {
        tracing::error!(target: "repro", %e, snap, "RL_REPRO power-loss: snapshot failed");
        return;
    }
    // One-shot: remove the arm file so later rounds do not re-snapshot.
    let _ = std::fs::remove_file(&arm);
    tracing::warn!(
        target: "repro",
        what,
        dbfile,
        snap,
        "RL_REPRO power-loss snapshot taken (force-synced, pre-write); this round's record is lost on restore"
    );
}
