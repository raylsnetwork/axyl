//! Test-only transaction-gossip flood hook.
//!
//! Compiled only with the `txn-flood-hook` cargo feature, so a normal build never contains
//! it. When armed by environment it loads a pre-built gossip payload from a file and republishes
//! it on the transaction topic in a tight loop with no rate limit, to reproduce on a live network
//! the issue where any peer can flood the transaction topic without penalty.
//!
//! Environment:
//! - `RL_FLOOD=1` arms the flood. Unset, the node behaves normally.
//! - `RL_FLOOD_PAYLOAD=<path>` is a file holding the encoded `WorkerGossip` bytes to publish.
//! - `RL_FLOOD_INTERVAL_MS=<n>` is the delay between publishes in milliseconds (default 100).
//! - `RL_FLOOD_WARMUP_SECS=<n>` is the wait before the first publish (default 10).

use super::WorkerNetworkHandle;
use rayls_infrastructure_types::TaskSpawner;
use std::time::Duration;
use tracing::{info, warn};

/// Arm the flood from environment and spawn its loop. Does nothing unless `RL_FLOOD=1`.
///
/// The worker network is respawned on each epoch, so this runs once per epoch and the flood binds
/// to the current epoch's live network handle. Over a short reproduction run that is one or two
/// loops; do not try to make it fire only once, because pinning to the first (bootstrap) handle
/// floods a network that is not yet connected and publishes almost nothing.
pub(crate) fn maybe_spawn(handle: &WorkerNetworkHandle, spawner: &TaskSpawner) {
    if std::env::var("RL_FLOOD").ok().as_deref() != Some("1") {
        return;
    }
    let path = match std::env::var("RL_FLOOD_PAYLOAD") {
        Ok(p) => p,
        Err(_) => {
            warn!(target: "worker::txn_flood", "RL_FLOOD set but RL_FLOOD_PAYLOAD missing; not flooding");
            return;
        }
    };
    let interval_ms: u64 =
        std::env::var("RL_FLOOD_INTERVAL_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(100);
    let warmup_secs: u64 =
        std::env::var("RL_FLOOD_WARMUP_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(10);
    let payload = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!(target: "worker::txn_flood", %path, %e, "could not read flood payload; not flooding");
            return;
        }
    };
    let handle = handle.clone();
    info!(target: "worker::txn_flood",
        bytes = payload.len(), interval_ms, warmup_secs,
        "TXN FLOOD armed: republishing transaction-topic payload with no rate limit");
    spawner.spawn_task("txn-flood", async move {
        // Let the node join the mesh before flooding so the messages are forwarded.
        tokio::time::sleep(Duration::from_secs(warmup_secs)).await;
        let mut sent: u64 = 0;
        loop {
            match handle.publish_raw_txn_gossip(payload.clone()).await {
                Ok(_) => {
                    sent += 1;
                    if sent == 1 || sent % 5 == 0 {
                        info!(target: "worker::txn_flood", sent, bytes = payload.len(),
                            "TXN FLOOD progress");
                    }
                }
                Err(e) => warn!(target: "worker::txn_flood", %e, "flood publish failed"),
            }
            if interval_ms > 0 {
                tokio::time::sleep(Duration::from_millis(interval_ms)).await;
            }
        }
    });
}
