//! Test-only fault: a node that is slow to stop its consensus tasks at an epoch boundary.
//!
//! Compiled only with the `fault-injection-test-hooks` feature, never in a production build. After
//! the epoch manager detects the boundary and writes its transition checkpoint, it shuts down the
//! epoch's consensus tasks. A slow checkpoint write or a stalled task delays that shutdown, and the
//! previous epoch's subscriber keeps processing commits meanwhile. It is armed per process by
//! environment:
//!
//!   RL_FAULT_TEARDOWN_DELAY_MS=<ms>  wait this long before the shutdown (arms the hook)

use rayls_infrastructure_types::Epoch;
use std::time::Duration;
use tracing::warn;

/// Waits before the epoch's consensus tasks are shut down, when the hook is armed.
pub(super) async fn delay_teardown(epoch: Epoch) {
    let Some(delay_ms) = std::env::var("RL_FAULT_TEARDOWN_DELAY_MS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|ms| *ms > 0)
    else {
        return;
    };
    warn!(
        target: "epoch-manager::fault",
        epoch,
        delay_ms,
        "FAULT TEARDOWN DELAY: delaying the shutdown of the epoch's consensus tasks"
    );
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
}
