//! Reproduction hook for the vote and header durability issue.
//!
//! This file exists only on the `repro/vote-durability-live` branch and is never shipped.
//!
//! A producer arms the next write it makes. An armed write updates the in-memory cache but is
//! not put on the durable disk queue, so the node keeps running and its vote or header still
//! goes out on the wire, yet the record is gone after a crash and restart. This matches the bug,
//! where the write is queued but not yet on disk when the process dies, without freezing the node.

use std::cell::Cell;

thread_local! {
    /// Set by a producer right before the one write it wants to lose.
    static SKIP_NEXT: Cell<bool> = const { Cell::new(false) };
}

/// Arms the next insert on this thread to be kept in memory only, not persisted.
pub fn arm_skip_next_write(what: &str) {
    SKIP_NEXT.with(|s| s.set(true));
    tracing::warn!(target: "repro", what, "RL_REPRO next write kept in memory only, not persisted");
}

/// Returns and clears the skip flag for the current write.
pub fn take_skip_next_write() -> bool {
    SKIP_NEXT.with(|s| {
        let v = s.get();
        s.set(false);
        v
    })
}

/// True when certificate writes on this node should be kept in memory only,
/// modelling the same durability gap for the certificate record.
pub fn should_skip_cert() -> bool {
    std::env::var("RL_REPRO_STALL_CERT_FILE").is_ok_and(|f| std::path::Path::new(&f).exists())
}
