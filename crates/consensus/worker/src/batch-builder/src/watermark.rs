// SPDX-License-Identifier: BUSL-1.1
//! This authority's executed batch-sequence watermark, as seen by the batch builder.

use tokio::sync::watch;

/// Receiver for the ordering layer's own-authority accepted-sequence watch.
#[derive(Clone, Debug)]
pub struct OwnWatermarkReceiver {
    rx: watch::Receiver<Option<u64>>,
}

impl OwnWatermarkReceiver {
    /// Wraps the ordering layer's own-executed-sequence watch for the builder.
    pub fn new(rx: watch::Receiver<Option<u64>>) -> Self {
        Self { rx }
    }

    /// Returns the current watermark without waiting for a change.
    pub fn get(&self) -> Option<u64> {
        *self.rx.borrow()
    }

    /// Returns the underlying watch receiver, for `changed()` wakeups in `select!`.
    pub fn inner_mut(&mut self) -> &mut watch::Receiver<Option<u64>> {
        &mut self.rx
    }

    /// Returns the sequence the builder resumes from: one past the executed watermark, or the
    /// persisted next sequence when execution has not reported one.
    pub fn resume_seq(&self, persisted_next: u64) -> u64 {
        self.get().map_or(persisted_next, |executed| executed.saturating_add(1))
    }
}
