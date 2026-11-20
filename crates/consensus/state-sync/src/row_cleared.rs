//! A signal from the forward streamer to the backwards walk: the streamer cleared a stored row.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// Set by the forward streamer when it clears a stored row, and taken by the backwards walk.
///
/// The walk stops at its covered anchor without checking that the rows below it are still stored.
/// A cleared row there would never be fetched again, so the walk drops the anchor and derives it
/// again from the stored rows, which stops below the cleared one.
#[derive(Clone, Debug, Default)]
pub(crate) struct RowCleared(Arc<AtomicBool>);

impl RowCleared {
    /// Tell the walk that the streamer cleared a stored row.
    pub(crate) fn mark(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// True when the streamer has cleared a stored row since the last call.
    pub(crate) fn take(&self) -> bool {
        self.0.swap(false, Ordering::AcqRel)
    }
}
