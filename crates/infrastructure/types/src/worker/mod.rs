//! Worker types.

use std::collections::HashMap;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    oneshot,
};
mod sealed_batch;
pub use sealed_batch::*;
mod pending_batch;
use crate::error::BlockSealError;
pub use pending_batch::*;

/// Min and max nonce for a single sender within a batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NonceRange {
    /// Lowest nonce observed for the sender.
    pub min: u64,
    /// Highest nonce observed for the sender; never below `min`.
    pub max: u64,
}

impl NonceRange {
    /// Returns the number of nonces the range covers, both ends inclusive.
    ///
    /// Saturates at `u64::MAX` for a range ending at the maximum nonce: the value is diagnostic
    /// only, and with overflow checks on and `panic = abort` an overflow here would let one
    /// crafted batch abort every node that logs the range.
    pub fn span(&self) -> u64 {
        (self.max - self.min).saturating_add(1)
    }
}

/// Per-sender nonce ranges observed during batch construction.
///
/// Maps sender address to `NonceRange` for that sender's transactions
/// within the batch. Empty map if batch has no transactions.
pub type SenderNonceRanges = HashMap<crate::Address, NonceRange>;

/// Type for the channel sender to submit sealed batches to the block provider.
///
/// The sending half (EL) pulls transactions from the public RPC transaction pool and seals a block
/// that extends the canonical tip.
///
/// The receiving half (CL) broadcasts to peers and tries to reach quorum.
pub type BatchSender =
    Sender<(SealedBatch, SenderNonceRanges, oneshot::Sender<Result<(), BlockSealError>>)>;
/// The receiving half of [`BatchSender`].
pub type BatchReceiver =
    Receiver<(SealedBatch, SenderNonceRanges, oneshot::Sender<Result<(), BlockSealError>>)>;

/// The default worker udp port for consensus messages.
pub const DEFAULT_WORKER_PORT: u16 = 44895;

/// The unique identifier for a worker (per primary).
///
/// Workers communicate with peers of the same `WorkerId`.
pub type WorkerId = u16;

#[cfg(test)]
mod tests {
    use super::NonceRange;

    #[test]
    fn nonce_span_saturates_at_the_max_nonce() {
        // nonce 0 paired with u64::MAX from one sender: the true count is u64::MAX + 1
        assert_eq!(NonceRange { min: 0, max: u64::MAX }.span(), u64::MAX);
        // any non-zero min leaves headroom for the +1, so no saturation
        assert_eq!(NonceRange { min: 5, max: u64::MAX }.span(), u64::MAX - 4);
    }

    #[test]
    fn nonce_span_counts_both_ends() {
        assert_eq!(NonceRange { min: 3, max: 3 }.span(), 1);
        assert_eq!(NonceRange { min: 3, max: 7 }.span(), 5);
    }
}
