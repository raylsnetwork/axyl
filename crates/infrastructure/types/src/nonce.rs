//! Canonical codec for the epoch/round packed into an execution block's nonce.
//!
//! An execution (Ethereum) header carries no `epoch`/`round` fields of its own; both are packed
//! into the standard header `nonce` as `(epoch << 32) | round` — epoch in the high 32 bits, round
//! in the low 32. This module is the single source of truth for that layout: encode with
//! [`pack_nonce`], decode with [`unpack_nonce`]. Every callsite on both the consensus and
//! execution sides must go through here so the two ends can never drift — a mismatch would
//! silently corrupt every epoch/round read that crosses the consensus/execution boundary.

use crate::{Epoch, Round};

/// Pack an epoch and round into an execution block nonce as `(epoch << 32) | round`.
pub fn pack_nonce(epoch: Epoch, round: Round) -> u64 {
    ((epoch as u64) << 32) | round as u64
}

/// Decode an execution block nonce into its `(epoch, round)` — the values *at block creation*.
///
/// Note these are the creation-time values, not the execution frontier: a block drained from a
/// parked, out-of-order batch carries the epoch/round of its origin output, which can sit below
/// the current frontier. Callers must treat the result accordingly.
pub fn unpack_nonce(nonce: u64) -> (Epoch, Round) {
    let epoch = (nonce >> 32) as Epoch; // high 32 bits
    let round = nonce as Round; // low 32 bits (truncates the epoch away)
    (epoch, round)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips() {
        for (e, r) in [(0, 0), (1, 0), (0, 1), (7, 498), (u32::MAX, u32::MAX)] {
            assert_eq!(unpack_nonce(pack_nonce(e, r)), (e, r));
        }
    }

    #[test]
    fn layout_is_epoch_high_round_low() {
        assert_eq!(pack_nonce(1, 0), 1u64 << 32);
        assert_eq!(pack_nonce(0, 1), 1);
        assert_eq!(unpack_nonce((3u64 << 32) | 200), (3, 200));
    }
}
