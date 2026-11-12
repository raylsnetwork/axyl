//! Headers this process has already BLS-verified, so the subscriber does not verify them again.

use rayls_infrastructure_types::{encode, keccak256, ConsensusHeader, B256};
use std::collections::BTreeSet;
use tokio::sync::mpsc;

/// Most records the channel holds, and most the streamer's set keeps. A record is 40 bytes, so the
/// two together stay under about 5 MB plus the set's node overhead.
///
/// The walk records from the top down and the streamer reads from the bottom up. On a gap larger
/// than this the channel fills before the lowest records arrive, and those are dropped. When the
/// set overflows it drops its highest records, which the streamer reaches last. Either way the
/// subscriber verifies a header that has no record itself, so a dropped record costs only time.
const MAX_VERIFIED_HEADERS: usize = 65_536;

/// Records each header the backwards walk verifies, for the forward streamer.
#[derive(Clone, Debug)]
pub(crate) struct VerifiedSender(mpsc::Sender<(u64, B256)>);

impl VerifiedSender {
    /// Record `header`, which has just passed `verify_header_with_keys`. Drops the record when the
    /// streamer is behind: the subscriber then verifies that header itself.
    pub(crate) fn record(&self, header: &ConsensusHeader) {
        let _ = self.0.try_send((header.number, full_hash(header)));
    }
}

/// The forward streamer's view of the headers verified in this process.
#[derive(Debug, Default)]
pub(crate) struct VerifiedHeaders {
    rx: Option<mpsc::Receiver<(u64, B256)>>,
    seen: BTreeSet<(u64, B256)>,
}

impl VerifiedHeaders {
    /// A connected sender and receiver.
    pub(crate) fn channel() -> (VerifiedSender, Self) {
        let (tx, rx) = mpsc::channel(MAX_VERIFIED_HEADERS);
        (VerifiedSender(tx), Self { rx: Some(rx), seen: BTreeSet::new() })
    }

    /// True when this process verified exactly `header`, signatures included.
    ///
    /// Moves new records from the channel into the set first. Then forgets the record for
    /// `header` and every record with a lower number, since the streamer only moves up. So each
    /// record is used at most once: a header streamed again is verified again by the subscriber.
    pub(crate) fn take(&mut self, header: &ConsensusHeader) -> bool {
        if let Some(rx) = self.rx.as_mut() {
            while let Ok(record) = rx.try_recv() {
                self.seen.insert(record);
            }
            while self.seen.len() > MAX_VERIFIED_HEADERS {
                self.seen.pop_last();
            }
        }
        self.seen = self.seen.split_off(&(header.number, B256::ZERO));
        self.seen.remove(&(header.number, full_hash(header)))
    }
}

/// Hash of the whole encoded header. The digest leaves out the certificate signatures, so it
/// cannot tell a verified header from the same one with a corrupted signature.
fn full_hash(header: &ConsensusHeader) -> B256 {
    keccak256(encode(header))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(number: u64, extra: u8) -> ConsensusHeader {
        ConsensusHeader { number, extra: B256::repeat_byte(extra), ..Default::default() }
    }

    #[test]
    fn takes_only_the_exact_header_once() {
        let (tx, mut verified) = VerifiedHeaders::channel();
        tx.record(&header(5, 1));
        assert!(!verified.take(&header(5, 2)), "same number, different contents");
        assert!(verified.take(&header(5, 1)));
        assert!(!verified.take(&header(5, 1)), "a second stream of it is verified again");
    }

    #[test]
    fn forgets_records_below_the_streamed_number() {
        let (tx, mut verified) = VerifiedHeaders::channel();
        tx.record(&header(5, 1));
        tx.record(&header(7, 1));
        assert!(!verified.take(&header(6, 1)));
        assert!(!verified.take(&header(5, 1)), "below a number already streamed");
        assert!(verified.take(&header(7, 1)));
    }

    #[test]
    fn without_a_sender_nothing_is_verified() {
        assert!(!VerifiedHeaders::default().take(&header(5, 1)));
    }
}
