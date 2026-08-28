//! Primary types used for consensus.

use std::time::{Duration, SystemTime};
mod block;
mod certificate;
mod epoch;
mod header;
mod header_meta;
mod info;
mod node_mode;
mod output;
mod participation_meta;
mod reputation;
mod vote;

pub use block::*;
pub use certificate::*;
pub use epoch::*;
pub use header::*;
pub use header_meta::*;
pub use info::*;
pub use node_mode::*;
pub use output::*;
pub use participation_meta::*;
pub use reputation::*;
pub use vote::*;

/// The default primary udp port for consensus messages.
pub const DEFAULT_PRIMARY_PORT: u16 = 44894;

/// 33% of nodes can be labelled as "bad".  This means no more than 33% of the committee can be
/// considered bad nodes and at least 33% of the committee should be considered "good" nodes.  This
/// can be violated only in some extreme edge cases where scores/number of nodes require it.  Note
/// that nodes will NOT be considered "bad" unless they actually have low reputation relative to the
/// other nodes.  The bad list is expected to be empty except in the case of node(s) being down or
/// having bad connectivity, etc.  Also note that nodes with the same reputation will wind up on the
/// same list (good or bad) not unfairly be punished while another node is rewarded.
pub const DEFAULT_BAD_NODES_STAKE_THRESHOLD: u64 = 33;

/// The round number.
/// Becomes the lower 32 bits of a nonce (with epoch the high bits).
pub type Round = u32;

/// The epoch UNIX timestamp in seconds.
pub type TimestampSec = u64;

/// The epoch UNIX timestamp in milliseconds.
pub type TimestampMillis = u64;

/// Timestamp trait for calculating the amount of time that elapsed between
/// timestamp and "now".
pub trait Timestamp {
    /// Returns the time elapsed between the timestamp
    /// and "now". The result is a Duration.
    fn elapsed(&self) -> Duration;
}

impl Timestamp for TimestampSec {
    fn elapsed(&self) -> Duration {
        let diff = now().saturating_sub(*self);
        Duration::from_secs(diff)
    }
}

/// Returns the current time expressed as UNIX
/// timestamp in seconds
pub fn now() -> TimestampSec {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_secs() as TimestampSec,
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

pub fn now_in_millis() -> TimestampMillis {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_millis() as TimestampMillis,
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}
