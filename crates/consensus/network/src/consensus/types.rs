use libp2p::{gossipsub::MessageAcceptance, StreamProtocol};
use thiserror::Error;

pub(super) const PRIMARY_KAD_PROTO_NAME: StreamProtocol =
    StreamProtocol::new("/narwhal/primary/kad/1.0.0");
pub(super) const WORKER_KAD_PROTO_NAME: StreamProtocol =
    StreamProtocol::new("/narwhal/worker/kad/1.0.0");

/// Maximum number of pending outbound requests before triggering cleanup.
pub(super) const MAX_PENDING_OUTBOUND_REQUESTS: usize = 5000;

/// Maximum number of pending inbound requests before triggering cleanup.
pub(super) const MAX_PENDING_INBOUND_REQUESTS: usize = 5000;

/// Maximum number of pending kad record queries.
pub(super) const MAX_PENDING_KAD_QUERIES: usize = 500;

// Maximum allowed timestamp for a NodeRecord to be considered valid (5 minutes in the future to
// allow for clock skew)
pub(super) const SECONDS_IN_FUTURE_RECORD_ALLOWANCE: u64 = 5 * 60; // 5 minutes

/// Reasons a kademlia record may be invalid. Used for metrics and logging to categorize types of
/// invalid records.
//derive string for this enum for better logging
#[derive(PartialEq, Debug, Error)]
pub(super) enum RecordInvalidReason {
    #[error("Record missing publisher")]
    MissingPublisher,
    #[error("Publisher is banned")]
    PublisherBanned,
    #[error("Source peer is banned")]
    SourceBanned,
    #[error("Record timestamp is too far in the future")]
    TimestampTooFarInFuture,
    #[error("Provided key was not a valid BLS public key")]
    InvalidKeyFormat,
    #[error("Record failed application-level validation: {0}")]
    InvalidPeerRecord(String),
}

/// Enum if the received gossip is initially accepted for further processing.
///
/// This is necessary because libp2p does not impl `PartialEq` on [MessageAcceptance].
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum GossipAcceptance {
    /// The message is considered valid, and it should be delivered and forwarded to the network.
    Accept,
    /// The message is considered invalid, and it should be rejected and trigger the P₄ penalty.
    Reject,
    /// The message is dropped without being forwarded, and its author is not penalized.
    ///
    /// For a publisher this node cannot vouch for: not on the topic's list, or with no peer id to
    /// BLS binding yet. That is what an attacker looks like, but also what an honest validator
    /// looks like to a node behind a committee rotation, so the message is not relayed and the
    /// author is not banned.
    Ignore,
}

impl From<GossipAcceptance> for MessageAcceptance {
    fn from(value: GossipAcceptance) -> Self {
        match value {
            GossipAcceptance::Accept => MessageAcceptance::Accept,
            GossipAcceptance::Reject => MessageAcceptance::Reject,
            GossipAcceptance::Ignore => MessageAcceptance::Ignore,
        }
    }
}
