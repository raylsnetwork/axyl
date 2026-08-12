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
/// This impl does not map to `MessageAcceptance::Ignore`.
#[derive(Debug, PartialEq)]
pub(super) enum GossipAcceptance {
    /// The message is considered valid, and it should be delivered and forwarded to the network.
    Accept,
    /// The message is considered invalid, and it should be rejected and trigger the P₄ penalty.
    Reject,
}

impl GossipAcceptance {
    /// Helper method indicating if the gossip message was accepted.
    pub(super) fn is_accepted(&self) -> bool {
        *self == GossipAcceptance::Accept
    }
}

impl From<GossipAcceptance> for MessageAcceptance {
    fn from(value: GossipAcceptance) -> Self {
        match value {
            GossipAcceptance::Accept => MessageAcceptance::Accept,
            GossipAcceptance::Reject => MessageAcceptance::Reject,
        }
    }
}
