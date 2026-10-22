use rayls_infrastructure_types::{BlockHash, WorkerId};
use tokio::sync::oneshot;

#[derive(Debug)]
pub struct OurDigestMessage {
    /// The digest for the worker's block that reached quorum.
    pub digest: BlockHash,
    /// The worker that produced this block.
    pub worker_id: WorkerId,
    /// A channel to send an () as an ack after this digest is processed by the primary.
    pub ack_channel: oneshot::Sender<()>,
}

impl OurDigestMessage {
    /// Process the message.
    ///
    /// Splits the message into components required for processing the batch.
    pub(super) fn process(self) -> (oneshot::Sender<()>, ProposerDigest) {
        let OurDigestMessage { digest, worker_id, ack_channel } = self;
        let digest = ProposerDigest { digest, worker_id };
        (ack_channel, digest)
    }
}

/// The returned type for processing `[OurDigestMessage]`.
///
/// Contains all the information needed to propose the new header.
#[derive(Debug)]
pub(super) struct ProposerDigest {
    /// The digest for the worker's block that reached quorum.
    pub digest: BlockHash,
    /// The worker that produced this block.
    pub worker_id: WorkerId,
}
