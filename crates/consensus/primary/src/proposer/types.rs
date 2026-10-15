use rayls_infrastructure_types::{
    dkg::{Deal, DealingParams},
    BlockHash, WorkerId,
};
use std::sync::Arc;
use tokio::sync::{oneshot, OnceCell};

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

/// This node's part in dealing the next epoch's threshold key.
#[derive(Clone)]
pub(super) struct Dealer {
    /// The epoch the key is for, its threshold and its recipients.
    params: Arc<DealingParams>,
    /// This node's position in the dealing committee, which the deal's proof is bound to.
    position: u32,
    /// The deal, made for the first new header that needs it and reused by every later one.
    deal: Arc<OnceCell<Deal>>,
}

impl Dealer {
    pub(super) fn new(params: Arc<DealingParams>, position: u32) -> Self {
        Self { params, position, deal: Default::default() }
    }

    /// This node's deal, made on first use. Making it costs a pairing per recipient, so it runs
    /// on the blocking pool.
    pub(super) async fn deal(&self) -> eyre::Result<Deal> {
        let deal = self
            .deal
            .get_or_try_init(|| async {
                let (params, position) = (self.params.clone(), self.position);
                let deal = tokio::task::spawn_blocking(move || {
                    let mut rng = rand::rng();
                    Deal::new(
                        params.epoch,
                        position,
                        params.threshold,
                        &params.recipients,
                        &mut rng,
                    )
                })
                .await??;
                Ok::<_, eyre::Report>(deal)
            })
            .await?;
        Ok(deal.clone())
    }
}
